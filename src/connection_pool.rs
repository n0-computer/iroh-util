//! A simple iroh connection pool.
//!
//! Entry point is [`ConnectionPool`]. You create a connection pool for a specific
//! ALPN and [`Options`]. Then the pool will manage connections for you.
//!
//! Access to connections is via the [`ConnectionPool::get_or_connect`] method, which
//! gives you access to a connection via a [`ConnectionRef`] if possible.
//! Connections the remote opened can be handed to the pool with
//! [`ConnectionPool::handle_connection`]. A protocol that both dials and
//! accepts then uses one connection per endpoint for both.
//!
//! It is important that you keep the [`ConnectionRef`] alive while you are using
//! the connection.
//!
//! This is using a single actor to manage all connections.

use std::{
    collections::{BTreeSet, HashMap},
    fmt, io,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use iroh::{
    Endpoint, EndpointId,
    endpoint::{ConnectError, Connection, VarInt},
};
use n0_error::{e, stack_error};
use n0_future::{
    FuturesUnordered, MaybeFuture, StreamExt,
    future::Boxed,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

/// A callback that runs for each new connection.
///
/// See [`Options::on_connected`].
pub type OnConnected = Arc<
    dyn Fn(&Endpoint, &ConnectionRef) -> n0_future::future::Boxed<io::Result<()>> + Send + Sync,
>;

/// The pool is a single actor, so we can afford a larger inbox.
const INBOX_CAPACITY: usize = 1024;

/// Configuration options for the connection pool.
///
/// Start from [`Options::default`] and set the fields you need.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use iroh_util::connection_pool::Options;
///
/// let mut options = Options::default();
/// options.idle_timeout = Duration::from_secs(30);
/// ```
#[derive(derive_more::Debug, Clone)]
#[non_exhaustive]
pub struct Options {
    /// How long to keep unused connections around.
    ///
    /// Unused means that there are no [`ConnectionRef`]s alive for the connection,
    /// not that no data flows on it.
    pub idle_timeout: Duration,
    /// Timeout for the dial. [`Options::on_connected`] runs after it.
    pub connect_timeout: Duration,
    /// Maximum number of connections the pool holds, superseded ones included.
    ///
    /// Running attempts do not count, so peers that do not answer cannot fill
    /// the pool.
    pub max_connections: usize,
    /// Maximum number of superseded connections to keep per peer.
    ///
    /// Beyond this many, the oldest are closed, even if in use.
    pub max_superseded_per_peer: usize,
    /// An optional callback that runs before the pool hands out a new connection.
    ///
    /// Use it to wait for the connection to reach some state, such as a direct
    /// path. It runs for dialed and adopted connections alike, which
    /// [`Connection::side`] tells apart. It runs on the pool's task and must
    /// not block. No timeout bounds it, and requests wait for it.
    ///
    /// A [`ConnectionRef`] kept past the callback keeps the connection in use,
    /// so the pool never closes it. Keep a [`WeakConnectionRef`] instead.
    #[debug(skip)]
    pub on_connected: Option<OnConnected>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(1),
            max_connections: 1024,
            max_superseded_per_peer: 2,
            on_connected: None,
        }
    }
}

impl Options {
    /// Sets the [`Options::on_connected`] callback.
    pub fn with_on_connected<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Endpoint, ConnectionRef) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = io::Result<()>> + Send + 'static,
    {
        self.on_connected = Some(Arc::new(move |ep, conn| {
            let ep = ep.clone();
            let conn = conn.clone();
            Box::pin(f(ep, conn))
        }));
        self
    }
}

/// A reference to a connection that is owned by a connection pool.
///
/// The connection counts as in use for as long as any reference to it is alive.
/// Each clone counts, like an `Arc`.
#[derive(Debug, Clone)]
pub struct ConnectionRef {
    connection: Connection,
    permit: OneConnection,
}

impl Deref for ConnectionRef {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl ConnectionRef {
    fn new(connection: Connection, permit: OneConnection) -> Self {
        Self { connection, permit }
    }

    /// Returns whether a newer connection to the same endpoint superseded this one.
    ///
    /// A superseded connection stays open while in use, but new work should use
    /// the one [`ConnectionPool::get_or_connect`] returns.
    pub fn is_superseded(&self) -> bool {
        self.permit.is_superseded()
    }

    /// Returns a reference to the connection that does not keep it in use.
    ///
    /// See [`WeakConnectionRef`].
    pub fn downgrade(&self) -> WeakConnectionRef {
        WeakConnectionRef {
            connection: self.connection.clone(),
            counter: self.permit.inner.clone(),
        }
    }
}

/// A reference to a pooled connection that does not keep it in use.
///
/// It relates to [`ConnectionRef`] as `Weak` does to `Arc`: [`Self::upgrade`]
/// returns a counting reference while the pool holds the connection. Unlike
/// `Weak`, it keeps the connection handle alive and derefs to it.
///
/// Use it in tasks that only watch the connection, such as an accept loop, and
/// upgrade before serving a stream. The deref does not count as a use, so the
/// pool can close the connection while you use it that way.
#[derive(Debug, Clone)]
pub struct WeakConnectionRef {
    connection: Connection,
    counter: Arc<ConnectionCounterInner>,
}

impl Deref for WeakConnectionRef {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl WeakConnectionRef {
    /// Returns a reference that keeps the connection in use, if the pool holds it.
    ///
    /// The pool holds a connection the remote closed until it handles the close
    /// event, so the reference may already be closed.
    pub fn upgrade(&self) -> Option<ConnectionRef> {
        let permit = self.counter.try_get_one()?;
        Some(ConnectionRef::new(self.connection.clone(), permit))
    }

    /// Returns whether a newer connection to the same endpoint superseded this one.
    ///
    /// See [`ConnectionRef::is_superseded`].
    pub fn is_superseded(&self) -> bool {
        self.counter.is_superseded()
    }
}

/// An error returned when the pool cannot get a connection.
///
/// This includes the normal iroh connection errors as well as pool specific
/// errors such as timeouts and connection limits.
#[stack_error(derive, add_meta)]
#[derive(Clone)]
#[non_exhaustive]
pub enum PoolConnectError {
    /// The connection pool is shut down.
    #[error("Connection pool is shut down")]
    Shutdown {},
    /// The connection or its endpoint was closed before the pool handed it out.
    #[error("Connection was closed")]
    Closed {},
    /// The dial did not finish within [`Options::connect_timeout`].
    #[error("Timeout during connect")]
    Timeout {},
    /// The pool was full when the attempt finished.
    #[error("Too many connections")]
    TooManyConnections {},
    /// The dial failed.
    #[error(transparent)]
    ConnectError { source: Arc<ConnectError> },
    /// [`Options::on_connected`] failed.
    #[error(transparent)]
    OnConnectError {
        #[error(std_err)]
        source: Arc<io::Error>,
    },
}

impl From<ConnectError> for PoolConnectError {
    fn from(e: ConnectError) -> Self {
        e!(PoolConnectError::ConnectError, Arc::new(e))
    }
}

impl From<io::Error> for PoolConnectError {
    fn from(e: io::Error) -> Self {
        e!(PoolConnectError::OnConnectError, Arc::new(e))
    }
}

/// Error when calling a fn on the [`ConnectionPool`].
///
/// The only thing that can go wrong is that the connection pool is shut down.
#[stack_error(derive, add_meta)]
pub enum ConnectionPoolError {
    /// The connection pool has been shut down.
    #[error("The connection pool has been shut down")]
    Shutdown {},
}

type RefSender = oneshot::Sender<Result<ConnectionRef, PoolConnectError>>;

enum ActorMessage {
    RequestRef(RequestRef),
    HandleConnection { conn: Connection, tx: RefSender },
    ConnectionShutdown { id: EndpointId },
}

struct RequestRef {
    id: EndpointId,
    tx: RefSender,
}

/// Which attempt to a peer made a connection.
///
/// Connections to a peer come and go, and an event about one can arrive after
/// the pool has moved on. The generation tells them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Generation(u64);

impl Generation {
    /// Returns the generation, and moves `self` on to the next one.
    fn next(&mut self) -> Self {
        let current = *self;
        self.0 += 1;
        current
    }
}

/// Identifies one connection: the peer, and the attempt that made it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ConnId {
    peer: EndpointId,
    generation: Generation,
}

impl fmt::Display for ConnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.peer.fmt_short(), self.generation.0)
    }
}

/// Reasons for which the pool may close a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseReason {
    /// Nothing used it when the pool closed it.
    Unused,
    /// It was in use when [`ConnectionPool::close`] or shutdown closed it.
    Dropped,
    /// A newer connection to the peer took its place.
    Superseded,
    /// The pool was full when the attempt that made it finished.
    TooManyConnections,
    /// [`Options::on_connected`] failed.
    Rejected,
    /// The peer was closed while the attempt that made it was running.
    Closed,
}

impl CloseReason {
    /// Returns the error code the pool closes with.
    fn code(self) -> VarInt {
        VarInt::from_u32(0)
    }

    /// Returns the reason the pool closes with, which the peer can read.
    fn reason(self) -> &'static [u8] {
        match self {
            Self::Unused => b"unused",
            Self::Dropped => b"drop",
            Self::Superseded => b"superseded",
            Self::TooManyConnections => b"too many connections",
            Self::Rejected => b"rejected",
            Self::Closed => b"closed",
        }
    }
}

/// Closes `connection`, telling the peer why.
fn close_connection(connection: &Connection, reason: CloseReason) {
    connection.close(reason.code(), reason.reason());
}

/// A connection the pool holds, with its reference count.
///
/// It is not `Clone`, since dropping it retires the counter.
#[derive(Debug)]
struct PooledConnection {
    connection: Connection,
    counter: ConnectionCounter,
}

impl PooledConnection {
    fn new(connection: Connection, counter: ConnectionCounter) -> Self {
        Self {
            connection,
            counter,
        }
    }

    fn conn_id(&self) -> ConnId {
        self.counter.conn_id()
    }

    fn conn_ref(&self) -> ConnectionRef {
        ConnectionRef::new(self.connection.clone(), self.counter.get_one())
    }

    fn is_unused(&self) -> bool {
        self.counter.is_unused()
    }

    fn close(self) {
        let reason = if self.is_unused() {
            CloseReason::Unused
        } else {
            CloseReason::Dropped
        };
        self.close_as(reason);
    }

    /// Retires the connection, then closes it, so no upgrade sees it closed.
    fn close_as(self, reason: CloseReason) {
        let Self {
            connection,
            counter,
        } = self;
        drop(counter);
        close_connection(&connection, reason);
    }
}

/// An attempt to get a connection to a peer: a dial, or an adoption.
#[derive(Debug)]
struct PendingConnection {
    generation: Generation,
    counter: ConnectionCounter,
    origin: Origin,
    /// Callers of [`ConnectionPool::handle_connection`] for this connection.
    callers: Vec<RefSender>,
    /// Callers of [`ConnectionPool::get_or_connect`], which want any connection.
    requests: Vec<RefSender>,
}

/// Where a pending connection comes from.
#[derive(Debug)]
enum Origin {
    /// The connection exists only once the dial ends.
    Dial,
    /// The peer opened the connection, and the pool is adopting it.
    Incoming(Connection),
}

impl PendingConnection {
    /// Returns the connection, if the attempt has one already.
    fn connection(&self) -> Option<&Connection> {
        match &self.origin {
            Origin::Dial => None,
            Origin::Incoming(connection) => Some(connection),
        }
    }

    fn is_dial(&self) -> bool {
        matches!(self.origin, Origin::Dial)
    }

    /// Retires the attempt and closes its connection, if any, returning the waiters.
    fn close_as(self, reason: CloseReason) -> Vec<RefSender> {
        let Self {
            counter,
            origin,
            mut callers,
            requests,
            ..
        } = self;
        drop(counter);
        if let Origin::Incoming(connection) = origin {
            close_connection(&connection, reason);
        }
        callers.extend(requests);
        callers
    }
}

fn fail_waiters(waiters: Vec<RefSender>, cause: &PoolConnectError) {
    for tx in waiters {
        let _ = tx.send(Err(cause.clone()));
    }
}

/// The peer's current connection, or the attempt that will produce it.
#[derive(Debug)]
enum PeerState {
    Connecting(PendingConnection),
    Ready(PooledConnection),
}

/// Everything the pool holds for one peer.
#[derive(Debug, Default)]
struct Peer {
    /// The connection requests get, or the attempt that will make it.
    current: Option<PeerState>,
    /// Connections the peer opened that the pool is adopting, oldest first.
    adopting: Vec<PendingConnection>,
    /// Connections that a newer one took the place of, oldest first.
    superseded: Vec<PooledConnection>,
}

impl Peer {
    fn is_empty(&self) -> bool {
        self.current.is_none() && self.adopting.is_empty() && self.superseded.is_empty()
    }

    /// Returns the peer's current connection.
    fn ready(&self) -> Option<&PooledConnection> {
        match &self.current {
            Some(PeerState::Ready(conn)) => Some(conn),
            _ => None,
        }
    }

    /// Returns the connection with `conn_id`, current or superseded.
    fn connection(&self, conn_id: ConnId) -> Option<&PooledConnection> {
        self.ready()
            .into_iter()
            .chain(&self.superseded)
            .find(|conn| conn.conn_id() == conn_id)
    }

    /// Returns the pooled connection with the same stable id as `connection`.
    fn find_by_connection(&self, connection: &Connection) -> Option<&PooledConnection> {
        self.ready()
            .into_iter()
            .chain(&self.superseded)
            .find(|conn| conn.connection.stable_id() == connection.stable_id())
    }

    /// Returns the adoption of `connection` that is running, if there is one.
    fn adoption_mut(&mut self, connection: &Connection) -> Option<&mut PendingConnection> {
        self.adopting.iter_mut().find(|attempt| {
            attempt
                .connection()
                .is_some_and(|incoming| incoming.stable_id() == connection.stable_id())
        })
    }

    /// Returns the running dial, or else the newest adoption.
    fn pending_mut(&mut self) -> Option<&mut PendingConnection> {
        match &mut self.current {
            Some(PeerState::Connecting(attempt)) => Some(attempt),
            _ => self.adopting.last_mut(),
        }
    }

    /// Removes the connection with `conn_id`, current or superseded.
    fn remove_connection(&mut self, conn_id: ConnId) -> Option<PooledConnection> {
        let is_current = |current: &mut PeerState| matches!(current, PeerState::Ready(conn) if conn.conn_id() == conn_id);
        if let Some(PeerState::Ready(conn)) = self.current.take_if(is_current) {
            return Some(conn);
        }
        let index = self
            .superseded
            .iter()
            .position(|conn| conn.conn_id() == conn_id)?;
        Some(self.superseded.remove(index))
    }

    /// Removes the attempt with `generation`, a dial or an adoption.
    fn remove_attempt(&mut self, generation: Generation) -> Option<PendingConnection> {
        let is_attempt = |current: &mut PeerState| matches!(current, PeerState::Connecting(attempt) if attempt.generation == generation);
        if let Some(PeerState::Connecting(attempt)) = self.current.take_if(is_attempt) {
            return Some(attempt);
        }
        let index = self
            .adopting
            .iter()
            .position(|attempt| attempt.generation == generation)?;
        Some(self.adopting.remove(index))
    }
}

/// The connections nothing uses, in the order they went unused.
///
/// The pool closes a connection once it has been unused for
/// [`Options::idle_timeout`], and evicts the one that has been unused longest
/// to make room. Entries are keyed both ways, so a connection's entry can be
/// found and removed without a scan.
#[derive(Debug, Default)]
struct UnusedSet {
    since: HashMap<ConnId, Instant>,
    order: BTreeSet<(Instant, ConnId)>,
}

impl UnusedSet {
    /// Records that the connection went unused at `now`.
    fn insert(&mut self, conn_id: ConnId, now: Instant) {
        self.remove(conn_id);
        self.since.insert(conn_id, now);
        self.order.insert((now, conn_id));
    }

    /// Removes the connection's entry, if it has one.
    fn remove(&mut self, conn_id: ConnId) {
        if let Some(since) = self.since.remove(&conn_id) {
            self.order.remove(&(since, conn_id));
        }
    }

    /// Returns when the connection that has been unused longest went unused.
    fn oldest(&self) -> Option<Instant> {
        self.order.first().map(|(since, _)| *since)
    }

    /// Removes and returns the connection that has been unused longest.
    fn pop_oldest(&mut self) -> Option<ConnId> {
        let (_, conn_id) = self.order.pop_first()?;
        self.since.remove(&conn_id);
        Some(conn_id)
    }

    /// Removes and returns a connection that has been unused for `timeout`.
    fn pop_expired(&mut self, now: Instant, timeout: Duration) -> Option<ConnId> {
        let (since, conn_id) = *self.order.first()?;
        if since + timeout > now {
            return None;
        }
        self.order.pop_first();
        self.since.remove(&conn_id);
        Some(conn_id)
    }
}

/// The result of an attempt: a dial, or adopting an incoming connection.
type ConnectResult = (ConnId, Result<Connection, PoolConnectError>);

struct Actor {
    /// The inbox for requests from [`ConnectionPool`] handles.
    rx: mpsc::Receiver<ActorMessage>,
    /// Separate inbox for unused events, gets processed before the main inbox.
    ///
    /// Each event names the connection that has no references left.
    unused_rx: mpsc::UnboundedReceiver<ConnId>,
    /// Sender for the unused inbox to be cloned into the connection counter.
    ///
    /// Unbounded, so that `Drop` can send.
    unused_tx: mpsc::UnboundedSender<ConnId>,
    options: Options,
    endpoint: Endpoint,
    alpn: Arc<[u8]>,
    peers: HashMap<EndpointId, Peer>,
    /// Currently unused connections, in order of when they became unused.
    unused: UnusedSet,
    /// Futures for currently connecting peers.
    connecting: FuturesUnordered<Boxed<ConnectResult>>,
    /// Generation counter used to distinguish between connection attempts to the same peer.
    next_generation: Generation,
    /// Futures for connection close watchers, each yielding which connection closed.
    conn_close: FuturesUnordered<Boxed<ConnId>>,
    /// The reason to close each dropped dial's connection with, once it ends.
    dropped_dials: HashMap<ConnId, CloseReason>,
}

impl Actor {
    fn new(
        endpoint: Endpoint,
        alpn: &[u8],
        options: Options,
    ) -> (Self, mpsc::Sender<ActorMessage>) {
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);
        let (unused_tx, unused_rx) = mpsc::unbounded_channel();
        (
            Self {
                rx,
                unused_rx,
                unused_tx,
                options,
                endpoint,
                alpn: alpn.to_vec().into(),
                peers: HashMap::new(),
                unused: UnusedSet::default(),
                connecting: FuturesUnordered::new(),
                conn_close: FuturesUnordered::new(),
                dropped_dials: HashMap::new(),
                next_generation: Generation(0),
            },
            tx,
        )
    }

    async fn run(mut self) {
        // We bias processing internal events before accepting more work from
        // the external mailbox.
        loop {
            let next_unused_timer_at = match self.unused.oldest() {
                None => MaybeFuture::None,
                Some(since) => {
                    let deadline = since + self.options.idle_timeout;
                    MaybeFuture::Some(n0_future::time::sleep_until(deadline))
                }
            };
            tokio::select! {
                biased;

                // Handle unused events first, since this might give us some room.
                Some(conn_id) = self.unused_rx.recv() => {
                    self.handle_unused_event(conn_id);
                }

                Some((conn_id, result)) = self.connecting.next(), if !self.connecting.is_empty() => {
                    self.handle_connect_result(conn_id, result);
                }

                Some(conn_id) = self.conn_close.next(), if !self.conn_close.is_empty() => {
                    self.handle_conn_closed(conn_id);
                }

                _ = next_unused_timer_at => self.close_unused(),

                msg = self.rx.recv() => {
                    let Some(msg) = msg else { break };
                    self.handle_msg(msg);
                }
            }
        }

        for id in self.peers.keys().copied().collect::<Vec<_>>() {
            self.close_peer(id, e!(PoolConnectError::Shutdown));
        }
    }

    fn handle_msg(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::RequestRef(req) => self.handle_request(req),
            ActorMessage::HandleConnection { conn, tx } => self.handle_incoming(conn, tx),
            ActorMessage::ConnectionShutdown { id } => {
                trace!(%id, "shutdown requested");
                self.close_peer(id, e!(PoolConnectError::Closed));
            }
        }
    }

    /// Hands out a reference to the peer's connection, making one if needed.
    fn handle_request(&mut self, req: RequestRef) {
        let RequestRef { id, tx } = req;
        // A connection that closed a moment ago is still the peer's current one
        // until its close event reaches the actor. Handing it out would give the
        // caller a connection that fails on first use.
        if let Some(conn_id) = self
            .ready(id)
            .filter(|conn| conn.connection.close_reason().is_some())
            .map(PooledConnection::conn_id)
        {
            debug!(%id, "current connection has closed, dialing again");
            self.remove_connection(conn_id);
        }
        if let Some(conn) = self.ready(id) {
            debug!(%id, count = conn.counter.current(), "handing out a ConnectionRef");
            let conn_id = conn.conn_id();
            let conn_ref = conn.conn_ref();
            self.unused.remove(conn_id);
            let _ = tx.send(Ok(conn_ref));
            return;
        }
        // An adoption counts: its connection will be the one requests get.
        if let Some(attempt) = self.peers.get_mut(&id).and_then(Peer::pending_mut) {
            attempt.requests.retain(|tx| !tx.is_closed());
            attempt.requests.push(tx);
            return;
        }
        // Whether there is room is decided when the attempt finishes. What the
        // pool holds may have changed by then, and only then is there a
        // connection to evict for.
        self.start_attempt(id, None, tx);
    }

    /// Starts adopting a connection the peer opened.
    ///
    /// See [`ConnectionPool::handle_connection`].
    fn handle_incoming(&mut self, conn: Connection, tx: RefSender) {
        let id = conn.remote_id();
        if conn.close_reason().is_some() {
            let _ = tx.send(Err(e!(PoolConnectError::Closed)));
            return;
        }
        // Adopting a connection twice would give it two counters, and closing
        // one would close the connection the other still hands out.
        if let Some(pooled) = self
            .peers
            .get(&id)
            .and_then(|peer| peer.find_by_connection(&conn))
        {
            let conn_id = pooled.conn_id();
            let conn_ref = pooled.conn_ref();
            self.unused.remove(conn_id);
            let _ = tx.send(Ok(conn_ref));
            return;
        }
        if let Some(attempt) = self
            .peers
            .get_mut(&id)
            .and_then(|peer| peer.adoption_mut(&conn))
        {
            attempt.callers.retain(|tx| !tx.is_closed());
            attempt.callers.push(tx);
            return;
        }
        self.start_attempt(id, Some(conn), tx);
    }

    /// Starts a dial, or the adoption of `incoming`.
    fn start_attempt(&mut self, id: EndpointId, incoming: Option<Connection>, tx: RefSender) {
        let conn_id = ConnId {
            peer: id,
            generation: self.next_generation.next(),
        };
        let counter = ConnectionCounter::new(conn_id, self.unused_tx.clone());
        let shared = counter.shared();
        let origin = match &incoming {
            Some(connection) => Origin::Incoming(connection.clone()),
            None => Origin::Dial,
        };
        let (callers, requests) = match origin {
            Origin::Dial => (Vec::new(), vec![tx]),
            Origin::Incoming(_) => (vec![tx], Vec::new()),
        };
        let pending = PendingConnection {
            generation: conn_id.generation,
            counter,
            origin,
            callers,
            requests,
        };
        let peer = self.peers.entry(id).or_default();
        if incoming.is_some() {
            peer.adopting.push(pending);
        } else {
            debug_assert!(peer.current.is_none(), "a peer got a second dial");
            peer.current = Some(PeerState::Connecting(pending));
        }
        self.connecting
            .push(self.make_connect_future(conn_id, shared, incoming));
    }

    /// Returns the peer's current connection.
    fn ready(&self, id: EndpointId) -> Option<&PooledConnection> {
        self.peers.get(&id).and_then(Peer::ready)
    }

    /// Returns the connection with `conn_id`, current or superseded.
    fn connection(&self, conn_id: ConnId) -> Option<&PooledConnection> {
        self.peers
            .get(&conn_id.peer)
            .and_then(|peer| peer.connection(conn_id))
    }

    /// Returns the number of connections the pool holds, current and superseded.
    fn connection_count(&self) -> usize {
        self.peers
            .values()
            .map(|peer| usize::from(peer.ready().is_some()) + peer.superseded.len())
            .sum()
    }

    /// Returns whether adding a connection to the peer grows the pool.
    ///
    /// At [`Options::max_superseded_per_peer`], the cap closes one as it adds one.
    fn adding_grows(&self, id: EndpointId) -> bool {
        self.peers.get(&id).is_none_or(|peer| {
            peer.ready().is_none() || peer.superseded.len() < self.options.max_superseded_per_peer
        })
    }

    /// Makes room for one more connection if the pool holds `max_connections`.
    ///
    /// Evicts the connection that has been unused the longest, and returns
    /// `false` if there is none.
    fn make_room(&mut self) -> bool {
        while self.connection_count() >= self.options.max_connections {
            let Some(conn_id) = self.unused.pop_oldest() else {
                return false;
            };
            let Some(conn) = self.take_unused(conn_id) else {
                continue;
            };
            debug!(%conn_id, "evicting the connection unused longest to make room");
            conn.close_as(CloseReason::Unused);
        }
        true
    }

    /// Stops holding the connection if it is still unused, and returns it.
    ///
    /// A connection on the unused list may be in use again, through an upgrade
    /// that did not go through the pool. Its next drop to zero lists it again.
    fn take_unused(&mut self, conn_id: ConnId) -> Option<PooledConnection> {
        if !self.connection(conn_id)?.counter.try_retire_unused() {
            return None;
        }
        self.remove_connection(conn_id)
    }

    /// Returns a future that dials the peer or adopts `incoming`, then runs `on_connected`.
    fn make_connect_future(
        &self,
        conn_id: ConnId,
        counter: Arc<ConnectionCounterInner>,
        incoming: Option<Connection>,
    ) -> Boxed<ConnectResult> {
        let endpoint = self.endpoint.clone();
        let alpn = self.alpn.clone();
        let on_connected = self.options.on_connected.clone();
        let connect_timeout = self.options.connect_timeout;
        Box::pin(async move {
            let connected = if let Some(incoming) = incoming {
                Ok(incoming)
            } else {
                n0_future::time::timeout(connect_timeout, endpoint.connect(conn_id.peer, &alpn[..]))
                    .await
                    .map_err(|_| e!(PoolConnectError::Timeout))
                    .and_then(|res| res.map_err(PoolConnectError::from))
            };

            let connection = match connected {
                Err(err) => return (conn_id, Err(err)),
                Ok(connection) => connection,
            };

            if let Some(f) = &on_connected {
                // The pool dropped the attempt while it dialed. It closes the
                // connection when the stale result arrives.
                let Some(permit) = counter.try_get_one() else {
                    return (conn_id, Ok(connection));
                };
                let conn_ref = ConnectionRef::new(connection.clone(), permit);
                if let Err(err) = f(&endpoint, &conn_ref).await {
                    // Retire before closing, as the pool does.
                    counter.retire();
                    close_connection(&connection, CloseReason::Rejected);
                    return (conn_id, Err(err.into()));
                }
            }

            (conn_id, Ok(connection))
        })
    }

    /// Makes the connection an attempt produced the peer's current one.
    fn handle_connect_result(
        &mut self,
        conn_id: ConnId,
        result: Result<Connection, PoolConnectError>,
    ) {
        let Some(attempt) = self
            .peers
            .get_mut(&conn_id.peer)
            .and_then(|peer| peer.remove_attempt(conn_id.generation))
        else {
            // The pool dropped the attempt in the meantime. An adoption's
            // connection was closed then, and a dial's is closed now.
            debug!(%conn_id, "stale connect result, discarding");
            let reason = self.dropped_dials.remove(&conn_id);
            if let (Ok(connection), Some(reason)) = (result, reason) {
                close_connection(&connection, reason);
            }
            return;
        };
        let connection = match result {
            Ok(connection) => connection,
            Err(cause) => {
                debug!(%conn_id, "attempt failed: {cause:?}");
                self.fail_attempt(conn_id.peer, attempt, cause);
                return;
            }
        };
        // The peer may have closed it while `on_connected` ran.
        if connection.close_reason().is_some() {
            debug!(%conn_id, "the connection closed before it was ready");
            self.fail_attempt(conn_id.peer, attempt, e!(PoolConnectError::Closed));
            return;
        }
        // Connections made since this attempt started may have filled the pool.
        if self.adding_grows(conn_id.peer) && !self.make_room() {
            debug!(%conn_id, "connected, but the pool is full");
            let cause = e!(PoolConnectError::TooManyConnections);
            self.fail_attempt(conn_id.peer, attempt, cause);
            close_connection(&connection, CloseReason::TooManyConnections);
            return;
        }
        let PendingConnection {
            counter,
            mut callers,
            requests,
            ..
        } = attempt;
        callers.extend(requests);
        let conn = PooledConnection::new(connection, counter);
        self.insert_connection(conn, callers);
    }

    /// Retires a failed attempt and fails its callers with `cause`.
    ///
    /// Requests that joined an adoption want any connection, so they are handled
    /// again. The requests of a dial fail with it.
    fn fail_attempt(
        &mut self,
        id: EndpointId,
        attempt: PendingConnection,
        cause: PoolConnectError,
    ) {
        let PendingConnection {
            counter,
            origin,
            callers,
            requests,
            ..
        } = attempt;
        drop(counter);
        fail_waiters(callers, &cause);
        match origin {
            Origin::Dial => fail_waiters(requests, &cause),
            Origin::Incoming(_) => {
                for tx in requests.into_iter().filter(|tx| !tx.is_closed()) {
                    self.handle_request(RequestRef { id, tx });
                }
            }
        }
        self.drop_peer_if_empty(id);
    }

    /// Adds `conn` to the pool and hands it to `waiters`.
    ///
    /// It becomes current, unless a newer connection is current already.
    fn insert_connection(&mut self, conn: PooledConnection, mut waiters: Vec<RefSender>) {
        let conn_id = conn.conn_id();
        let peer = self.peers.entry(conn_id.peer).or_default();
        // Adoptions finish out of order, and the one the peer opened last wins.
        // No dial starts while an adoption runs, so only adoptions compare here.
        let newer_is_current = matches!(
            &peer.current,
            Some(PeerState::Ready(current)) if current.conn_id().generation > conn_id.generation
        );
        if newer_is_current {
            debug!(%conn_id, "a newer connection is current, adding this one as superseded");
            conn.counter.mark_superseded();
        } else {
            match peer.current.take() {
                Some(PeerState::Ready(previous)) => {
                    // The peer may still use it, so it stays until unused.
                    debug!(%conn_id, "the new connection supersedes the current one");
                    previous.counter.mark_superseded();
                    peer.superseded.push(previous);
                }
                Some(PeerState::Connecting(dial)) => {
                    // A running dial loses, and its waiters get this connection.
                    debug!(%conn_id, "the new connection serves a running dial");
                    let dial_id = ConnId {
                        peer: conn_id.peer,
                        generation: dial.generation,
                    };
                    self.dropped_dials.insert(dial_id, CloseReason::Superseded);
                    waiters.extend(dial.close_as(CloseReason::Superseded));
                }
                None => {}
            }
        }
        for tx in waiters {
            // A gone caller's reference would only send a spurious unused event.
            if tx.is_closed() {
                continue;
            }
            let _ = tx.send(Ok(conn.conn_ref()));
        }
        debug!(%conn_id, refs = conn.counter.current(), "connected");
        let connection = conn.connection.clone();
        let unused = conn.is_unused();
        if newer_is_current {
            // Keep the list oldest first, which is the order the cap closes in.
            let index = peer
                .superseded
                .partition_point(|superseded| superseded.conn_id().generation < conn_id.generation);
            peer.superseded.insert(index, conn);
        } else {
            peer.current = Some(PeerState::Ready(conn));
        }

        // Create a future that waits for the connection to close.
        self.conn_close.push(Box::pin(async move {
            connection.closed().await;
            conn_id
        }));
        if unused {
            self.unused.insert(conn_id, Instant::now());
        }
        self.close_oldest_superseded(conn_id.peer);
    }

    /// Closes the peer's oldest superseded connections beyond the cap.
    fn close_oldest_superseded(&mut self, id: EndpointId) {
        loop {
            let Some(peer) = self.peers.get_mut(&id) else {
                return;
            };
            if peer.superseded.len() <= self.options.max_superseded_per_peer {
                return;
            }
            let conn = peer.superseded.remove(0);
            debug!(conn_id = %conn.conn_id(), "too many superseded connections, closing the oldest");
            self.unused.remove(conn.conn_id());
            conn.close_as(CloseReason::Superseded);
        }
    }

    /// Handles a connection closing, by us or by the peer.
    fn handle_conn_closed(&mut self, conn_id: ConnId) {
        if self.remove_connection(conn_id).is_some() {
            trace!(%conn_id, "connection closed");
        }
    }

    /// Handles a connection going unused.
    ///
    /// Only acts if the pool still holds the connection. References to a
    /// connection the pool replaced can outlive it, and their last drop must
    /// not restart its successor's idle timeout.
    fn handle_unused_event(&mut self, conn_id: ConnId) {
        let Some(conn) = self.connection(conn_id) else {
            return;
        };
        // Connection was handed out in the meantime.
        if !conn.is_unused() {
            return;
        }
        self.unused.insert(conn_id, Instant::now());
        trace!(%conn_id, "connection unused");
    }

    /// Closes the connections that have been unused for the idle timeout.
    fn close_unused(&mut self) {
        let now = Instant::now();
        let timeout = self.options.idle_timeout;
        while let Some(conn_id) = self.unused.pop_expired(now, timeout) {
            let Some(conn) = self.take_unused(conn_id) else {
                continue;
            };
            trace!(%conn_id, "unused timeout, closing");
            conn.close_as(CloseReason::Unused);
        }
    }

    /// Closes every connection to `id`, and fails the attempts with `cause`.
    fn close_peer(&mut self, id: EndpointId, cause: PoolConnectError) {
        let Some(peer) = self.peers.remove(&id) else {
            return;
        };
        let Peer {
            current,
            adopting,
            superseded,
        } = peer;
        let mut attempts = adopting;
        match current {
            Some(PeerState::Ready(conn)) => self.release(conn),
            Some(PeerState::Connecting(attempt)) => attempts.push(attempt),
            None => {}
        }
        for conn in superseded {
            self.release(conn);
        }
        for attempt in attempts {
            if attempt.is_dial() {
                let dial_id = ConnId {
                    peer: id,
                    generation: attempt.generation,
                };
                self.dropped_dials.insert(dial_id, CloseReason::Closed);
            }
            fail_waiters(attempt.close_as(CloseReason::Closed), &cause);
        }
    }

    /// Stops holding the connection, and closes it.
    fn release(&mut self, conn: PooledConnection) {
        self.unused.remove(conn.conn_id());
        conn.close();
    }

    /// Stops holding the connection, and returns it.
    ///
    /// The caller closes it if it is still open: a connection the pool evicts
    /// is closed differently from one the peer closed.
    fn remove_connection(&mut self, conn_id: ConnId) -> Option<PooledConnection> {
        let peer = self.peers.get_mut(&conn_id.peer)?;
        let conn = peer.remove_connection(conn_id)?;
        self.unused.remove(conn_id);
        self.drop_peer_if_empty(conn_id.peer);
        Some(conn)
    }

    /// Forgets the peer if the pool holds nothing for it.
    fn drop_peer_if_empty(&mut self, id: EndpointId) {
        if self.peers.get(&id).is_some_and(Peer::is_empty) {
            self.peers.remove(&id);
        }
    }
}
/// A pool of connections to endpoints, for one ALPN.
///
/// Clones share the pool. Once the last clone is dropped, the pool closes
/// every connection it holds.
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    tx: mpsc::Sender<ActorMessage>,
}

impl ConnectionPool {
    /// Creates a pool that dials with `endpoint`, for `alpn`.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime, since the pool spawns a task.
    pub fn new(endpoint: Endpoint, alpn: &[u8], options: Options) -> Self {
        let (actor, tx) = Actor::new(endpoint, alpn, options);
        n0_future::task::spawn(actor.run());
        Self { tx }
    }

    /// Returns either a fresh connection or a reference to an existing one.
    ///
    /// If the pool is adopting a connection from the endpoint, this waits for
    /// the adoption, and falls back to a dial if it fails.
    pub async fn get_or_connect(
        &self,
        id: EndpointId,
    ) -> std::result::Result<ConnectionRef, PoolConnectError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::RequestRef(RequestRef { id, tx }))
            .await
            .map_err(|_| e!(PoolConnectError::Shutdown))?;
        rx.await.map_err(|_| e!(PoolConnectError::Shutdown))?
    }

    /// Adopts an incoming connection and returns a reference to it.
    ///
    /// [`Options::on_connected`] runs for the connection as it would for a
    /// dialed one. Then the connection becomes the current one for its
    /// endpoint:
    ///
    /// - Later [`Self::get_or_connect`] calls return it.
    /// - The waiters of a dial to the endpoint that is still running get it.
    /// - [`ConnectionRef::is_superseded`] tells holders of the previous
    ///   connection to move on.
    ///
    /// If a connection the endpoint opened later is current already, this one is
    /// returned as superseded.
    ///
    /// The previous connection stays open until it is unused, since the remote
    /// may still be using it.
    ///
    /// The pool does not check the connection's ALPN.
    ///
    /// # Errors
    ///
    /// - [`PoolConnectError::OnConnectError`] if `on_connected` fails.
    /// - [`PoolConnectError::TooManyConnections`] if the pool is full.
    /// - [`PoolConnectError::Closed`] if the connection closes before it is
    ///   adopted, or [`Self::close`] closes its endpoint meanwhile.
    /// - [`PoolConnectError::Shutdown`] if the pool is shut down.
    pub async fn handle_connection(
        &self,
        conn: Connection,
    ) -> std::result::Result<ConnectionRef, PoolConnectError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::HandleConnection { conn, tx })
            .await
            .map_err(|_| e!(PoolConnectError::Shutdown))?;
        rx.await.map_err(|_| e!(PoolConnectError::Shutdown))?
    }

    /// Closes every connection to `id`.
    ///
    /// That includes superseded connections, even in use. Requests waiting for
    /// a connection to `id` fail with [`PoolConnectError::Closed`].
    ///
    /// A running dial's connection is closed once the dial ends, after
    /// [`Options::on_connected`] if that is already running.
    pub async fn close(&self, id: EndpointId) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionShutdown { id })
            .await
            .map_err(|_| e!(ConnectionPoolError::Shutdown))?;
        Ok(())
    }
}

#[derive(Debug)]
struct ConnectionCounterInner {
    /// The reference count, with the [`RETIRED`] bit.
    ///
    /// One atomic, so retiring an unused connection and upgrading it exclude
    /// each other.
    state: AtomicU64,
    /// Which connection this counts, which its unused events name.
    conn_id: ConnId,
    unused_tx: mpsc::UnboundedSender<ConnId>,
    /// Set once a newer connection to the same endpoint took this one's place.
    superseded: AtomicBool,
}

impl ConnectionCounterInner {
    /// Takes a reference, for a connection the pool holds as open.
    fn get_one(self: &Arc<Self>) -> OneConnection {
        self.state.fetch_add(1, Ordering::AcqRel);
        OneConnection {
            inner: self.clone(),
        }
    }

    /// Takes a reference, unless the pool no longer holds the connection.
    fn try_get_one(self: &Arc<Self>) -> Option<OneConnection> {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & RETIRED == 0).then_some(state + 1)
            })
            .ok()?;
        Some(OneConnection {
            inner: self.clone(),
        })
    }

    /// Retires the connection, so that upgrades fail from now on.
    fn retire(&self) {
        self.state.fetch_or(RETIRED, Ordering::AcqRel);
    }

    /// Returns whether a newer connection to the peer took this one's place.
    fn is_superseded(&self) -> bool {
        self.superseded.load(Ordering::SeqCst)
    }
}

/// The bit of [`ConnectionCounterInner::state`] that marks a connection the pool no longer holds.
const RETIRED: u64 = 1 << 63;

/// The pool's hold on a connection's reference count, which retires it on drop.
///
/// It is not `Clone`: the pool keeps one per connection it holds or is making,
/// so the counter retires exactly when the pool lets go. Weak references and
/// the connect future share the [`ConnectionCounterInner`].
#[derive(Debug)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new(conn_id: ConnId, unused_tx: mpsc::UnboundedSender<ConnId>) -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                state: AtomicU64::new(0),
                conn_id,
                unused_tx,
                superseded: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the state that references to the connection share.
    fn shared(&self) -> Arc<ConnectionCounterInner> {
        self.inner.clone()
    }

    /// Returns which connection this counts.
    fn conn_id(&self) -> ConnId {
        self.inner.conn_id
    }

    fn current(&self) -> u64 {
        self.inner.state.load(Ordering::Acquire) & !RETIRED
    }

    fn is_unused(&self) -> bool {
        self.current() == 0
    }

    /// Takes a reference, for a connection the pool holds as open.
    fn get_one(&self) -> OneConnection {
        self.inner.get_one()
    }

    /// Retires an unused connection, and returns whether it was unused.
    fn try_retire_unused(&self) -> bool {
        self.inner
            .state
            .compare_exchange(0, RETIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Marks that a newer connection to the peer took this one's place.
    fn mark_superseded(&self) {
        self.inner.superseded.store(true, Ordering::SeqCst);
    }
}

impl Drop for ConnectionCounter {
    fn drop(&mut self) {
        self.inner.retire();
    }
}

/// Handle to a connection counter that decrements it on drop.
#[derive(Debug)]
struct OneConnection {
    inner: Arc<ConnectionCounterInner>,
}

impl OneConnection {
    /// Returns whether a newer connection to the peer took this one's place.
    fn is_superseded(&self) -> bool {
        self.inner.is_superseded()
    }
}

impl Clone for OneConnection {
    fn clone(&self) -> Self {
        // No retirement check: the count is at least one while `self` lives.
        self.inner.state.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for OneConnection {
    fn drop(&mut self) {
        let previous = self.inner.state.fetch_sub(1, Ordering::AcqRel);
        if previous & !RETIRED == 1 {
            // Send an unused event to the actor.
            let _ = self.inner.unused_tx.send(self.inner.conn_id);
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use iroh::{
        Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr,
        address_lookup::MemoryLookup,
        endpoint::{Connection, ConnectionError, presets},
        protocol::{AcceptError, ProtocolHandler, Router},
    };
    use n0_error::{Result, StdResultExt};
    use n0_future::{BufferedStreamExt, StreamExt, io, stream};
    use testresult::TestResult;
    use tokio::{sync::oneshot, task::JoinHandle};

    use super::{
        Actor, CloseReason, ConnId, ConnectionCounter, ConnectionPool, ConnectionRef, Generation,
        Options, Peer, PeerState, PoolConnectError, PooledConnection, RequestRef,
        WeakConnectionRef,
    };

    const ECHO_ALPN: &[u8] = b"echo";
    const INCOMING_ALPN: &[u8] = b"iroh-util/pool-incoming-test/0";
    const SHORT_IDLE: Duration = Duration::from_millis(200);

    /// Asserts that `$res` is the `PoolConnectError` variant `$variant`.
    macro_rules! assert_err {
        ($res:expr, $variant:ident) => {{
            let res = $res;
            assert!(
                matches!(res, Err(PoolConnectError::$variant { .. })),
                "expected {}: {res:?}",
                stringify!($variant)
            );
        }};
    }

    #[derive(Debug, Clone)]
    struct Echo;

    impl ProtocolHandler for Echo {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                tokio::io::copy(&mut recv, &mut send).await?;
                send.finish().map_err(AcceptError::from_err)?;
            }
            Ok(())
        }
    }

    async fn echo(conn: &Connection, text: &[u8]) -> Result<Vec<u8>> {
        let (mut send, mut recv) = conn.open_bi().await.anyerr()?;
        send.write_all(text).await.anyerr()?;
        send.finish().anyerr()?;
        recv.read_to_end(1000).await.anyerr()
    }

    fn test_options() -> Options {
        Options {
            idle_timeout: Duration::from_millis(100),
            connect_timeout: Duration::from_secs(5),
            max_connections: 32,
            ..Default::default()
        }
    }

    fn short_idle_options() -> Options {
        Options {
            idle_timeout: SHORT_IDLE,
            ..Default::default()
        }
    }

    /// Binds a UDP socket on loopback that never answers, so dials to it time out.
    ///
    /// The caller keeps the socket, so the OS cannot give the port to another.
    fn dead_addr() -> TestResult<(std::net::UdpSocket, TransportAddr)> {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = TransportAddr::Ip(sock.local_addr()?);
        Ok((sock, addr))
    }

    /// Requests a connection to `id` on a task.
    fn spawn_get(
        pool: &ConnectionPool,
        id: EndpointId,
    ) -> JoinHandle<Result<ConnectionRef, PoolConnectError>> {
        let pool = pool.clone();
        tokio::spawn(async move { pool.get_or_connect(id).await })
    }

    /// Asserts that the pool closed a connection with `reason`.
    fn assert_closed_as(err: &ConnectionError, reason: CloseReason) {
        assert!(
            matches!(
                err,
                ConnectionError::ApplicationClosed(frame)
                    if frame.error_code == reason.code() && frame.reason[..] == *reason.reason()
            ),
            "closed for the wrong reason: {err:?}"
        );
    }

    /// Waits until the pool no longer holds the connection behind `weak`.
    async fn until_retired(weak: &WeakConnectionRef) -> TestResult<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                n0_future::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "the pool kept the connection")?;
        Ok(())
    }

    /// Returns options whose `on_connected` handles its `n`th call per `script(n)`.
    ///
    /// The script gives a delay, and whether to accept the connection after it.
    /// Also returns a weak reference to each connection the callback ran for.
    fn scripted(
        options: Options,
        script: impl Fn(usize) -> (Duration, bool) + Send + Sync + 'static,
    ) -> (Options, Arc<Mutex<Vec<WeakConnectionRef>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let options = options.with_on_connected({
            let seen = seen.clone();
            move |_, conn: ConnectionRef| {
                let (delay, accept) = {
                    let mut seen = seen.lock().expect("poisoned");
                    seen.push(conn.downgrade());
                    script(seen.len() - 1)
                };
                async move {
                    n0_future::time::sleep(delay).await;
                    accept
                        .then_some(())
                        .ok_or_else(|| io::Error::other("rejected"))
                }
            }
        });
        (options, seen)
    }

    /// Returns options whose `on_connected` takes `delay` and accepts.
    fn slow(delay: Duration) -> (Options, Arc<Mutex<Vec<WeakConnectionRef>>>) {
        scripted(short_idle_options(), move |_| (delay, true))
    }

    /// Echo servers, and a client endpoint that reaches them by direct address.
    ///
    /// Everything stays on loopback, so the tests need no network.
    struct EchoNet {
        ids: Vec<EndpointId>,
        routers: Vec<Router>,
        lookup: MemoryLookup,
        client: Endpoint,
    }

    impl EchoNet {
        async fn new(n: usize) -> TestResult<Self> {
            let lookup = MemoryLookup::new();
            let mut ids = Vec::new();
            let mut routers = Vec::new();
            for _ in 0..n {
                let endpoint = Endpoint::builder(presets::Minimal)
                    .alpns(vec![ECHO_ALPN.to_vec()])
                    .bind()
                    .await?;
                lookup.add_endpoint_info(endpoint.addr());
                ids.push(endpoint.id());
                routers.push(Router::builder(endpoint).accept(ECHO_ALPN, Echo).spawn());
            }
            let client = Endpoint::builder(presets::Minimal)
                .address_lookup(lookup.clone())
                .bind()
                .await?;
            Ok(Self {
                ids,
                routers,
                lookup,
                client,
            })
        }

        fn pool(&self, options: Options) -> ConnectionPool {
            ConnectionPool::new(self.client.clone(), ECHO_ALPN, options)
        }

        /// Adds a peer whose address never answers.
        fn dead_peer(&self, seed: u8) -> TestResult<(std::net::UdpSocket, EndpointId)> {
            let (sock, addr) = dead_addr()?;
            let id = SecretKey::from_bytes(&[seed; 32]).public();
            self.lookup
                .add_endpoint_info(EndpointAddr::from_parts(id, [addr]));
            Ok((sock, id))
        }

        async fn shutdown(self) {
            stream::iter(self.routers)
                .for_each_concurrent(16, |router| async move {
                    let _ = router.shutdown().await;
                })
                .await;
            self.client.close().await;
        }
    }

    /// A pool on a server endpoint, and a client that opens connections to it.
    struct Incoming {
        pool: ConnectionPool,
        server: Endpoint,
        client: Endpoint,
    }

    impl Incoming {
        async fn new(options: Options) -> TestResult<Self> {
            let server = Endpoint::builder(presets::Minimal)
                .alpns(vec![INCOMING_ALPN.to_vec()])
                .bind()
                .await?;
            let client = Endpoint::bind(presets::Minimal).await?;
            let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
            Ok(Self {
                pool,
                server,
                client,
            })
        }

        /// Opens a connection from the client, and returns both ends.
        async fn pair(&self) -> TestResult<(Connection, Connection)> {
            self.pair_from(&self.client).await
        }

        /// Opens a connection from `client`, and returns both ends.
        async fn pair_from(&self, client: &Endpoint) -> TestResult<(Connection, Connection)> {
            let (outgoing, incoming) =
                tokio::join!(client.connect(self.server.addr(), INCOMING_ALPN), async {
                    self.server.accept().await.expect("endpoint closed").await
                });
            Ok((outgoing?, incoming?))
        }

        /// Opens a connection, and hands the server end to the pool.
        async fn adopt(&self) -> TestResult<(Connection, ConnectionRef)> {
            let (outgoing, incoming) = self.pair().await?;
            Ok((outgoing, self.pool.handle_connection(incoming).await?))
        }

        /// Hands `incoming` to the pool on a task.
        fn spawn_adopt(
            &self,
            incoming: Connection,
        ) -> JoinHandle<Result<ConnectionRef, PoolConnectError>> {
            let pool = self.pool.clone();
            tokio::spawn(async move { pool.handle_connection(incoming).await })
        }

        /// Returns the pool's connection to the client.
        async fn current(&self) -> Result<ConnectionRef, PoolConnectError> {
            self.pool.get_or_connect(self.client.id()).await
        }
    }

    /// Two connections from the client, both adopted: the second supersedes the first.
    struct Superseded {
        net: Incoming,
        /// Client ends of the two connections.
        first: Connection,
        second: Connection,
        first_ref: ConnectionRef,
        second_ref: ConnectionRef,
    }

    impl Superseded {
        async fn new(options: Options) -> TestResult<Self> {
            let net = Incoming::new(options).await?;
            let (first, first_ref) = net.adopt().await?;
            let (second, second_ref) = net.adopt().await?;
            Ok(Self {
                net,
                first,
                second,
                first_ref,
                second_ref,
            })
        }
    }

    /// An actor that is not running, and holds one unused connection as current.
    ///
    /// Tests call its handlers directly, so the order of events is certain.
    struct Held {
        actor: Actor,
        conn_id: ConnId,
        /// The server end, which the actor holds.
        conn: Connection,
        _outgoing: Connection,
        net: Incoming,
    }

    impl Held {
        async fn new(options: Options) -> TestResult<Self> {
            let net = Incoming::new(Options::default()).await?;
            let (outgoing, conn) = net.pair().await?;
            let (mut actor, _tx) = Actor::new(net.server.clone(), INCOMING_ALPN, options);
            let conn_id = ConnId {
                peer: conn.remote_id(),
                generation: Generation(1),
            };
            let counter = ConnectionCounter::new(conn_id, actor.unused_tx.clone());
            actor.insert_connection(PooledConnection::new(conn.clone(), counter), Vec::new());
            Ok(Self {
                actor,
                conn_id,
                conn,
                _outgoing: outgoing,
                net,
            })
        }

        /// Returns the id of a connection to the same peer that the pool never made.
        fn stale_id(&self) -> ConnId {
            ConnId {
                peer: self.conn_id.peer,
                generation: Generation(0),
            }
        }

        /// Takes a reference the way an upgrade does, without the actor.
        fn upgrade(&self) -> ConnectionRef {
            let weak = self
                .actor
                .connection(self.conn_id)
                .expect("not held")
                .conn_ref()
                .downgrade();
            weak.upgrade().expect("not held")
        }

        /// Asserts that the actor still holds the connection, and it is open.
        fn assert_kept(&self) {
            assert!(
                self.actor.connection(self.conn_id).is_some(),
                "dropped a connection in use"
            );
            assert!(
                self.conn.close_reason().is_none(),
                "closed a connection in use"
            );
        }
    }

    #[tokio::test]
    async fn connection_pool_errors() -> TestResult<()> {
        let net = EchoNet::new(0).await?;
        let pool = net.pool(Options {
            connect_timeout: Duration::from_secs(1),
            ..test_options()
        });
        let unknown = SecretKey::from_bytes(&[0; 32]).public();
        assert_err!(pool.get_or_connect(unknown).await, ConnectError);
        let (_sock, dead) = net.dead_peer(1)?;
        assert_err!(pool.get_or_connect(dead).await, Timeout);
        net.shutdown().await;
        Ok(())
    }

    /// A connection is reused while in use or recently used, and replaced once idle.
    #[tokio::test]
    async fn connection_pool_smoke() -> TestResult<()> {
        let net = EchoNet::new(32).await?;
        let pool = net.pool(test_options());
        let msg = b"Hello, pool!";
        let mut first_ids = BTreeMap::new();
        for id in &net.ids {
            let conn = pool.get_or_connect(*id).await?;
            assert_eq!(echo(&conn, msg).await?, msg);
            let again = pool.get_or_connect(*id).await?;
            assert_eq!(again.stable_id(), conn.stable_id());
            first_ids.insert(*id, conn.stable_id());
        }
        n0_future::time::sleep(Duration::from_millis(1000)).await;
        for id in &net.ids {
            let conn = pool.get_or_connect(*id).await?;
            assert_eq!(echo(&conn, msg).await?, msg);
            assert_ne!(conn.stable_id(), first_ids[id]);
        }
        net.shutdown().await;
        Ok(())
    }

    /// Unused connections are evicted to make room at the connection limit.
    #[tokio::test]
    async fn connection_pool_unused() -> TestResult<()> {
        let net = EchoNet::new(32).await?;
        let pool = net.pool(Options {
            idle_timeout: Duration::from_secs(100),
            max_connections: 8,
            ..test_options()
        });
        for id in &net.ids {
            let conn = pool.get_or_connect(*id).await?;
            assert_eq!(echo(&conn, b"Hello, pool!").await?, b"Hello, pool!");
        }
        net.shutdown().await;
        Ok(())
    }

    /// A connection whose `on_connected` failed is closed, and does not upgrade.
    ///
    /// Dropping it would not be enough: the callback may keep a handle to it.
    #[tokio::test]
    async fn on_connected_error_closes_the_connection() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let (options, seen) = scripted(test_options(), |_| (Duration::ZERO, false));
        let pool = net.pool(options);
        assert_err!(pool.get_or_connect(net.ids[0]).await, OnConnectError);
        let conn = seen.lock().expect("poisoned").pop().expect("not called");
        assert!(conn.close_reason().is_some(), "the connection stayed open");
        assert!(conn.upgrade().is_none(), "upgraded a rejected connection");
        net.shutdown().await;
        Ok(())
    }

    /// Uses an on_connected callback to ensure that the connection is direct.
    #[tokio::test]
    async fn on_connected_direct() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let pool = net.pool(test_options().with_on_connected(
            |_, conn: ConnectionRef| async move {
                let mut stream = conn.paths_stream();
                while let Some(paths) = stream.next().await {
                    if paths.iter().any(|path| path.is_ip()) {
                        return Ok(());
                    }
                }
                Err(io::Error::other("connection closed before becoming direct"))
            },
        ));
        pool.get_or_connect(net.ids[0]).await?;
        net.shutdown().await;
        Ok(())
    }

    /// Spawns `n` `get_or_connect(id)` calls and waits until each has started.
    async fn enter_get_or_connect(
        pool: &ConnectionPool,
        id: EndpointId,
        n: usize,
    ) -> Vec<JoinHandle<Result<ConnectionRef, PoolConnectError>>> {
        let started = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let pool = pool.clone();
            let started = started.clone();
            handles.push(tokio::spawn(async move {
                started.fetch_add(1, Ordering::SeqCst);
                pool.get_or_connect(id).await
            }));
        }
        while started.load(Ordering::SeqCst) < n {
            tokio::task::yield_now().await;
        }
        handles
    }

    /// Many requests for an unreachable peer do not delay a request for another.
    #[tokio::test]
    async fn connection_pool_dead_peer_does_not_delay_other_peers() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let (_sock, dead) = net.dead_peer(7)?;
        let connect_timeout = Duration::from_secs(1);
        let pool = net.pool(Options {
            connect_timeout,
            ..test_options()
        });
        let backlog = enter_get_or_connect(&pool, dead, 150).await;

        let start = Instant::now();
        pool.get_or_connect(net.ids[0]).await?;
        let elapsed = start.elapsed();
        assert!(elapsed < connect_timeout, "the live peer took {elapsed:?}");
        for handle in backlog {
            handle.abort();
        }
        net.shutdown().await;
        Ok(())
    }

    /// Closing a slow connection attempt fails it, and discards its result.
    ///
    /// A later request starts a new attempt rather than taking the stale one.
    #[tokio::test]
    async fn stale_connect_result_is_discarded() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let id = net.ids[0];
        let handshake = Duration::from_millis(1000);
        let (options, _) = scripted(test_options(), move |_| (handshake, true));
        let pool = net.pool(options);
        let first = spawn_get(&pool, id);
        n0_future::time::sleep(Duration::from_millis(800)).await;
        pool.close(id).await?;
        n0_future::time::sleep(Duration::from_millis(10)).await;

        let start = Instant::now();
        pool.get_or_connect(id).await?;
        let elapsed = start.elapsed();
        assert_err!(first.await?, Closed);
        assert!(
            elapsed >= Duration::from_millis(900),
            "served by the stale attempt after {elapsed:?}"
        );
        net.shutdown().await;
        Ok(())
    }

    /// After `close`, a request gets a new connection, and the old one's references leave it alone.
    #[tokio::test]
    async fn close_then_reconnect() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let id = net.ids[0];
        let pool = net.pool(Options {
            idle_timeout: Duration::from_millis(50),
            ..test_options()
        });
        let old = pool.get_or_connect(id).await?;
        pool.close(id).await?;
        let new = pool.get_or_connect(id).await?;
        assert_ne!(new.stable_id(), old.stable_id());
        drop(old);
        n0_future::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(echo(&new, b"still alive").await?, b"still alive");
        net.shutdown().await;
        Ok(())
    }

    /// A closed connection is replaced on the next request.
    #[tokio::test]
    async fn watch_close() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let pool = net.pool(test_options());
        let conn = pool.get_or_connect(net.ids[0]).await?;
        conn.close(0u32.into(), b"test");
        n0_future::time::sleep(Duration::from_millis(500)).await;
        let again = pool.get_or_connect(net.ids[0]).await?;
        assert_ne!(again.stable_id(), conn.stable_id());
        net.shutdown().await;
        Ok(())
    }

    /// In-flight attempts do not count towards [`Options::max_connections`].
    ///
    /// Otherwise peers we are still trying to reach take every slot, and
    /// unrelated peers fail until `connect_timeout` expires.
    #[tokio::test]
    async fn inflight_connects_do_not_exhaust_slots() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let max_connections = 2;
        let pool = net.pool(Options {
            max_connections,
            ..test_options()
        });
        let mut parked = Vec::new();
        for seed in 0..max_connections {
            let (sock, dead) = net.dead_peer(20 + seed as u8)?;
            parked.push((sock, spawn_get(&pool, dead)));
        }
        n0_future::time::sleep(Duration::from_millis(100)).await;

        pool.get_or_connect(net.ids[0]).await?;
        for (_sock, attempt) in parked {
            let _ = attempt.await;
        }
        net.shutdown().await;
        Ok(())
    }

    /// A connection made after the pool filled up fails with `TooManyConnections`.
    ///
    /// Attempts do not reserve a slot, so without a second check the pool would
    /// go over `max_connections`.
    #[tokio::test]
    async fn connect_into_a_full_pool_fails() -> TestResult<()> {
        let net = EchoNet::new(2).await?;
        let pool = net.pool(Options {
            max_connections: 1,
            ..test_options()
        });
        let (a, b) = tokio::join!(
            pool.get_or_connect(net.ids[0]),
            pool.get_or_connect(net.ids[1])
        );
        let full = |res: &Result<_, PoolConnectError>| {
            matches!(res, Err(PoolConnectError::TooManyConnections { .. }))
        };
        assert!(
            (a.is_ok() && full(&b)) || (full(&a) && b.is_ok()),
            "expected one connection and one TooManyConnections: {a:?}, {b:?}"
        );
        drop((a, b));
        net.shutdown().await;
        Ok(())
    }

    /// Using a connection again restarts its idle timeout.
    #[tokio::test]
    async fn use_restarts_the_idle_timeout() -> TestResult<()> {
        let net = EchoNet::new(1).await?;
        let idle_timeout = Duration::from_millis(500);
        let pool = net.pool(Options {
            idle_timeout,
            ..test_options()
        });
        let conn = pool.get_or_connect(net.ids[0]).await?.downgrade();

        n0_future::time::sleep(idle_timeout * 3 / 5).await;
        drop(pool.get_or_connect(net.ids[0]).await?);
        // Past the first timeout, within the second.
        n0_future::time::sleep(idle_timeout * 3 / 5).await;
        assert!(conn.close_reason().is_none(), "closed before its timeout");
        n0_future::time::sleep(idle_timeout * 4 / 5).await;
        assert!(
            conn.close_reason().is_some(),
            "not closed after its timeout"
        );
        net.shutdown().await;
        Ok(())
    }

    /// A stale close event leaves the current connection alone.
    ///
    /// A request that finds the current connection closed removes it before its
    /// close event arrives, so the event is stale then.
    #[tokio::test]
    async fn stale_close_event_keeps_the_current_connection() -> TestResult<()> {
        let mut h = Held::new(test_options()).await?;
        let peer = h.conn_id.peer;
        h.actor.handle_conn_closed(h.stale_id());
        assert!(h.actor.peers.contains_key(&peer), "removed the peer");
        assert!(h.conn.close_reason().is_none(), "closed the connection");
        h.actor.handle_conn_closed(h.conn_id);
        assert!(!h.actor.peers.contains_key(&peer));
        assert_eq!(h.actor.connection_count(), 0);
        Ok(())
    }

    /// A connection that has closed is not handed out, though its close event is pending.
    #[tokio::test]
    async fn closed_connection_is_not_handed_out() -> TestResult<()> {
        let mut h = Held::new(test_options()).await?;
        let id = h.conn_id.peer;
        h.conn.close(0u32.into(), b"gone");
        h.conn.closed().await;

        let (tx, mut rx) = oneshot::channel();
        h.actor.handle_request(RequestRef { id, tx });
        assert!(rx.try_recv().is_err(), "handed out the closed connection");
        assert!(
            matches!(
                h.actor
                    .peers
                    .get(&id)
                    .and_then(|peer| peer.current.as_ref()),
                Some(PeerState::Connecting(_))
            ),
            "did not start a new connection"
        );
        Ok(())
    }

    /// A stale unused event leaves the current connection's idle time alone.
    ///
    /// References to a replaced connection can outlive it, and their last drop
    /// names that connection.
    #[tokio::test]
    async fn stale_unused_event_keeps_the_idle_time() -> TestResult<()> {
        let mut h = Held::new(test_options()).await?;
        let since = h.actor.unused.oldest().expect("the connection is unused");
        h.actor.handle_unused_event(h.stale_id());
        assert_eq!(
            h.actor.unused.oldest(),
            Some(since),
            "restarted the timeout"
        );
        // Taken off the list as if handed out, its own event lists it again.
        h.actor.unused.remove(h.conn_id);
        h.actor.handle_unused_event(h.conn_id);
        assert!(h.actor.unused.oldest().is_some(), "ignored its own event");
        Ok(())
    }

    /// The idle timeout does not close a connection that was upgraded meanwhile.
    #[tokio::test]
    async fn close_unused_keeps_an_upgraded_connection() -> TestResult<()> {
        let options = test_options();
        let idle_timeout = options.idle_timeout;
        let mut h = Held::new(options).await?;
        let _upgraded = h.upgrade();
        n0_future::time::sleep(idle_timeout * 2).await;
        h.actor.close_unused();
        h.assert_kept();
        Ok(())
    }

    /// A full pool does not evict a connection that was upgraded meanwhile.
    #[tokio::test]
    async fn make_room_keeps_an_upgraded_connection() -> TestResult<()> {
        let mut h = Held::new(Options {
            max_connections: 1,
            ..test_options()
        })
        .await?;
        let _upgraded = h.upgrade();
        assert!(!h.actor.make_room(), "made room in a pool that is in use");
        h.assert_kept();
        Ok(())
    }

    /// Requests whose callers gave up do not pile up on a running attempt.
    #[tokio::test]
    async fn cancelled_requests_do_not_pile_up() -> TestResult<()> {
        let mut h = Held::new(test_options()).await?;
        let peer = SecretKey::from_bytes(&[6u8; 32]).public();
        for _ in 0..10 {
            let (tx, rx) = oneshot::channel();
            drop(rx);
            h.actor.handle_request(RequestRef { id: peer, tx });
        }
        let attempt = h
            .actor
            .peers
            .get_mut(&peer)
            .and_then(Peer::pending_mut)
            .expect("no attempt");
        assert_eq!(attempt.requests.len(), 1, "kept requests of gone callers");
        h.net.server.close().await;
        Ok(())
    }

    /// An upgrade and the pool retiring an unused connection cannot both succeed.
    #[test]
    fn upgrade_and_unused_close_exclude_each_other() {
        let peer = SecretKey::from_bytes(&[4u8; 32]).public();
        let conn_id = ConnId {
            peer,
            generation: Generation(0),
        };
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let counter = ConnectionCounter::new(conn_id, tx);

        let permit = counter.inner.try_get_one().expect("open connection");
        assert!(!counter.try_retire_unused(), "retired a connection in use");
        drop(permit);
        assert!(counter.try_retire_unused());
        assert!(
            counter.inner.try_get_one().is_none(),
            "upgraded a retired one"
        );
        assert!(!counter.try_retire_unused(), "retired twice");
    }

    /// The last reference to drop reports which connection went unused.
    #[test]
    fn the_last_reference_reports_the_connection_unused() {
        let peer = SecretKey::from_bytes(&[5u8; 32]).public();
        let conn_id = ConnId {
            peer,
            generation: Generation(7),
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let counter = ConnectionCounter::new(conn_id, tx);

        let permit = counter.get_one();
        let clone = permit.clone();
        drop(permit);
        assert!(rx.try_recv().is_err(), "reported unused while in use");
        drop(clone);
        assert_eq!(rx.try_recv().ok(), Some(conn_id));
    }

    /// A full pool does not adopt a connection from a peer it is dialing.
    ///
    /// A dial holds no slot, so the room check has to run when the adoption
    /// finishes, whatever state the peer is in.
    #[tokio::test]
    async fn adopting_for_a_dialing_peer_respects_max_connections() -> TestResult<()> {
        let net = Incoming::new(Options {
            max_connections: 1,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .await?;
        let second = Endpoint::bind(presets::Minimal).await?;
        let (_dead_sock, dead) = dead_addr()?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(EndpointAddr::from_parts(second.id(), [dead]));
        net.server.address_lookup()?.add(lookup);

        let dial = spawn_get(&net.pool, second.id());
        n0_future::time::sleep(Duration::from_millis(300)).await;
        assert!(!dial.is_finished(), "the dial finished early");
        // One connection in use fills the pool.
        let (first, _first_ref) = net.adopt().await?;

        let (_outgoing, incoming) = net.pair_from(&second).await?;
        assert_err!(
            net.pool.handle_connection(incoming).await,
            TooManyConnections
        );
        assert!(
            first.close_reason().is_none(),
            "closed the connection in use"
        );
        dial.abort();
        net.server.close().await;
        Ok(())
    }

    /// Handing the pool a connection it is already adopting waits for that.
    #[tokio::test]
    async fn adopting_one_connection_twice_waits_for_the_first() -> TestResult<()> {
        let (options, seen) = slow(Duration::from_millis(300));
        let net = Incoming::new(options).await?;
        let (outgoing, incoming) = net.pair().await?;

        let (first, second) = tokio::join!(
            net.pool.handle_connection(incoming.clone()),
            net.pool.handle_connection(incoming),
        );
        let (first, second) = (first?, second?);
        assert_eq!(first.stable_id(), second.stable_id(), "not one connection");
        assert_eq!(seen.lock().expect("poisoned").len(), 1, "adopted twice");
        assert!(!first.is_superseded(), "superseded by itself");

        // One counter, so dropping one reference leaves the other's use alone.
        drop(first);
        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(outgoing.close_reason().is_none(), "closed while in use");
        net.server.close().await;
        Ok(())
    }

    /// Handing the pool a connection it superseded returns it, still superseded.
    #[tokio::test]
    async fn adopting_a_superseded_connection_returns_it() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let again = s.net.pool.handle_connection((*s.first_ref).clone()).await?;
        assert_eq!(again.stable_id(), s.first_ref.stable_id());
        assert!(
            again.is_superseded(),
            "made a superseded connection current"
        );
        assert_eq!(s.net.current().await?.stable_id(), s.second_ref.stable_id());
        s.net.server.close().await;
        Ok(())
    }

    /// A connection that closes during its adoption does not become current.
    #[tokio::test]
    async fn a_connection_that_closes_while_adopted_is_not_current() -> TestResult<()> {
        let (options, _) = slow(Duration::from_millis(300));
        let net = Incoming::new(options).await?;
        let (_first, first_ref) = net.adopt().await?;

        let (outgoing, incoming) = net.pair().await?;
        let adopting = net.spawn_adopt(incoming.clone());
        n0_future::time::sleep(Duration::from_millis(50)).await;
        outgoing.close(0u32.into(), b"gone");

        assert_err!(adopting.await?, Closed);
        assert!(
            !first_ref.is_superseded(),
            "superseded by a closed connection"
        );
        assert_eq!(net.current().await?.stable_id(), first_ref.stable_id());
        // Once it has closed, it is refused right away.
        assert_err!(net.pool.handle_connection(incoming).await, Closed);
        net.server.close().await;
        Ok(())
    }

    /// An adoption that finishes after a newer one does not replace it.
    #[tokio::test]
    async fn an_older_adoption_that_finishes_last_is_superseded() -> TestResult<()> {
        let (options, _) = scripted(short_idle_options(), |n| {
            let delay = if n == 0 { 500 } else { 0 };
            (Duration::from_millis(delay), true)
        });
        let net = Incoming::new(options).await?;
        let (_older, older) = net.pair().await?;
        let (_newer, newer) = net.pair().await?;

        let older = net.spawn_adopt(older);
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let newer = net.pool.handle_connection(newer).await?;
        let older = older.await??;
        assert!(older.is_superseded(), "the older connection is current");
        assert!(!newer.is_superseded(), "an older connection superseded it");
        assert_eq!(net.current().await?.stable_id(), newer.stable_id());
        net.server.close().await;
        Ok(())
    }

    /// A request waits for a running adoption instead of dialing.
    #[tokio::test]
    async fn a_request_waits_for_a_running_adoption() -> TestResult<()> {
        let (options, seen) = slow(Duration::from_millis(300));
        let net = Incoming::new(options).await?;
        let (_outgoing, incoming) = net.pair().await?;
        let adopting = net.spawn_adopt(incoming);
        n0_future::time::sleep(Duration::from_millis(50)).await;

        // The pool has no address for the client, so a dial would fail.
        let requested = net.current().await?;
        assert_eq!(requested.stable_id(), adopting.await??.stable_id());
        assert_eq!(seen.lock().expect("poisoned").len(), 1, "dialed as well");
        net.server.close().await;
        Ok(())
    }

    /// A request that joined a failed adoption gets the current connection.
    #[tokio::test]
    async fn a_request_outlives_a_failed_adoption() -> TestResult<()> {
        let (options, _) = scripted(short_idle_options(), |n| match n {
            0 => (Duration::from_millis(300), true),
            _ => (Duration::from_millis(600), false),
        });
        let net = Incoming::new(options).await?;
        let (_first, first) = net.pair().await?;
        let (_second, second) = net.pair().await?;
        let first = net.spawn_adopt(first);
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let second = net.spawn_adopt(second);
        n0_future::time::sleep(Duration::from_millis(50)).await;

        // Nothing is ready yet, so the request joins the newest adoption.
        let requested = net.current().await?;
        assert_eq!(requested.stable_id(), first.await??.stable_id());
        assert_err!(second.await?, OnConnectError);
        net.server.close().await;
        Ok(())
    }

    /// An adoption that `on_connected` rejects leaves the current connection.
    #[tokio::test]
    async fn a_rejected_adoption_keeps_the_current_connection() -> TestResult<()> {
        let (options, seen) = scripted(short_idle_options(), |n| (Duration::ZERO, n == 0));
        let net = Incoming::new(options).await?;
        let (_first, first_ref) = net.adopt().await?;

        let (outgoing, incoming) = net.pair().await?;
        assert_err!(net.pool.handle_connection(incoming).await, OnConnectError);
        let err = tokio::time::timeout(SHORT_IDLE * 5, outgoing.closed()).await?;
        assert_closed_as(&err, CloseReason::Rejected);
        let rejected = seen.lock().expect("poisoned").pop().expect("not called");
        assert!(
            rejected.upgrade().is_none(),
            "upgraded a rejected connection"
        );
        assert!(!first_ref.is_superseded(), "superseded by a rejected one");
        assert_eq!(net.current().await?.stable_id(), first_ref.stable_id());
        net.server.close().await;
        Ok(())
    }

    /// Closing a peer closes the connection of an adoption that is running.
    #[tokio::test]
    async fn close_closes_the_connection_of_a_running_attempt() -> TestResult<()> {
        let (options, _) = slow(Duration::from_secs(10));
        let net = Incoming::new(options).await?;
        let (outgoing, incoming) = net.pair().await?;
        let adopting = net.spawn_adopt(incoming);
        n0_future::time::sleep(Duration::from_millis(100)).await;

        net.pool.close(net.client.id()).await?;
        let err = tokio::time::timeout(Duration::from_secs(5), outgoing.closed()).await?;
        assert_closed_as(&err, CloseReason::Closed);
        assert_err!(adopting.await?, Closed);
        net.server.close().await;
        Ok(())
    }

    /// The pool keeps at most `max_superseded_per_peer` superseded connections.
    #[tokio::test]
    async fn superseded_connections_are_capped() -> TestResult<()> {
        let net = Incoming::new(Options {
            max_superseded_per_peer: 1,
            ..short_idle_options()
        })
        .await?;
        // The references keep each one in use, so none closes for being unused.
        let mut adopted = Vec::new();
        for _ in 0..3 {
            adopted.push(net.adopt().await?);
        }
        let err = tokio::time::timeout(SHORT_IDLE * 5, adopted[0].0.closed()).await?;
        assert_closed_as(&err, CloseReason::Superseded);
        assert!(
            adopted[1].0.close_reason().is_none(),
            "closed one within the cap"
        );
        assert!(
            adopted[2].0.close_reason().is_none(),
            "closed the current one"
        );
        net.server.close().await;
        Ok(())
    }

    /// A full pool adopts a connection when the superseded cap makes room for it.
    #[tokio::test]
    async fn adopting_at_the_superseded_cap_fits_a_full_pool() -> TestResult<()> {
        let net = Incoming::new(Options {
            max_connections: 2,
            max_superseded_per_peer: 1,
            ..short_idle_options()
        })
        .await?;
        // Two connections in use fill the pool, and put the peer at its cap.
        let (oldest, _oldest_ref) = net.adopt().await?;
        let (middle, _middle_ref) = net.adopt().await?;

        let (_newest, newest) = net.adopt().await?;
        assert!(!newest.is_superseded(), "the adoption is not current");
        tokio::time::timeout(SHORT_IDLE * 5, oldest.closed()).await?;
        assert!(middle.close_reason().is_none(), "closed one within the cap");
        net.server.close().await;
        Ok(())
    }

    /// A dial that connects after its peer was closed skips `on_connected`, and is closed.
    #[tokio::test]
    async fn a_dial_that_connects_after_close_is_closed_when_it_ends() -> TestResult<()> {
        let net = Incoming::new(Options::default()).await?;
        let (options, seen) = slow(Duration::from_secs(10));
        net.client
            .address_lookup()?
            .add(MemoryLookup::from_endpoint_info([net.server.addr()]));
        let pool = ConnectionPool::new(net.client.clone(), INCOMING_ALPN, options);

        let dial = spawn_get(&pool, net.server.id());
        // The handshake waits for the server, so the dial is still connecting.
        let incoming = net.server.accept().await.expect("endpoint closed");
        pool.close(net.server.id()).await?;
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let connection = incoming.await?;

        assert_err!(dial.await?, Closed);
        let err = tokio::time::timeout(SHORT_IDLE * 5, connection.closed()).await?;
        assert_closed_as(&err, CloseReason::Closed);
        assert!(
            seen.lock().expect("poisoned").is_empty(),
            "ran on_connected"
        );
        net.server.close().await;
        Ok(())
    }

    /// An incoming connection becomes the one `get_or_connect` returns.
    #[tokio::test]
    async fn handle_connection_becomes_current() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let conn = s.net.current().await?;
        assert_eq!(conn.stable_id(), s.second_ref.stable_id());
        assert!(!conn.is_superseded());
        assert!(s.first_ref.is_superseded());
        s.net.server.close().await;
        Ok(())
    }

    /// A superseded connection stays open while in use, and closes once unused.
    #[tokio::test]
    async fn superseded_connection_is_closed_once_unused() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(s.first.close_reason().is_none(), "closed while in use");

        drop(s.first_ref);
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as(&err, CloseReason::Unused);
        assert!(s.second.close_reason().is_none(), "closed the current one");
        Ok(())
    }

    /// A reference kept from `on_connected` keeps a superseded connection open.
    #[tokio::test]
    async fn on_connected_ref_keeps_superseded_connection_open() -> TestResult<()> {
        let kept = Arc::new(Mutex::new(Vec::new()));
        let options = short_idle_options().with_on_connected({
            let kept = kept.clone();
            move |_, conn: ConnectionRef| {
                kept.lock().expect("poisoned").push(conn.clone());
                async { Ok(()) }
            }
        });
        let s = Superseded::new(options).await?;
        drop(s.first_ref);
        drop(s.second_ref);

        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(s.first.close_reason().is_none(), "closed while kept");
        kept.lock().expect("poisoned").clear();
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as(&err, CloseReason::Unused);
        Ok(())
    }

    /// [`ConnectionPool::close`] closes superseded connections, even in use.
    #[tokio::test]
    async fn close_closes_superseded_connections() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        s.net.pool.close(s.net.client.id()).await?;
        for (name, conn) in [("superseded", &s.first), ("current", &s.second)] {
            let err = tokio::time::timeout(SHORT_IDLE * 2, conn.closed())
                .await
                .map_err(|_| format!("the {name} connection was not closed"))?;
            assert_closed_as(&err, CloseReason::Dropped);
        }
        Ok(())
    }

    /// An incoming connection serves the waiters of a running dial, and outlives it.
    ///
    /// This is an endpoint that can dial us, but that we cannot dial.
    #[tokio::test]
    async fn incoming_connection_serves_a_running_dial() -> TestResult<()> {
        let net = Incoming::new(Options {
            connect_timeout: Duration::from_millis(500),
            ..short_idle_options()
        })
        .await?;
        // TEST-NET-1: nothing answers, so the dial runs until its timeout.
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(EndpointAddr::from_parts(
            net.client.id(),
            [TransportAddr::Ip("192.0.2.1:1".parse()?)],
        ));
        net.server.address_lookup()?.add(lookup);
        let dial = spawn_get(&net.pool, net.client.id());
        n0_future::time::sleep(Duration::from_millis(100)).await;

        let (outgoing, conn_ref) = net.adopt().await?;
        assert_eq!(dial.await??.stable_id(), conn_ref.stable_id());
        // Past the dial's timeout.
        n0_future::time::sleep(Duration::from_millis(600)).await;
        assert!(outgoing.close_reason().is_none(), "closed the adopted one");
        assert_eq!(net.current().await?.stable_id(), conn_ref.stable_id());
        net.server.close().await;
        Ok(())
    }

    /// A superseded connection closing leaves the current one alone.
    #[tokio::test]
    async fn superseded_connection_closing_keeps_the_current_one() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        // `first_ref` keeps it in use, so only its close event can retire it.
        let first = s.first_ref.downgrade();
        s.first.close(0u32.into(), b"bye");
        until_retired(&first).await?;

        assert!(s.second.close_reason().is_none(), "closed the current one");
        assert_eq!(s.net.current().await?.stable_id(), s.second_ref.stable_id());
        Ok(())
    }

    /// A slow `on_connected` for an incoming connection does not hold up requests.
    #[tokio::test]
    async fn slow_on_connected_for_incoming_keeps_serving() -> TestResult<()> {
        let (options, _) = scripted(short_idle_options(), |n| {
            let delay = if n == 0 { 0 } else { 3600 };
            (Duration::from_secs(delay), true)
        });
        let net = Incoming::new(options).await?;
        let (_first, first_ref) = net.adopt().await?;
        let (_second, second) = net.pair().await?;
        let second = net.spawn_adopt(second);
        n0_future::time::sleep(Duration::from_millis(100)).await;

        let current = tokio::time::timeout(Duration::from_secs(1), net.current())
            .await
            .map_err(|_| "the request waited for on_connected")??;
        assert_eq!(current.stable_id(), first_ref.stable_id());
        second.abort();
        net.server.close().await;
        Ok(())
    }

    /// A downgraded reference does not keep the connection open.
    #[tokio::test]
    async fn downgraded_ref_does_not_keep_connection_open() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let _weak = s.first_ref.downgrade();
        drop(s.first_ref);
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as(&err, CloseReason::Unused);
        Ok(())
    }

    /// A clone of a [`ConnectionRef`] keeps the connection open on its own.
    #[tokio::test]
    async fn cloned_ref_keeps_connection_open() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let clone = s.first_ref.clone();
        drop(s.first_ref);
        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(
            s.first.close_reason().is_none(),
            "closed while a clone lives"
        );
        drop(clone);
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as(&err, CloseReason::Unused);
        Ok(())
    }

    /// Upgrading fails once the pool has closed the connection.
    #[tokio::test]
    async fn upgrade_after_close_fails() -> TestResult<()> {
        let net = Incoming::new(short_idle_options()).await?;
        let (_outgoing, conn_ref) = net.adopt().await?;
        let weak = conn_ref.downgrade();
        assert!(weak.upgrade().is_some());

        net.pool.close(net.client.id()).await?;
        tokio::time::timeout(SHORT_IDLE * 5, conn_ref.closed()).await?;
        assert!(weak.upgrade().is_none(), "upgraded a closed connection");
        net.server.close().await;
        Ok(())
    }

    /// Upgrading fails once the pool has seen the remote close the connection.
    #[tokio::test]
    async fn upgrade_after_a_remote_close_fails() -> TestResult<()> {
        let net = Incoming::new(short_idle_options()).await?;
        let (outgoing, conn_ref) = net.adopt().await?;
        let weak = conn_ref.downgrade();
        drop(conn_ref);
        outgoing.close(0u32.into(), b"gone");
        until_retired(&weak).await?;
        net.server.close().await;
        Ok(())
    }

    /// Dropping the last pool handle closes its connections, in use ones as `drop`.
    #[tokio::test]
    async fn dropping_the_pool_closes_its_connections() -> TestResult<()> {
        let net = Incoming::new(short_idle_options()).await?;
        let (outgoing, conn_ref) = net.adopt().await?;
        let weak = conn_ref.downgrade();

        drop(net.pool);
        let err = tokio::time::timeout(SHORT_IDLE * 5, outgoing.closed()).await?;
        assert_closed_as(&err, CloseReason::Dropped);
        assert!(weak.upgrade().is_none(), "upgraded after shutdown");
        net.server.close().await;
        Ok(())
    }
}
