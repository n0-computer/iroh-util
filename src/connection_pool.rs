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
#[derive(derive_more::Debug, Clone)]
pub struct Options {
    /// How long to keep unused connections around.
    ///
    /// Unused means that there are no [`ConnectionRef`]s alive for the connection,
    /// not that no data flows on it.
    pub idle_timeout: Duration,
    /// Timeout for connect.
    ///
    /// This covers the dial only. [`Options::on_connected`] runs after it, for
    /// as long as it takes.
    pub connect_timeout: Duration,
    /// Maximum number of connections the pool holds.
    ///
    /// Connections a newer one superseded count, since they stay open until
    /// they are unused. Attempts that are still running do not, so attempts to
    /// peers that do not answer cannot take the place of connections.
    pub max_connections: usize,
    /// Maximum number of superseded connections to keep per peer.
    ///
    /// A connection the peer superseded stays open while the peer uses it. A
    /// peer that opens connections in a loop would thus make the pool hold one
    /// per attempt. Beyond this many, the oldest are closed, in use or not.
    pub max_superseded_per_peer: usize,
    /// An optional callback that runs before the pool hands out a new connection.
    ///
    /// Use it to wait for the connection to reach some state, for example to
    /// wait for a direct path before the connection is handed out.
    ///
    /// It runs on the pool's task, so it must not block the thread: that would
    /// stall the whole pool. [`Options::connect_timeout`] does not bound it,
    /// and requests for the connection wait for it. Keep it short, or give it
    /// a timeout of its own.
    ///
    /// It also runs for connections handed to [`ConnectionPool::handle_connection`].
    /// Which of the two it is running for is [`Connection::side`]: `Client` for
    /// a connection the pool dialed, `Server` for one the peer opened.
    ///
    /// The [`ConnectionRef`] it receives counts as a use of the connection, and
    /// is dropped when the callback returns. A task that holds a
    /// [`ConnectionRef`] keeps the connection in use for as long as it runs.
    /// The connection then never counts as unused, and the pool neither closes
    /// nor evicts it. So hand a [`WeakConnectionRef`], via
    /// [`ConnectionRef::downgrade`], to anything that outlives the callback and
    /// only watches the connection.
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
    /// A superseded connection stays open for as long as it is used. New work
    /// should move to the current one, which [`ConnectionPool::get_or_connect`]
    /// returns. The old one may lead to an endpoint that has since restarted,
    /// dead without us having noticed yet.
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
/// It relates to [`ConnectionRef`] as `Weak` does to `Arc`. Holding one does
/// not count as a use, so a task that watches the connection for as long as it
/// lives can hold it. [`Self::upgrade`] returns a reference that does count. As
/// with `Weak`, upgrading fails once the connection is gone: once the pool no
/// longer holds it. Unlike `Weak`, it keeps the connection handle itself alive,
/// and it derefs to the connection.
///
/// A task that outlives [`Options::on_connected`] and only watches the
/// connection takes one, via [`ConnectionRef::downgrade`]. Work that should
/// keep the connection open upgrades it. That includes serving streams the
/// remote opened, since the remote may keep using a connection the pool has
/// superseded.
///
/// What you reach through the deref is not counted as a use, so the pool can
/// close the connection while you use it. An accept loop waits for work by
/// accepting a stream on a `WeakConnectionRef`, and serves the stream after
/// [`Self::upgrade`]. Closing the connection through the
/// deref goes behind the pool's back: upgrades succeed until the pool handles
/// its close event. [`ConnectionPool::close`] closes it through the pool,
/// together with every other connection to the endpoint.
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
    /// Returns `None` once the pool no longer holds the connection. The pool
    /// stops holding it before it closes it. A connection that closes
    /// otherwise stays in the pool until its close event arrives. An upgrade in
    /// that window returns a reference that fails on first use.
    ///
    /// An upgrade and the pool closing an unused connection cannot both
    /// succeed: whichever comes first wins.
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
    ///
    /// The pool closes a connection that was unused for
    /// [`Options::idle_timeout`], or evicts it to make room. It also uses this
    /// reason for an unused connection that [`ConnectionPool::close`] or the
    /// pool shutting down closes.
    Unused,
    /// It was in use when [`ConnectionPool::close`] or shutdown closed it.
    Dropped,
    /// A newer connection to the peer took its place.
    ///
    /// The pool closes a superseded connection beyond
    /// [`Options::max_superseded_per_peer`], and the connection of a dial that
    /// an adoption won over.
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
///
/// Every connection the pool closes goes through here, so a peer always gets a
/// [`CloseReason`] rather than a bare code.
fn close_connection(connection: &Connection, reason: CloseReason) {
    connection.close(reason.code(), reason.reason());
}

/// A connection the pool holds, with its reference count.
///
/// It is not `Clone`: the one value lives in the actor's peer map, and dropping
/// it retires the counter. See [`ConnectionCounter`].
#[derive(Debug)]
struct PooledConnection {
    connection: Connection,
    counter: ConnectionCounter,
}

impl PooledConnection {
    /// Pairs a connection with the counter that was made for it.
    fn new(connection: Connection, counter: ConnectionCounter) -> Self {
        Self {
            connection,
            counter,
        }
    }

    /// Returns which connection this is.
    fn conn_id(&self) -> ConnId {
        self.counter.conn_id()
    }

    /// Returns a reference that counts as a use of the connection.
    fn conn_ref(&self) -> ConnectionRef {
        ConnectionRef::new(self.connection.clone(), self.counter.get_one())
    }

    /// Returns whether nothing uses the connection.
    fn is_unused(&self) -> bool {
        self.counter.is_unused()
    }

    /// Closes the connection, with a reason that says whether it was in use.
    fn close(self) {
        let reason = if self.is_unused() {
            CloseReason::Unused
        } else {
            CloseReason::Dropped
        };
        self.close_as(reason);
    }

    /// Retires the connection, and closes it with `reason`.
    fn close_as(self, reason: CloseReason) {
        let Self {
            connection,
            counter,
        } = self;
        drop(counter);
        close_connection(&connection, reason);
    }
}

/// An attempt to get a connection to a peer.
///
/// The attempt is a dial, or the adoption of a connection the peer opened. Both
/// run [`Options::on_connected`] before the connection is handed out.
#[derive(Debug)]
struct PendingConnection {
    generation: Generation,
    /// The reference count of the connection the attempt will produce.
    counter: ConnectionCounter,
    origin: Origin,
    /// Callers of [`ConnectionPool::handle_connection`] for this connection.
    ///
    /// A dial has none.
    callers: Vec<RefSender>,
    /// Callers of [`ConnectionPool::get_or_connect`], which want any connection.
    requests: Vec<RefSender>,
}

/// Where a pending connection comes from.
#[derive(Debug)]
enum Origin {
    /// The pool is dialing the peer.
    ///
    /// The connection only exists inside the dial, so the pool has nothing to
    /// close for it until the dial finishes.
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

    /// Returns whether the attempt is a dial.
    fn is_dial(&self) -> bool {
        matches!(self.origin, Origin::Dial)
    }

    /// Retires the attempt and closes its connection, if it has one.
    ///
    /// Returns everyone who waits for it, for the caller to serve or fail. The
    /// attempt runs until it finishes, and its result is discarded. A dial's
    /// connection is closed then.
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

/// Fails every waiter with `cause`.
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
    ///
    /// The current connection keeps serving requests until one is adopted.
    adopting: Vec<PendingConnection>,
    /// Connections that a newer one took the place of, oldest first.
    ///
    /// The peer may still be using one, so it stays open until it is unused
    /// for [`Options::idle_timeout`], as a current connection would.
    superseded: Vec<PooledConnection>,
}

impl Peer {
    /// Returns whether the pool holds nothing for the peer any more.
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

    /// Returns the connection the pool holds for `connection`, if it has it.
    ///
    /// Connections are compared by their stable id, which is unique among the
    /// connections that are open.
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

    /// Returns the attempt whose connection becomes the peer's current one.
    ///
    /// That is the dial that is running, or else the newest adoption, whose
    /// connection supersedes whatever the peer has when it is adopted.
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
    /// This is unbounded so it can be used in `Drop`. Nothing bounds it: every
    /// drop of the last reference sends an event. An upgrade followed by a
    /// drop does that without going through the actor. The actor drains it
    /// before anything else, so it only grows while events come in faster than
    /// the actor handles them.
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
    /// Why the pool dropped each dial that is still running.
    ///
    /// A dial's connection only exists once the dial ends, so the pool closes
    /// it then, with the reason it had when it dropped the dial.
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
        // An attempt is running. An adoption counts: once it is adopted, its
        // connection is the one requests get. Dialing as well would make a
        // connection that the adoption supersedes right away. If the adoption
        // fails, the request is handled again.
        if let Some(attempt) = self.peers.get_mut(&id).and_then(Peer::pending_mut) {
            // Callers that gave up would otherwise stay until the attempt ends.
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
    /// Its `on_connected` runs like a dial's, and the connection becomes the
    /// peer's current one once it has finished. See
    /// [`ConnectionPool::handle_connection`].
    fn handle_incoming(&mut self, conn: Connection, tx: RefSender) {
        let id = conn.remote_id();
        // A connection that has closed already would only take the place of
        // one that works.
        if conn.close_reason().is_some() {
            let _ = tx.send(Err(e!(PoolConnectError::Closed)));
            return;
        }
        // The pool may hold this connection already, as the peer's current
        // connection or as one it superseded, or be adopting it. Adopting it
        // again would give one connection two generations and two reference
        // counts. Closing either would close the connection the other one
        // still hands out.
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
    ///
    /// This is what [`Options::max_connections`] limits.
    fn connection_count(&self) -> usize {
        self.peers
            .values()
            .map(|peer| usize::from(peer.ready().is_some()) + peer.superseded.len())
            .sum()
    }

    /// Returns whether adding a connection to the peer grows the pool.
    ///
    /// It does not when the peer has a current connection and its superseded
    /// ones are at [`Options::max_superseded_per_peer`]. One of them becomes
    /// superseded, and the cap closes the oldest.
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

    /// Returns a future that dials the peer or adopts `incoming`.
    ///
    /// The future runs `on_connected` as well, after the dial. The connect
    /// timeout does not cover it.
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
                // The pool drops an attempt while it dials when the peer is
                // closed, or when an adoption takes its place. Its result is
                // stale then, and the pool closes the connection when it
                // arrives, so `on_connected` must not hold it open.
                let Some(permit) = counter.try_get_one() else {
                    return (conn_id, Ok(connection));
                };
                let conn_ref = ConnectionRef::new(connection.clone(), permit);
                if let Err(err) = f(&endpoint, &conn_ref).await {
                    // The pool never holds a rejected connection. Retire it
                    // before closing it, as the pool does.
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
        // The peer may have closed the connection while `on_connected` ran.
        // Making it current would supersede a working connection with a dead
        // one, and may evict another to make room for it.
        if connection.close_reason().is_some() {
            debug!(%conn_id, "the connection closed before it was ready");
            self.fail_attempt(conn_id.peer, attempt, e!(PoolConnectError::Closed));
            return;
        }
        // Connections made since this attempt started may have filled the pool.
        // A connection this one supersedes stays open, so an attempt that
        // finishes adds one to the connections the pool holds. The exception
        // is a peer whose superseded connections are at the cap.
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

    /// Retires a failed attempt, and fails its callers with `cause`.
    ///
    /// Requests that joined an adoption want any connection to the peer, not
    /// this one. So they are handled again, as if they had just come in. They
    /// get the current connection, wait for another attempt, or start a dial.
    /// The requests of a dial fail, since the dial was made for them.
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
                // A caller that is gone would only start a dial for nobody.
                for tx in requests.into_iter().filter(|tx| !tx.is_closed()) {
                    self.handle_request(RequestRef { id, tx });
                }
            }
        }
        self.drop_peer_if_empty(id);
    }

    /// Adds `conn` to the pool and hands it to `waiters`.
    ///
    /// It becomes the peer's current connection, unless a newer one is current
    /// already. Then it goes straight to the superseded ones.
    fn insert_connection(&mut self, conn: PooledConnection, mut waiters: Vec<RefSender>) {
        let conn_id = conn.conn_id();
        let peer = self.peers.entry(conn_id.peer).or_default();
        // Adoptions can finish out of order, since each runs `on_connected` for
        // as long as it takes. The connection the peer opened last wins, as it
        // would have if they had finished in order. A dial never starts while
        // an adoption runs, so only a newer adoption can be current here.
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
                    // Two endpoints that dial each other at once each keep the
                    // connection they saw last. They may disagree, so the peer
                    // can still be using this one. It closes when it is unused,
                    // like any other connection the pool holds.
                    debug!(%conn_id, "the new connection supersedes the current one");
                    previous.counter.mark_superseded();
                    peer.superseded.push(previous);
                }
                Some(PeerState::Connecting(dial)) => {
                    // A dial that is still running loses to this connection.
                    // Its waiters get this one, and its own connection, if it
                    // has made one, is closed. Its result is discarded when it
                    // arrives.
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
            // A caller that is gone takes no reference: it would put the
            // connection in use and send an unused event when it drops.
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

    /// Closes the peer's superseded connections beyond the cap.
    ///
    /// A peer that opens connections in a loop would otherwise make the pool
    /// hold one per attempt, each until it goes unused. The oldest are closed,
    /// in use or not.
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
    ///
    /// Only acts if the pool still holds the connection. The event names one
    /// connection, so an event for one the pool dropped already cannot remove
    /// another connection to the same peer.
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

    /// Closes every connection to `id`, and fails the attempts to get one.
    ///
    /// Waiters get `cause`, which says whether the pool is shutting down or the
    /// peer was closed. See [`ConnectionPool::close`].
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
            // The connection an attempt has made is closed here. The attempt
            // runs to its end, and its result is discarded when it arrives.
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
    /// A dial ends within [`Options::connect_timeout`], plus the time
    /// [`Options::on_connected`] takes. A request for an endpoint the pool is
    /// adopting a connection from waits for the adoption instead, which only
    /// `on_connected` bounds. If the adoption fails, the request is handled
    /// again: it gets the current connection, or a dial.
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
    /// If a connection the endpoint opened later became current first, because
    /// its `on_connected` finished sooner, that one stays current. This one is
    /// then returned as superseded.
    ///
    /// The previous connection is not closed right away. Two endpoints that dial
    /// each other at once each keep the connection they saw last. They may
    /// disagree, so the remote can still be using the one we superseded. It is
    /// closed when it is unused instead, like any connection the pool holds.
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
    /// That includes connections that [`Self::handle_connection`] superseded,
    /// even while they are in use. A connection is closed with the reason `drop`
    /// if it is still in use. Requests waiting for a connection to `id` fail with
    /// [`PoolConnectError::Closed`]. Requests made after this call get a new
    /// connection.
    ///
    /// An adoption that is still running has its connection closed right here.
    /// A dial that is still running does not: its connection only exists
    /// inside the dial. Its result is discarded when it arrives, and the
    /// connection is closed then. A dial that is still connecting skips
    /// [`Options::on_connected`], so this happens within
    /// [`Options::connect_timeout`]. A dial whose `on_connected` is running
    /// is closed once the callback returns.
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
    /// The count and the bit share one atomic. Thus retiring an unused
    /// connection and upgrading a reference to it cannot both succeed: the
    /// first update wins.
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

/// Bit that marks a connection the pool no longer holds.
///
/// It is the high bit of [`ConnectionCounterInner::state`]. The bits below it
/// hold the count. See [`ConnectionCounter`] for when it is set.
const RETIRED: u64 = 1 << 63;

/// The pool's hold on a connection's reference count, which retires it on drop.
///
/// It is not `Clone`: the pool keeps one for each connection it holds or is
/// making. Thus a counter is retired when the pool stops holding its
/// connection, and [`WeakConnectionRef::upgrade`] needs to check nothing else.
/// The one other retirement is a connection that `on_connected` rejected,
/// which the pool never holds. Where the pool closes a connection, it drops
/// the counter first, so no upgrade succeeds on a connection that the pool
/// closed.
///
/// Weak references and the connect future hold the [`ConnectionCounterInner`]
/// instead.
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
    ///
    /// If it returns `true`, the caller removes the connection and closes it.
    /// If it returns `false`, a reference was taken in the meantime, and the
    /// pool keeps the connection.
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
        // The count is at least one while `self` lives, so a clone never takes
        // a connection off the unused list behind the pool's back.
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
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use iroh::{
        EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr,
        address_lookup::MemoryLookup,
        endpoint::{Connection, presets},
        protocol::{AcceptError, ProtocolHandler, Router},
    };
    use n0_error::{AnyError, Result, StdResultExt};
    use n0_future::{BufferedStreamExt, StreamExt, io, stream};
    use testresult::TestResult;
    use tokio::sync::oneshot;
    use tracing::trace;

    use super::{
        Actor, CloseReason, ConnId, ConnectionCounter, ConnectionPool, ConnectionRef, Generation,
        OnConnected, Options, Peer, PeerState, PoolConnectError, PooledConnection, RequestRef,
    };

    const ECHO_ALPN: &[u8] = b"echo";

    #[derive(Debug, Clone)]
    struct Echo;

    impl ProtocolHandler for Echo {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            let conn_id = connection.stable_id();
            let id = connection.remote_id();
            trace!(%id, %conn_id, "Accepting echo connection");
            loop {
                match connection.accept_bi().await {
                    Ok((mut send, mut recv)) => {
                        trace!(%id, %conn_id, "Accepted echo request");
                        tokio::io::copy(&mut recv, &mut send).await?;
                        send.finish().map_err(AcceptError::from_err)?;
                    }
                    Err(e) => {
                        trace!(%id, %conn_id, "Failed to accept echo request {e}");
                        break;
                    }
                }
            }
            Ok(())
        }
    }

    async fn echo_client(conn: &Connection, text: &[u8]) -> Result<Vec<u8>> {
        let conn_id = conn.stable_id();
        let id = conn.remote_id();
        trace!(%id, %conn_id, "Sending echo request");
        let (mut send, mut recv) = conn.open_bi().await.anyerr()?;
        send.write_all(text).await.anyerr()?;
        send.finish().anyerr()?;
        let response = recv.read_to_end(1000).await.anyerr()?;
        trace!(%id, %conn_id, "Received echo response");
        Ok(response)
    }

    async fn echo_server() -> TestResult<(EndpointAddr, Router)> {
        let endpoint = iroh::Endpoint::builder(presets::N0)
            .alpns(vec![ECHO_ALPN.to_vec()])
            .bind()
            .await?;
        endpoint.online().await;
        let addr = endpoint.addr();
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(ECHO_ALPN, Echo)
            .spawn();

        Ok((addr, router))
    }

    async fn echo_servers(n: usize) -> TestResult<(Vec<EndpointId>, Vec<Router>, MemoryLookup)> {
        let res = stream::iter(0..n)
            .map(|_| echo_server())
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await;
        let res: Vec<(EndpointAddr, Router)> = res.into_iter().collect::<TestResult<Vec<_>>>()?;
        let (addrs, routers): (Vec<_>, Vec<_>) = res.into_iter().unzip();
        let ids = addrs.iter().map(|a| a.id).collect::<Vec<_>>();
        let address_lookup = MemoryLookup::from_endpoint_info(addrs);
        Ok((ids, routers, address_lookup))
    }

    async fn shutdown_routers(routers: Vec<Router>) {
        stream::iter(routers)
            .for_each_concurrent(16, |router| async move {
                let _ = router.shutdown().await;
            })
            .await;
    }

    fn test_options() -> Options {
        Options {
            idle_timeout: Duration::from_millis(100),
            connect_timeout: Duration::from_secs(5),
            max_connections: 32,
            ..Default::default()
        }
    }

    /// Puts `conn` into `actor` as its peer's current connection.
    ///
    /// Returns which connection it is, for the events a test hands the actor.
    fn insert_ready(actor: &mut Actor, conn: &Connection) -> ConnId {
        let conn_id = ConnId {
            peer: conn.remote_id(),
            generation: Generation(1),
        };
        let counter = ConnectionCounter::new(conn_id, actor.unused_tx.clone());
        let pooled = PooledConnection::new(conn.clone(), counter);
        actor.insert_connection(pooled, Vec::new());
        conn_id
    }

    /// Returns the id of a connection to `peer` that the pool never made.
    fn stale_conn_id(peer: EndpointId) -> ConnId {
        ConnId {
            peer,
            generation: Generation(0),
        }
    }

    struct EchoClient {
        pool: ConnectionPool,
    }

    impl EchoClient {
        async fn echo(
            &self,
            id: EndpointId,
            text: Vec<u8>,
        ) -> Result<Result<(usize, Vec<u8>), AnyError>, PoolConnectError> {
            let conn = self.pool.get_or_connect(id).await?;
            let id = conn.stable_id();
            match echo_client(&conn, &text).await {
                Ok(res) => Ok(Ok((id, res))),
                Err(e) => Ok(Err(e)),
            }
        }
    }

    #[tokio::test]
    async fn connection_pool_errors() -> TestResult<()> {
        let address_lookup = MemoryLookup::new();
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup.clone())
            .bind()
            .await?;
        let pool = ConnectionPool::new(endpoint.clone(), ECHO_ALPN, test_options());
        let client = EchoClient { pool };
        {
            let non_existing = SecretKey::from_bytes(&[0; 32]).public();
            let res = client.echo(non_existing, b"Hello, world!".to_vec()).await;
            assert!(matches!(res, Err(PoolConnectError::ConnectError { .. })));
        }
        {
            let non_listening = SecretKey::from_bytes(&[0; 32]).public();
            address_lookup.add_endpoint_info(EndpointAddr {
                id: non_listening,
                addrs: vec![TransportAddr::Ip("127.0.0.1:12121".parse().unwrap())]
                    .into_iter()
                    .collect(),
            });
            let res = client.echo(non_listening, b"Hello, world!".to_vec()).await;
            assert!(matches!(res, Err(PoolConnectError::Timeout { .. })));
        }
        endpoint.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn connection_pool_smoke() -> TestResult<()> {
        let n = 32;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup.clone())
            .bind()
            .await?;
        let pool = ConnectionPool::new(endpoint.clone(), ECHO_ALPN, test_options());
        let client = EchoClient { pool };
        let mut connection_ids = BTreeMap::new();
        let msg = b"Hello, pool!".to_vec();
        for id in &ids {
            let (cid1, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
            let (cid2, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
            assert_eq!(cid1, cid2);
            connection_ids.insert(id, cid1);
        }
        n0_future::time::sleep(Duration::from_millis(1000)).await;
        for id in &ids {
            let cid1 = *connection_ids.get(id).expect("Connection ID not found");
            let (cid2, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
            assert_ne!(cid1, cid2);
        }
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Unused connections are evicted to make room at the connection limit.
    #[tokio::test]
    async fn connection_pool_unused() -> TestResult<()> {
        let n = 32;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup.clone())
            .bind()
            .await?;
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                idle_timeout: Duration::from_secs(100),
                max_connections: 8,
                ..test_options()
            },
        );
        let client = EchoClient { pool };
        let msg = b"Hello, pool!".to_vec();
        for id in &ids {
            let (_, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
        }
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Uses an on_connected callback that just errors out every time.
    #[tokio::test]
    async fn on_connected_error() -> TestResult<()> {
        let n = 1;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let on_connected: OnConnected =
            Arc::new(|_, _| Box::pin(async { Err(io::Error::other("on_connect failed")) }));
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                on_connected: Some(on_connected),
                ..test_options()
            },
        );
        let client = EchoClient { pool };
        let msg = b"Hello, pool!".to_vec();
        for id in &ids {
            let res = client.echo(*id, msg.clone()).await;
            assert!(matches!(res, Err(PoolConnectError::OnConnectError { .. })));
        }
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Uses an on_connected callback to ensure that the connection is direct.
    #[tokio::test]
    async fn on_connected_direct() -> TestResult<()> {
        let n = 1;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let on_connected = |_, conn: ConnectionRef| async move {
            let mut stream = conn.paths_stream();
            while let Some(paths) = stream.next().await {
                if paths.iter().any(|path| path.is_ip()) {
                    return Ok(());
                }
            }
            Err(io::Error::other("connection closed before becoming direct"))
        };
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            test_options().with_on_connected(on_connected),
        );
        let client = EchoClient { pool };
        let msg = b"Hello, pool!".to_vec();
        for id in &ids {
            let res = client.echo(*id, msg.clone()).await;
            assert!(res.is_ok());
        }
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Binds a UDP socket on loopback that never answers.
    ///
    /// A dial to it times out, since nothing answers the QUIC handshake. The
    /// caller keeps the socket, so the OS cannot give the port to another
    /// socket. A fixed port could clash with other tests.
    fn dead_addr() -> TestResult<(std::net::UdpSocket, TransportAddr)> {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = TransportAddr::Ip(sock.local_addr()?);
        Ok((sock, addr))
    }

    /// Spawns `n` `get_or_connect(id)` calls and waits until each has started.
    async fn enter_get_or_connect(
        pool: ConnectionPool,
        id: EndpointId,
        n: usize,
    ) -> Vec<
        tokio::task::JoinHandle<std::result::Result<super::ConnectionRef, super::PoolConnectError>>,
    > {
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

    /// Many requests for an unreachable peer do not stall a request for another.
    ///
    /// The request for the reachable peer must finish in bounded time.
    #[tokio::test]
    async fn connection_pool_dead_peer_backlog_does_not_stall() -> TestResult<()> {
        let (live_ids, routers, address_lookup) = echo_servers(1).await?;
        let live_peer = live_ids[0];

        let (_dead_sock, dead_transport_addr) = dead_addr()?;
        let dead_peer = SecretKey::from_bytes(&[7; 32]).public();
        address_lookup.add_endpoint_info(EndpointAddr {
            id: dead_peer,
            addrs: vec![dead_transport_addr].into_iter().collect(),
        });

        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;

        let connect_timeout = Duration::from_secs(1);
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                connect_timeout,
                ..test_options()
            },
        );

        let backlog = enter_get_or_connect(pool.clone(), dead_peer, 150).await;

        let probe_timeout = connect_timeout * 5;
        let probe = Instant::now();
        let probe_result =
            n0_future::time::timeout(probe_timeout, pool.get_or_connect(live_peer)).await;
        let elapsed = probe.elapsed();

        for h in backlog {
            h.abort();
        }
        shutdown_routers(routers).await;
        endpoint.close().await;

        match probe_result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => panic!("live-peer probe errored after {elapsed:?}: {e:?}"),
            Err(_) => {
                panic!(
                    "live-peer probe did not complete within {probe_timeout:?}: the pool stalled"
                )
            }
        }
    }

    /// A smaller dead-peer backlog does not delay an unrelated peer at all.
    ///
    /// Same setup as `connection_pool_dead_peer_backlog_does_not_stall`, with a
    /// stricter bound: the unrelated-peer probe must complete within one
    /// `connect_timeout` window.
    #[tokio::test]
    async fn connection_pool_dead_peer_does_not_delay_other_peers() -> TestResult<()> {
        let (live_ids, routers, address_lookup) = echo_servers(1).await?;
        let live_peer = live_ids[0];

        let (_dead_sock, dead_transport_addr) = dead_addr()?;
        let dead_peer = SecretKey::from_bytes(&[8; 32]).public();
        address_lookup.add_endpoint_info(EndpointAddr {
            id: dead_peer,
            addrs: vec![dead_transport_addr].into_iter().collect(),
        });

        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let connect_timeout = Duration::from_secs(1);
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                connect_timeout,
                ..test_options()
            },
        );

        let backlog = enter_get_or_connect(pool.clone(), dead_peer, 50).await;

        let probe = Instant::now();
        let res = pool.get_or_connect(live_peer).await;
        let elapsed = probe.elapsed();
        assert!(res.is_ok(), "live-peer connect failed: {res:?}");
        assert!(
            elapsed < connect_timeout,
            "live-peer probe took {elapsed:?}"
        );

        for h in backlog {
            h.abort();
        }
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn close_during_connect_returns_closed() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let handshake = Duration::from_millis(1000);
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            test_options().with_on_connected(move |_, _| async move {
                n0_future::time::sleep(handshake).await;
                Ok(())
            }),
        );
        let first = {
            let pool = pool.clone();
            n0_future::task::spawn(async move { pool.get_or_connect(id).await })
        };
        n0_future::time::sleep(Duration::from_millis(800)).await;
        pool.close(id).await.expect("close failed");
        let first = first.await.expect("join failed");
        assert!(
            matches!(first, Err(PoolConnectError::Closed { .. })),
            "in-flight connect after close: {first:?}"
        );
        let second = pool.get_or_connect(id).await;
        assert!(second.is_ok(), "pool unusable after close: {second:?}");
        drop(second);
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn close_then_immediate_reconnect() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let pool = ConnectionPool::new(endpoint.clone(), ECHO_ALPN, test_options());
        let conn1 = pool.get_or_connect(id).await?;
        let cid1 = conn1.stable_id();
        pool.close(id).await.expect("close failed");
        let conn2 = pool
            .get_or_connect(id)
            .await
            .unwrap_or_else(|e| panic!("reconnect after close failed: {e:?}"));
        assert_ne!(conn2.stable_id(), cid1);
        let msg = b"after close";
        assert_eq!(echo_client(&conn2, msg).await?, msg);
        drop(conn1);
        drop(conn2);
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_old_ref_does_not_close_new_connection() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                idle_timeout: Duration::from_millis(50),
                ..test_options()
            },
        );
        let conn1 = pool.get_or_connect(id).await?;
        pool.close(id).await.expect("close failed");
        let conn2 = pool.get_or_connect(id).await?;
        drop(conn1);
        n0_future::time::sleep(Duration::from_millis(200)).await;
        let msg = b"still alive";
        assert_eq!(echo_client(&conn2, msg).await?, msg);
        drop(conn2);
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Closing a slow connection attempt discards it.
    ///
    /// A later connect starts a new attempt.
    #[tokio::test]
    async fn stale_connect_result_is_discarded() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let handshake = Duration::from_millis(1000);
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            test_options().with_on_connected(move |_, _| async move {
                n0_future::time::sleep(handshake).await;
                Ok(())
            }),
        );
        let first = {
            let pool = pool.clone();
            n0_future::task::spawn(async move { pool.get_or_connect(id).await })
        };
        n0_future::time::sleep(Duration::from_millis(800)).await;
        pool.close(id).await.expect("close failed");
        n0_future::time::sleep(Duration::from_millis(10)).await;
        let t = std::time::Instant::now();
        let second = pool.get_or_connect(id).await;
        let elapsed = t.elapsed();
        let first = first.await.expect("join failed");
        assert!(matches!(first, Err(PoolConnectError::Closed { .. })));
        assert!(second.is_ok(), "second connect failed: {second:?}");
        assert!(
            elapsed >= Duration::from_millis(900),
            "second connect completed in {elapsed:?}, served by a stale connect attempt"
        );
        drop(second);
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// A closed connection is replaced on the next request.
    #[tokio::test]
    async fn watch_close() -> TestResult<()> {
        let n = 1;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;

        let pool = ConnectionPool::new(endpoint.clone(), ECHO_ALPN, test_options());
        let conn = pool.get_or_connect(ids[0]).await?;
        let cid1 = conn.stable_id();
        conn.close(0u32.into(), b"test");
        n0_future::time::sleep(Duration::from_millis(500)).await;
        let conn = pool.get_or_connect(ids[0]).await?;
        let cid2 = conn.stable_id();
        assert_ne!(cid1, cid2);
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// In-flight attempts do not count towards [`Options::max_connections`].
    ///
    /// Otherwise peers we are still trying to reach take every slot, and
    /// unrelated peers fail with `TooManyConnections` until `connect_timeout`
    /// expires.
    #[tokio::test]
    async fn inflight_connects_do_not_exhaust_slots() -> TestResult<()> {
        let (live_ids, routers, address_lookup) = echo_servers(1).await?;
        let live_peer = live_ids[0];

        let max_connections = 2;
        let mut dead = Vec::new();
        let mut _socks = Vec::new();
        for i in 0..max_connections {
            let (sock, addr) = dead_addr()?;
            _socks.push(sock);
            let id = SecretKey::from_bytes(&[20 + i as u8; 32]).public();
            address_lookup.add_endpoint_info(EndpointAddr {
                id,
                addrs: vec![addr].into_iter().collect(),
            });
            dead.push(id);
        }

        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                connect_timeout: Duration::from_secs(5),
                max_connections,
                ..test_options()
            },
        );

        // Park the pool at its connection limit with attempts that neither
        // succeed nor fail until connect_timeout expires.
        let mut parked = Vec::new();
        for id in dead {
            let pool = pool.clone();
            parked.push(n0_future::task::spawn(async move {
                pool.get_or_connect(id).await
            }));
        }
        n0_future::time::sleep(Duration::from_millis(100)).await;

        let res = pool.get_or_connect(live_peer).await;
        assert!(
            res.is_ok(),
            "unrelated peer rejected while pool holds no connections: {res:?}"
        );
        drop(res);

        // Let the parked attempts finish before tearing the endpoint down.
        for p in parked {
            let _ = p.await;
        }
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// A connection made after the pool filled up fails with `TooManyConnections`.
    ///
    /// Attempts no longer reserve a slot, so without a second check the pool
    /// would go over `max_connections`.
    #[tokio::test]
    async fn connect_into_a_full_pool_fails() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(2).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                max_connections: 1,
                ..test_options()
            },
        );
        // Both attempts start before either connection exists.
        let (a, b) = tokio::join!(pool.get_or_connect(ids[0]), pool.get_or_connect(ids[1]));
        let full = |res: &Result<_, PoolConnectError>| {
            matches!(res, Err(PoolConnectError::TooManyConnections { .. }))
        };
        assert!(
            (a.is_ok() && full(&b)) || (full(&a) && b.is_ok()),
            "expected one connection and one TooManyConnections: {a:?}, {b:?}"
        );
        drop((a, b));
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// A stale close event leaves the current connection alone.
    ///
    /// The event is for a connection the pool no longer holds. That happens when
    /// a request finds the current connection closed and removes it before its
    /// close event arrives. The test calls the handler directly, so the order
    /// of events is certain.
    #[tokio::test]
    async fn stale_close_event_keeps_the_current_connection() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let conn = endpoint.connect(id, ECHO_ALPN).await?;
        let (mut actor, _tx) = Actor::new(endpoint.clone(), ECHO_ALPN, test_options());
        let conn_id = insert_ready(&mut actor, &conn);

        actor.handle_conn_closed(stale_conn_id(id));
        assert!(
            actor.peers.contains_key(&id),
            "a stale close event removed the peer"
        );
        assert!(
            conn.close_reason().is_none(),
            "a stale close event closed the connection"
        );
        actor.handle_conn_closed(conn_id);
        assert!(!actor.peers.contains_key(&id));
        assert_eq!(actor.connection_count(), 0);

        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// A connection that has closed is not handed out.
    ///
    /// The pool learns of a close through a watcher, so a connection can be
    /// closed while it is still the peer's current one. A request that lands in
    /// that window gets a new connection.
    #[tokio::test]
    async fn closed_connection_is_not_handed_out() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let conn = endpoint.connect(id, ECHO_ALPN).await?;
        let (mut actor, _tx) = Actor::new(endpoint.clone(), ECHO_ALPN, test_options());
        insert_ready(&mut actor, &conn);
        // The pool has not handled the close event yet.
        conn.close(0u32.into(), b"gone");
        conn.closed().await;

        let (tx, mut rx) = oneshot::channel();
        actor.handle_request(RequestRef { id, tx });
        assert!(
            rx.try_recv().is_err(),
            "the closed connection was handed out"
        );
        assert!(
            matches!(
                actor.peers.get(&id).and_then(|peer| peer.current.as_ref()),
                Some(PeerState::Connecting(_))
            ),
            "the request did not start a new connection"
        );

        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// A connection whose `on_connected` failed is closed.
    ///
    /// Dropping it would not be enough: the callback may keep a handle to it.
    #[tokio::test]
    async fn on_connected_error_closes_the_connection() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let kept = Arc::new(std::sync::Mutex::new(None));
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            test_options().with_on_connected({
                let kept = kept.clone();
                move |_, conn: ConnectionRef| {
                    *kept.lock().expect("poisoned") = Some(conn.downgrade());
                    async { Err(io::Error::other("on_connect failed")) }
                }
            }),
        );
        let res = pool.get_or_connect(ids[0]).await;
        assert!(matches!(res, Err(PoolConnectError::OnConnectError { .. })));
        let conn = kept
            .lock()
            .expect("poisoned")
            .take()
            .expect("callback not called");
        assert!(
            conn.close_reason().is_some(),
            "the connection stayed open after on_connected failed"
        );
        assert!(conn.upgrade().is_none(), "upgraded a rejected connection");
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Using a connection again restarts its idle timeout.
    ///
    /// The connection is closed `idle_timeout` after it last went idle, not
    /// after it first did.
    #[tokio::test]
    async fn use_restarts_the_idle_timeout() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let idle_timeout = Duration::from_millis(500);
        let kept = Arc::new(std::sync::Mutex::new(None));
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                idle_timeout,
                ..test_options()
            }
            .with_on_connected({
                let kept = kept.clone();
                move |_, conn: ConnectionRef| {
                    *kept.lock().expect("poisoned") = Some(conn.downgrade());
                    async { Ok(()) }
                }
            }),
        );
        drop(pool.get_or_connect(ids[0]).await?);
        let conn = kept
            .lock()
            .expect("poisoned")
            .take()
            .expect("callback not called");

        n0_future::time::sleep(idle_timeout * 3 / 5).await;
        drop(pool.get_or_connect(ids[0]).await?);
        // Past the first timeout, within the second.
        n0_future::time::sleep(idle_timeout * 3 / 5).await;
        assert!(
            conn.close_reason().is_none(),
            "closed before its restarted timeout"
        );
        // Past the second.
        n0_future::time::sleep(idle_timeout * 4 / 5).await;
        assert!(
            conn.close_reason().is_some(),
            "not closed after its timeout"
        );

        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// A stale unused event leaves the current connection's idle time alone.
    ///
    /// The event is for a connection the peer no longer has. References to a
    /// replaced connection can outlive it. When the last one drops, its event
    /// names that connection, while the peer's current one is another.
    #[tokio::test]
    async fn stale_unused_event_keeps_the_idle_time() -> TestResult<()> {
        let (ids, routers, address_lookup) = echo_servers(1).await?;
        let id = ids[0];
        let endpoint = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .address_lookup(address_lookup)
            .bind()
            .await?;
        let conn = endpoint.connect(id, ECHO_ALPN).await?;
        let (mut actor, _tx) = Actor::new(endpoint.clone(), ECHO_ALPN, test_options());
        let conn_id = insert_ready(&mut actor, &conn);
        let since = actor.unused.oldest().expect("the connection is unused");

        actor.handle_unused_event(stale_conn_id(id));
        assert_eq!(
            actor.unused.oldest(),
            Some(since),
            "a stale unused event restarted the idle timeout"
        );
        // Taken off the list as if handed out, its own event lists it again.
        actor.unused.remove(conn_id);
        actor.handle_unused_event(conn_id);
        assert!(
            actor.unused.oldest().is_some(),
            "the connection's own event was ignored"
        );

        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    const INCOMING_ALPN: &[u8] = b"iroh-util/pool-incoming-test/0";
    const SHORT_IDLE: Duration = Duration::from_millis(200);

    /// Two connections from one client, both handed to a server-side pool.
    ///
    /// Uses direct addresses only, so it needs no network.
    struct Superseded {
        /// Client end of the connection that was superseded.
        first: Connection,
        /// Client end of the connection that superseded it.
        second: Connection,
        /// The server's references to `first` and `second`.
        first_ref: ConnectionRef,
        second_ref: ConnectionRef,
        pool: ConnectionPool,
        server: iroh::Endpoint,
        _client: iroh::Endpoint,
    }

    impl Superseded {
        async fn new(options: Options) -> TestResult<Self> {
            let server = iroh::Endpoint::builder(presets::Minimal)
                .alpns(vec![INCOMING_ALPN.to_vec()])
                .bind()
                .await?;
            let client = iroh::Endpoint::bind(presets::Minimal).await?;
            let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);

            let (first, first_ref) = Self::connect(&client, &server, &pool).await?;
            let (second, second_ref) = Self::connect(&client, &server, &pool).await?;
            assert_ne!(first.stable_id(), second.stable_id(), "connection reused");
            Ok(Self {
                first,
                second,
                first_ref,
                second_ref,
                pool,
                server,
                _client: client,
            })
        }

        /// Dials the server and hands the incoming side to the pool.
        async fn connect(
            client: &iroh::Endpoint,
            server: &iroh::Endpoint,
            pool: &ConnectionPool,
        ) -> TestResult<(Connection, ConnectionRef)> {
            let (outgoing, incoming) =
                tokio::join!(client.connect(server.addr(), INCOMING_ALPN), async {
                    server.accept().await.expect("endpoint closed").await
                });
            let conn_ref = pool.handle_connection(incoming?).await?;
            Ok((outgoing?, conn_ref))
        }
    }

    /// Asserts that the pool closed a connection with `reason`.
    fn assert_closed_as(err: &iroh::endpoint::ConnectionError, reason: CloseReason) {
        assert!(
            matches!(
                err,
                iroh::endpoint::ConnectionError::ApplicationClosed(frame)
                    if frame.error_code == reason.code() && frame.reason[..] == *reason.reason()
            ),
            "closed for the wrong reason: {err:?}"
        );
    }

    /// Asserts that the pool closed a connection because it was unused.
    fn assert_closed_as_unused(err: &iroh::endpoint::ConnectionError) {
        assert_closed_as(err, CloseReason::Unused);
    }

    fn short_idle_options() -> Options {
        Options {
            idle_timeout: SHORT_IDLE,
            ..Default::default()
        }
    }

    /// Connects `client` to `server` and returns both ends.
    ///
    /// The pool is not told about the connection: a test that hands it over
    /// itself decides when that happens.
    async fn connect_pair(
        client: &iroh::Endpoint,
        server: &iroh::Endpoint,
    ) -> TestResult<(Connection, Connection)> {
        let (outgoing, incoming) =
            tokio::join!(client.connect(server.addr(), INCOMING_ALPN), async {
                server.accept().await.expect("endpoint closed").await
            });
        Ok((outgoing?, incoming?))
    }

    /// Binds a server endpoint that accepts [`INCOMING_ALPN`].
    async fn incoming_server() -> TestResult<iroh::Endpoint> {
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![INCOMING_ALPN.to_vec()])
            .bind()
            .await?;
        Ok(server)
    }

    /// An `on_connected` callback that takes `delay` and counts its calls.
    fn slow_on_connected(delay: Duration) -> (Options, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let options = short_idle_options().with_on_connected({
            let calls = calls.clone();
            move |_ep, _conn: ConnectionRef| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    n0_future::time::sleep(delay).await;
                    Ok(())
                }
            }
        });
        (options, calls)
    }

    /// A full pool does not adopt a connection from a peer it is dialing.
    ///
    /// A dial holds no slot, so the room check has to run when the adoption
    /// finishes, whatever state the peer is in.
    #[tokio::test]
    async fn adopting_for_a_dialing_peer_respects_max_connections() -> TestResult<()> {
        let server = incoming_server().await?;
        let first = iroh::Endpoint::bind(presets::Minimal).await?;
        let second = iroh::Endpoint::bind(presets::Minimal).await?;
        // A socket that absorbs packets: the pool's dial to `second` runs until
        // the connect timeout, so `second` stays in the pool as connecting.
        let (_dead_sock, dead) = dead_addr()?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(EndpointAddr::from_parts(second.id(), [dead]));
        server.address_lookup()?.add(lookup);
        let options = Options {
            max_connections: 1,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(10),
            ..Default::default()
        };
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);

        // A dial started while the pool has room, and that does not finish.
        let dial = tokio::spawn({
            let pool = pool.clone();
            let id = second.id();
            async move { pool.get_or_connect(id).await }
        });
        n0_future::time::sleep(Duration::from_millis(300)).await;
        assert!(!dial.is_finished(), "the dial finished early");

        // The pool fills up: one connection, held in use.
        let (first_conn, _first_ref) = Superseded::connect(&first, &server, &pool).await?;

        // `second` dials us while the pool is dialing it, and is full.
        let (outgoing, incoming) = connect_pair(&second, &server).await?;
        let adopted = pool.handle_connection(incoming).await;
        assert!(
            matches!(adopted, Err(PoolConnectError::TooManyConnections { .. })),
            "adopted into a full pool: {adopted:?}"
        );
        assert!(
            first_conn.close_reason().is_none(),
            "the connection in use was closed"
        );
        drop(outgoing);
        dial.abort();
        server.close().await;
        Ok(())
    }

    /// Handing the pool a connection it is already adopting waits for that.
    ///
    /// Adopting it twice would give one connection two generations and two
    /// reference counts, and closing either would close the other.
    #[tokio::test]
    async fn adopting_one_connection_twice_waits_for_the_first() -> TestResult<()> {
        let (options, calls) = slow_on_connected(Duration::from_millis(300));
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (outgoing, incoming) = connect_pair(&client, &server).await?;

        let (first, second) = tokio::join!(
            pool.handle_connection(incoming.clone()),
            pool.handle_connection(incoming.clone()),
        );
        let (first, second) = (first?, second?);
        assert_eq!(first.stable_id(), second.stable_id(), "not one connection");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "adopted twice");
        assert!(!first.is_superseded(), "superseded by itself");

        // The pool holds one connection, so letting go of one reference does
        // not close what the other still uses.
        drop(first);
        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(
            outgoing.close_reason().is_none(),
            "closed a connection that is in use: {:?}",
            outgoing.close_reason()
        );
        drop(second);
        server.close().await;
        Ok(())
    }

    /// Handing the pool a connection it superseded returns a reference to it.
    ///
    /// The peer may still be using it, which is why the pool kept it, so this
    /// is not a reason to make it current again.
    #[tokio::test]
    async fn adopting_a_superseded_connection_returns_it() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let superseded = (*s.first_ref).clone();

        let again = s.pool.handle_connection(superseded).await?;
        assert_eq!(again.stable_id(), s.first_ref.stable_id());
        assert!(
            again.is_superseded(),
            "made a superseded connection current"
        );
        let current = s.pool.get_or_connect(s.first_ref.remote_id()).await?;
        assert_eq!(
            current.stable_id(),
            s.second_ref.stable_id(),
            "the current connection changed"
        );
        s.server.close().await;
        Ok(())
    }

    /// A connection that closes during its adoption does not become current.
    ///
    /// It would supersede the working connection with a dead one, and the next
    /// request would dial although the working one is still open.
    #[tokio::test]
    async fn a_connection_that_closes_while_adopted_is_not_current() -> TestResult<()> {
        let (options, _calls) = slow_on_connected(Duration::from_millis(300));
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_first, first_ref) = Superseded::connect(&client, &server, &pool).await?;

        let (outgoing, incoming) = connect_pair(&client, &server).await?;
        let adopting = tokio::spawn({
            let pool = pool.clone();
            let incoming = incoming.clone();
            async move { pool.handle_connection(incoming).await }
        });
        n0_future::time::sleep(Duration::from_millis(50)).await;
        outgoing.close(0u32.into(), b"gone");

        let adopted = adopting.await?;
        assert!(
            matches!(adopted, Err(PoolConnectError::Closed { .. })),
            "adopted a closed connection: {adopted:?}"
        );
        assert!(
            !first_ref.is_superseded(),
            "superseded by a closed connection"
        );
        let current = pool.get_or_connect(client.id()).await?;
        assert_eq!(current.stable_id(), first_ref.stable_id());

        // Once it has closed, it is refused right away.
        let again = pool.handle_connection(incoming).await;
        assert!(
            matches!(again, Err(PoolConnectError::Closed { .. })),
            "adopted a closed connection: {again:?}"
        );
        server.close().await;
        Ok(())
    }

    /// An adoption that finishes after a newer one does not replace it.
    ///
    /// The connection the peer opened last is current, whichever adoption's
    /// `on_connected` finished first.
    #[tokio::test]
    async fn an_older_adoption_that_finishes_last_is_superseded() -> TestResult<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let options = short_idle_options().with_on_connected({
            let calls = calls.clone();
            move |_ep, _conn: ConnectionRef| {
                // The first adoption is slow, the ones after it are not.
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if first {
                        n0_future::time::sleep(Duration::from_millis(500)).await;
                    }
                    Ok(())
                }
            }
        });
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_older, older_incoming) = connect_pair(&client, &server).await?;
        let (_newer, newer_incoming) = connect_pair(&client, &server).await?;

        let older = tokio::spawn({
            let pool = pool.clone();
            async move { pool.handle_connection(older_incoming).await }
        });
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let newer = pool.handle_connection(newer_incoming).await?;
        let older = older.await??;

        assert!(older.is_superseded(), "the older connection is current");
        assert!(!newer.is_superseded(), "an older connection superseded it");
        let current = pool.get_or_connect(client.id()).await?;
        assert_eq!(current.stable_id(), newer.stable_id());
        server.close().await;
        Ok(())
    }

    /// A request waits for an adoption that is running.
    ///
    /// The adopted connection becomes the current one, so dialing as well would
    /// make a connection that it supersedes right away.
    #[tokio::test]
    async fn a_request_waits_for_a_running_adoption() -> TestResult<()> {
        let (options, calls) = slow_on_connected(Duration::from_millis(300));
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_outgoing, incoming) = connect_pair(&client, &server).await?;
        let adopting = tokio::spawn({
            let pool = pool.clone();
            async move { pool.handle_connection(incoming).await }
        });
        n0_future::time::sleep(Duration::from_millis(50)).await;

        // The pool has no address for the client, so a dial would fail.
        let requested = pool.get_or_connect(client.id()).await?;
        let adopted = adopting.await??;
        assert_eq!(requested.stable_id(), adopted.stable_id());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the request dialed as well"
        );
        server.close().await;
        Ok(())
    }

    /// A request that joined a failed adoption gets the current connection.
    ///
    /// It wants any connection to the peer, so the adoption's error is not its
    /// own.
    #[tokio::test]
    async fn a_request_outlives_a_failed_adoption() -> TestResult<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let options = short_idle_options().with_on_connected({
            let calls = calls.clone();
            move |_ep, _conn: ConnectionRef| {
                // The first adoption succeeds, and the second fails later.
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if first {
                        n0_future::time::sleep(Duration::from_millis(300)).await;
                        Ok(())
                    } else {
                        n0_future::time::sleep(Duration::from_millis(600)).await;
                        Err(io::Error::other("rejected"))
                    }
                }
            }
        });
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_first, first_incoming) = connect_pair(&client, &server).await?;
        let (_second, second_incoming) = connect_pair(&client, &server).await?;

        let first = tokio::spawn({
            let pool = pool.clone();
            async move { pool.handle_connection(first_incoming).await }
        });
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let second = tokio::spawn({
            let pool = pool.clone();
            async move { pool.handle_connection(second_incoming).await }
        });
        n0_future::time::sleep(Duration::from_millis(50)).await;

        // Nothing is ready yet, so the request joins the newest adoption.
        let requested = pool.get_or_connect(client.id()).await?;
        let first = first.await??;
        assert_eq!(requested.stable_id(), first.stable_id());
        let second = second.await?;
        assert!(
            matches!(second, Err(PoolConnectError::OnConnectError { .. })),
            "the failed adoption succeeded: {second:?}"
        );
        server.close().await;
        Ok(())
    }

    /// Closing a peer closes the connection of an attempt that is running.
    ///
    /// The attempt itself runs to its end, so waiting for that would leave the
    /// connection open for as long as `on_connected` takes.
    #[tokio::test]
    async fn close_closes_the_connection_of_a_running_attempt() -> TestResult<()> {
        let (options, _calls) = slow_on_connected(Duration::from_secs(10));
        let options = Options {
            connect_timeout: Duration::from_secs(30),
            ..options
        };
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (outgoing, incoming) = connect_pair(&client, &server).await?;
        let adopting = tokio::spawn({
            let pool = pool.clone();
            async move { pool.handle_connection(incoming).await }
        });
        n0_future::time::sleep(Duration::from_millis(100)).await;

        pool.close(client.id()).await?;
        let err = tokio::time::timeout(Duration::from_secs(5), outgoing.closed()).await?;
        assert_closed_as(&err, CloseReason::Closed);
        let adopted = adopting.await?;
        assert!(
            matches!(adopted, Err(PoolConnectError::Closed { .. })),
            "the caller was not told: {adopted:?}"
        );
        server.close().await;
        Ok(())
    }

    /// The pool keeps at most `max_superseded_per_peer` superseded connections.
    ///
    /// A peer that opens connections in a loop would otherwise make the pool
    /// hold one per attempt, each until it goes unused.
    #[tokio::test]
    async fn superseded_connections_are_capped() -> TestResult<()> {
        let options = Options {
            max_superseded_per_peer: 1,
            ..short_idle_options()
        };
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);

        let mut connections = Vec::new();
        let mut refs = Vec::new();
        for _ in 0..3 {
            let (outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
            connections.push(outgoing);
            // Hold each one, so nothing closes for being unused.
            refs.push(conn_ref);
        }

        let err = tokio::time::timeout(SHORT_IDLE * 5, connections[0].closed()).await?;
        assert_closed_as(&err, CloseReason::Superseded);
        assert!(
            connections[1].close_reason().is_none(),
            "closed the connection within the cap"
        );
        assert!(
            connections[2].close_reason().is_none(),
            "closed the current connection"
        );
        server.close().await;
        Ok(())
    }

    /// A full pool adopts a connection that does not make it hold more.
    ///
    /// When the peer's superseded connections are at the cap, the adoption
    /// supersedes the current one, and the cap closes the oldest.
    #[tokio::test]
    async fn adopting_at_the_superseded_cap_fits_a_full_pool() -> TestResult<()> {
        let options = Options {
            max_connections: 2,
            max_superseded_per_peer: 1,
            ..short_idle_options()
        };
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        // Two connections in use, one current and one superseded: the pool is
        // full, and the peer is at its cap.
        let (oldest, _oldest_ref) = Superseded::connect(&client, &server, &pool).await?;
        let (middle, _middle_ref) = Superseded::connect(&client, &server, &pool).await?;

        let (_newest, newest_incoming) = connect_pair(&client, &server).await?;
        let newest = pool.handle_connection(newest_incoming).await?;
        assert!(!newest.is_superseded(), "the adoption is not current");
        tokio::time::timeout(SHORT_IDLE * 5, oldest.closed()).await?;
        assert!(
            middle.close_reason().is_none(),
            "closed the connection within the cap"
        );
        server.close().await;
        Ok(())
    }

    /// A dial that connects after its peer was closed is closed when it ends.
    ///
    /// The pool has nothing to close while the dial is still connecting. The
    /// dial's result is stale when it arrives, and the connection goes with it.
    /// `on_connected` does not run for it, so it cannot hold the connection
    /// open.
    #[tokio::test]
    async fn a_dial_that_connects_after_close_is_closed_when_it_ends() -> TestResult<()> {
        let (options, calls) = slow_on_connected(Duration::from_secs(10));
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(server.addr());
        client.address_lookup()?.add(lookup);
        let pool = ConnectionPool::new(client.clone(), INCOMING_ALPN, options);

        let dial = tokio::spawn({
            let pool = pool.clone();
            let id = server.id();
            async move { pool.get_or_connect(id).await }
        });
        // The handshake does not finish until the server drives it, so the dial
        // is connecting until `incoming` is awaited.
        let incoming = server.accept().await.expect("endpoint closed");
        pool.close(server.id()).await?;
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let connection = incoming.await?;

        assert!(
            matches!(dial.await?, Err(PoolConnectError::Closed { .. })),
            "the dial did not report the close"
        );
        let err = tokio::time::timeout(SHORT_IDLE * 5, connection.closed()).await?;
        assert_closed_as(&err, CloseReason::Closed);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "ran `on_connected` for a closed peer"
        );
        server.close().await;
        Ok(())
    }

    /// An incoming connection becomes the one `get_or_connect` returns.
    #[tokio::test]
    async fn handle_connection_becomes_current() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let client_id = s.second_ref.remote_id();

        let conn = s.pool.get_or_connect(client_id).await?;
        assert_eq!(conn.stable_id(), s.second_ref.stable_id());
        assert!(!conn.is_superseded());
        assert!(s.first_ref.is_superseded());
        s.server.close().await;
        Ok(())
    }

    /// A superseded connection is closed once nothing uses it.
    #[tokio::test]
    async fn superseded_connection_is_closed_once_unused() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        drop(s.first_ref);

        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as_unused(&err);
        assert!(
            s.second.close_reason().is_none(),
            "the new connection was closed"
        );
        Ok(())
    }

    /// A superseded connection stays open for as long as something uses it.
    ///
    /// Two endpoints that dial each other at once each keep the connection they
    /// saw last. They may disagree, so the remote can still be using the one we
    /// superseded.
    #[tokio::test]
    async fn superseded_connection_stays_open_while_used() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;

        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(
            s.first.close_reason().is_none(),
            "a superseded connection was closed while in use"
        );
        drop(s.first_ref);
        Ok(())
    }

    /// A reference taken in `on_connected` keeps a superseded connection open.
    ///
    /// This is how streams the remote opened stay served: whatever accepts them
    /// keeps a [`ConnectionRef`] for as long as it uses the connection.
    #[tokio::test]
    async fn on_connected_ref_keeps_superseded_connection_open() -> TestResult<()> {
        let refs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let options = short_idle_options().with_on_connected({
            let refs = refs.clone();
            move |_ep, conn: ConnectionRef| {
                refs.lock().expect("poisoned").push(conn.clone());
                async { Ok(()) }
            }
        });
        let s = Superseded::new(options).await?;
        drop(s.first_ref);
        drop(s.second_ref);

        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(
            s.first.close_reason().is_none(),
            "closed although `on_connected` holds a reference"
        );

        refs.lock().expect("poisoned").clear();
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as_unused(&err);
        Ok(())
    }

    /// An upgraded [`WeakConnectionRef`] keeps an unused connection open.
    ///
    /// Upgrading does not go through the pool, so the connection is still on
    /// the unused list when its idle timeout passes.
    #[tokio::test]
    async fn upgrade_after_unused_keeps_connection_open() -> TestResult<()> {
        let handle = Arc::new(std::sync::Mutex::new(None));
        let options = short_idle_options().with_on_connected({
            let handle = handle.clone();
            move |_ep, conn: ConnectionRef| {
                *handle.lock().expect("poisoned") = Some(conn.downgrade());
                async { Ok(()) }
            }
        });
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![INCOMING_ALPN.to_vec()])
            .bind()
            .await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
        drop(conn_ref);
        // Let the actor see the connection go unused and start its
        // timer before the reference comes in.
        n0_future::time::sleep(SHORT_IDLE / 4).await;
        let handle = handle.lock().expect("poisoned").take().expect("no handle");
        let stream_ref = handle.upgrade().expect("open");

        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(
            outgoing.close_reason().is_none(),
            "closed while a reference is alive: {:?}",
            outgoing.close_reason()
        );
        drop(stream_ref);
        server.close().await;
        Ok(())
    }

    /// [`ConnectionPool::close`] closes superseded connections, even in use.
    #[tokio::test]
    async fn close_closes_superseded_connections() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        s.pool.close(s.second_ref.remote_id()).await?;

        for (name, conn) in [("superseded", &s.first), ("current", &s.second)] {
            let err = tokio::time::timeout(SHORT_IDLE * 2, conn.closed())
                .await
                .map_err(|_| format!("the {name} connection was not closed"))?;
            assert_closed_as(&err, CloseReason::Dropped);
        }
        Ok(())
    }

    /// An incoming connection serves the waiters of a running dial.
    ///
    /// It arrives while a dial to the same endpoint runs, and stays open when
    /// the dial fails. This is the common case of an endpoint that can dial us but that we
    /// cannot dial.
    #[tokio::test]
    async fn incoming_connection_serves_a_running_dial() -> TestResult<()> {
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![INCOMING_ALPN.to_vec()])
            .bind()
            .await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        // TEST-NET-1: nothing answers, so dialing the client runs until the
        // timeout.
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(EndpointAddr::from_parts(
            client.id(),
            [TransportAddr::Ip("192.0.2.1:1".parse()?)],
        ));
        server.address_lookup()?.add(lookup);
        let options = Options {
            connect_timeout: Duration::from_millis(500),
            ..short_idle_options()
        };
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let dial = tokio::spawn({
            let pool = pool.clone();
            let id = client.id();
            async move { pool.get_or_connect(id).await }
        });
        n0_future::time::sleep(Duration::from_millis(100)).await;

        let (outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
        let dialed = dial.await??;
        assert_eq!(dialed.stable_id(), conn_ref.stable_id());
        // Past the dial's timeout.
        n0_future::time::sleep(Duration::from_millis(600)).await;
        assert!(
            outgoing.close_reason().is_none(),
            "the adopted connection was closed: {:?}",
            outgoing.close_reason()
        );
        let current = pool.get_or_connect(client.id()).await?;
        assert_eq!(current.stable_id(), conn_ref.stable_id());
        server.close().await;
        Ok(())
    }

    /// A superseded connection closing leaves the current one alone.
    #[tokio::test]
    async fn superseded_connection_closing_keeps_the_current_one() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        s.first.close(0u32.into(), b"bye");
        n0_future::time::sleep(SHORT_IDLE).await;

        assert!(
            s.second.close_reason().is_none(),
            "the current connection was closed: {:?}",
            s.second.close_reason()
        );
        let current = s.pool.get_or_connect(s.second_ref.remote_id()).await?;
        assert_eq!(current.stable_id(), s.second_ref.stable_id());
        Ok(())
    }

    /// A slow `on_connected` for an incoming connection does not hold up requests.
    ///
    /// The current connection serves them until the callback has finished.
    #[tokio::test]
    async fn slow_on_connected_for_incoming_keeps_serving() -> TestResult<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let options = short_idle_options().with_on_connected({
            let calls = calls.clone();
            move |_ep, _conn: ConnectionRef| {
                let first = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                async move {
                    if !first {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                }
            }
        });
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![INCOMING_ALPN.to_vec()])
            .bind()
            .await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_first, first_ref) = Superseded::connect(&client, &server, &pool).await?;
        let second = tokio::spawn({
            let client = client.clone();
            let server = server.clone();
            let pool = pool.clone();
            async move {
                Superseded::connect(&client, &server, &pool)
                    .await
                    .map(|_| ())
            }
        });
        n0_future::time::sleep(Duration::from_millis(100)).await;

        let current =
            tokio::time::timeout(Duration::from_secs(1), pool.get_or_connect(client.id()))
                .await
                .map_err(|_| "get_or_connect waited for on_connected")??;
        assert_eq!(current.stable_id(), first_ref.stable_id());
        second.abort();
        server.close().await;
        Ok(())
    }

    /// A full pool does not evict a connection that is in use again.
    ///
    /// An upgraded [`WeakConnectionRef`] put it back in use. Such a reference
    /// does not go through the pool, so the connection is still on the list of
    /// unused connections that eviction picks from.
    #[tokio::test]
    async fn eviction_skips_a_peer_in_use_again() -> TestResult<()> {
        let handle = Arc::new(std::sync::Mutex::new(None));
        let options = Options {
            max_connections: 1,
            idle_timeout: Duration::from_secs(10),
            ..Default::default()
        }
        .with_on_connected({
            let handle = handle.clone();
            move |_ep, conn: ConnectionRef| {
                *handle.lock().expect("poisoned") = Some(conn.downgrade());
                async { Ok(()) }
            }
        });
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![INCOMING_ALPN.to_vec()])
            .bind()
            .await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
        drop(conn_ref);
        // Let the pool see the peer go unused before the reference comes in.
        n0_future::time::sleep(Duration::from_millis(50)).await;
        let handle = handle.lock().expect("poisoned").take().expect("no handle");
        let stream_ref = handle.upgrade().expect("open");

        // The pool has no address for `other`, so the attempt fails. What
        // matters is what the pool did not do to make room for it.
        let other = SecretKey::from_bytes(&[9u8; 32]).public();
        let res = pool.get_or_connect(other).await;
        assert!(res.is_err(), "connected to a peer with no address: {res:?}");
        assert!(
            outgoing.close_reason().is_none(),
            "the connection in use was closed: {:?}",
            outgoing.close_reason()
        );
        drop(stream_ref);
        server.close().await;
        Ok(())
    }

    /// A downgraded reference does not keep the connection open.
    #[tokio::test]
    async fn downgraded_ref_does_not_keep_connection_open() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let weak = s.first_ref.downgrade();
        drop(s.first_ref);

        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as_unused(&err);
        drop(weak);
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
            "closed although a clone of its reference is alive"
        );
        drop(clone);
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as_unused(&err);
        Ok(())
    }

    /// An upgrade and the pool retiring an unused connection cannot both succeed.
    ///
    /// Whichever comes first wins: an upgrade keeps the pool from retiring the
    /// connection, and retiring it makes later upgrades fail.
    #[test]
    fn upgrade_and_unused_close_exclude_each_other() {
        let id = SecretKey::from_bytes(&[4u8; 32]).public();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let counter = ConnectionCounter::new(stale_conn_id(id), tx);

        let permit = counter.inner.try_get_one().expect("open connection");
        assert!(!counter.try_retire_unused(), "retired a connection in use");
        drop(permit);
        assert!(counter.try_retire_unused());
        assert!(
            counter.inner.try_get_one().is_none(),
            "upgraded a retired connection"
        );
        assert!(!counter.try_retire_unused(), "retired twice");
    }

    /// Requests whose callers gave up do not pile up on a running attempt.
    #[tokio::test]
    async fn cancelled_requests_do_not_pile_up() -> TestResult<()> {
        let endpoint = iroh::Endpoint::bind(presets::Minimal).await?;
        let (mut actor, _tx) = Actor::new(endpoint.clone(), ECHO_ALPN, test_options());
        let peer = SecretKey::from_bytes(&[6u8; 32]).public();
        for _ in 0..10 {
            let (tx, rx) = oneshot::channel();
            drop(rx);
            actor.handle_request(RequestRef { id: peer, tx });
        }
        let attempt = actor
            .peers
            .get_mut(&peer)
            .and_then(Peer::pending_mut)
            .expect("no attempt");
        assert_eq!(
            attempt.requests.len(),
            1,
            "kept requests whose callers are gone"
        );
        endpoint.close().await;
        Ok(())
    }

    /// The last reference to drop reports which connection went unused.
    ///
    /// Every stale event the actor ignores rests on that: an event names one
    /// connection, and arrives only when nothing uses it any more.
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
        assert_eq!(
            rx.try_recv().ok(),
            Some(conn_id),
            "the last reference did not report the connection unused"
        );
    }

    /// Upgrading fails once the pool has closed the connection.
    #[tokio::test]
    async fn upgrade_after_close_fails() -> TestResult<()> {
        let handle = Arc::new(std::sync::Mutex::new(None));
        let options = short_idle_options().with_on_connected({
            let handle = handle.clone();
            move |_ep, conn: ConnectionRef| {
                *handle.lock().expect("poisoned") = Some(conn.downgrade());
                async { Ok(()) }
            }
        });
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![INCOMING_ALPN.to_vec()])
            .bind()
            .await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
        let weak = handle.lock().expect("poisoned").take().expect("no handle");
        assert!(weak.upgrade().is_some());

        pool.close(conn_ref.remote_id()).await?;
        // `close` returns once the request is queued. Wait for the pool to act.
        tokio::time::timeout(SHORT_IDLE * 5, conn_ref.closed()).await?;
        assert!(weak.upgrade().is_none(), "upgraded a closed connection");
        server.close().await;
        Ok(())
    }

    /// Upgrading fails once the pool has seen the remote close the connection.
    #[tokio::test]
    async fn upgrade_after_a_remote_close_fails() -> TestResult<()> {
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, short_idle_options());
        let (outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
        let weak = conn_ref.downgrade();
        drop(conn_ref);

        outgoing.close(0u32.into(), b"gone");
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                n0_future::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "upgrades kept succeeding after the remote closed")?;
        server.close().await;
        Ok(())
    }

    /// Dropping the last pool handle closes every connection it holds.
    ///
    /// A connection in use closes as `drop`, and its weak references no longer
    /// upgrade.
    #[tokio::test]
    async fn dropping_the_pool_closes_its_connections() -> TestResult<()> {
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, short_idle_options());
        let (outgoing, conn_ref) = Superseded::connect(&client, &server, &pool).await?;
        let weak = conn_ref.downgrade();

        drop(pool);
        let err = tokio::time::timeout(SHORT_IDLE * 5, outgoing.closed()).await?;
        assert_closed_as(&err, CloseReason::Dropped);
        assert!(
            weak.upgrade().is_none(),
            "upgraded after the pool shut down"
        );
        drop(conn_ref);
        server.close().await;
        Ok(())
    }

    /// An adoption that `on_connected` rejects leaves the current connection.
    ///
    /// The rejected connection is closed as `rejected`, and a weak reference
    /// the callback kept does not upgrade.
    #[tokio::test]
    async fn a_rejected_adoption_keeps_the_current_connection() -> TestResult<()> {
        let kept = Arc::new(std::sync::Mutex::new(Vec::new()));
        let options = short_idle_options().with_on_connected({
            let kept = kept.clone();
            move |_ep, conn: ConnectionRef| {
                let mut kept = kept.lock().expect("poisoned");
                kept.push(conn.downgrade());
                // Accept the first connection, reject the ones after it.
                let accept = kept.len() == 1;
                async move {
                    if accept {
                        Ok(())
                    } else {
                        Err(io::Error::other("rejected"))
                    }
                }
            }
        });
        let server = incoming_server().await?;
        let client = iroh::Endpoint::bind(presets::Minimal).await?;
        let pool = ConnectionPool::new(server.clone(), INCOMING_ALPN, options);
        let (_first, first_ref) = Superseded::connect(&client, &server, &pool).await?;

        let (outgoing, incoming) = connect_pair(&client, &server).await?;
        let adopted = pool.handle_connection(incoming).await;
        assert!(
            matches!(adopted, Err(PoolConnectError::OnConnectError { .. })),
            "adopted a rejected connection: {adopted:?}"
        );
        let err = tokio::time::timeout(SHORT_IDLE * 5, outgoing.closed()).await?;
        assert_closed_as(&err, CloseReason::Rejected);
        let rejected = kept.lock().expect("poisoned").pop().expect("not called");
        assert!(
            rejected.upgrade().is_none(),
            "upgraded a rejected connection"
        );

        assert!(
            !first_ref.is_superseded(),
            "superseded by a rejected connection"
        );
        let current = pool.get_or_connect(client.id()).await?;
        assert_eq!(current.stable_id(), first_ref.stable_id());
        server.close().await;
        Ok(())
    }
}
