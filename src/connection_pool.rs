//! A simple iroh connection pool
//!
//! Entry point is [`ConnectionPool`]. You create a connection pool for a specific
//! ALPN and [`Options`]. Then the pool will manage connections for you.
//!
//! Access to connections is via the [`ConnectionPool::get_or_connect`] method, which
//! gives you access to a connection via a [`ConnectionRef`] if possible.
//! Connections the remote opened can be handed to the pool with
//! [`ConnectionPool::handle_connection`], so a protocol that both dials and
//! accepts uses one connection per endpoint for both.
//!
//! It is important that you keep the [`ConnectionRef`] alive while you are using
//! the connection.
//!
//! This is using a single actor to manage all connections.

use std::{
    collections::{BTreeSet, HashMap},
    io,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use iroh::{
    Endpoint, EndpointId,
    endpoint::{ConnectError, Connection},
};
use n0_error::{e, stack_error};
use n0_future::{
    FuturesUnordered, MaybeFuture, StreamExt,
    future::Boxed,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace};

pub type OnConnected = Arc<
    dyn Fn(&Endpoint, &ConnectionHandle) -> n0_future::future::Boxed<io::Result<()>> + Send + Sync,
>;

/// The pool is a single actor, so we can afford a larger inbox.
const INBOX_CAPACITY: usize = 1024;

/// Configuration options for the connection pool
#[derive(derive_more::Debug, Clone)]
pub struct Options {
    /// How long to keep unused connections around.
    ///
    /// Idle here means that there are no [`ConnectionRef`]s alive for the connection,
    /// not that the connection itself is idle.
    pub idle_timeout: Duration,
    /// Timeout for connect. This includes the time spent in on_connect, if set.
    pub connect_timeout: Duration,
    /// Maximum number of connections to hand out.
    ///
    /// Attempts to connect that are still running do not count, so attempts to
    /// peers that do not answer cannot take the place of connections.
    pub max_connections: usize,
    /// An optional callback that can be used to wait for the connection to enter some state.
    /// An example usage could be to wait for the connection to become direct before handing
    /// it out to the user.
    ///
    /// It runs on the pool's task, so it must not block the thread: that would
    /// stall the whole pool.
    ///
    /// It also runs for connections handed to [`ConnectionPool::handle_connection`],
    /// within [`Options::connect_timeout`] as well.
    #[debug(skip)]
    pub on_connected: Option<OnConnected>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(1),
            max_connections: 1024,
            on_connected: None,
        }
    }
}

impl Options {
    /// Set the on_connected callback
    pub fn with_on_connected<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Endpoint, ConnectionHandle) -> Fut + Send + Sync + 'static,
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
#[derive(Debug)]
pub struct ConnectionRef {
    connection: iroh::endpoint::Connection,
    permit: OneConnection,
}

impl Deref for ConnectionRef {
    type Target = iroh::endpoint::Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl ConnectionRef {
    fn new(connection: iroh::endpoint::Connection, permit: OneConnection) -> Self {
        Self { connection, permit }
    }

    /// Returns whether a newer connection to the same endpoint superseded this one.
    ///
    /// A superseded connection stays open for as long as it is used, but new
    /// work should move to the current one, which [`ConnectionPool::get_or_connect`]
    /// returns: the old one may lead to an endpoint that has since restarted,
    /// dead without us having noticed yet.
    pub fn is_superseded(&self) -> bool {
        self.permit.inner.superseded.load(Ordering::SeqCst)
    }
}

/// A connection as handed to [`Options::on_connected`].
///
/// Unlike a [`ConnectionRef`], holding one does not keep the connection in use,
/// so a task that watches the connection for as long as it lives can hold it.
/// Work that should keep the connection open takes a [`ConnectionRef`] from
/// [`Self::get_ref`] instead. That includes serving streams the remote opened:
/// the remote may keep using a connection the pool has superseded.
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    connection: Connection,
    counter: ConnectionCounter,
}

impl Deref for ConnectionHandle {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl ConnectionHandle {
    fn new(connection: &Connection, counter: &ConnectionCounter) -> Self {
        Self {
            connection: connection.clone(),
            counter: counter.clone(),
        }
    }

    /// Returns a reference that keeps the connection in use while it is alive.
    pub fn get_ref(&self) -> ConnectionRef {
        ConnectionRef::new(self.connection.clone(), self.counter.get_one())
    }
}

/// Error when a connection can not be acquired
///
/// This includes the normal iroh connection errors as well as pool specific
/// errors such as timeouts and connection limits.
#[stack_error(derive, add_meta)]
#[derive(Clone)]
pub enum PoolConnectError {
    /// Connection pool is shut down
    #[error("Connection pool is shut down")]
    Shutdown {},
    /// Connection attempt was cancelled by [`ConnectionPool::close`]
    #[error("Connection was closed")]
    Closed {},
    /// Timeout during connect
    #[error("Timeout during connect")]
    Timeout {},
    /// Too many connections
    #[error("Too many connections")]
    TooManyConnections {},
    /// Error during connect
    #[error(transparent)]
    ConnectError { source: Arc<ConnectError> },
    /// Error during on_connect callback
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
    /// The connection pool has been shut down
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

/// State for a peer in the connection pool
enum PeerState {
    /// We are currently connecting to this peer.
    Connecting {
        generation: u64,
        /// Waiters that need to be notified when the connection is established or fails.
        waiters: Vec<RefSender>,
    },
    /// We have a connection to the peer.
    Ready {
        /// The generation of the attempt that made the connection.
        generation: u64,
        connection: Connection,
        counter: ConnectionCounter,
        unused_since: Option<Instant>,
    },
}

/// A connection that a newer one to the same peer took the place of.
///
/// The peer may still be using it, so it stays open until it is unused for
/// [`Options::idle_timeout`], as a current connection would.
struct Superseded {
    connection: Connection,
    counter: ConnectionCounter,
    unused_since: Option<Instant>,
}

/// The result of a connect attempt: a dial, or adopting an incoming connection.
type ConnectResult = (
    EndpointId,
    u64,
    Result<(Connection, ConnectionCounter), PoolConnectError>,
);

struct Actor {
    /// Inbox
    rx: mpsc::Receiver<ActorMessage>,
    /// Separate inbox for unused events, gets processed before the main inbox.
    ///
    /// Each event names the peer and the generation of the connection.
    unused_rx: mpsc::UnboundedReceiver<(EndpointId, u64)>,
    /// Sender for the unused inbox to be cloned into the connection counter.
    ///
    /// This is unbounded so it can be used in Drop. It holds at most one event per
    /// connection going unused, and the actor drains it before anything else.
    unused_tx: mpsc::UnboundedSender<(EndpointId, u64)>,
    options: Options,
    endpoint: Endpoint,
    alpn: Arc<[u8]>,
    peers: HashMap<EndpointId, PeerState>,
    /// Futures for currently connecting peers.
    connecting: FuturesUnordered<Boxed<ConnectResult>>,
    /// Incoming connections whose `on_connected` is running, by generation.
    ///
    /// The peer's current connection keeps serving requests until the incoming
    /// one is adopted.
    adopting: HashMap<u64, (EndpointId, RefSender)>,
    /// Superseded connections that are still open, by peer and generation.
    superseded: HashMap<(EndpointId, u64), Superseded>,
    /// Generation counter used to distinguish between connection attempts to the same peer.
    next_generation: u64,
    /// Futures for connection close watchers.
    ///
    /// Each yields the peer and the generation of the connection it watches.
    conn_close: FuturesUnordered<Boxed<(EndpointId, u64)>>,
    /// Currently unused connections, in order of when they became unused.
    ///
    /// Keyed by each peer's `unused_since`, so a peer's entry can be found and
    /// removed without a scan. The first entry is the next connection to close
    /// for being unused.
    unused: BTreeSet<(Instant, EndpointId)>,
    /// Unused superseded connections, keyed like `unused` plus the generation.
    ///
    /// Kept apart because they do not count against `max_connections`, so
    /// eviction does not pick them.
    superseded_unused: BTreeSet<(Instant, EndpointId, u64)>,
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
                connecting: FuturesUnordered::new(),
                adopting: HashMap::new(),
                superseded: HashMap::new(),
                next_generation: 0,
                conn_close: FuturesUnordered::new(),
                unused: BTreeSet::new(),
                superseded_unused: BTreeSet::new(),
            },
            tx,
        )
    }

    async fn run(mut self) {
        // We bias processing internal events before accepting more work from
        // the external mailbox.
        loop {
            let first_unused = self.unused.first().map(|(since, _)| *since);
            let first_superseded = self.superseded_unused.first().map(|(since, ..)| *since);
            let next_unused_timer_at = match first_unused.into_iter().chain(first_superseded).min()
            {
                None => MaybeFuture::None,
                Some(unused_since) => {
                    let deadline = unused_since + self.options.idle_timeout;
                    MaybeFuture::Some(n0_future::time::sleep_until(deadline))
                }
            };
            tokio::select! {
                biased;

                // Handle unused events first, since this might give us some room.
                Some((id, generation)) = self.unused_rx.recv() => {
                    self.handle_unused_event(id, generation);
                }

                Some((id, generation, result)) = self.connecting.next(), if !self.connecting.is_empty() => {
                    self.handle_connect_result(id, generation, result);
                }

                Some((id, generation)) = self.conn_close.next(), if !self.conn_close.is_empty() => {
                    self.handle_conn_closed(id, generation);
                }

                _ = next_unused_timer_at => self.close_unused(),

                msg = self.rx.recv() => {
                    let Some(msg) = msg else { break };
                    self.handle_msg(msg);
                }
            }
        }

        // Notify waiters during shutdown and close connections.
        for (_, state) in self.peers.drain() {
            match state {
                PeerState::Connecting { waiters, .. } => {
                    for tx in waiters {
                        let _ = tx.send(Err(e!(PoolConnectError::Shutdown)));
                    }
                }
                PeerState::Ready {
                    connection,
                    counter,
                    ..
                } => close(&connection, &counter),
            }
        }
        for (_, (_, tx)) in self.adopting.drain() {
            let _ = tx.send(Err(e!(PoolConnectError::Shutdown)));
        }
        for (_, superseded) in self.superseded.drain() {
            close(&superseded.connection, &superseded.counter);
        }
    }

    fn handle_msg(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::RequestRef(req) => self.handle_request(req),
            ActorMessage::HandleConnection { conn, tx } => self.handle_incoming(conn, tx),
            ActorMessage::ConnectionShutdown { id } => {
                trace!(%id, "shutdown requested");
                self.close_peer(id);
            }
        }
    }

    fn handle_request(&mut self, req: RequestRef) {
        let id = req.id;
        // A connection that closed a moment ago is still the peer's current one
        // until its close event reaches the actor. Handing it out would give the
        // caller a connection that fails on first use.
        if let Some(PeerState::Ready { connection, .. }) = self.peers.get(&id)
            && connection.close_reason().is_some()
        {
            debug!(%id, "current connection has closed, dialing again");
            self.remove_peer(id);
        }
        if let Some(state) = self.peers.get_mut(&id) {
            match state {
                PeerState::Connecting { waiters, .. } => {
                    waiters.push(req.tx);
                    return;
                }
                PeerState::Ready {
                    connection,
                    counter,
                    unused_since,
                    ..
                } => {
                    if let Some(since) = unused_since.take() {
                        self.unused.remove(&(since, id));
                    }
                    let one = counter.get_one();
                    debug!(%id, count = counter.current(), "Handing out ConnectionRef");
                    let _ = req.tx.send(Ok(ConnectionRef::new(connection.clone(), one)));
                    return;
                }
            }
        }

        if !self.make_room() {
            let _ = req.tx.send(Err(e!(PoolConnectError::TooManyConnections)));
            return;
        }

        let generation = self.next_generation;
        self.next_generation += 1;
        self.peers.insert(
            id,
            PeerState::Connecting {
                generation,
                waiters: vec![req.tx],
            },
        );
        self.connecting
            .push(self.make_connect_future(id, generation, None));
    }

    /// Starts adopting a connection the peer opened.
    ///
    /// Its `on_connected` runs like a dial's, and the connection becomes the
    /// peer's current one once it has finished. See
    /// [`ConnectionPool::handle_connection`].
    fn handle_incoming(&mut self, conn: Connection, tx: RefSender) {
        let id = conn.remote_id();
        let current = matches!(
            self.peers.get(&id),
            Some(PeerState::Ready { connection, .. }) if connection.stable_id() == conn.stable_id()
        );
        if current {
            return self.handle_request(RequestRef { id, tx });
        }
        if !self.peers.contains_key(&id) && !self.make_room() {
            let _ = tx.send(Err(e!(PoolConnectError::TooManyConnections)));
            return;
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        self.adopting.insert(generation, (id, tx));
        self.connecting
            .push(self.make_connect_future(id, generation, Some(conn)));
    }

    /// Makes room for one more connection if the pool holds `max_connections`.
    ///
    /// Evicts the connection that has been unused the longest, and returns
    /// `false` if there is none.
    fn make_room(&mut self) -> bool {
        let connections = self
            .peers
            .values()
            .filter(|state| matches!(state, PeerState::Ready { .. }))
            .count();
        if connections < self.options.max_connections {
            return true;
        }
        while let Some((since, id)) = self.unused.pop_first() {
            let Some(PeerState::Ready {
                counter,
                unused_since,
                ..
            }) = self.peers.get_mut(&id)
            else {
                debug_assert!(false, "the unused list names a peer that is not ready");
                continue;
            };
            debug_assert_eq!(*unused_since, Some(since));
            // A `ConnectionHandle` takes references without going through the
            // pool, so a peer on the list may be in use again. Its next drop to
            // zero lists it again.
            if !counter.is_unused() {
                *unused_since = None;
                continue;
            }
            trace!("evicting oldest unused peer {id} to make room");
            self.remove_peer(id);
            return true;
        }
        false
    }

    /// Returns a future that dials `id` or adopts `incoming`.
    ///
    /// The future runs `on_connected` as well, all within the connect timeout.
    fn make_connect_future(
        &self,
        id: EndpointId,
        generation: u64,
        incoming: Option<Connection>,
    ) -> Boxed<ConnectResult> {
        let endpoint = self.endpoint.clone();
        let alpn = self.alpn.clone();
        let on_connected = self.options.on_connected.clone();
        let connect_timeout = self.options.connect_timeout;
        let unused_tx = self.unused_tx.clone();
        Box::pin(async move {
            let mut connected = None;
            let attempt = async {
                let conn = match incoming {
                    Some(conn) => conn,
                    None => endpoint
                        .connect(id, &alpn[..])
                        .await
                        .map_err(PoolConnectError::from)?,
                };
                connected = Some(conn.clone());
                let counter = ConnectionCounter::new(id, generation, unused_tx);
                if let Some(f) = &on_connected {
                    f(&endpoint, &ConnectionHandle::new(&conn, &counter))
                        .await
                        .map_err(PoolConnectError::from)?;
                }
                Result::<_, PoolConnectError>::Ok((conn, counter))
            };
            let result = match n0_future::time::timeout(connect_timeout, attempt).await {
                Ok(r) => r,
                Err(_) => Err(e!(PoolConnectError::Timeout)),
            };
            // `on_connected` failed or ran out of time. Close the connection
            // rather than drop it: the callback may have handed it to a task.
            if result.is_err()
                && let Some(conn) = connected
            {
                conn.close(0u32.into(), b"on_connected failed");
            }
            (id, generation, result)
        })
    }

    fn handle_connect_result(
        &mut self,
        id: EndpointId,
        generation: u64,
        result: Result<(Connection, ConnectionCounter), PoolConnectError>,
    ) {
        if let Some((_, tx)) = self.adopting.remove(&generation) {
            return self.handle_adopt_result(id, generation, result, tx);
        }
        let current = matches!(
            self.peers.get(&id),
            Some(PeerState::Connecting { generation: g, .. }) if *g == generation
        );
        if !current {
            // PeerState was removed or changed in the meantime, discard the connection.
            debug!(%id, "stale connect result, discarding");
            if let Ok((conn, _)) = result {
                conn.close(0u32.into(), b"discarded");
            }
            return;
        }
        let Some(PeerState::Connecting { waiters, .. }) = self.peers.remove(&id) else {
            return;
        };
        match result {
            // Connections made since this attempt started may have filled the pool.
            Ok((conn, _)) if !self.make_room() => {
                debug!(%id, "connected, but the pool is full");
                conn.close(0u32.into(), b"too many connections");
                for tx in waiters {
                    let _ = tx.send(Err(e!(PoolConnectError::TooManyConnections)));
                }
            }
            Ok((conn, counter)) => self.insert_ready(id, generation, conn, counter, waiters),
            Err(cause) => {
                debug!(%id, "connect failed: {cause:?}");
                for tx in waiters {
                    let _ = tx.send(Err(cause.clone()));
                }
            }
        }
    }

    /// Makes an adopted incoming connection the peer's current one.
    ///
    /// A current connection is superseded rather than closed: two endpoints
    /// that dial each other at once each keep the connection they saw last, and
    /// may disagree, so the peer can still be using it. A dial that is still
    /// running is superseded too. Its waiters get the incoming connection, and
    /// its result is discarded when it arrives.
    fn handle_adopt_result(
        &mut self,
        id: EndpointId,
        generation: u64,
        result: Result<(Connection, ConnectionCounter), PoolConnectError>,
        tx: RefSender,
    ) {
        let (conn, counter) = match result {
            Ok(adopted) => adopted,
            Err(cause) => {
                debug!(%id, "adopting incoming connection failed: {cause:?}");
                let _ = tx.send(Err(cause));
                return;
            }
        };
        // Connections made since the adoption started may have filled the pool.
        if !self.peers.contains_key(&id) && !self.make_room() {
            debug!(%id, "adopted, but the pool is full");
            conn.close(0u32.into(), b"too many connections");
            let _ = tx.send(Err(e!(PoolConnectError::TooManyConnections)));
            return;
        }
        let mut waiters = match self.peers.remove(&id) {
            Some(PeerState::Ready {
                generation: old,
                connection,
                counter,
                unused_since,
            }) => {
                debug!(%id, "incoming connection supersedes the current one");
                if let Some(since) = unused_since {
                    self.unused.remove(&(since, id));
                }
                let superseded = Superseded {
                    connection,
                    counter,
                    unused_since,
                };
                self.supersede(id, old, superseded);
                Vec::new()
            }
            Some(PeerState::Connecting { waiters, .. }) => waiters,
            None => Vec::new(),
        };
        waiters.push(tx);
        self.insert_ready(id, generation, conn, counter, waiters);
    }

    /// Makes `conn` the peer's current connection and hands it to `waiters`.
    fn insert_ready(
        &mut self,
        id: EndpointId,
        generation: u64,
        conn: Connection,
        counter: ConnectionCounter,
        waiters: Vec<RefSender>,
    ) {
        for tx in waiters {
            if tx.is_closed() {
                continue;
            }
            let permit = counter.get_one();
            if tx
                .send(Ok(ConnectionRef::new(conn.clone(), permit)))
                .is_err()
            {
                // User is no longer interested in the ConnectionRef.
            }
        }
        info!(%id, "connected, {} ref(s) outstanding", counter.current());

        // Create a future that waits for the connection to close.
        let close_fut: Boxed<(EndpointId, u64)> = {
            let conn = conn.clone();
            Box::pin(async move {
                conn.closed().await;
                (id, generation)
            })
        };
        self.conn_close.push(close_fut);

        let unused_since = counter.is_unused().then(Instant::now);
        if let Some(since) = unused_since {
            self.unused.insert((since, id));
        }
        self.peers.insert(
            id,
            PeerState::Ready {
                generation,
                connection: conn,
                counter,
                unused_since,
            },
        );
    }

    /// Keeps a superseded connection open until it is unused.
    ///
    /// It keeps the idle state it had as the current connection.
    fn supersede(&mut self, id: EndpointId, generation: u64, superseded: Superseded) {
        superseded
            .counter
            .inner
            .superseded
            .store(true, Ordering::SeqCst);
        if let Some(since) = superseded.unused_since {
            self.superseded_unused.insert((since, id, generation));
        }
        self.superseded.insert((id, generation), superseded);
    }

    /// Stops tracking a superseded connection, and returns it.
    fn remove_superseded(&mut self, id: EndpointId, generation: u64) -> Option<Superseded> {
        let superseded = self.superseded.remove(&(id, generation))?;
        if let Some(since) = superseded.unused_since {
            self.superseded_unused.remove(&(since, id, generation));
        }
        Some(superseded)
    }

    /// Handles a connection closing, by us or by the peer.
    ///
    /// Only acts if the connection is still the peer's current one. One the
    /// pool replaced has closed already, and must not take down its successor.
    fn handle_conn_closed(&mut self, id: EndpointId, generation: u64) {
        let current = matches!(
            self.peers.get(&id),
            Some(PeerState::Ready { generation: g, .. }) if *g == generation
        );
        if current {
            trace!(%id, "connection closed");
            self.remove_peer(id);
        } else if self.remove_superseded(id, generation).is_some() {
            trace!(%id, "superseded connection closed");
        }
    }

    /// Handles a connection going unused.
    ///
    /// Only acts on the connection the event names: the peer's current one, or
    /// one it superseded. References to a connection the pool has let go of can
    /// outlive it, and their last drop must not restart another connection's
    /// idle timeout.
    fn handle_unused_event(&mut self, id: EndpointId, generation: u64) {
        if let Some(superseded) = self.superseded.get_mut(&(id, generation)) {
            if !superseded.counter.is_unused() {
                return;
            }
            let now = Instant::now();
            if let Some(since) = superseded.unused_since.replace(now) {
                self.superseded_unused.remove(&(since, id, generation));
            }
            self.superseded_unused.insert((now, id, generation));
            return;
        }
        let Some(PeerState::Ready {
            generation: g,
            counter,
            unused_since,
            ..
        }) = self.peers.get_mut(&id)
        else {
            return;
        };
        if *g != generation {
            return;
        }
        // Connection was handed out in the meantime.
        if !counter.is_unused() {
            return;
        }
        let now = Instant::now();
        if let Some(since) = unused_since.replace(now) {
            self.unused.remove(&(since, id));
        }
        self.unused.insert((now, id));
        trace!(%id, "peer unused");
    }

    /// Closes the connections that have been unused for the idle timeout.
    ///
    /// A `ConnectionHandle` takes references without going through the pool, so
    /// a listed connection may be in use again. It is taken off the list, and
    /// its next drop to zero lists it again.
    fn close_unused(&mut self) {
        let now = Instant::now();
        let timeout = self.options.idle_timeout;
        while let Some(&(since, id)) = self.unused.first()
            && since + timeout <= now
        {
            self.unused.pop_first();
            let Some(PeerState::Ready {
                counter,
                unused_since,
                ..
            }) = self.peers.get_mut(&id)
            else {
                continue;
            };
            if counter.is_unused() {
                trace!(%id, "unused timeout, removing");
                self.remove_peer(id);
            } else {
                *unused_since = None;
            }
        }
        while let Some(&(since, id, generation)) = self.superseded_unused.first()
            && since + timeout <= now
        {
            self.superseded_unused.pop_first();
            let Some(superseded) = self.superseded.get_mut(&(id, generation)) else {
                continue;
            };
            if superseded.counter.is_unused() {
                debug!(%id, "closing unused superseded connection");
                close(&superseded.connection, &superseded.counter);
                self.superseded.remove(&(id, generation));
            } else {
                superseded.unused_since = None;
            }
        }
    }

    /// Closes every connection to `id`, and fails the attempts to get one.
    ///
    /// See [`ConnectionPool::close`].
    fn close_peer(&mut self, id: EndpointId) {
        self.remove_peer(id);
        // Their results are discarded as stale when they arrive.
        for (_, (_, tx)) in self.adopting.extract_if(|_, (peer, _)| *peer == id) {
            let _ = tx.send(Err(e!(PoolConnectError::Closed)));
        }
        for (_, superseded) in self.superseded.extract_if(|(peer, _), _| *peer == id) {
            close(&superseded.connection, &superseded.counter);
        }
        self.superseded_unused.retain(|(_, peer, _)| *peer != id);
    }

    fn remove_peer(&mut self, id: EndpointId) {
        if let Some(state) = self.peers.remove(&id) {
            match state {
                PeerState::Connecting { waiters, .. } => {
                    for tx in waiters {
                        let _ = tx.send(Err(e!(PoolConnectError::Closed)));
                    }
                }
                PeerState::Ready {
                    connection,
                    counter,
                    unused_since,
                    ..
                } => {
                    close(&connection, &counter);
                    if let Some(since) = unused_since {
                        self.unused.remove(&(since, id));
                    }
                }
            }
        }
    }
}

/// Closes `connection`, with a reason that says whether it was in use.
///
/// The reason is `unused` if nothing uses it, and `drop` otherwise.
fn close(connection: &Connection, counter: &ConnectionCounter) {
    let reason: &[u8] = if counter.is_unused() {
        b"unused"
    } else {
        b"drop"
    };
    connection.close(0u32.into(), reason);
}

/// A connection pool
///
/// Dropping the last handle stops the pool and closes its connections, including
/// ones that are still in use. If the pool's task panics, every call fails with
/// [`PoolConnectError::Shutdown`] or [`ConnectionPoolError::Shutdown`].
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    tx: mpsc::Sender<ActorMessage>,
}

impl ConnectionPool {
    pub fn new(endpoint: Endpoint, alpn: &[u8], options: Options) -> Self {
        let (actor, tx) = Actor::new(endpoint, alpn, options);
        n0_future::task::spawn(actor.run());
        Self { tx }
    }

    /// Returns either a fresh connection or a reference to an existing one.
    ///
    /// This is guaranteed to return after approximately [Options::connect_timeout]
    /// with either an error or a connection, plus the time the request waits in
    /// the pool's inbox.
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
    /// endpoint: later [`Self::get_or_connect`] calls return it, a dial to the
    /// endpoint that is still running is dropped in its favor, and
    /// [`ConnectionRef::is_superseded`] tells holders of the previous connection
    /// to move on.
    ///
    /// The previous connection is not closed right away. Two endpoints that dial
    /// each other at once each keep the connection they saw last, and may
    /// disagree, so the remote can still be using the one we superseded. It is
    /// closed when it is unused instead, like any connection the pool holds.
    ///
    /// The pool does not check the connection's ALPN.
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
    count: AtomicUsize,
    id: EndpointId,
    /// The generation of the connection, which unused events name.
    generation: u64,
    unused_tx: mpsc::UnboundedSender<(EndpointId, u64)>,
    /// Set once a newer connection to the same endpoint took this one's place.
    superseded: AtomicBool,
}

#[derive(Debug, Clone)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new(
        id: EndpointId,
        generation: u64,
        unused_tx: mpsc::UnboundedSender<(EndpointId, u64)>,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                count: AtomicUsize::new(0),
                id,
                generation,
                unused_tx,
                superseded: AtomicBool::new(false),
            }),
        }
    }

    fn current(&self) -> usize {
        self.inner.count.load(Ordering::SeqCst)
    }

    fn is_unused(&self) -> bool {
        self.current() == 0
    }

    fn get_one(&self) -> OneConnection {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        OneConnection {
            inner: self.inner.clone(),
        }
    }
}

/// Handle to a connection counter that decrements it on drop.
#[derive(Debug)]
struct OneConnection {
    inner: Arc<ConnectionCounterInner>,
}

impl Drop for OneConnection {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            // Send an unused event to the actor.
            let _ = self
                .inner
                .unused_tx
                .send((self.inner.id, self.inner.generation));
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
        Actor, ConnectionCounter, ConnectionHandle, ConnectionPool, ConnectionRef, OnConnected,
        Options, PeerState, PoolConnectError, RequestRef,
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
            on_connected: None,
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

    /// Tests that unused connections are being reclaimed to make room if we hit the
    /// maximum connection limit.
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
        let on_connected = |_, conn: ConnectionHandle| async move {
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

    /// Bind a UDP socket to a free loopback port and keep it alive for the
    /// caller's lifetime. iroh `connect()` against this address will time
    /// out (no QUIC handshake response), avoiding both hardcoded ports
    /// (which can clash) and unbound ones (which OSes may rebind).
    fn dead_addr() -> TestResult<(std::net::UdpSocket, TransportAddr)> {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = TransportAddr::Ip(sock.local_addr()?);
        Ok((sock, addr))
    }

    /// Spawn `n` concurrent `get_or_connect(id)` calls and yield until each
    /// task has at least entered its body.
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

    /// Concurrent get_or_connect calls for an unreachable peer must not
    /// prevent a probe against an unrelated reachable peer from completing
    /// in bounded time.
    #[tokio::test]
    async fn connection_pool_dead_peer_backlog_does_not_wedge() -> TestResult<()> {
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

        let probe_budget = connect_timeout * 5;
        let probe = Instant::now();
        let probe_result =
            n0_future::time::timeout(probe_budget, pool.get_or_connect(live_peer)).await;
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
                panic!("live-peer probe did not complete within {probe_budget:?} — pool wedged")
            }
        }
    }

    /// A smaller dead-peer backlog does not delay an unrelated peer at all.
    ///
    /// Same setup as `connection_pool_dead_peer_backlog_does_not_wedge`, with a
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

    /// Closing a slow connection attempt discards it. A later connect starts
    /// a new attempt.
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

    /// Check that when a connection is closed, the pool will give you a new
    /// connection next time you want one.
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
    /// The event is for a connection the peer no longer has. The pool closes a
    /// connection before it replaces it, and polls close events before its
    /// inbox, so this cannot happen through the public API yet. The test calls
    /// the handler directly.
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
        let counter = ConnectionCounter::new(id, 1, actor.unused_tx.clone());
        actor.peers.insert(
            id,
            PeerState::Ready {
                generation: 1,
                connection: conn.clone(),
                counter,
                unused_since: None,
            },
        );

        actor.handle_conn_closed(id, 0);
        assert!(
            actor.peers.contains_key(&id),
            "a stale close event removed the peer"
        );
        assert!(
            conn.close_reason().is_none(),
            "a stale close event closed the connection"
        );
        actor.handle_conn_closed(id, 1);
        assert!(!actor.peers.contains_key(&id));

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
        let counter = ConnectionCounter::new(id, 1, actor.unused_tx.clone());
        actor.peers.insert(
            id,
            PeerState::Ready {
                generation: 1,
                connection: conn.clone(),
                counter,
                unused_since: None,
            },
        );
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
            matches!(actor.peers.get(&id), Some(PeerState::Connecting { .. })),
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
                move |_, conn: ConnectionHandle| {
                    *kept.lock().expect("poisoned") = Some(conn);
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
        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }

    /// Using a connection again restarts its idle timeout.
    ///
    /// The connection is closed `idle_timeout` after it last went unused, not
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
                move |_, conn: ConnectionHandle| {
                    *kept.lock().expect("poisoned") = Some(conn);
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
    /// replaced connection can outlive it, and when the last one drops, its
    /// event names the peer, whose current connection is another.
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
        let counter = ConnectionCounter::new(id, 1, actor.unused_tx.clone());
        let since = n0_future::time::Instant::now();
        actor.unused.insert((since, id));
        actor.peers.insert(
            id,
            PeerState::Ready {
                generation: 1,
                connection: conn,
                counter,
                unused_since: Some(since),
            },
        );

        actor.handle_unused_event(id, 0);
        assert!(
            matches!(
                actor.peers.get(&id),
                Some(PeerState::Ready { unused_since: Some(s), .. }) if *s == since
            ),
            "a stale unused event restarted the idle timeout"
        );
        assert_eq!(actor.unused.first(), Some(&(since, id)));

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

    /// Asserts that the pool closed a connection because it was unused.
    fn assert_closed_as_unused(err: &iroh::endpoint::ConnectionError) {
        assert!(
            matches!(
                err,
                iroh::endpoint::ConnectionError::ApplicationClosed(frame)
                    if &frame.reason[..] == b"unused"
            ),
            "closed for the wrong reason: {err:?}"
        );
    }

    fn short_idle_options() -> Options {
        Options {
            idle_timeout: SHORT_IDLE,
            ..Default::default()
        }
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
    /// saw last, and may disagree, so the remote can still be using the one we
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
    /// holds the [`ConnectionHandle`] and takes a reference per stream.
    #[tokio::test]
    async fn on_connected_ref_keeps_superseded_connection_open() -> TestResult<()> {
        let held = Arc::new(std::sync::Mutex::new(Vec::new()));
        let options = short_idle_options().with_on_connected({
            let held = held.clone();
            move |_ep, conn: ConnectionHandle| {
                held.lock().expect("poisoned").push(conn.get_ref());
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

        held.lock().expect("poisoned").clear();
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert_closed_as_unused(&err);
        Ok(())
    }

    /// A reference from a [`ConnectionHandle`] keeps an unused connection open.
    ///
    /// Such a reference does not go through the pool, so the connection is
    /// still on the unused list when its idle timeout passes.
    #[tokio::test]
    async fn handle_ref_after_idle_keeps_connection_open() -> TestResult<()> {
        let handle = Arc::new(std::sync::Mutex::new(None));
        let options = short_idle_options().with_on_connected({
            let handle = handle.clone();
            move |_ep, conn: ConnectionHandle| {
                *handle.lock().expect("poisoned") = Some(conn);
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
        // Let the connection actor see the connection go idle and start its
        // timer before the reference comes in.
        n0_future::time::sleep(SHORT_IDLE / 4).await;
        let handle = handle.lock().expect("poisoned").take().expect("no handle");
        let stream_ref = handle.get_ref();

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

    /// [`ConnectionPool::close`] closes superseded connections too, even while
    /// they are in use.
    #[tokio::test]
    async fn close_closes_superseded_connections() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        s.pool.close(s.second_ref.remote_id()).await?;

        for (name, conn) in [("superseded", &s.first), ("current", &s.second)] {
            let err = tokio::time::timeout(SHORT_IDLE * 2, conn.closed())
                .await
                .map_err(|_| format!("the {name} connection was not closed"))?;
            assert!(
                matches!(
                    &err,
                    iroh::endpoint::ConnectionError::ApplicationClosed(frame)
                        if &frame.reason[..] == b"drop"
                ),
                "the {name} connection was closed for the wrong reason: {err:?}"
            );
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
            move |_ep, _conn: ConnectionHandle| {
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

    /// A full pool does not evict a peer that is in use again.
    ///
    /// A reference from a [`ConnectionHandle`] put it back in use.
    ///
    /// Such a reference does not go through the pool, so the peer is still on
    /// the list of unused peers that eviction picks from.
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
            move |_ep, conn: ConnectionHandle| {
                *handle.lock().expect("poisoned") = Some(conn);
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
        let stream_ref = handle.get_ref();

        let other = SecretKey::from_bytes(&[9u8; 32]).public();
        let res = pool.get_or_connect(other).await;
        assert!(
            matches!(res, Err(PoolConnectError::TooManyConnections { .. })),
            "made room by evicting a peer in use: {res:?}"
        );
        assert!(
            outgoing.close_reason().is_none(),
            "the connection in use was closed: {:?}",
            outgoing.close_reason()
        );
        drop(stream_ref);
        server.close().await;
        Ok(())
    }
}
