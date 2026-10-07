//! A simple iroh connection pool
//!
//! Entry point is [`ConnectionPool`]. You create a connection pool for a specific
//! ALPN and [`Options`]. Then the pool will manage connections for you.
//!
//! Access to connections is via the [`ConnectionPool::get_or_connect`] method, which
//! gives you access to a connection via a [`ConnectionRef`] if possible.
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
        atomic::{AtomicUsize, Ordering},
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

pub type OnConnected =
    Arc<dyn Fn(&Endpoint, &Connection) -> n0_future::future::Boxed<io::Result<()>> + Send + Sync>;

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
        F: Fn(Endpoint, Connection) -> Fut + Send + Sync + 'static,
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
#[derive(Debug)]
pub struct ConnectionRef {
    connection: iroh::endpoint::Connection,
    _permit: OneConnection,
}

impl Deref for ConnectionRef {
    type Target = iroh::endpoint::Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl ConnectionRef {
    fn new(connection: iroh::endpoint::Connection, permit: OneConnection) -> Self {
        Self {
            connection,
            _permit: permit,
        }
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

enum ActorMessage {
    RequestRef(RequestRef),
    ConnectionShutdown { id: EndpointId },
}

struct RequestRef {
    id: EndpointId,
    tx: oneshot::Sender<Result<ConnectionRef, PoolConnectError>>,
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

/// State for a peer in the connection pool
enum PeerState {
    /// We are currently connecting to this peer.
    Connecting {
        generation: Generation,
        /// Waiters that need to be notified when the connection is established or fails.
        waiters: Vec<oneshot::Sender<Result<ConnectionRef, PoolConnectError>>>,
    },
    /// We have a connection to the peer.
    Ready {
        /// The generation of the attempt that made the connection.
        generation: Generation,
        connection: Connection,
        counter: ConnectionCounter,
    },
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

type ConnectResult = (ConnId, Result<Connection, PoolConnectError>);

struct Actor {
    /// Inbox
    rx: mpsc::Receiver<ActorMessage>,
    /// Separate inbox for unused events, gets processed before the main inbox.
    ///
    /// Each event names the connection that has no references left.
    unused_rx: mpsc::UnboundedReceiver<ConnId>,
    /// Sender for the unused inbox to be cloned into the connection counter.
    ///
    /// This is unbounded so it can be used in Drop, but it is bounded by the number
    /// of ConnectionRefs we give out, which is bounded by max_connections.
    unused_tx: mpsc::UnboundedSender<ConnId>,
    options: Options,
    endpoint: Endpoint,
    alpn: Arc<[u8]>,
    peers: HashMap<EndpointId, PeerState>,
    /// Futures for currently connecting peers.
    connecting: FuturesUnordered<Boxed<ConnectResult>>,
    /// Generation counter used to distinguish between connection attempts to the same peer.
    next_generation: Generation,
    /// Futures for connection close watchers, each yielding which connection closed.
    conn_close: FuturesUnordered<Boxed<ConnId>>,
    /// Currently unused connections, in order of when they became unused.
    unused: UnusedSet,
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
                next_generation: Generation(0),
                conn_close: FuturesUnordered::new(),
                unused: UnusedSet::default(),
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
                Some(unused_since) => {
                    let deadline = unused_since + self.options.idle_timeout;
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
                } => {
                    let reason: &[u8] = if counter.is_unused() {
                        b"unused"
                    } else {
                        b"drop"
                    };
                    connection.close(0u32.into(), reason);
                }
            }
        }
    }

    fn handle_msg(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::RequestRef(req) => self.handle_request(req),
            ActorMessage::ConnectionShutdown { id } => {
                trace!(%id, "shutdown requested");
                self.remove_peer(id);
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
                    generation,
                    connection,
                    counter,
                } => {
                    self.unused.remove(ConnId {
                        peer: id,
                        generation: *generation,
                    });
                    let one = counter.get_one();
                    info!(%id, "Handing out ConnectionRef {}", counter.current());
                    let _ = req.tx.send(Ok(ConnectionRef::new(connection.clone(), one)));
                    return;
                }
            }
        }

        if !self.make_room() {
            let _ = req.tx.send(Err(e!(PoolConnectError::TooManyConnections)));
            return;
        }

        let conn_id = ConnId {
            peer: id,
            generation: self.next_generation.next(),
        };
        self.peers.insert(
            id,
            PeerState::Connecting {
                generation: conn_id.generation,
                waiters: vec![req.tx],
            },
        );
        self.connecting.push(self.make_connect_future(conn_id));
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
        let Some(conn_id) = self.unused.pop_oldest() else {
            return false;
        };
        debug_assert!(
            matches!(
                self.peers.get(&conn_id.peer),
                Some(PeerState::Ready { generation, counter, .. })
                    if *generation == conn_id.generation && counter.is_unused()
            ),
            "the unused list names a connection that is not unused"
        );
        trace!(%conn_id, "evicting oldest unused peer to make room");
        self.remove_peer(conn_id.peer);
        true
    }

    fn make_connect_future(&self, conn_id: ConnId) -> Boxed<ConnectResult> {
        let endpoint = self.endpoint.clone();
        let alpn = self.alpn.clone();
        let on_connected = self.options.on_connected.clone();
        let connect_timeout = self.options.connect_timeout;
        Box::pin(async move {
            let mut connected = None;
            let attempt = async {
                let conn = endpoint
                    .connect(conn_id.peer, &alpn[..])
                    .await
                    .map_err(PoolConnectError::from)?;
                connected = Some(conn.clone());
                if let Some(f) = &on_connected {
                    f(&endpoint, &conn).await.map_err(PoolConnectError::from)?;
                }
                Result::<Connection, PoolConnectError>::Ok(conn)
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
            (conn_id, result)
        })
    }

    fn handle_connect_result(
        &mut self,
        conn_id: ConnId,
        result: Result<Connection, PoolConnectError>,
    ) {
        let ConnId {
            peer: id,
            generation,
        } = conn_id;
        let current = matches!(
            self.peers.get(&id),
            Some(PeerState::Connecting { generation: g, .. }) if *g == generation
        );
        if !current {
            // PeerState was removed or changed in the meantime, discard the connection.
            debug!(%id, "stale connect result, discarding");
            if let Ok(conn) = result {
                conn.close(0u32.into(), b"discarded");
            }
            return;
        }
        let Some(PeerState::Connecting { waiters, .. }) = self.peers.remove(&id) else {
            return;
        };
        match result {
            // Connections made since this attempt started may have filled the pool.
            Ok(conn) if !self.make_room() => {
                debug!(%id, "connected, but the pool is full");
                conn.close(0u32.into(), b"too many connections");
                for tx in waiters {
                    let _ = tx.send(Err(e!(PoolConnectError::TooManyConnections)));
                }
            }
            Ok(conn) => {
                let counter = ConnectionCounter::new(conn_id, self.unused_tx.clone());
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
                let close_fut: Boxed<ConnId> = {
                    let conn = conn.clone();
                    Box::pin(async move {
                        conn.closed().await;
                        conn_id
                    })
                };
                self.conn_close.push(close_fut);

                if counter.is_unused() {
                    self.unused.insert(conn_id, Instant::now());
                }
                self.peers.insert(
                    id,
                    PeerState::Ready {
                        generation,
                        connection: conn,
                        counter,
                    },
                );
            }
            Err(cause) => {
                debug!(%id, "connect failed: {cause:?}");
                for tx in waiters {
                    let _ = tx.send(Err(cause.clone()));
                }
            }
        }
    }

    /// Handles a connection closing, by us or by the peer.
    ///
    /// Only acts if the connection is still the peer's current one. One the
    /// pool replaced has closed already, and must not take down its successor.
    fn handle_conn_closed(&mut self, conn_id: ConnId) {
        let current = matches!(
            self.peers.get(&conn_id.peer),
            Some(PeerState::Ready { generation, .. }) if *generation == conn_id.generation
        );
        if current {
            trace!(%conn_id, "connection closed");
            self.remove_peer(conn_id.peer);
        }
    }

    /// Handles a connection going unused.
    ///
    /// Only acts if the connection is still the peer's current one. References
    /// to a connection the pool replaced can outlive it, and their last drop
    /// must not restart its successor's idle timeout.
    fn handle_unused_event(&mut self, conn_id: ConnId) {
        let Some(PeerState::Ready {
            generation,
            counter,
            ..
        }) = self.peers.get(&conn_id.peer)
        else {
            return;
        };
        if *generation != conn_id.generation {
            return;
        }
        // Connection was handed out in the meantime.
        if !counter.is_unused() {
            return;
        }
        self.unused.insert(conn_id, Instant::now());
        trace!(%conn_id, "peer unused");
    }

    /// Closes the connections that have been unused for the idle timeout.
    fn close_unused(&mut self) {
        let now = Instant::now();
        while let Some(conn_id) = self.unused.pop_expired(now, self.options.idle_timeout) {
            trace!(%conn_id, "unused timeout, removing");
            self.remove_peer(conn_id.peer);
        }
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
                    generation,
                    connection,
                    counter,
                } => {
                    let reason: &[u8] = if counter.is_unused() {
                        b"unused"
                    } else {
                        b"drop"
                    };
                    connection.close(0u32.into(), reason);
                    self.unused.remove(ConnId {
                        peer: id,
                        generation,
                    });
                }
            }
        }
    }
}

/// A connection pool
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
    /// with either an error or a connection.
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

    /// Close an existing connection, if it exists
    ///
    /// This will finish pending tasks and close the connection. New tasks will
    /// get a new connection if they are submitted after this call
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
    /// Which connection this counts, which its unused events name.
    conn_id: ConnId,
    unused_tx: mpsc::UnboundedSender<ConnId>,
}

#[derive(Debug, Clone)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new(conn_id: ConnId, unused_tx: mpsc::UnboundedSender<ConnId>) -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                count: AtomicUsize::new(0),
                conn_id,
                unused_tx,
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
        Actor, ConnId, ConnectionCounter, ConnectionPool, Generation, OnConnected, Options,
        PeerState, PoolConnectError, RequestRef,
    };

    /// Puts `conn` into `actor` as its peer's current connection.
    ///
    /// Returns which connection it is, for the events a test hands the actor.
    fn insert_ready(actor: &mut Actor, conn: &Connection) -> ConnId {
        let conn_id = ConnId {
            peer: conn.remote_id(),
            generation: Generation(1),
        };
        let counter = ConnectionCounter::new(conn_id, actor.unused_tx.clone());
        if counter.is_unused() {
            actor
                .unused
                .insert(conn_id, n0_future::time::Instant::now());
        }
        actor.peers.insert(
            conn_id.peer,
            PeerState::Ready {
                generation: conn_id.generation,
                connection: conn.clone(),
                counter,
            },
        );
        conn_id
    }

    /// Returns the id of a connection to `peer` that the pool never made.
    fn stale_conn_id(peer: EndpointId) -> ConnId {
        ConnId {
            peer,
            generation: Generation(0),
        }
    }

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
        let on_connected = |_, conn: Connection| async move {
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

    /// Same setup as `connection_pool_dead_peer_backlog_does_not_wedge`,
    /// with concurrency below the inbox capacity. The unrelated-peer probe
    /// must complete within one `connect_timeout` window.
    #[tokio::test]
    async fn connection_pool_dead_peer_below_inbox_cap_is_unaffected() -> TestResult<()> {
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
                move |_, conn: Connection| {
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
                move |_, conn: Connection| {
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
        let conn_id = insert_ready(&mut actor, &conn);
        let since = actor.unused.oldest().expect("the connection is unused");

        actor.handle_unused_event(stale_conn_id(id));
        assert_eq!(
            actor.unused.oldest(),
            Some(since),
            "a stale unused event restarted the idle timeout"
        );
        actor.handle_unused_event(conn_id);
        assert!(
            actor.unused.oldest().expect("still unused") >= since,
            "the connection's own event was ignored"
        );

        shutdown_routers(routers).await;
        endpoint.close().await;
        Ok(())
    }
}
