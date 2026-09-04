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
    collections::{HashMap, VecDeque},
    io,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use iroh::{
    Endpoint, EndpointId,
    endpoint::{ConnectError, Connection},
};
use n0_error::{e, stack_error};
use n0_future::{FuturesUnordered, StreamExt, future::Boxed, time::Duration};
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
    fn new(connection: iroh::endpoint::Connection, counter: OneConnection) -> Self {
        Self {
            connection,
            _permit: counter,
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

/// State for a peer in the connection pool
enum PeerState {
    /// We are currently connecting to this peer.
    Connecting {
        generation: u64,
        /// Waiters that need to be notified when the connection is established or fails.
        waiters: Vec<oneshot::Sender<Result<ConnectionRef, PoolConnectError>>>,
    },
    /// We have a connection to the peer.
    Ready {
        connection: Connection,
        counter: ConnectionCounter,
        unused_since: Option<Instant>,
    },
}

type ConnectResult = (EndpointId, u64, Result<Connection, PoolConnectError>);

struct Actor {
    /// Inbox
    rx: mpsc::Receiver<ActorMessage>,
    /// Separate inbox for unused events, gets processed before the main inbox.
    unused_rx: mpsc::UnboundedReceiver<EndpointId>,
    /// Sender for the unused inbox to be cloned into the connection counter.
    ///
    /// This is unbounded so it can be used in Drop, but it is bounded by the number
    /// of ConnectionRefs we give out, which is bounded by max_connections.
    unused_tx: mpsc::UnboundedSender<EndpointId>,
    options: Options,
    endpoint: Endpoint,
    alpn: Arc<[u8]>,
    peers: HashMap<EndpointId, PeerState>,
    /// Futures for currently connecting peers.
    connecting: FuturesUnordered<Boxed<ConnectResult>>,
    /// Generation counter used to distinguish between connection attempts to the same peer.
    next_generation: u64,
    /// Futures for connection close watchers.
    conn_close: FuturesUnordered<Boxed<EndpointId>>,
    /// Futures for cleaning up unused connections after the unused timeout.
    unused_timers: FuturesUnordered<Boxed<EndpointId>>,
    /// Currently unused connections, in order of when they became unused.
    unused: VecDeque<EndpointId>,
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
                next_generation: 0,
                conn_close: FuturesUnordered::new(),
                unused_timers: FuturesUnordered::new(),
                unused: VecDeque::new(),
            },
            tx,
        )
    }

    async fn run(mut self) {
        // We bias processing internal events before accepting more work from
        // the external mailbox.
        loop {
            tokio::select! {
                biased;

                // Handle unused events first, since this might give us some room.
                Some(id) = self.unused_rx.recv() => {
                    self.handle_unused_event(id);
                }

                Some((id, generation, result)) = self.connecting.next(), if !self.connecting.is_empty() => {
                    self.handle_connect_result(id, generation, result);
                }

                Some(id) = self.conn_close.next(), if !self.conn_close.is_empty() => {
                    trace!(%id, "connection closed by peer");
                    self.remove_peer(id);
                }

                Some(id) = self.unused_timers.next(), if !self.unused_timers.is_empty() => {
                    self.handle_unused_timer(id);
                }

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
        // Remove the id from the unused list,
        self.unused.retain(|x| *x != id);

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
                } => {
                    *unused_since = None;
                    let one = counter.get_one();
                    info!(%id, "Handing out ConnectionRef {}", counter.current());
                    let _ = req.tx.send(Ok(ConnectionRef::new(connection.clone(), one)));
                    return;
                }
            }
        }

        // If we exceed max_connections, do a last attempt to make room, otherwise fail.
        if self.peers.len() >= self.options.max_connections {
            if let Some(id) = self.unused.pop_front() {
                trace!("evicting oldest unused peer {id} to make room");
                self.remove_peer_inner(id);
            } else {
                let _ = req.tx.send(Err(e!(PoolConnectError::TooManyConnections)));
                return;
            }
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
            .push(self.make_connect_future(id, generation));
    }

    fn make_connect_future(&self, id: EndpointId, generation: u64) -> Boxed<ConnectResult> {
        let endpoint = self.endpoint.clone();
        let alpn = self.alpn.clone();
        let on_connected = self.options.on_connected.clone();
        let connect_timeout = self.options.connect_timeout;
        Box::pin(async move {
            let attempt = async {
                let conn = endpoint
                    .connect(id, &alpn[..])
                    .await
                    .map_err(PoolConnectError::from)?;
                if let Some(f) = &on_connected {
                    f(&endpoint, &conn).await.map_err(PoolConnectError::from)?;
                }
                Result::<Connection, PoolConnectError>::Ok(conn)
            };
            let result = match n0_future::time::timeout(connect_timeout, attempt).await {
                Ok(r) => r,
                Err(_) => Err(e!(PoolConnectError::Timeout)),
            };
            (id, generation, result)
        })
    }

    fn handle_connect_result(
        &mut self,
        id: EndpointId,
        generation: u64,
        result: Result<Connection, PoolConnectError>,
    ) {
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
            Ok(conn) => {
                let counter = ConnectionCounter::new(id, self.unused_tx.clone());
                for tx in waiters {
                    if tx.is_closed() {
                        continue;
                    }
                    let one = counter.get_one();
                    if tx.send(Ok(ConnectionRef::new(conn.clone(), one))).is_err() {
                        // User is no longer interested in the ConnectionRef.
                    }
                }
                info!(%id, "connected, {} ref(s) outstanding", counter.current());

                // Create a future that waits for the connection to close.
                let close_fut: Boxed<EndpointId> = {
                    let conn = conn.clone();
                    Box::pin(async move {
                        conn.closed().await;
                        id
                    })
                };
                self.conn_close.push(close_fut);

                let unused_since = if counter.is_unused() {
                    // Schedule an idle timer if it is already unused here.
                    self.unused.push_back(id);
                    self.schedule_unused_timer(id);
                    Some(Instant::now())
                } else {
                    None
                };
                self.peers.insert(
                    id,
                    PeerState::Ready {
                        connection: conn,
                        counter,
                        unused_since,
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

    fn handle_unused_event(&mut self, id: EndpointId) {
        let Some(PeerState::Ready {
            counter,
            unused_since,
            ..
        }) = self.peers.get_mut(&id)
        else {
            return;
        };
        // Connection was handed out in the meantime.
        if !counter.is_unused() {
            return;
        }
        *unused_since = Some(Instant::now());
        self.unused.retain(|x| *x != id);
        self.unused.push_back(id);
        trace!(%id, "peer unused");
        self.schedule_unused_timer(id);
    }

    fn schedule_unused_timer(&mut self, id: EndpointId) {
        let timeout = self.options.idle_timeout;
        let timer: Boxed<EndpointId> = Box::pin(async move {
            n0_future::time::sleep(timeout).await;
            id
        });
        self.unused_timers.push(timer);
    }

    fn handle_unused_timer(&mut self, id: EndpointId) {
        let Some(PeerState::Ready {
            counter,
            unused_since,
            ..
        }) = self.peers.get(&id)
        else {
            // PeerState is no longer what we expect. Either mising entirely
            // or Connecting. In either case, we must not do anything.
            return;
        };
        if !counter.is_unused() {
            return;
        }
        let Some(since) = unused_since else { return };
        if since.elapsed() >= self.options.idle_timeout {
            trace!(%id, "unused timeout, removing");
            self.remove_peer_inner(id);
        }
    }

    fn remove_peer(&mut self, id: EndpointId) {
        self.remove_peer_inner(id);
    }

    fn remove_peer_inner(&mut self, id: EndpointId) {
        self.unused.retain(|x| *x != id);
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
    id: EndpointId,
    unused_tx: mpsc::UnboundedSender<EndpointId>,
}

#[derive(Debug, Clone)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new(id: EndpointId, unused_tx: mpsc::UnboundedSender<EndpointId>) -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                count: AtomicUsize::new(0),
                id,
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
            let _ = self.inner.unused_tx.send(self.inner.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use iroh::{
        EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr,
        address_lookup::MemoryLookup,
        endpoint::{Connection, presets},
        protocol::{AcceptError, ProtocolHandler, Router},
    };
    use n0_error::{AnyError, Result, StdResultExt};
    use n0_future::{BufferedStreamExt, StreamExt, io, stream};
    use testresult::TestResult;
    use tracing::trace;

    use super::{ConnectionPool, OnConnected, Options, PoolConnectError};

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
        use std::sync::atomic::{AtomicUsize, Ordering};
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
        use std::time::Instant;

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
        use std::time::Instant;

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

    /// Checks that in-flight connection attempts do not count towards
    /// [`Options::max_connections`], so that peers we are still trying to reach
    /// cannot starve unrelated peers of a slot.
    ///
    /// Currently fails: `Actor::handle_request` sizes the pool with
    /// `self.peers.len()`, and `peers` also holds `PeerState::Connecting`
    /// entries, so `max_connections` attempts against unreachable peers make
    /// every other peer fail with `TooManyConnections` until `connect_timeout`
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
}
