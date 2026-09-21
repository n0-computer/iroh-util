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
use std::{
    collections::{HashMap, VecDeque},
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
    FuturesUnordered, MaybeFuture, Stream, StreamExt,
    future::{self},
    time::Duration,
};
use tokio::sync::{
    Notify,
    mpsc::{self, error::SendError as TokioSendError},
    oneshot,
};
use tracing::{debug, error, trace};

pub type OnConnected = Arc<
    dyn Fn(&Endpoint, &ConnectionHandle) -> n0_future::future::Boxed<io::Result<()>> + Send + Sync,
>;

/// Close reason for a superseded connection that nothing used for a while.
///
/// See [`ConnectionPool::handle_connection`].
pub const CLOSE_SUPERSEDED: &[u8] = b"superseded";

/// Configuration options for the connection pool
#[derive(derive_more::Debug, Clone)]
pub struct Options {
    /// How long to keep idle connections around.
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
#[derive(Debug, Clone)]
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

    /// Returns whether a newer connection to the same endpoint has taken this
    /// one's place.
    ///
    /// A superseded connection stays open for as long as it is used, but new
    /// work should move to the current one, which [`ConnectionPool::get_or_connect`]
    /// returns: the old one may lead to an endpoint that has since restarted,
    /// dead without us having noticed yet.
    pub fn is_superseded(&self) -> bool {
        self.permit.inner.superseded.load(Ordering::SeqCst)
    }

    /// Bundles `value` with this reference, so the connection stays in use for
    /// as long as `value` does.
    ///
    /// See [`Guarded`].
    pub fn guard<T>(self, value: T) -> Guarded<T> {
        Guarded { value, conn: self }
    }
}

/// A value that keeps a pooled connection in use for as long as it lives.
///
/// The pool counts a connection as in use while any [`ConnectionRef`] to it is
/// alive, and closes it once nothing has used it for a while. Anything that
/// works on the connection for longer than a single call -- a stream, or a
/// codec wrapped around one -- has to keep a reference for that long. For a
/// connection that [`ConnectionPool::handle_connection`] may later supersede,
/// that includes serving the streams the remote opened, since the remote may
/// keep using that connection. `Guarded` makes the two inseparable.
///
/// Created with [`ConnectionRef::guard`] or [`ConnectionHandle::guard`], and
/// derefs to the value.
#[derive(Debug)]
pub struct Guarded<T> {
    value: T,
    conn: ConnectionRef,
}

impl<T> Guarded<T> {
    /// Returns the reference that keeps the connection in use.
    pub fn connection(&self) -> &ConnectionRef {
        &self.conn
    }

    /// Splits into the value and the reference.
    pub fn into_parts(self) -> (T, ConnectionRef) {
        (self.value, self.conn)
    }
}

impl<T> Deref for Guarded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Guarded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
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

    /// Bundles `value` with a new reference, so the connection stays in use for
    /// as long as `value` does.
    ///
    /// See [`Guarded`].
    pub fn guard<T>(&self, value: T) -> Guarded<T> {
        self.get_ref().guard(value)
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
    ConnectionIdle { id: EndpointId },
    ConnectionShutdown { id: EndpointId },
}

struct RequestRef {
    mode: Mode,
    tx: oneshot::Sender<Result<ConnectionRef, PoolConnectError>>,
}

/// How a [`RequestRef`] wants its connection.
enum Mode {
    /// Use the current connection, dialing one if there is none.
    Connect(EndpointId),
    /// Adopt this incoming connection, superseding the current one.
    Handle(Connection),
}

impl Mode {
    fn remote_id(&self) -> EndpointId {
        match self {
            Mode::Connect(id) => *id,
            Mode::Handle(conn) => conn.remote_id(),
        }
    }
}

struct Context {
    options: Options,
    endpoint: Endpoint,
    owner: ConnectionPool,
    alpn: Vec<u8>,
}

impl Context {
    async fn run_connection_actor(self: Arc<Self>, mode: Mode, mut rx: mpsc::Receiver<RequestRef>) {
        let context = self;
        let node_id = mode.remote_id();
        // One counter per connection rather than per endpoint: a superseded
        // connection has to be able to go idle on its own while the current one
        // is in use.
        let mut counter = ConnectionCounter::new();

        let conn_fut = {
            let context = context.clone();
            let counter = counter.clone();
            async move {
                let conn = match mode {
                    Mode::Handle(conn) => conn,
                    Mode::Connect(node_id) => context
                        .endpoint
                        .connect(node_id, &context.alpn)
                        .await
                        .map_err(PoolConnectError::from)?,
                };
                if let Some(on_connect) = &context.options.on_connected {
                    on_connect(&context.endpoint, &ConnectionHandle::new(&conn, &counter))
                        .await
                        .map_err(PoolConnectError::from)?;
                }
                Result::<Connection, PoolConnectError>::Ok(conn)
            }
        };

        // Connect to the node
        let mut state = n0_future::time::timeout(context.options.connect_timeout, conn_fut)
            .await
            .map_err(|_| e!(PoolConnectError::Timeout))
            .and_then(|r| r);
        let conn_close = match &state {
            Ok(conn) => MaybeFuture::Some(closed(conn.clone())),
            Err(e) => {
                debug!(%node_id, "Failed to connect {e:?}, requesting shutdown");
                if context.owner.close(node_id).await.is_err() {
                    return;
                }
                MaybeFuture::None
            }
        };

        let idle_timer = MaybeFuture::default();
        // Boxed rather than pinned in place, so it can follow `counter` when a new
        // connection supersedes the current one.
        let mut idle_stream = Box::pin(counter.clone().idle_stream());

        tokio::pin!(idle_timer, conn_close);

        loop {
            tokio::select! {
                biased;

                // Handle new work
                handler = rx.recv() => {
                    match handler {
                        Some(RequestRef { mode, tx }) => {
                            assert!(mode.remote_id() == node_id, "Not for me!");
                            let supersedes = match (&mode, &state) {
                                (Mode::Handle(conn), Ok(current)) => {
                                    conn.stable_id() != current.stable_id()
                                }
                                (Mode::Handle(_), Err(_)) => true,
                                (Mode::Connect(_), _) => false,
                            };
                            if let (Mode::Handle(conn), true) = (mode, supersedes) {
                                debug!(%node_id, "incoming connection supersedes the current one");
                                let new_counter = ConnectionCounter::new();
                                if let Some(on_connect) = &context.options.on_connected {
                                    let handle = ConnectionHandle::new(&conn, &new_counter);
                                    if let Err(err) = on_connect(&context.endpoint, &handle)
                                        .await
                                        .map_err(PoolConnectError::from)
                                    {
                                        tx.send(Err(err)).ok();
                                        continue;
                                    }
                                }
                                conn_close.as_mut().set_future(closed(conn.clone()));
                                let old_counter = std::mem::replace(&mut counter, new_counter);
                                old_counter.inner.superseded.store(true, Ordering::SeqCst);
                                idle_stream = Box::pin(counter.clone().idle_stream());
                                // Not closed here: the remote may still be using it.
                                if let Ok(old_conn) = std::mem::replace(&mut state, Ok(conn)) {
                                    let grace = context.options.idle_timeout;
                                    n0_future::task::spawn(close_when_unused(
                                        old_conn,
                                        old_counter,
                                        grace,
                                    ));
                                }
                            }
                            match &state {
                                Ok(state) => {
                                    let res = ConnectionRef::new(state.clone(), counter.get_one());
                                    debug!(%node_id, count = counter.current(), "Handing out ConnectionRef");

                                    // clear the idle timer
                                    idle_timer.as_mut().set_none();
                                    tx.send(Ok(res)).ok();
                                }
                                Err(cause) => {
                                    tx.send(Err(cause.clone())).ok();
                                }
                            }
                        }
                        None => {
                            // Channel closed - exit
                            break;
                        }
                    }
                }

                _ = &mut conn_close => {
                    // connection was closed by somebody, notify owner that we should be removed
                    context.owner.close(node_id).await.ok();
                }

                _ = idle_stream.next() => {
                    if !counter.is_idle() {
                        continue;
                    };
                    // notify the pool that we are idle.
                    trace!(%node_id, "Idle");
                    if context.owner.idle(node_id).await.is_err() {
                        // If we can't notify the pool, we are shutting down
                        break;
                    }
                    // set the idle timer
                    idle_timer.as_mut().set_future(n0_future::time::sleep(context.options.idle_timeout));
                }

                // Idle timeout - request shutdown
                _ = &mut idle_timer => {
                    trace!(%node_id, "Idle timer expired, requesting shutdown");
                    context.owner.close(node_id).await.ok();
                    // Don't break here - wait for main actor to close our channel
                }
            }
        }

        if let Ok(connection) = state {
            let reason = if counter.is_idle() { b"idle" } else { b"drop" };
            connection.close(0u32.into(), reason);
        }

        trace!(%node_id, "Connection actor shutting down");
    }
}

/// Resolves when `conn` closes.
///
/// A named function rather than an `async` block, so that the connection
/// actor's `conn_close` future has one type for the first connection and for
/// every connection that supersedes it.
async fn closed(conn: Connection) -> iroh::endpoint::ConnectionError {
    conn.closed().await
}

/// Closes a superseded connection once nothing has used it for `grace`.
///
/// Closing it as soon as it is superseded would be wrong. Two endpoints that
/// dial each other at once each keep the connection they saw last, and the two
/// may disagree, so each side can be receiving on the connection the other side
/// superseded. The connection therefore stays open while any [`ConnectionRef`]
/// to it is alive, which includes those serving the remote's streams (see
/// [`ConnectionHandle`]).
///
/// Detached rather than owned by the connection actor, because the connection
/// can outlive the actor that superseded it. It ends when the connection
/// closes, whoever closes it.
async fn close_when_unused(conn: Connection, counter: ConnectionCounter, grace: Duration) {
    loop {
        tokio::select! {
            _ = conn.closed() => return,
            _ = counter.idle() => {}
        }
        tokio::select! {
            _ = conn.closed() => return,
            _ = n0_future::time::sleep(grace) => {}
        }
        if counter.is_idle() {
            debug!(
                conn_id = conn.stable_id(),
                "closing unused superseded connection"
            );
            conn.close(0u32.into(), CLOSE_SUPERSEDED);
            return;
        }
    }
}

struct Actor {
    rx: mpsc::Receiver<ActorMessage>,
    connections: HashMap<EndpointId, mpsc::Sender<RequestRef>>,
    context: Arc<Context>,
    // idle set (most recent last)
    // todo: use a better data structure if this becomes a performance issue
    idle: VecDeque<EndpointId>,
    // per connection tasks
    tasks: FuturesUnordered<future::Boxed<()>>,
}

impl Actor {
    pub fn new(
        endpoint: Endpoint,
        alpn: &[u8],
        options: Options,
    ) -> (Self, mpsc::Sender<ActorMessage>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                rx,
                connections: HashMap::new(),
                idle: VecDeque::new(),
                context: Arc::new(Context {
                    options,
                    alpn: alpn.to_vec(),
                    endpoint,
                    owner: ConnectionPool { tx: tx.clone() },
                }),
                tasks: FuturesUnordered::new(),
            },
            tx,
        )
    }

    fn add_idle(&mut self, id: EndpointId) {
        self.remove_idle(id);
        self.idle.push_back(id);
    }

    fn remove_idle(&mut self, id: EndpointId) {
        self.idle.retain(|&x| x != id);
    }

    fn pop_oldest_idle(&mut self) -> Option<EndpointId> {
        self.idle.pop_front()
    }

    fn remove_connection(&mut self, id: EndpointId) {
        self.connections.remove(&id);
        self.remove_idle(id);
    }

    async fn handle_msg(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::RequestRef(mut msg) => {
                let id = msg.mode.remote_id();
                self.remove_idle(id);
                // Try to send to existing connection actor
                if let Some(conn_tx) = self.connections.get(&id) {
                    if let Err(TokioSendError(e)) = conn_tx.send(msg).await {
                        msg = e;
                    } else {
                        return;
                    }
                    // Connection actor died, remove it
                    self.remove_connection(id);
                }

                // No connection actor or it died - check limits
                if self.connections.len() >= self.context.options.max_connections {
                    if let Some(idle) = self.pop_oldest_idle() {
                        // remove the oldest idle connection to make room for one more
                        trace!("removing oldest idle connection {}", idle);
                        self.connections.remove(&idle);
                    } else {
                        msg.tx
                            .send(Err(e!(PoolConnectError::TooManyConnections)))
                            .ok();
                        return;
                    }
                }
                let (conn_tx, conn_rx) = mpsc::channel(100);
                self.connections.insert(id, conn_tx.clone());

                let context = self.context.clone();

                // The new actor starts from the request's mode: it dials, or adopts
                // the incoming connection. The request itself then only asks for a
                // reference to whatever the actor ended up with.
                let mode = std::mem::replace(&mut msg.mode, Mode::Connect(id));
                self.tasks
                    .push(Box::pin(context.run_connection_actor(mode, conn_rx)));

                // Send the handler to the new actor
                if conn_tx.send(msg).await.is_err() {
                    error!(%id, "Failed to send handler to new connection actor");
                    self.connections.remove(&id);
                }
            }
            ActorMessage::ConnectionIdle { id } => {
                self.add_idle(id);
                trace!(%id, "connection idle");
            }
            ActorMessage::ConnectionShutdown { id } => {
                // Remove the connection from our map - this closes the channel
                self.remove_connection(id);
                trace!(%id, "removed connection");
            }
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;

                msg = self.rx.recv() => {
                    if let Some(msg) = msg {
                        self.handle_msg(msg).await;
                    } else {
                        break;
                    }
                }

                _ = self.tasks.next(), if !self.tasks.is_empty() => {}
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

        // Spawn the main actor
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
        self.request_ref(Mode::Connect(id)).await
    }

    /// Adopts an incoming connection and returns a reference to it.
    ///
    /// The connection becomes the current one for its endpoint: later
    /// [`Self::get_or_connect`] calls return it. A connection it supersedes is
    /// not closed right away, since the remote may still be using it -- two
    /// endpoints that dial each other at once may each keep a different one. It
    /// is closed with [`CLOSE_SUPERSEDED`] once no [`ConnectionRef`] to it has
    /// been alive for [`Options::idle_timeout`], and [`ConnectionRef::is_superseded`]
    /// tells holders to move on.
    ///
    /// [`Options::on_connected`] runs for the connection as it would for a
    /// dialed one.
    pub async fn handle_connection(
        &self,
        conn: Connection,
    ) -> std::result::Result<ConnectionRef, PoolConnectError> {
        self.request_ref(Mode::Handle(conn)).await
    }

    async fn request_ref(
        &self,
        mode: Mode,
    ) -> std::result::Result<ConnectionRef, PoolConnectError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::RequestRef(RequestRef { mode, tx }))
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

    /// Notify the connection pool that a connection is idle.
    ///
    /// Should only be called from connection handlers.
    pub(crate) async fn idle(
        &self,
        id: EndpointId,
    ) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionIdle { id })
            .await
            .map_err(|_| e!(ConnectionPoolError::Shutdown))?;
        Ok(())
    }
}

#[derive(Debug)]
struct ConnectionCounterInner {
    count: AtomicUsize,
    notify: Notify,
    /// Set once a newer connection to the same endpoint took this one's place.
    superseded: AtomicBool,
}

#[derive(Debug, Clone)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new() -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                count: Default::default(),
                notify: Notify::new(),
                superseded: AtomicBool::new(false),
            }),
        }
    }

    fn current(&self) -> usize {
        self.inner.count.load(Ordering::SeqCst)
    }

    /// Increase the connection count and return a guard for the new connection
    fn get_one(&self) -> OneConnection {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        OneConnection {
            inner: self.inner.clone(),
        }
    }

    fn is_idle(&self) -> bool {
        self.inner.count.load(Ordering::SeqCst) == 0
    }

    /// Resolves once the count is zero.
    async fn idle(&self) {
        loop {
            // Registered before the check, so a drop to zero in between still
            // wakes us.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_idle() {
                return;
            }
            notified.await;
        }
    }

    /// Infinite stream that yields when the connection is briefly idle.
    ///
    /// Note that you still have to check if the connection is still idle when
    /// you get the notification.
    ///
    /// Also note that this stream is triggered on [OneConnection::drop], so it
    /// won't trigger initially even though a [ConnectionCounter] starts up as
    /// idle.
    fn idle_stream(self) -> impl Stream<Item = ()> {
        n0_future::stream::unfold(self, |c| async move {
            c.inner.notify.notified().await;
            Some(((), c))
        })
    }
}

/// Guard for one connection
#[derive(Debug)]
struct OneConnection {
    inner: Arc<ConnectionCounterInner>,
}

impl Clone for OneConnection {
    fn clone(&self) -> Self {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        OneConnection {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for OneConnection {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.notify.notify_waiters();
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

    use super::{
        CLOSE_SUPERSEDED, ConnectionHandle, ConnectionPool, ConnectionRef, Guarded, OnConnected,
        Options, PoolConnectError,
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
    // #[traced_test]
    async fn connection_pool_errors() -> TestResult<()> {
        // set up static address lookup for all addrs
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
            // trying to connect to a non-existing id will fail with ConnectError
            // because we don't have any information about the endpoint.
            assert!(matches!(res, Err(PoolConnectError::ConnectError { .. })));
        }
        {
            let non_listening = SecretKey::from_bytes(&[0; 32]).public();
            // make up fake node info
            address_lookup.add_endpoint_info(EndpointAddr {
                id: non_listening,
                addrs: vec![TransportAddr::Ip("127.0.0.1:12121".parse().unwrap())]
                    .into_iter()
                    .collect(),
            });
            // trying to connect to an id for which we have info, but the other
            // end is not listening, will lead to a timeout.
            let res = client.echo(non_listening, b"Hello, world!".to_vec()).await;
            assert!(matches!(res, Err(PoolConnectError::Timeout { .. })));
        }
        endpoint.close().await;
        Ok(())
    }

    #[tokio::test]
    // #[traced_test]
    async fn connection_pool_smoke() -> TestResult<()> {
        let n = 32;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        // build a client endpoint that can resolve all the endpoint ids
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

    /// Tests that idle connections are being reclaimed to make room if we hit the
    /// maximum connection limit.
    #[tokio::test]
    // #[traced_test]
    async fn connection_pool_idle() -> TestResult<()> {
        let n = 32;
        let (ids, routers, address_lookup) = echo_servers(n).await?;
        // build a client endpoint that can resolve all the endpoint ids
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
    ///
    /// This is a basic smoke test that on_connected gets called at all.
    #[tokio::test]
    // #[traced_test]
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
    // #[traced_test]
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

    /// Check that when a connection is closed, the pool will give you a new
    /// connection next time you want one.
    ///
    /// This test fails if the connection watch is disabled.
    #[tokio::test]
    // #[traced_test]
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
        assert!(
            matches!(
                &err,
                iroh::endpoint::ConnectionError::ApplicationClosed(frame)
                    if frame.reason == CLOSE_SUPERSEDED
            ),
            "closed for the wrong reason: {err:?}"
        );
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
        assert!(
            matches!(
                &err,
                iroh::endpoint::ConnectionError::ApplicationClosed(frame)
                    if frame.reason == CLOSE_SUPERSEDED
            ),
            "closed for the wrong reason: {err:?}"
        );
        Ok(())
    }

    /// A [`Guarded`] value keeps a superseded connection open until it is
    /// dropped.
    #[tokio::test]
    async fn guarded_value_keeps_connection_in_use() -> TestResult<()> {
        let s = Superseded::new(short_idle_options()).await?;
        let guarded: Guarded<&str> = s.first_ref.guard("a stream");
        assert_eq!(*guarded, "a stream");
        assert!(guarded.connection().is_superseded());

        n0_future::time::sleep(SHORT_IDLE * 5).await;
        assert!(
            s.first.close_reason().is_none(),
            "closed although a guarded value is alive"
        );

        drop(guarded);
        let err = tokio::time::timeout(SHORT_IDLE * 10, s.first.closed()).await?;
        assert!(
            matches!(
                &err,
                iroh::endpoint::ConnectionError::ApplicationClosed(frame)
                    if frame.reason == CLOSE_SUPERSEDED
            ),
            "closed for the wrong reason: {err:?}"
        );
        Ok(())
    }
}
