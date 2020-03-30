use crate::{
    auth::{
        sysgmapper::Mapper,
        syskrb5::{sys_krb5, ServerCtx},
        Krb5, Krb5Ctx, Krb5ServerCtx, PMap, Permissions, UserDb, UserInfo, ANONYMOUS,
    },
    channel::Channel,
    config,
    path::Path,
    protocol::{resolver, Id},
    resolver_store::Store,
};
use arc_swap::{ArcSwap, Guard};
use failure::Error;
use futures::{prelude::*, select};
use fxhash::FxBuildHasher;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::{
    collections::HashMap,
    io, mem,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task,
    time::{self, Instant},
};

type ClientInfo = Option<oneshot::Sender<()>>;

struct SecStoreInner {
    next: Id,
    ctxts: HashMap<Id, ServerCtx, FxBuildHasher>,
    userdb: UserDb<Mapper>,
}

impl SecStoreInner {
    fn get(&mut self, id: &Id) -> Option<ServerCtx> {
        self.ctxts.get(id).and_then(|ctx| match ctx.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(ctx.clone()),
            _ => None,
        })
    }

    fn delete(&mut self, id: &Id) {
        self.ctxts.remove(id);
    }

    fn id(&mut self) -> Id {
        self.next.take()
    }

    fn gc(&mut self) -> Result<(), Error> {
        let mut delete = SmallVec::<[Id; 64]>::new();
        for (id, ctx) in self.ctxts.iter() {
            if ctx.ttl()?.as_secs() == 0 {
                delete.push(*id);
            }
        }
        for id in delete.into_iter() {
            self.ctxts.remove(&id);
        }
        Ok(())
    }

    fn ifo(&mut self, user: &str) -> Result<Arc<UserInfo>, Error> {
        self.userdb.ifo(user)
    }
}

#[derive(Clone)]
struct SecStore {
    principal: Arc<String>,
    pmap: ArcSwap<PMap>,
    store: Arc<Mutex<SecStoreInner>>,
}

impl SecStore {
    fn new(principal: String, pmap: PMap) -> Self {
        SecStore {
            principal: Arc::new(principal),
            pmap: ArcSwap::from(Arc::new(pmap)),
            store: Arc::new(Mutex::new(SecStoreInner {
                next: Id::zero(),
                ctxts: HashMap::with_hasher(FxBuildHasher::default()),
            })),
        }
    }

    fn pmap(&self) -> Guard<'static, Arc<PMap>> {
        self.pmap.load()
    }

    fn update_pmap(&self, pmap: PMap) {
        self.pmap.swap(Arc::new(pmap));
    }

    fn get(&self, id: &Id) -> Option<ServerCtx> {
        let mut inner = self.store.lock();
        inner.get(id)
    }

    fn delete(&self, id: &Id) {
        let mut inner = self.store.lock();
        inner.delete(id);
    }

    fn create(&self, tok: &[u8]) -> Result<(Id, ServerCtx, Option<Vec<u8>>), Error> {
        let ctx = sys_krb5.create_server_ctx(self.principal.as_bytes())?;
        let tok = ctx.step(Some(tok))?.map(|b| Vec::from(&*b));
        let mut inner = self.store.lock();
        let id = inner.id();
        inner.ctxts.insert(id, ctx);
        Ok((id, ctx, tok))
    }

    fn gc(&self) -> Result<(), Error> {
        let mut inner = self.store.lock();
        Ok(inner.gc()?)
    }

    fn ifo(&self, user: &str) -> Result<Arc<UserInfo>, Error> {
        let mut inner = self.store.lock();
        Ok(inner.ifo(user)?)
    }
}

fn allowed_for(
    pmap: Option<&PMap>,
    paths: &Vec<Path>,
    perm: Permissions,
    user: &UserInfo,
) -> bool {
}

fn handle_batch(
    store: &Store<ClientInfo>,
    secstore: Option<&SecStore>,
    user: &UserInfo,
    msgs: impl Iterator<Item = resolver::To>,
    con: &mut Channel,
    wa: Option<SocketAddr>,
) -> Result<(), Error> {
    let pmap = secstore.map(|s| s.pmap());
    let allowed_for = |paths: &Vec<Path>, perm: Permissions| -> bool {
        pmap.map(|pm| pm.allowed_forall(paths.iter().map(|p| &*p), perm, user))
            .unwrap_or(true)
    };
    match wa {
        None => {
            let s = store.read();
            for m in msgs {
                match m {
                    resolver::To::Heartbeat => (),
                    resolver::To::Resolve(paths) => {
                        if allowed_for(&paths, Permissions::SUBSCRIBE) {
                            let res = paths.iter().map(|p| s.resolve(p)).collect();
                            con.queue_send(&resolver::From::Resolved(res))?
                        } else {
                            con.queue_send(&resolver::From::Error("denied".into()))?
                        }
                    }
                    resolver::To::List(path) => {
                        let allowed = pmap
                            .map(|pm| pm.allowed(&*path, user, Permissions::LIST))
                            .unwrap_or(true);
                        if allowed {
                            con.queue_send(&resolver::From::List(s.list(&path)))?
                        } else {
                            con.queue_send(&resolver::From::Error("denied".into()))
                        }
                    }
                    resolver::To::Publish(_)
                    | resolver::To::Unpublish(_)
                    | resolver::To::Clear => {
                        con.queue_send(&resolver::From::Error("read only".into()))?
                    }
                }
            }
        }
        Some(write_addr) => {
            let mut s = store.write();
            for m in msgs {
                match m {
                    resolver::To::Heartbeat => (),
                    resolver::To::Resolve(_) | resolver::To::List(_) => {
                        con.queue_send(&resolver::From::Error("write only".into()))?
                    }
                    resolver::To::Publish(paths) => {
                        if !paths.iter().all(Path::is_absolute) {
                            con.queue_send(&resolver::From::Error(
                                "absolute paths required".into(),
                            ))?
                        } else {
                            if allowed_for(&paths, Permissions::PUBLISH) {
                                for path in paths {
                                    s.publish(path, write_addr);
                                }
                                con.queue_send(&resolver::From::Published)?
                            } else {
                                con.queue_send(&resolver::From::Error("denied".into()))
                            }
                        }
                    }
                    resolver::To::Unpublish(paths) => {
                        if allowed_for(&paths, Permissions::PUBLISH) {
                            for path in paths {
                                s.unpublish(path, write_addr);
                            }
                            con.queue_send(&resolver::From::Unpublished)?
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

static HELLO_TIMEOUT: Duration = Duration::from_secs(10);
static READER_TTL: Duration = Duration::from_secs(120);
static MAX_TTL: u64 = 3600;

struct ClientState {
    ctx: Option<(Id, ServerCtx)>,
    uifo: Arc<UserInfo>,
    write_addr: Option<SocketAddr>,
    ttl: Duration,
}

async fn hello_client(
    store: &Store<ClientInfo>,
    con: &mut Channel,
    origin: SocketAddr,
    secstore: Option<&SecStore>,
    tx_stop: oneshot::Sender<()>,
) -> Result<ClientState, Error> {
    let hello: resolver::ClientHello =
        time::timeout(HELLO_TIMEOUT, con.receive()).await??;
    let (ttl, ttl_expired, write_addr, auth) = match hello {
        resolver::ClientHello::ReadOnly(auth) => (READER_TTL, false, None, auth),
        resolver::ClientHello::WriteOnly {
            ttl,
            write_addr,
            auth,
        } => {
            if ttl <= 0 || ttl > MAX_TTL {
                bail!("invalid ttl")
            }
            let ttl = Duration::from_secs(ttl);
            if write_addr.ip() != origin.ip() {
                bail!("invalid publisher ip addr")
            }
            (ttl, true, Some(write_addr), auth)
        }
    };
    let ctx = {
        let (tok, ctx) = match secstore {
            None => (None, None),
            Some(ref secstore) => match auth {
                resolver::ClientAuth::Anonymous => (None, None),
                resolver::ClientAuth::Reuse(id) => match secstore.get(&id) {
                    None => bail!("invalid security context id"),
                    Some(ctx) => (None, Some((id, ctx))),
                },
                resolver::ClientAuth::Token(tok) => {
                    let (id, ctx, tok) = secstore.create(&tok)?;
                    (tok, Some((id, ctx)))
                }
            },
        };
        let auth = match tok {
            None => match &ctx {
                None => resolver::ServerAuth::Anonymous,
                Some(_) => resolver::ServerAuth::Reused,
            },
            Some(tok) => match &ctx {
                None => unreachable!(),
                Some((id, _)) => resolver::ServerAuth::Accepted(tok, *id),
            },
        };
        time::timeout(
            HELLO_TIMEOUT,
            con.send_one(&resolver::ServerHello { ttl_expired, auth }),
        )
        .await??;
        ctx
    };
    let uifo = match (secstore, ctx) {
        (None, _) | (_, None) => ANONYMOUS.clone(),
        (Some(ref secstore), Some((_, ref ctx))) => secstore.ifo(&ctx.client()?)?,
    };
    if let Some(write_addr) = write_addr {
        let mut store = store.write();
        let clinfos = store.clinfo_mut();
        match clinfos.get_mut(&write_addr) {
            None => {
                clinfos.insert(write_addr, Some(tx_stop));
            }
            Some(cl) => {
                if let Some(old_stop) = mem::replace(cl, Some(tx_stop)) {
                    let _ = old_stop.send(());
                }
            }
        }
    }
    Ok(ClientState {
        ctx,
        uifo,
        write_addr,
        ttl,
    })
}

async fn client_loop(
    store: Store<ClientInfo>,
    s: TcpStream,
    origin: SocketAddr,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
) -> Result<(), Error> {
    // CR estokes: require that a write client asserting write addr ip
    // is actually coming from that ip.
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    let (tx_stop, rx_stop) = oneshot::channel();
    let state = hello_client(&store, &mut con, origin, secstore.as_ref(), tx_stop).await?;
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = Vec::new();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + state.ttl, state.ttl).fuse();
    async fn receive_batch(
        con: &mut Option<Channel>,
        batch: &mut Vec<resolver::To>,
    ) -> Result<(), io::Error> {
        match con {
            Some(ref mut con) => con.receive_batch(batch).await,
            None => future::pending().await,
        }
    }
    loop {
        select! {
            _ = server_stop => break Ok(()),
            _ = rx_stop => break Ok(()),
            m = receive_batch(&mut con, &mut batch).fuse() => match m {
                Err(e) => {
                    batch.clear();
                    con = None;
                    // CR estokes: use proper log module
                    println!("error reading message: {}", e)
                },
                Ok(()) => {
                    act = true;
                    let c = con.as_mut().unwrap();
                    let r = handle_batch(
                        &store, secstore.as_ref(), &state.uifo,
                        batch.drain(..), c, state.write_addr
                    );
                    match r {
                        Err(_) => { con = None },
                        Ok(()) => match c.flush().await {
                            Err(_) => { con = None }, // CR estokes: Log this
                            Ok(()) => ()
                        }
                    }
                }
            },
            _ = timeout.next() => {
                if act {
                    act = false;
                } else {
                    if let Some(ref secstore) = secstore {
                        if let Some((id, _)) = state.ctx {
                            secstore.delete(&id);
                        }
                    }
                    if let Some(write_addr) = state.write_addr {
                        let mut store = store.write();
                        if let Some(ref mut cl) = store.clinfo_mut().remove(&write_addr) {
                            if let Some(stop) = mem::replace(cl, None) {
                                let _ = stop.send(());
                            }
                        }
                        store.unpublish_addr(write_addr);
                        store.gc();
                    }
                    bail!("client timed out");
                }
            },
        }
    }
}

async fn server_loop(
    cfg: config::Resolver,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr, Error> {
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> = Store::new();
    let secstore = match cfg.auth {
        config::Auth::Anonymous => None,
        config::Auth::Krb5 { principal } => Some(SecStore::new(principal.clone())),
    };
    let mut listener = TcpListener::bind(cfg.addr).await?;
    let local_addr = listener.local_addr()?;
    let mut stop = stop.fuse();
    let mut client_stops = Vec::new();
    let _ = ready.send(local_addr);
    loop {
        select! {
            cl = listener.accept().fuse() => match cl {
                Err(_) => (),
                Ok((client, sa)) => {
                    if connections.fetch_add(1, Ordering::Relaxed) < cfg.max_connections {
                        let connections = connections.clone();
                        let published = published.clone();
                        let secstore = secstore.clone();
                        let (tx, rx) = oneshot::channel();
                        client_stops.push(tx);
                        task::spawn(async move {
                            let _ = client_loop(
                                published, client, sa, rx, secstore
                            ).await;
                            connections.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                }
            },
            _ = stop => {
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(local_addr)
            },
        }
    }
}

#[derive(Debug)]
pub struct Server {
    stop: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = mem::replace(&mut self.stop, None) {
            let _ = stop.send(());
        }
    }
}

impl Server {
    pub async fn new(cfg: config::Resolver) -> Result<Server, Error> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, recv_stop, send_ready);
        let local_addr = select! {
            a = task::spawn(tsk).fuse() => a??,
            a = recv_ready.fuse() => a?,
        };
        Ok(Server {
            stop: Some(send_stop),
            local_addr,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

#[cfg(test)]
mod test {
    use crate::{
        config,
        path::Path,
        resolver::{ReadOnly, Resolver, WriteOnly},
        resolver_server::Server,
    };
    use std::net::SocketAddr;

    async fn init_server() -> Server {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Server::new(addr, 100, "".into())
            .await
            .expect("start server")
    }

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve() {
        use tokio::runtime::Runtime;
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = init_server().await;
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let cfg = config::Resolver {
                addr: *server.local_addr(),
            };
            let mut w = Resolver::<WriteOnly>::new_w(cfg, paddr).unwrap();
            let mut r = Resolver::<ReadOnly>::new_r(cfg).unwrap();
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            w.publish(paths.clone()).await.unwrap();
            for addrs in r.resolve(paths.clone()).await.unwrap() {
                assert_eq!(addrs.len(), 1);
                assert_eq!(addrs[0], paddr);
            }
            assert_eq!(r.list(p("/")).await.unwrap(), vec![p("/app"), p("/foo")]);
            assert_eq!(
                r.list(p("/foo")).await.unwrap(),
                vec![p("/foo/bar"), p("/foo/baz")]
            );
            assert_eq!(
                r.list(p("/app")).await.unwrap(),
                vec![p("/app/v0"), p("/app/v1")]
            );
        });
    }
}
