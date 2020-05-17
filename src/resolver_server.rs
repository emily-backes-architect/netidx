use crate::{
    auth::{Permissions, UserInfo, ANONYMOUS},
    channel::Channel,
    chars::Chars,
    config::resolver_server::{Auth, Config},
    os::{Krb5Ctx, Krb5ServerCtx, ServerCtx},
    path::Path,
    protocol::{
        publisher,
        resolver::v1::{
            ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, FromRead,
            FromWrite, Resolved, ServerAuthWrite, ServerHelloRead,
            ServerHelloWrite, ToRead, ToWrite,
        },
    },
    resolver_store::{Store, StoreInner},
    secstore::SecStore,
    utils,
};
use anyhow::Result;
use futures::{prelude::*, select_biased};
use fxhash::FxBuildHasher;
use log::{debug, info};
use parking_lot::RwLockWriteGuard;
use rand::Rng;
use std::{
    collections::HashMap,
    convert::TryFrom,
    mem,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task,
    time::{self, Instant},
};

type ClientInfo = Option<oneshot::Sender<()>>;

fn handle_batch_write(
    store: &Store<ClientInfo>,
    con: &mut Channel<ServerCtx>,
    secstore: Option<&SecStore>,
    uifo: &Arc<UserInfo>,
    write_addr: SocketAddr,
    msgs: impl Iterator<Item = ToWrite>,
) -> Result<()> {
    let mut s = store.write();
    let publish = |s: &mut RwLockWriteGuard<StoreInner<ClientInfo>>,
                   con: &mut Channel<ServerCtx>,
                   path: Path,
                   default: bool|
     -> Result<()> {
        if !Path::is_absolute(&*path) {
            con.queue_send(&FromWrite::Error("absolute paths required".into()))?
        } else if let Some(r) = s.check_referral(&path) {
            con.queue_send(&FromWrite::Referral(r))?
        } else {
            let perm =
                if default { Permissions::PUBLISH_DEFAULT } else { Permissions::PUBLISH };
            if secstore.map(|s| s.pmap().allowed(&*path, perm, uifo)).unwrap_or(true) {
                s.publish(path, write_addr, default);
                con.queue_send(&FromWrite::Published)?
            } else {
                con.queue_send(&FromWrite::Denied)?
            }
        }
        Ok(())
    };
    for m in msgs {
        match m {
            ToWrite::Heartbeat => (),
            ToWrite::Publish(path) => publish(&mut s, con, path, false)?,
            ToWrite::PublishDefault(path) => publish(&mut s, con, path, true)?,
            ToWrite::Unpublish(path) => {
                if !Path::is_absolute(&*path) {
                    con.queue_send(&FromWrite::Error("absolute paths required".into()))?
                } else if let Some(r) = s.check_referral(&path) {
                    con.queue_send(&FromWrite::Referral(r))?
                } else {
                    s.unpublish(path, write_addr);
                    con.queue_send(&FromWrite::Unpublished)?
                }
            }
            ToWrite::Clear => {
                s.unpublish_addr(write_addr);
                s.gc();
                con.queue_send(&FromWrite::Unpublished)?
            }
        }
    }
    Ok(())
}

async fn client_loop_write(
    cfg: Arc<Config>,
    store: Store<ClientInfo>,
    con: Channel<ServerCtx>,
    secstore: Option<SecStore>,
    server_stop: oneshot::Receiver<()>,
    rx_stop: oneshot::Receiver<()>,
    uifo: Arc<UserInfo>,
    write_addr: SocketAddr,
) -> Result<()> {
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = Vec::new();
    let mut act = false;
    let mut timeout =
        time::interval_at(Instant::now() + cfg.writer_ttl, cfg.writer_ttl).fuse();
    async fn receive_batch(
        con: &mut Option<Channel<ServerCtx>>,
        batch: &mut Vec<ToWrite>,
    ) -> Result<()> {
        match con {
            Some(ref mut con) => con.receive_batch(batch).await,
            None => future::pending().await,
        }
    }
    loop {
        select_biased! {
            _ = server_stop => break Ok(()),
            _ = rx_stop => break Ok(()),
            _ = timeout.next() => {
                if act {
                    act = false;
                } else {
                    let mut store = store.write();
                    if let Some(ref mut cl) = store.clinfo_mut().remove(&write_addr) {
                        if let Some(stop) = mem::replace(cl, None) {
                            let _ = stop.send(());
                        }
                    }
                    store.unpublish_addr(write_addr);
                    store.gc();
                    bail!("client timed out");
                }
            },
            m = receive_batch(&mut con, &mut batch).fuse() => match m {
                Err(e) => {
                    batch.clear();
                    con = None;
                    info!("write client loop error reading message: {}", e)
                },
                Ok(()) => {
                    act = true;
                    let c = con.as_mut().unwrap();
                    let r = handle_batch_write(
                        &store,
                        c,
                        secstore.as_ref(),
                        &uifo,
                        write_addr,
                        batch.drain(..)
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
        }
    }
}

async fn hello_client_write(
    cfg: Arc<Config>,
    listen_addr: SocketAddr,
    store: Store<ClientInfo>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    resolver_id: SocketAddr,
    hello: ClientHelloWrite,
) -> Result<()> {
    info!("hello_write starting negotiation");
    debug!("hello_write client_hello: {:?}", hello);
    async fn send(
        cfg: &Arc<Config>,
        con: &mut Channel<ServerCtx>,
        hello: ServerHelloWrite,
    ) -> Result<()> {
        Ok(time::timeout(cfg.hello_timeout, con.send_one(&hello)).await??)
    }
    fn salt() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0, u64::max_value() - 2)
    }
    utils::check_addr(hello.write_addr.ip(), &[listen_addr])?;
    let ttl_expired = !store.read().clinfo().contains_key(&hello.write_addr);
    let uifo = match hello.auth {
        ClientAuthWrite::Anonymous => {
            let h = ServerHelloWrite {
                ttl: cfg.writer_ttl.as_secs(),
                ttl_expired,
                resolver_id,
                auth: ServerAuthWrite::Anonymous,
            };
            info!("hello_write accepting Anonymous authentication");
            debug!("hello_write sending hello {:?}", h);
            send(&cfg, &mut con, h).await?;
            ANONYMOUS.clone()
        }
        ClientAuthWrite::Reuse => match secstore {
            None => bail!("authentication not supported"),
            Some(ref secstore) => match secstore.get_write(&hello.write_addr) {
                None => bail!("session not found"),
                Some(ctx) => {
                    let h = ServerHelloWrite {
                        ttl: cfg.writer_ttl.as_secs(),
                        ttl_expired,
                        resolver_id,
                        auth: ServerAuthWrite::Reused,
                    };
                    info!("hello_write reusing krb5 context");
                    debug!("hello_write sending {:?}", h);
                    send(&cfg, &mut con, h).await?;
                    con.set_ctx(ctx.clone()).await;
                    info!("hello_write all traffic now encrypted");
                    secstore.ifo(Some(&ctx.client()?))?
                }
            },
        },
        ClientAuthWrite::Initiate { spn, token } => match secstore {
            None => bail!("authentication not supported"),
            Some(ref secstore) => {
                info!(
                    "hello_write initiating new krb5 context for {:?}",
                    hello.write_addr
                );
                let (ctx, tok) = secstore.create(&token)?;
                let h = ServerHelloWrite {
                    ttl: cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id,
                    auth: ServerAuthWrite::Accepted(tok),
                };
                info!("hello_write created context for {:?}", hello.write_addr);
                debug!("hello_write sending {:?}", h);
                send(&cfg, &mut con, h).await?;
                con.set_ctx(ctx.clone()).await;
                info!("hello_write all traffic now encrypted");
                info!(
                    "hello_write connecting to {:?} for listener ownership check",
                    hello.write_addr
                );
                let mut con: Channel<ServerCtx> = Channel::new(
                    time::timeout(
                        cfg.hello_timeout,
                        TcpStream::connect(hello.write_addr),
                    )
                    .await??,
                );
                time::timeout(cfg.hello_timeout, con.send_one(&1u64)).await??;
                // we will need to select a protocol version here when
                // we have more than one.
                let _version: u64 =
                    time::timeout(cfg.hello_timeout, con.receive()).await??;
                let salt = salt();
                let tok = utils::bytes(&*ctx.wrap(true, &salt.to_be_bytes())?);
                let m = publisher::v1::Hello::ResolverAuthenticate(resolver_id, tok);
                time::timeout(cfg.hello_timeout, con.send_one(&m)).await??;
                match time::timeout(cfg.hello_timeout, con.receive()).await?? {
                    publisher::v1::Hello::Anonymous | publisher::v1::Hello::Token(_) => {
                        bail!("listener ownership check unexpected response")
                    }
                    publisher::v1::Hello::ResolverAuthenticate(_, tok) => {
                        let d = Vec::from(&*ctx.unwrap(&tok)?);
                        let dsalt = u64::from_be_bytes(TryFrom::try_from(&*d)?);
                        if dsalt != salt + 2 {
                            bail!("listener ownership check failed");
                        }
                        let client = ctx.client()?;
                        let uifo = secstore.ifo(Some(&client))?;
                        let spn = spn.unwrap_or(Chars::from(client));
                        info!("hello_write listener ownership check succeeded");
                        secstore.store_write(hello.write_addr, spn, ctx.clone());
                        uifo
                    }
                }
            }
        },
    };
    let (tx_stop, rx_stop) = oneshot::channel();
    {
        let mut store = store.write();
        let clinfos = store.clinfo_mut();
        match clinfos.get_mut(&hello.write_addr) {
            None => {
                clinfos.insert(hello.write_addr, Some(tx_stop));
            }
            Some(cl) => {
                if let Some(old_stop) = mem::replace(cl, Some(tx_stop)) {
                    let _ = old_stop.send(());
                }
            }
        }
    }
    Ok(client_loop_write(
        cfg,
        store,
        con,
        secstore,
        server_stop,
        rx_stop,
        uifo,
        hello.write_addr,
    )
    .await?)
}

fn handle_batch_read(
    store: &Store<ClientInfo>,
    con: &mut Channel<ServerCtx>,
    secstore: Option<&SecStore>,
    uifo: &Arc<UserInfo>,
    id: SocketAddr,
    msgs: impl Iterator<Item = ToRead>,
) -> Result<()> {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
    let s = store.read();
    let sec = secstore.map(|s| s.store.read());
    for m in msgs {
        match m {
            ToRead::Resolve(path) => {
                if let Some(r) = s.check_referral(&path) {
                    con.queue_send(&FromRead::Referral(r))?
                } else {
                    match secstore {
                        None => {
                            let a = Resolved {
                                krb5_spns: HashMap::with_hasher(FxBuildHasher::default()),
                                resolver: id,
                                addrs: s.resolve(&path),
                            };
                            con.queue_send(&FromRead::Resolved(a))?;
                        }
                        Some(ref secstore) => {
                            let perm = secstore.pmap().permissions(&*path, &**uifo);
                            if !perm.contains(Permissions::SUBSCRIBE) {
                                con.queue_send(&FromRead::Denied)?;
                            } else {
                                let mut krb5_spns =
                                    HashMap::with_hasher(FxBuildHasher::default());
                                let addrs = s.resolve_and_sign(
                                    &**sec.as_ref().unwrap(),
                                    &mut krb5_spns,
                                    now,
                                    perm,
                                    &path,
                                )?;
                                let a = Resolved { krb5_spns, resolver: id, addrs };
                                con.queue_send(&FromRead::Resolved(a))?
                            }
                        }
                    }
                }
            }
            ToRead::List(path) => {
                if let Some(r) = s.check_referral(&path) {
                    con.queue_send(&FromRead::Referral(r))?
                } else {
                    let allowed = secstore
                        .map(|s| s.pmap().allowed(&*path, Permissions::LIST, uifo))
                        .unwrap_or(true);
                    if allowed {
                        con.queue_send(&FromRead::List(s.list(&path)))?
                    } else {
                        con.queue_send(&FromRead::Denied)?
                    }
                }
            }
        }
    }
    Ok(())
}

async fn client_loop_read(
    cfg: Arc<Config>,
    store: Store<ClientInfo>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: SocketAddr,
    uifo: Arc<UserInfo>,
) -> Result<()> {
    let mut batch: Vec<ToRead> = Vec::new();
    let mut server_stop = server_stop.fuse();
    let mut act = false;
    let mut timeout =
        time::interval_at(Instant::now() + cfg.reader_ttl, cfg.reader_ttl).fuse();
    loop {
        select_biased! {
            _ = server_stop => break Ok(()),
            _ = timeout.next() => {
                if act {
                    act = false;
                } else {
                    bail!("client timed out");
                }
            }
            m = con.receive_batch(&mut batch).fuse() => {
                m?;
                act = true;
                handle_batch_read(
                    &store,
                    &mut con,
                    secstore.as_ref(),
                    &uifo,
                    id,
                    batch.drain(0..)
                )?;
                con.flush().await?;
            },
        }
    }
}

async fn hello_client_read(
    cfg: Arc<Config>,
    store: Store<ClientInfo>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: SocketAddr,
    hello: ClientAuthRead,
) -> Result<()> {
    async fn send(
        cfg: &Arc<Config>,
        con: &mut Channel<ServerCtx>,
        hello: ServerHelloRead,
    ) -> Result<()> {
        Ok(time::timeout(cfg.hello_timeout, con.send_one(&hello)).await??)
    }
    let uifo = match hello {
        ClientAuthRead::Anonymous => {
            send(&cfg, &mut con, ServerHelloRead::Anonymous).await?;
            ANONYMOUS.clone()
        }
        ClientAuthRead::Reuse(id) => match secstore {
            None => bail!("authentication requested but not supported"),
            Some(ref secstore) => match secstore.get_read(&id) {
                None => bail!("ctx id not found"),
                Some(ctx) => {
                    send(&cfg, &mut con, ServerHelloRead::Reused).await?;
                    con.set_ctx(ctx.clone()).await;
                    secstore.ifo(Some(&ctx.client()?))?
                }
            },
        },
        ClientAuthRead::Initiate(tok) => match secstore {
            None => bail!("authentication requested but not supported"),
            Some(ref secstore) => {
                let (ctx, tok) = secstore.create(&tok)?;
                let id = secstore.store_read(ctx.clone());
                send(&cfg, &mut con, ServerHelloRead::Accepted(tok, id)).await?;
                con.set_ctx(ctx.clone()).await;
                secstore.ifo(Some(&ctx.client()?))?
            }
        },
    };
    Ok(client_loop_read(cfg, store, con, server_stop, secstore, id, uifo).await?)
}

async fn hello_client(
    cfg: Arc<Config>,
    delay_reads: Option<Instant>,
    listen_addr: SocketAddr,
    store: Store<ClientInfo>,
    s: TcpStream,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: SocketAddr,
) -> Result<()> {
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    time::timeout(cfg.hello_timeout, con.send_one(&1u64)).await??;
    // we will use this to select a protocol version when there is more than one
    let _version: u64 = time::timeout(cfg.hello_timeout, con.receive()).await??;
    let hello: ClientHello = time::timeout(cfg.hello_timeout, con.receive()).await??;
    match hello {
        ClientHello::ReadOnly(hello) => {
            if let Some(t) = delay_reads {
                if Instant::now() < t {
                    bail!("no read clients allowed yet");
                }
            }
            Ok(hello_client_read(cfg, store, con, server_stop, secstore, id, hello)
                .await?)
        }
        ClientHello::WriteOnly(hello) => Ok(hello_client_write(
            cfg,
            listen_addr,
            store,
            con,
            server_stop,
            secstore,
            id,
            hello,
        )
        .await?),
    }
}

async fn server_loop(
    cfg: Config,
    delay_reads: bool,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr> {
    let delay_reads =
        if delay_reads { Some(Instant::now() + cfg.writer_ttl) } else { None };
    let cfg = Arc::new(cfg);
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> =
        Store::new(cfg.parent.clone(), cfg.children.clone());
    let secstore = match &cfg.auth {
        Auth::Anonymous => None,
        Auth::Krb5 { spn, permissions } => {
            Some(SecStore::new(spn.clone(), permissions.clone(), &cfg)?)
        }
    };
    let mut listener = TcpListener::bind(cfg.addr).await?;
    let local_addr = listener.local_addr()?;
    let mut stop = stop.fuse();
    let mut client_stops: Vec<oneshot::Sender<()>> = Vec::new();
    let max_connections = cfg.max_connections;
    let id = cfg.addr;
    let _ = ready.send(local_addr);
    loop {
        select_biased! {
            _ = stop => {
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(local_addr)
            },
            cl = listener.accept().fuse() => match cl {
                Err(_) => (),
                Ok((client, _)) => {
                    if connections.fetch_add(1, Ordering::Relaxed) < max_connections {
                        let connections = connections.clone();
                        let published = published.clone();
                        let secstore = secstore.clone();
                        let cfg = cfg.clone();
                        let (tx, rx) = oneshot::channel();
                        client_stops.push(tx);
                        task::spawn(async move {
                            let r = hello_client(
                                cfg,
                                delay_reads,
                                local_addr,
                                published,
                                client,
                                rx,
                                secstore,
                                id
                            ).await;
                            info!("server_loop client connection shutting down {:?}", r);
                            connections.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                }
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
    pub async fn new(cfg: Config, delay_reads: bool) -> Result<Server> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, delay_reads, recv_stop, send_ready);
        let local_addr = select_biased! {
            a = task::spawn(tsk).fuse() => a??,
            a = recv_ready.fuse() => a?,
        };
        Ok(Server { stop: Some(send_stop), local_addr })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}
