use anyhow::{Context, Result};
use arcstr::ArcStr;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    prelude::*,
    select_biased,
};
use netidx::{
    chars::Chars,
    resolver_client::{ChangeTracker, Glob, GlobSet, ResolverRead},
};
use netidx::{
    config::Config,
    path::Path,
    pool::{Pool, Pooled},
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags},
    utils::{BatchItem, Batched},
};
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, iter, path::PathBuf, time::Duration};
use structopt::StructOpt;
use tokio::time;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelayConfig {
    #[serde(flatten)]
    pub delay: HashMap<String, Vec<String>>,
}

impl DelayConfig {
    /// Load the config from the specified file
    pub async fn load<F: AsRef<std::path::Path>>(file: F) -> Result<Self> {
        let s = tokio::fs::read(file).await?;
        let f = serde_json::from_slice::<DelayConfig>(&s)?;
        Ok(f)
    }

    /// Provide an example JSON configuration
    pub fn example() -> String {
        let mut hm: HashMap<String, Vec<String>> = HashMap::new();
        hm.insert("10s".to_string(), vec!["/foo/**".to_string(), "/time/**".to_string()]);
        serde_json::to_string(&DelayConfig { delay: hm })
            .expect("internal example config")
    }

    pub fn delays_for_path(&self, p: Path) -> Result<Vec<Duration>> {
        let mut v: Vec<Duration> = Vec::new();
        for (dur, paths) in self.delay {
            let path_pat = if !Glob::is_glob(&*p) { p.append("*") } else { p };
            let glob = Glob::new(Chars::from(String::from(&*path_pat))).unwrap();
            let mut ct = ChangeTracker::new(Path::from(ArcStr::from(glob.base())));
            let globs = GlobSet::new(false, iter::once(glob)).unwrap();
        }
        Ok(vec![])
    }
}

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(
        short = "d",
        long = "delay-config",
        help = "path to JSON delay config file"
    )]
    pub(crate) delay_config: PathBuf,
}

pub(crate) struct Ctx {
    config: Config,
    auth: DesiredAuth,
    delay_config: DelayConfig,
    globset_by_delay: HashMap<Duration, GlobSet>,
    globset: GlobSet,
    global_base: Path,
    sender_updates: Sender<Pooled<Vec<(SubId, Event)>>>,
    sub_by_path: HashMap<Path, SubId>,
    paths_by_sub: HashMap<SubId, Path>,
    dval_by_path: HashMap<Path, Dval>,
    subscriber: Subscriber,
    updates: Batched<Receiver<Pooled<Vec<(SubId, Event)>>>>,
}

fn parse_duration(s: &str) -> Result<Duration> {
    if let Some(count) = s.strip_suffix("s") {
        Ok(Duration::new(count.parse::<u64>()?, 0))
    } else {
        Err(anyhow!("expected 's' suffix for seconds"))
    }
}

#[derive(Debug)]
struct ResolverChecker {
    task: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    cancel: CancellationToken,
    new_paths: Receiver<Pooled<Vec<Path>>>,
}

impl Ctx {
    async fn new(
        config: Config,
        subscriber: Subscriber,
        auth: DesiredAuth,
        p: Params,
    ) -> Result<Self> {
        let (sender_updates, updates) = mpsc::channel(100);
        let delay_config = DelayConfig::load(p.delay_config).await?;
        let mut globsets: HashMap<Duration, GlobSet> = HashMap::new();
        let mut all_paths =
            Vec::with_capacity(delay_config.delay.values().map(|p| p.len()).sum());
        let mut ct_by_path = HashMap::new();
        for (delay, paths) in &delay_config.delay {
            let dur = parse_duration(&*delay)?;
            let mut delay_paths: Vec<Glob> = Vec::with_capacity(paths.len());
            for p in paths {
                let glob = if Glob::is_glob(&p) {
                    Glob::new(Chars::from(String::from(&*p)))?
                } else {
                    let mut p = p.clone();
                    p.push('*');
                    Glob::new(Chars::from(String::from(&*p)))?
                };
                delay_paths.push(glob.clone());
                all_paths.push(glob.clone());
                let path_for_ct = Path::from(ArcStr::from(glob.base()));
                ct_by_path.insert(path_for_ct.clone(), ChangeTracker::new(path_for_ct));
            }
            globsets.insert(dur, GlobSet::new(false, delay_paths.into_iter())?);
        }
        let global_base = Path::from(ArcStr::from(
            all_paths.iter().map(|g| g.base().clone()).fold("/", |a, b| Path::lcp(a, b)),
        ));
        Ok(Ctx {
            config,
            auth,
            delay_config,
            globset_by_delay: globsets,
            globset: GlobSet::new(false, all_paths)?,
            global_base,
            sender_updates,
            sub_by_path: HashMap::new(),
            paths_by_sub: HashMap::new(),
            subscriber,
            dval_by_path: HashMap::new(),
            updates: Batched::new(updates, 100_000),
        })
    }

    /// Return a cancellable task that checks the resolver for new publishers.
    async fn check_resolver(self) -> Result<ResolverChecker, anyhow::Error> {
        let (mut resolver_update_send, resolver_updates): (
            Sender<Pooled<Vec<Path>>>,
            Receiver<Pooled<Vec<Path>>>,
        ) = mpsc::channel(10);
        let resolver = ResolverRead::new(self.config, self.auth);
        let canceller = CancellationToken::new();
        let cloned_token = canceller.clone();
        let pool: Pool<Vec<Path>> = Pool::new(100, 10 * 1024);
        let mut ct = ChangeTracker::new(self.global_base);
        let resolver_checker: tokio::task::JoinHandle<std::result::Result<(), _>> =
            tokio::spawn(async move {
                loop {
                    let mut to_add: Pooled<Vec<Path>> = pool.take();
                    if resolver
                        .check_changed(&mut ct)
                        .await
                        .context("check resolver for new paths")?
                    {
                        for b in
                            resolver.list_matching(&self.globset).await.unwrap().iter()
                        {
                            for p in b.iter() {
                                if !self.dval_by_path.contains_key(p) {
                                    to_add.push((*p).clone());
                                }
                            }
                        }
                    }
                    // Halt the checker if subscriber dies
                    if let Err(_) = resolver_update_send.try_send(to_add) {
                        break;
                    }
                    // Halt if cancelled, otherwise wait and recheck resolver
                    tokio::select! {
                        _ = cloned_token.cancelled() => {
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
                // Cancelled or subscriber lost, exit loop
                resolver_update_send.close();
                Ok(())
            });
        Ok(ResolverChecker {
            task: resolver_checker,
            cancel: canceller,
            new_paths: resolver_updates,
        })
    }

    fn remove_subscription(&mut self, path: &Path) {
        if let Some(sub) = self.sub_by_path.remove(path) {
            self.paths_by_sub.remove(&sub);
        }
        self.dval_by_path.remove(path);
    }

    fn add_subscription(&mut self, path: &Path) -> &Dval {
        let subscriber = &self.subscriber;
        let sender_updates = self.sender_updates.clone();
        self.sub_by_path.entry(path.clone()).or_insert_with(|| {
            let new_dval = subscriber.subscribe(path.clone());
            self.paths_by_sub.insert(new_dval.id(), path.clone());
            new_dval.updates(
                UpdatesFlags::BEGIN_WITH_LAST | UpdatesFlags::STOP_COLLECTING_LAST,
                sender_updates,
            );
            self.dval_by_path.insert(path.clone(), new_dval);
            new_dval.id()
        });
        self.dval_by_path.get(path).unwrap()
    }

    async fn check_timeouts(&mut self, timeout: Duration) -> Result<()> {
        todo!();
        // scan delayed batches from min heap
    }

    async fn process_update(
        &mut self,
        u: Option<BatchItem<Pooled<Vec<(SubId, Event)>>>>,
    ) -> Result<()> {
        Ok(match u {
            None => unreachable!(), // channel will never close
            Some(BatchItem::EndBatch) => (),
            Some(BatchItem::InBatch(mut batch)) => {
                for (id, value) in batch.drain(..) {
                    if let Some(path) = self.paths.get(&id) {
                        todo!();
                    }
                }
            }
        })
    }
}

pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let subscriber =
        Subscriber::new(cfg.clone(), auth.clone()).context("create subscriber")?;
    let mut ctx = Ctx::new(cfg, subscriber, auth, p).await?;
    let mut tick = time::interval(Duration::from_secs(1));
    loop {
        select_biased! {
            _ = tick.tick().fuse() =>
                match ctx.check_timeouts(Duration::from_secs(30)).await {
                    Ok(()) => (),
                    Err(_) => break,
                }
            ,
            u = ctx.updates.next() => match ctx.process_update(u).await {
                Ok(()) => (),
                Err(_) => break,
            },
        }
    }
    Ok(())
}
