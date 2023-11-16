use bytes::Bytes;
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
    pool::Pooled,
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags},
    utils::{BatchItem, Batched},
};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    iter,
    path::PathBuf,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio::time;

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
        let mut hm: HashMap<String, Vec<Path>> = HashMap::new();
        hm.insert(
            "10s".to_string(),
            vec!["/foo/**", "/time/**"]
                .into_iter()
                .map(|x| Into::<Path>::into(x))
                .collect(),
        );
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

struct Ctx {
    config: Config,
    auth: DesiredAuth,
    delay_config: DelayConfig,
    globset_by_delay: HashMap<Duration, GlobSet>,
    globset: GlobSet,
    global_base: Path,
    sender_updates: Sender<Pooled<Vec<(SubId, Event)>>>,
    paths: HashMap<SubId, Path>,
    subscriptions: HashMap<Path, Dval>,
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
        let mut all_paths = Vec::with_capacity(delay_config.delay.values().map(|p|p.len()).sum());
        let mut ct_by_path = HashMap::new();
        for (delay, paths) in &delay_config.delay {
            let dur = parse_duration(&*delay)?;
            let mut delay_paths: Vec<Glob> = Vec::with_capacity(paths.len());
            for p in paths {
                let glob =
                    if Glob::is_glob(&p) {
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
        let global_base = Path::from(ArcStr::from(all_paths.iter().map(|g|g.base().clone()).fold("/", |a, b| Path::lcp(a, b))));
        Ok(Ctx {
            config,
            auth,
            delay_config,
            globset_by_delay: globsets,
            globset: GlobSet::new(false, all_paths)?,
            global_base,
            sender_updates,
            paths: HashMap::new(),
            subscriber,
            subscriptions: HashMap::new(),
            updates: Batched::new(updates, 100_000),
        })
    }

    async fn check_resolver(&mut self) -> Result<()> {
        let resolver = ResolverRead::new(self.config, self.auth);

        let mut ct = ChangeTracker::new(self.global_base);

        loop {
            if resolver.check_changed(&mut ct).await.context("check changed")? {
                for b in resolver.list_matching(&self.globset).await.unwrap().iter() {
                    for p in b.iter() {
                        
                    }
                }
            } else {
                time(sleep(Duration::from_secs(5))).await;
            }
        }
        let globs = GlobSet::new(no_structure, iter::once(glob)).unwrap();
        let mut paths = HashSet::new();

        loop {
            if resolver.check_changed(&mut ct).await.context("check changed")? {
                for b in resolver.list_matching(&globs).await.unwrap().iter() {
                    for p in b.iter() {
                        if !paths.contains(p) {
                            paths.insert(p.clone());
                            println!("{}", p);
                        }
                    }
                }
            }
            time::sleep(Duration::from_secs(5)).await
        }
        Ok(())
    }

    fn remove_subscription(&mut self, path: &str) {
        if let Some(dv) = self.subscriptions.remove(path) {
            self.subscribe_ts.remove(path);
            self.paths.remove(&dv.id());
        }
    }

    fn add_subscription(&mut self, path: &Path) -> &Dval {
        let subscriptions = &mut self.subscriptions;
        let paths = &mut self.paths;
        let subscriber = &self.subscriber;
        let sender_updates = self.sender_updates.clone();
        subscriptions.entry(path.clone()).or_insert_with(|| {
            let s = subscriber.subscribe(path.clone());
            paths.insert(s.id(), path.clone());
            s.updates(
                UpdatesFlags::BEGIN_WITH_LAST | UpdatesFlags::STOP_COLLECTING_LAST,
                sender_updates,
            );
            s
        })
    }

    async fn check_timeouts(&mut self, timeout: Duration) -> Result<()> {
        let mut failed = Vec::new();
        for (path, started) in &self.subscribe_ts {
            if started.elapsed() > timeout {
                failed.push(path.clone())
            }
        }
        for path in failed {
            eprintln!(
                "WARNING: subscription to {} did not succeed before timeout and will be canceled",
                &path
            );
            self.remove_subscription(&path)
        }
        Ok(())
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
                        // Out { raw: self.raw, path: &**path, value }
                        //   .write(&mut self.to_stdout)?;
                    }
                }
            }
        })
    }
}

pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
    let mut ctx = Ctx::new(cfg, subscriber, auth, p);
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
