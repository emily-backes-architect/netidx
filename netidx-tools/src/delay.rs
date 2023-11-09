use anyhow::{Context, Result};
use arcstr::ArcStr;
use futures::stream::{self, FusedStream};
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    prelude::*,
    select_biased,
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
use std::{collections::HashMap, path::PathBuf, time::*};
use structopt::StructOpt;
use tokio::time;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelayConfig {
    #[serde(flatten)]
    pub delay: HashMap<ArcStr, Vec<Path>>,
}

impl DelayConfig {
    /// Load the config from the specified file
    pub async fn load<F: AsRef<std::path::Path>>(file: F) -> Result<Self> {
        let s = tokio::fs::read(file).await?;
        let f = serde_json::from_slice::<DelayConfig>(&s)?;
        Ok(f)
    }

    pub fn example() -> String {
        todo!();
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
    sender_updates: Sender<Pooled<Vec<(SubId, Event)>>>,
    paths: HashMap<SubId, Path>,
    subscriptions: HashMap<Path, Dval>,
    subscribe_ts: HashMap<Path, Instant>,
    subscriber: Subscriber,
    updates: Batched<Receiver<Pooled<Vec<(SubId, Event)>>>>,
}

impl Ctx {
    fn new(subscriber: Subscriber, p: Params) -> Self {
        let (sender_updates, updates) = mpsc::channel(100);
        Ctx {
            sender_updates,
            paths: HashMap::new(),
            subscriber,
            subscriptions: HashMap::new(),
            subscribe_ts: HashMap::new(),
            updates: Batched::new(updates, 100_000),
        }
    }

    fn remove_subscription(&mut self, path: &str) {
        if let Some(dv) = self.subscriptions.remove(path) {
            self.subscribe_ts.remove(path);
            self.paths.remove(&dv.id());
        }
    }

    fn add_subscription(&mut self, path: &Path) -> &Dval {
        let subscriptions = &mut self.subscriptions;
        let subscribe_ts = &mut self.subscribe_ts;
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
    let mut ctx = Ctx::new(subscriber, p);
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
