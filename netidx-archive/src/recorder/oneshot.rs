use super::{
    logfile_collection::LogfileCollection,
    publish::{parse_bound, parse_filter},
};
use crate::{
    logfile::{ArchiveReader, BatchItem, Id, Seek, CURSOR_BATCH_POOL},
    recorder::{
        publish::{END_DOC, FILTER_DOC, START_DOC},
        BCastMsg, Config, PublishConfig,
    },
    recorder_client::OneshotReply,
};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::FxHashMap;
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path,
    publisher::{Publisher, Value},
    resolver_client::GlobSet,
};
use netidx_core::utils::pack;
use netidx_protocols::{
    define_rpc,
    rpc::server::{ArgSpec, Proc, RpcCall, RpcReply},
    rpc_err,
};
use std::{ops::Bound, sync::Arc};
use tokio::{sync::broadcast, task::JoinSet};

struct OneshotConfig {
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    filter: GlobSet,
}

impl OneshotConfig {
    fn new(
        mut req: RpcCall,
        start: Value,
        end: Value,
        filter: Vec<Chars>,
    ) -> Option<(Self, RpcReply)> {
        let start = match parse_bound(start) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid start {}", e)),
        };
        let end = match parse_bound(end) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid end {}", e)),
        };
        let filter = match parse_filter(filter) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("could not parse filter {}", e)),
        };
        Some((Self { start, end, filter }, req.reply))
    }
}

async fn do_oneshot(
    head: Option<ArchiveReader>,
    config: Arc<Config>,
    limit: usize,
    args: OneshotConfig,
    reply: &mut RpcReply,
) -> Result<()> {
    let mut log = LogfileCollection::new(config, head, args.start, args.end).await?;
    log.seek(Seek::Beginning)?;
    let img = log.reimage().ok_or_else(|| anyhow!("no data source"))??;
    let path_by_id: FxHashMap<Id, Path> =
        img.idx.iter().map(|(id, path)| (*id, path.clone())).collect();
    let mut data = OneshotReply {
        pathmap: img.idx,
        image: img.img,
        deltas: CURSOR_BATCH_POOL.take(),
    };
    loop {
        let mut batches = log.read_deltas(100)??;
        if batches.is_empty() {
            reply.send(Value::Bytes(pack(&data)?.freeze()));
            break;
        } else {
            batches.retain_mut(|(_, batch)| {
                batch.retain(|BatchItem(id, _)| match path_by_id.get(id) {
                    None => false,
                    Some(path) => args.filter.is_match(path),
                });
                !batch.is_empty()
            });
            data.deltas.extend(batches.drain(..));
            if data.encoded_len() > limit {
                reply.send(Value::Error(Chars::from("data is too large")));
                break;
            }
        }
    }
    Ok(())
}

async fn wait_complete(set: &mut JoinSet<()>) {
    if set.is_empty() {
        future::pending().await
    } else {
        let _ = set.join_next().await;
    }
}

pub(super) async fn run(
    mut bcast_rx: broadcast::Receiver<BCastMsg>,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    publisher: Publisher,
) -> Result<()> {
    let (control_tx, mut control_rx) = mpsc::channel(3);
    let mut archive: Option<ArchiveReader> = None;
    let mut pending: JoinSet<()> = JoinSet::new();
    let _proc = define_rpc!(
        &publisher,
        publish_config.base.append("oneshot"),
        "read data in one shot",
        OneshotConfig::new,
        Some(control_tx),
        start: Value = "Unbounded"; START_DOC,
        end: Value = "Unbounded"; END_DOC,
        filter: Vec<Chars> = vec![Chars::from("/**")]; FILTER_DOC
    )?;
    loop {
        select_biased! {
            _ = wait_complete(&mut pending).fuse() => (),
            m = bcast_rx.recv().fuse() => match m {
                Err(_) => (),
                Ok(m) => match m {
                    BCastMsg::Batch(_, _) | BCastMsg::LogRotated(_) => (),
                    BCastMsg::NewCurrent(rdr) => archive = Some(rdr),
                    BCastMsg::Stop => break Ok(())
                }
            },
            (args, mut reply) = control_rx.select_next_some() => {
                if pending.len() > publish_config.max_sessions {
                    // your call is important to us. please stay on the line
                    // until the next available representive is available to
                    // assist you.
                    reply.send(Value::Error(Chars::from("busy")));
                } else {
                    let archive = archive.clone();
                    let config = config.clone();
                    let limit = publish_config.oneshot_data_limit;
                    pending.spawn(async move {
                        let r = do_oneshot(
                            archive.clone(),
                            config.clone(),
                            limit,
                            args,
                            &mut reply
                        ).await;
                        if let Err(e) = r {
                            reply.send(Value::Error(Chars::from(format!("internal error {}", e))));
                        }
                    });
                }
            }
        }
    }
}
