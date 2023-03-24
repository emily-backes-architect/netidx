use crate::{
    logfile::{ArchiveReader, BatchItem, Cursor, Id, Seek},
    recorder::{
        logfile_collection::{File, LogfileCollection},
        BCastMsg, Config, PublishConfig,
    },
};
use anyhow::{Error, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::{FxHashMap, FxHashSet};
use log::{error, info, warn};
use netidx::{
    chars::Chars,
    path::Path,
    pool::Pooled,
    protocol::value::FromValue,
    publisher::{
        self, ClId, PublishFlags, Publisher, PublisherBuilder, UpdateBatch, Val, Value,
        WriteRequest,
    },
    subscriber::{Event, Subscriber},
};
use netidx_protocols::rpc::server::{RpcCall, RpcReply};
use netidx_protocols::{
    cluster::{uuid_string, Cluster},
    define_rpc,
    rpc::server::{ArgSpec, Proc},
    rpc_err,
};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    ops::Bound,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::broadcast, task, time};
use uuid::Uuid;

static START_DOC: &'static str = "The timestamp you want to replay to start at, or Unbounded for the beginning of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. Default Unbounded.";
static END_DOC: &'static str = "Time timestamp you want to replay end at, or Unbounded for the end of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. default Unbounded";
static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, 0.5 = 1/2 realtime, Unlimited = as fast as data can be read and sent. Default is 1";
static STATE_DOC: &'static str = "The current state of playback, {pause, play, tail}. Tail, seek to the end of the archive and play any new messages that arrive. Default pause.";
static POS_DOC: &'static str = "The current playback position. Null if the archive is empty, or the timestamp of the current record. Set to any timestamp where start <= t <= end to seek. Set to [+-][0-9]+ to seek a specific number of batches, e.g. +1 to single step forward -1 to single step back. Set to [+-][0-9]+[yMdhmsu] to step forward or back that amount of time, e.g. -1y step back 1 year. -1u to step back 1 microsecond. set to 'beginning' to seek to the beginning and 'end' to seek to the end. By default the initial position is set to 'beginning' when opening the archive.";
static PLAY_AFTER_DOC: &'static str = "Start playing after waiting the specified timeout";

fn session_base(publish_base: &Path, id: Uuid) -> Path {
    use uuid::fmt::Simple;
    let mut buf = [0u8; Simple::LENGTH];
    publish_base.append(Simple::from_uuid(id).encode_lower(&mut buf))
}

fn parse_speed(v: Value) -> Result<Option<f64>> {
    match v.clone().cast_to::<f64>() {
        Ok(speed) => Ok(Some(speed)),
        Err(_) => match v.cast_to::<Chars>() {
            Err(_) => bail!("expected a float, or unlimited"),
            Ok(s) => {
                if s.trim().to_lowercase().as_str() == "unlimited" {
                    Ok(None)
                } else {
                    bail!("expected a float, or unlimited")
                }
            }
        },
    }
}

fn parse_bound(v: Value) -> Result<Bound<DateTime<Utc>>> {
    match v {
        Value::DateTime(ts) => Ok(Bound::Included(ts)),
        Value::String(c) if c.trim().to_lowercase().as_str() == "unbounded" => {
            Ok(Bound::Unbounded)
        }
        v => match v.cast_to::<Seek>()? {
            Seek::Beginning => Ok(Bound::Unbounded),
            Seek::End => Ok(Bound::Unbounded),
            Seek::Absolute(ts) => Ok(Bound::Included(ts)),
            Seek::TimeRelative(offset) => Ok(Bound::Included(Utc::now() + offset)),
            Seek::BatchRelative(_) => bail!("invalid bound"),
        },
    }
}

fn get_bound(r: WriteRequest) -> Option<Bound<DateTime<Utc>>> {
    match parse_bound(r.value) {
        Ok(b) => Some(b),
        Err(e) => {
            if let Some(reply) = r.send_result {
                reply.send(Value::Error(Chars::from(format!("{}", e))))
            }
            None
        }
    }
}

fn bound_to_val(b: Bound<DateTime<Utc>>) -> Value {
    match b {
        Bound::Unbounded => Value::String(Chars::from("Unbounded")),
        Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ClusterCmd {
    NotIdle,
    SeekTo(String),
    SetStart(Bound<DateTime<Utc>>),
    SetEnd(Bound<DateTime<Utc>>),
    SetSpeed(Option<f64>),
    SetState(State),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum State {
    Play,
    Pause,
    Tail,
}

impl FromStr for State {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if s.as_str() == "play" {
            Ok(State::Play)
        } else if s.as_str() == "pause" {
            Ok(State::Play)
        } else if s.as_str() == "tail" {
            Ok(State::Tail)
        } else {
            bail!("expected state [play, pause, tail]")
        }
    }
}

impl FromValue for State {
    fn from_value(v: Value) -> Result<Self> {
        Ok(v.cast_to::<Chars>()?.parse::<State>()?)
    }

    fn get(_: Value) -> Option<Self> {
        None
    }
}

impl State {
    fn play(&self) -> bool {
        match self {
            State::Play => true,
            State::Pause | State::Tail => false,
        }
    }
}

#[derive(Debug)]
enum Speed {
    Unlimited(Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>),
    Limited {
        rate: f64,
        next: time::Instant,
        current: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
    },
}

struct Controls {
    _start_doc: Val,
    start_ctl: Val,
    _end_doc: Val,
    end_ctl: Val,
    _speed_doc: Val,
    speed_ctl: Val,
    _state_doc: Val,
    state_ctl: Val,
    _pos_doc: Val,
    pos_ctl: Val,
}

impl Controls {
    async fn new(
        session_base: &Path,
        publisher: &Publisher,
        control_tx: &mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    ) -> Result<Self> {
        let _start_doc = publisher.publish(
            session_base.append("control/start/doc"),
            Value::String(Chars::from(START_DOC)),
        )?;
        let _end_doc = publisher.publish(
            session_base.append("control/end/doc"),
            Value::String(Chars::from(END_DOC)),
        )?;
        let _speed_doc = publisher.publish(
            session_base.append("control/speed/doc"),
            Value::String(Chars::from(SPEED_DOC)),
        )?;
        let _state_doc = publisher.publish(
            session_base.append("control/state/doc"),
            Value::String(Chars::from(STATE_DOC)),
        )?;
        let _pos_doc = publisher.publish(
            session_base.append("control/pos/doc"),
            Value::String(Chars::from(POS_DOC)),
        )?;
        let start_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/start/current"),
            Value::String(Chars::from("Unbounded")),
        )?;
        publisher.writes(start_ctl.id(), control_tx.clone());
        let end_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/end/current"),
            Value::String(Chars::from("Unbounded")),
        )?;
        publisher.writes(end_ctl.id(), control_tx.clone());
        let speed_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/speed/current"),
            Value::F64(1.),
        )?;
        publisher.writes(speed_ctl.id(), control_tx.clone());
        let state_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/state/current"),
            Value::String(Chars::from("pause")),
        )?;
        publisher.writes(state_ctl.id(), control_tx.clone());
        let pos_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/pos/current"),
            Value::Null,
        )?;
        publisher.writes(pos_ctl.id(), control_tx.clone());
        publisher.flushed().await;
        Ok(Controls {
            _start_doc,
            start_ctl,
            _end_doc,
            end_ctl,
            _speed_doc,
            speed_ctl,
            _state_doc,
            state_ctl,
            _pos_doc,
            pos_ctl,
        })
    }
}

struct NewSessionConfig {
    client: ClId,
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    speed: Option<f64>,
    pos: Option<Seek>,
    state: Option<State>,
    play_after: Option<Duration>,
}

impl NewSessionConfig {
    fn new(
        mut req: RpcCall,
        start: Value,
        end: Value,
        speed: Value,
        pos: Option<Seek>,
        state: Option<State>,
        play_after: Option<Duration>,
    ) -> Option<(NewSessionConfig, RpcReply)> {
        let start = match parse_bound(start) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid start {}", e)),
        };
        let end = match parse_bound(end) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid end {}", e)),
        };
        let speed = match parse_speed(speed) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid speed {}", e)),
        };
        let s = NewSessionConfig {
            client: req.client,
            start,
            end,
            speed,
            pos,
            state,
            play_after,
        };
        Some((s, req.reply))
    }
}

struct DataSource {
    file: File,
    archive: ArchiveReader,
    cursor: Cursor,
}

impl DataSource {
    fn new(
        archive_dir: &PathBuf,
        file: File,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<Self> {
        let path = file.path(archive_dir);
        let archive = task::block_in_place(|| ArchiveReader::open(path))?;
        let mut cursor = Cursor::new();
        cursor.set_start(start);
        cursor.set_end(end);
        Ok(Self { file, archive, cursor })
    }

    fn from_head(
        head: ArchiveReader,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Self {
        let mut cursor = Cursor::new();
        cursor.set_start(start);
        cursor.set_end(end);
        Self { file: File::Head, archive: head, cursor }
    }
}

struct Session {
    controls: Controls,
    publisher: Publisher,
    published: FxHashMap<Id, Val>,
    published_ids: FxHashSet<publisher::Id>,
    speed: Speed,
    state: State,
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    data_base: Path,
    archive_dir: PathBuf,
    files: LogfileCollection,
    source: Option<DataSource>,
    head: Option<ArchiveReader>,
}

impl Session {
    async fn new(
        archive_dir: PathBuf,
        publisher: Publisher,
        head: Option<ArchiveReader>,
        session_base: Path,
        control_tx: &mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    ) -> Result<Self> {
        let controls = Controls::new(&session_base, &publisher, &control_tx).await?;
        let files = LogfileCollection::new(&archive_dir).await?;
        Ok(Self {
            controls,
            publisher,
            published: HashMap::default(),
            published_ids: HashSet::default(),
            speed: Speed::Limited {
                rate: 1.,
                next: time::Instant::now(),
                current: Pooled::orphan(VecDeque::new()),
            },
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            state: State::Pause,
            data_base: session_base.append("data"),
            source: None,
            files,
            archive_dir,
            head,
        })
    }

    fn source(
        files: &LogfileCollection,
        source: &mut Option<DataSource>,
        head: &Option<ArchiveReader>,
        archive_dir: &PathBuf,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<bool> {
        if source.is_some() {
            Ok(true)
        } else {
            *source = match files.first() {
                File::Head => head.clone().map(|h| DataSource::from_head(h, start, end)),
                f => Some(DataSource::new(archive_dir, f, start, end)?),
            };
            Ok(source.is_some())
        }
    }

    fn next_source(
        files: &LogfileCollection,
        source: &mut Option<DataSource>,
        head: &Option<ArchiveReader>,
        archive_dir: &PathBuf,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<bool> {
        if source.is_none() {
            Self::source(files, source, head, archive_dir, start, end)
        } else {
            match source.as_ref().unwrap().file {
                File::Head => Ok(true),
                f => {
                    *source = match files.next(f) {
                        File::Head => {
                            head.clone().map(|h| DataSource::from_head(h, start, end))
                        }
                        f => Some(DataSource::new(&archive_dir, f, start, end)?),
                    };
                    Ok(source.is_some())
                }
            }
        }
    }

    fn find_source(
        files: &LogfileCollection,
        source: &mut Option<DataSource>,
        head: &Option<ArchiveReader>,
        archive_dir: &PathBuf,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
        ts: DateTime<Utc>,
    ) -> Result<bool> {
        let file = files.find(ts);
        if source.is_some() && source.as_ref().unwrap().file == file {
            Ok(source.is_some())
        } else {
            *source = match file {
                File::Head => head.clone().map(|h| DataSource::from_head(h, start, end)),
                f => Some(DataSource::new(archive_dir, f, start, end)?),
            };
            Ok(source.is_some())
        }
    }

    async fn next(&mut self) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
        macro_rules! set_state {
            ($st:expr) => {
                let mut cbatch = self.publisher.start_batch();
                self.set_state(&mut cbatch, $st);
                cbatch.commit(None).await;
                break future::pending().await
            };
        }
        loop {
            if !self.state.play() {
                break future::pending().await;
            } else {
                match &mut self.speed {
                    Speed::Unlimited(batches) => match batches.pop_front() {
                        Some(batch) => break Ok(batch),
                        None => {
                            if Self::source(
                                &self.files,
                                &mut self.source,
                                &self.head,
                                &self.archive_dir,
                                self.start,
                                self.end,
                            )? {
                                time::sleep(Duration::from_secs(1)).await;
                                continue;
                            } else {
                                let ds = self.source.as_mut().unwrap();
                                let archive = &ds.archive;
                                let cursor = &mut ds.cursor;
                                *batches = task::block_in_place(|| {
                                    archive.read_deltas(cursor, 3)
                                })?;
                                match batches.pop_front() {
                                    Some(batch) => break Ok(batch),
                                    None => match ds.file {
                                        File::Head => {
                                            set_state!(State::Tail);
                                        }
                                        File::Historical(ts) => {
                                            Self::next_source(
                                                &self.files,
                                                &mut self.source,
                                                &self.head,
                                                &self.archive_dir,
                                                self.start,
                                                self.end,
                                            )?;
                                            continue;
                                        }
                                    },
                                }
                            }
                        }
                    },
                    Speed::Limited { rate, next, current } => {
                        use tokio::time::Instant;
                        if current.len() < 2 {
                            if Self::source(
                                &self.files,
                                &mut self.source,
                                &self.head,
                                &self.archive_dir,
                                self.start,
                                self.end,
                            )? {
                                let ds = self.source.as_mut().unwrap();
                                let archive = &ds.archive;
                                let cursor = &mut ds.cursor;
                                let mut cur = task::block_in_place(|| {
                                    archive.read_deltas(cursor, 3)
                                })?;
                                for v in current.drain(..) {
                                    cur.push_front(v);
                                }
                                *current = cur;
                                if current.is_empty() {
                                    match ds.file {
                                        File::Head => {
                                            set_state!(State::Tail);
                                        }
                                        f => {
                                            Self::next_source(
                                                &self.files,
                                                &mut self.source,
                                                &self.head,
                                                &self.archive_dir,
                                                self.start,
                                                self.end,
                                            )?;
                                            continue;
                                        }
                                    }
                                }
                            } else if current.is_empty() {
                                time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                        let (ts, batch) = current.pop_front().unwrap();
                        let mut now = Instant::now();
                        if *next >= now {
                            time::sleep_until(*next).await;
                            now = Instant::now();
                        }
                        if current.is_empty() {
                            let mut cbatch = self.publisher.start_batch();
                            self.set_state(&mut cbatch, State::Tail);
                            cbatch.commit(None).await;
                        } else {
                            let wait = {
                                let ms = (current[0].0 - ts).num_milliseconds() as f64;
                                (ms / *rate).trunc() as u64
                            };
                            *next = now + Duration::from_millis(wait);
                        }
                        break Ok((ts, batch));
                    }
                }
            }
        }
    }

    async fn process_batch(
        &mut self,
        batch: (DateTime<Utc>, &mut Vec<BatchItem>),
    ) -> Result<()> {
        let pause = match self.end {
            Bound::Unbounded => false,
            Bound::Excluded(dt) => batch.0 >= dt,
            Bound::Included(dt) => batch.0 > dt,
        };
        if pause {
            let mut cbatch = self.publisher.start_batch();
            self.set_state(&mut cbatch, State::Pause);
            cbatch.commit(None).await;
            Ok(())
        } else {
            let mut pbatch = self.publisher.start_batch();
            for BatchItem(id, ev) in batch.1.drain(..) {
                let v = match ev {
                    Event::Unsubscribed => Value::Null,
                    Event::Update(v) => v,
                };
                match self.published.get(&id) {
                    Some(val) => {
                        val.update(&mut pbatch, v);
                    }
                    None => {
                        let path = self
                            .source
                            .as_ref()
                            .unwrap()
                            .archive
                            .path_for_id(&id)
                            .unwrap();
                        let path = self.data_base.append(&path);
                        let val = self.publisher.publish(path, v)?;
                        self.published_ids.insert(val.id());
                        self.published.insert(id, val);
                    }
                }
            }
            self.controls.pos_ctl.update(&mut pbatch, Value::DateTime(batch.0));
            Ok(pbatch.commit(None).await)
        }
    }

    async fn process_bcast(
        &mut self,
        msg: Result<BCastMsg, broadcast::error::RecvError>,
    ) -> Result<()> {
        match msg {
            Err(broadcast::error::RecvError::Closed) => {
                bail!("broadcast channel closed")
            }
            Err(broadcast::error::RecvError::Lagged(missed)) => match self.state {
                State::Play | State::Pause => Ok(()),
                State::Tail => {
                    // safe because it's impossible to get into state
                    // Tail without an archive.
                    let ds = self.source.as_mut().unwrap();
                    let archive = &ds.archive;
                    let cursor = &mut ds.cursor;
                    let mut batches = task::block_in_place(|| {
                        archive.read_deltas(cursor, missed as usize)
                    })?;
                    for (ts, mut batch) in batches.drain(..) {
                        self.process_batch((ts, &mut *batch)).await?;
                        self.source.as_mut().unwrap().cursor.set_current(ts);
                    }
                    Ok(())
                }
            },
            Ok(BCastMsg::Batch(ts, batch)) => match self.state {
                State::Play | State::Pause => Ok(()),
                State::Tail => {
                    let ds = self.source.as_mut().unwrap();
                    let dt = ts.datetime();
                    let pos = ds.cursor.current().unwrap_or(DateTime::<Utc>::MIN_UTC);
                    if ds.cursor.contains(&dt) && pos < dt {
                        let mut batch = (*batch).clone();
                        self.process_batch((dt, &mut batch)).await?;
                        self.source.as_mut().unwrap().cursor.set_current(dt);
                    }
                    Ok(())
                }
            },
            Ok(BCastMsg::LogRotated(ts)) => {
                let files = LogfileCollection::new(&self.archive_dir).await?;
                if let Some(d) = self.source.as_mut() {
                    if d.file == File::Head {
                        d.file = File::Historical(ts);
                    }
                }
                self.files = files;
                Ok(())
            }
            Ok(BCastMsg::NewCurrent(head)) => {
                self.head = Some(head);
                Ok(())
            }
            Ok(BCastMsg::Stop) => bail!("stop signal")
        }
    }

    fn set_start(
        &mut self,
        cbatch: &mut UpdateBatch,
        new_start: Bound<DateTime<Utc>>,
    ) -> Result<()> {
        self.controls.start_ctl.update(cbatch, bound_to_val(new_start));
        self.start = new_start;
        if let Some(ds) = self.source.as_mut() {
            ds.cursor.set_start(new_start);
        }
        Ok(())
    }

    fn set_end(
        &mut self,
        cbatch: &mut UpdateBatch,
        new_end: Bound<DateTime<Utc>>,
    ) -> Result<()> {
        self.controls.end_ctl.update(cbatch, bound_to_val(new_end));
        self.end = new_end;
        if let Some(ds) = self.source.as_mut() {
            ds.cursor.set_end(new_end);
        }
        Ok(())
    }

    fn set_state(&mut self, cbatch: &mut UpdateBatch, state: State) {
        match (self.state, state) {
            (State::Tail, State::Play) => (),
            (s0, s1) if s0 == s1 => (),
            (_, state) => {
                self.state = state;
                self.controls.state_ctl.update(
                    cbatch,
                    Value::from(match state {
                        State::Play => "play",
                        State::Pause => "pause",
                        State::Tail => "tail",
                    }),
                );
            }
        }
    }

    async fn apply_config(
        &mut self,
        cbatch: &mut UpdateBatch,
        cluster: &Cluster<ClusterCmd>,
        cfg: NewSessionConfig,
    ) -> Result<()> {
        self.set_start(cbatch, cfg.start)?;
        cluster.send_cmd(&ClusterCmd::SetStart(cfg.start));
        self.set_end(cbatch, cfg.end)?;
        cluster.send_cmd(&ClusterCmd::SetEnd(cfg.end));
        self.set_speed(cbatch, cfg.speed);
        cluster.send_cmd(&ClusterCmd::SetSpeed(cfg.speed));
        if let Some(pos) = cfg.pos {
            self.seek(cbatch, pos)?;
            cluster.send_cmd(&ClusterCmd::SeekTo(pos.to_string()));
        }
        if let Some(state) = cfg.state {
            self.set_state(cbatch, state);
            cluster.send_cmd(&ClusterCmd::SetState(state));
        }
        if let Some(play_after) = cfg.play_after {
            time::sleep(play_after).await;
            self.set_state(cbatch, State::Play);
            cluster.send_cmd(&ClusterCmd::SetState(State::Play));
        }
        Ok(())
    }

    async fn process_control_batch(
        &mut self,
        session_id: Uuid,
        cluster: &Cluster<ClusterCmd>,
        mut batch: Pooled<Vec<WriteRequest>>,
    ) -> Result<()> {
        let mut inst = HashMap::new();
        let mut cbatch = self.publisher.start_batch();
        for req in batch.drain(..) {
            inst.insert(req.id, req);
        }
        for (_, req) in inst {
            if req.id == self.controls.start_ctl.id() {
                info!("set start {}: {}", session_id, req.value);
                if let Some(new_start) = get_bound(req) {
                    self.set_start(&mut cbatch, new_start)?;
                    cluster.send_cmd(&ClusterCmd::SetStart(new_start));
                }
            } else if req.id == self.controls.end_ctl.id() {
                info!("set end {}: {}", session_id, req.value);
                if let Some(new_end) = get_bound(req) {
                    self.set_end(&mut cbatch, new_end)?;
                    cluster.send_cmd(&ClusterCmd::SetEnd(new_end));
                }
            } else if req.id == self.controls.speed_ctl.id() {
                info!("set speed {}: {}", session_id, req.value);
                match parse_speed(req.value) {
                    Ok(speed) => {
                        self.set_speed(&mut cbatch, speed);
                        cluster.send_cmd(&ClusterCmd::SetSpeed(speed));
                    }
                    Err(e) => {
                        if let Some(reply) = req.send_result {
                            reply.send(Value::Error(Chars::from(format!("{}", e))));
                        }
                    }
                }
            } else if req.id == self.controls.state_ctl.id() {
                info!("set state {}: {}", session_id, req.value);
                match req.value.cast_to::<State>() {
                    Ok(state) => {
                        self.set_state(&mut cbatch, state);
                        cluster.send_cmd(&ClusterCmd::SetState(state));
                    }
                    Err(e) => {
                        if let Some(reply) = req.send_result {
                            reply.send(Value::Error(Chars::from(format!("{}", e))))
                        }
                    }
                }
            } else if req.id == self.controls.pos_ctl.id() {
                info!("set pos {}: {}", session_id, req.value);
                match req.value.cast_to::<Seek>() {
                    Ok(pos) => {
                        self.seek(&mut cbatch, pos)?;
                        match self.state {
                            State::Pause | State::Play => (),
                            State::Tail => {
                                self.set_state(&mut cbatch, State::Play);
                            }
                        }
                        cluster.send_cmd(&ClusterCmd::SeekTo(pos.to_string()));
                    }
                    Err(e) => {
                        if let Some(reply) = req.send_result {
                            reply.send(Value::Error(Chars::from(format!("{}", e))))
                        }
                    }
                }
            }
        }
        Ok(cbatch.commit(None).await)
    }

    fn process_control_cmd(
        &mut self,
        cbatch: &mut UpdateBatch,
        cmd: ClusterCmd,
    ) -> Result<()> {
        match cmd {
            ClusterCmd::SeekTo(s) => match s.parse::<Seek>() {
                Ok(pos) => self.seek(cbatch, pos),
                Err(e) => {
                    warn!("invalid seek from cluster {}, {}", s, e);
                    Ok(())
                }
            },
            ClusterCmd::SetStart(new_start) => self.set_start(cbatch, new_start),
            ClusterCmd::SetEnd(new_end) => self.set_end(cbatch, new_end),
            ClusterCmd::SetSpeed(sp) => Ok(self.set_speed(cbatch, sp)),
            ClusterCmd::SetState(st) => Ok(self.set_state(cbatch, st)),
            ClusterCmd::NotIdle => Ok(()),
        }
    }

    fn reimage(&mut self, pbatch: &mut UpdateBatch) -> Result<()> {
        if let Some(ds) = self.source.as_mut() {
            let mut img = task::block_in_place(|| ds.archive.build_image(&ds.cursor))?;
            let mut idx = task::block_in_place(|| ds.archive.get_index());
            self.controls.pos_ctl.update(
                pbatch,
                match ds.cursor.current() {
                    Some(ts) => Value::DateTime(ts),
                    None => match ds.cursor.start() {
                        Bound::Unbounded => Value::Null,
                        Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
                    },
                },
            );
            for (id, path) in idx.drain(..) {
                let v = match img.remove(&id) {
                    None | Some(Event::Unsubscribed) => Value::Null,
                    Some(Event::Update(v)) => v,
                };
                match self.published.get(&id) {
                    Some(val) => {
                        val.update(pbatch, v);
                    }
                    None => {
                        let path = self.data_base.append(path.as_ref());
                        let val = self.publisher.publish(path, v)?;
                        self.published_ids.insert(val.id());
                        self.published.insert(id, val);
                    }
                }
            }
        }
        Ok(())
    }

    fn seek(&mut self, pbatch: &mut UpdateBatch, seek: Seek) -> Result<()> {
        match seek {
            Seek::Beginning => {
                let file = self.files.first();
                match self.source.as_ref() {
                    Some(ds) if ds.file == file => (),
                    Some(_) | None => {
                        self.source = Some(DataSource::new(
                            &self.archive_dir,
                            file,
                            self.start,
                            self.end,
                        )?);
                    }
                }
            }
            Seek::End => match self.source.as_ref() {
                Some(ds) if ds.file == File::Head => (),
                Some(_) | None => {
                    self.source = self
                        .head
                        .clone()
                        .map(|h| DataSource::from_head(h, self.start, self.end));
                }
            },
            Seek::BatchRelative(i) => match self.source.as_ref() {
                None => {
                    let file = self.files.first();
                    self.source = Some(DataSource::new(
                        &self.archive_dir,
                        file,
                        self.start,
                        self.end,
                    )?);
                }
                Some(ds) => {
                    let mut cursor = ds.cursor;
                    cursor.set_start(Bound::Unbounded);
                    cursor.set_end(Bound::Unbounded);
                    if !ds.archive.index().seek_steps(&mut cursor, i) {
                        if i < 0 {
                            let file = self.files.prev(ds.file);
                            if file != ds.file {
                                self.source = Some(DataSource::new(
                                    &self.archive_dir,
                                    file,
                                    self.start,
                                    self.end,
                                )?);
                            }
                        } else {
                            let file = self.files.next(ds.file);
                            if file != ds.file {
                                self.source = Some(DataSource::new(
                                    &self.archive_dir,
                                    file,
                                    self.start,
                                    self.end,
                                )?);
                            }
                        }
                    }
                }
            },
            Seek::TimeRelative(offset) => match self.source {
                None => {
                    let file = self.files.first();
                    self.source = Some(DataSource::new(
                        &self.archive_dir,
                        file,
                        self.start,
                        self.end,
                    )?);
                }
                Some(ds) => {
                    let mut cursor = ds.cursor;
                    cursor.set_start(Bound::Unbounded);
                    cursor.set_end(Bound::Unbounded);
                    let (ok, ts) =
                        ds.archive.index().seek_time_relative(&mut cursor, offset);
                    if !ok {
                        let file = self.files.find(ts);
                        if ds.file != file {
                            self.source = Some(DataSource::new(
                                &self.archive_dir,
                                file,
                                self.start,
                                self.end,
                            )?);
                        }
                    }
                }
            },
            Seek::Absolute(ts) => {
                let file = self.files.find(ts);
                let cur_ok = match self.source {
                    None => false,
                    Some(ds) => ds.file == file,
                };
                if !cur_ok {
                    self.source = Some(DataSource::new(
                        &self.archive_dir,
                        file,
                        self.start,
                        self.end,
                    )?);
                }
            }
        }
        let current = match &mut self.speed {
            Speed::Unlimited(v) => v,
            Speed::Limited { current, next, .. } => {
                *next = time::Instant::now();
                current
            }
        };
        if let Some((ts, _)) = current.pop_front() {
            self.source.iter_mut().for_each(|ds| ds.cursor.set_current(ts));
            current.clear()
        }
        if let Some(ds) = self.source.as_mut() {
            ds.archive.seek(&mut ds.cursor, seek);
            self.reimage(pbatch)?
        }
        Ok(())
    }

    fn set_speed(&mut self, cbatch: &mut UpdateBatch, new_rate: Option<f64>) {
        match new_rate {
            None => {
                self.controls
                    .speed_ctl
                    .update(cbatch, Value::String(Chars::from("unlimited")));
            }
            Some(new_rate) => {
                self.controls.speed_ctl.update(cbatch, Value::F64(new_rate));
            }
        };
        match &mut self.speed {
            Speed::Limited { rate, current, .. } => match new_rate {
                Some(new_rate) => {
                    *rate = new_rate;
                }
                None => {
                    let c = mem::replace(current, Pooled::orphan(VecDeque::new()));
                    self.speed = Speed::Unlimited(c);
                }
            },
            Speed::Unlimited(v) => {
                if let Some(new_rate) = new_rate {
                    let v = mem::replace(v, Pooled::orphan(VecDeque::new()));
                    self.speed = Speed::Limited {
                        rate: new_rate,
                        next: time::Instant::now(),
                        current: v,
                    };
                }
            }
        }
    }
}

fn not_idle(idle: &mut bool, cluster: &Cluster<ClusterCmd>) {
    *idle = false;
    cluster.send_cmd(&ClusterCmd::NotIdle);
}

async fn session(
    mut bcast: broadcast::Receiver<BCastMsg>,
    head: Option<ArchiveReader>,
    subscriber: Subscriber,
    publisher: Publisher,
    session_id: Uuid,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    cfg: Option<NewSessionConfig>,
) -> Result<()> {
    let (control_tx, control_rx) = mpsc::channel(3);
    let (events_tx, mut events_rx) = mpsc::unbounded();
    publisher.events(events_tx);
    let session_base = session_base(&publish_config.base, session_id);
    let mut cluster = Cluster::new(
        &publisher,
        subscriber,
        session_base.append("cluster"),
        publish_config.shards.unwrap_or(0),
    )
    .await?;
    head.as_ref().map(|a| a.check_remap_rescan()).transpose()?;
    let mut t = Session::new(
        config.archive_directory.clone(),
        publisher.clone(),
        head,
        session_base,
        &control_tx,
    )
    .await?;
    let mut batch = publisher.start_batch();
    t.seek(&mut batch, Seek::End)?;
    if let Some(cfg) = cfg {
        t.apply_config(&mut batch, &cluster, cfg).await?
    }
    batch.commit(None).await;
    let mut control_rx = control_rx.fuse();
    let mut idle_check = time::interval(std::time::Duration::from_secs(30));
    let mut idle = false;
    let mut used = 0;
    loop {
        select_biased! {
            e = events_rx.select_next_some() => match e {
                publisher::Event::Subscribe(id, _) => if t.published_ids.contains(&id) {
                    used += 1;
                },
                publisher::Event::Unsubscribe(id, _) => if t.published_ids.contains(&id) {
                    used -= 1;
                },
                publisher::Event::Destroyed(_) => (),
            },
            _ = idle_check.tick().fuse() => {
                let has_clients = used > 0;
                if !has_clients && idle {
                    break Ok(())
                } else if has_clients {
                    not_idle(&mut idle, &cluster)
                } else {
                    idle = true;
                }
            },
            _ = publisher.wait_any_new_client().fuse() => {
                if publisher.clients() > cluster.others() {
                    not_idle(&mut idle, &cluster)
                }
            },
            m = bcast.recv().fuse() => t.process_bcast(m).await?,
            cmds = cluster.wait_cmds().fuse() => {
                let mut cbatch = publisher.start_batch();
                for cmd in cmds? {
                    t.process_control_cmd(&mut cbatch, cmd)?
                }
                cbatch.commit(None).await;
            },
            r = control_rx.next() => match r {
                None => break Ok(()),
                Some(batch) => {
                    t.process_control_batch(session_id, &cluster, batch).await?
                }
            },
            r = t.next().fuse() => match r {
                Err(e) => break Err(e),
                Ok((ts, mut batch)) => { t.process_batch((ts, &mut *batch)).await?; }
            }
        }
    }
}

struct SessionIdsInner {
    max_total: usize,
    max_by_client: usize,
    total: usize,
    by_client: FxHashMap<ClId, usize>,
}

#[derive(Clone)]
struct SessionIds(Arc<Mutex<SessionIdsInner>>);

impl SessionIds {
    fn new(max_total: usize, max_by_client: usize) -> Self {
        Self(Arc::new(Mutex::new(SessionIdsInner {
            max_total,
            max_by_client,
            total: 0,
            by_client: HashMap::default(),
        })))
    }

    fn add_session(&self, client: ClId) -> Option<SessionId> {
        let mut inner = self.0.lock();
        let inner = &mut *inner;
        let by_client = inner.by_client.entry(client).or_insert(0);
        if inner.total < inner.max_total && *by_client < inner.max_by_client {
            inner.total += 1;
            *by_client += 1;
            Some(SessionId(self.clone(), client))
        } else {
            None
        }
    }

    fn delete_session(&self, session: &SessionId) {
        let mut inner = self.0.lock();
        if let Some(c) = inner.by_client.get_mut(&session.1) {
            *c -= 1;
            inner.total -= 1;
        }
    }
}

struct SessionId(SessionIds, ClId);

impl Drop for SessionId {
    fn drop(&mut self) {
        self.0.delete_session(self)
    }
}

async fn start_session(
    publisher: Publisher,
    session_id: Uuid,
    session_token: SessionId,
    bcast: &broadcast::Sender<BCastMsg>,
    subscriber: &Subscriber,
    head: &Option<ArchiveReader>,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    cfg: Option<NewSessionConfig>,
) -> Result<()> {
    let bcast = bcast.subscribe();
    let head = head.clone();
    let publish_base = publish_config.base.clone();
    let subscriber = subscriber.clone();
    let publisher_cl = publisher.clone();
    task::spawn(async move {
        let res = session(
            bcast,
            head,
            subscriber,
            publisher_cl,
            session_id,
            config.clone(),
            publish_config.clone(),
            cfg,
        )
        .await;
        match res {
            Ok(()) => {
                info!("session {} existed", session_id)
            }
            Err(e) => {
                error!("session {} exited {}", session_id, e)
            }
        }
        drop(session_token)
    });
    Ok(())
}

pub(super) async fn run(
    bcast: broadcast::Sender<BCastMsg>,
    mut bcast_rx: broadcast::Receiver<BCastMsg>,
    subscriber: Subscriber,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
) -> Result<()> {
    let sessions = SessionIds::new(
        publish_config.max_sessions,
        publish_config.max_sessions_per_client,
    );
    let publisher = PublisherBuilder::new()
        .config(config.netidx_config.clone())
        .desired_auth(config.desired_auth.clone())
        .bind_cfg(publish_config.bind.clone())
        .build()
        .await?;
    let (control_tx, control_rx) = mpsc::channel(3);
    let _new_session: Result<Proc> = define_rpc!(
        &publisher,
        publish_config.base.append("session"),
        "create a new playback session",
        NewSessionConfig::new,
        Some(control_tx.clone()),
        start: Value = "Unbounded"; START_DOC,
        end: Value = "Unbounded"; END_DOC,
        speed: Value = "1."; SPEED_DOC,
        pos: Option<Seek> = Value::Null; POS_DOC,
        state: Option<State> = Value::Null; STATE_DOC,
        play_after: Option<Duration> = None::<Duration>; PLAY_AFTER_DOC
    );
    let _new_session = _new_session?;
    let mut cluster = Cluster::<(ClId, Uuid)>::new(
        &publisher,
        subscriber.clone(),
        publish_config.base.append("cluster"),
        publish_config.shards.unwrap_or(0),
    )
    .await?;
    let mut control_rx = control_rx.fuse();
    let mut poll_members = time::interval(std::time::Duration::from_secs(30));
    let mut archive = None;
    loop {
        select_biased! {
            m = bcast_rx.recv().fuse() => match m {
                Err(_) => (),
                Ok(m) => match m {
                    BCastMsg::Batch(_, _) | BCastMsg::LogRotated(_) => (),
                    BCastMsg::NewCurrent(rdr) => archive = Some(rdr),
                    BCastMsg::Stop => break Ok(())
                }
            },
            _ = poll_members.tick().fuse() => {
                if let Err(e) = cluster.poll_members().await {
                    warn!("failed to poll cluster members, will retry {}", e)
                }
            },
            cmds = cluster.wait_cmds().fuse() => match cmds {
                Err(e) => {
                    error!("received unparsable cluster commands {}", e)
                }
                Ok(cmds) => for (client, session_id) in cmds {
                    match sessions.add_session(client) {
                        None => {
                            error!("can't start session requested by cluster member, too many sessions")
                        },
                        Some(session_token) => {
                            let r = start_session(
                                publisher.clone(),
                                session_id,
                                session_token,
                                &bcast,
                                &subscriber,
                                &archive,
                                config.clone(),
                                publish_config.clone(),
                                None
                            ).await;
                            if let Err(e) = r {
                                warn!("failed to start session {}, {}", session_id, e)
                            }
                        }
                    }
                }
            },
            m = control_rx.next() => match m {
                None => break Ok(()),
                Some((cfg, mut reply)) => {
                    match sessions.add_session(cfg.client) {
                        None => {
                            let m = format!("too many sessions, client {:?}", cfg.client);
                            reply.send(Value::Error(Chars::from(m)));
                        },
                        Some(session_token) => {
                            let session_id = Uuid::new_v4();
                            let client = cfg.client;
                            info!("start session {}", session_id);
                            let r = start_session(
                                publisher.clone(),
                                session_id,
                                session_token,
                                &bcast,
                                &subscriber,
                                &archive,
                                config.clone(),
                                publish_config.clone(),
                                Some(cfg)
                            ).await;
                            match r {
                                Err(e) => {
                                    let e = Chars::from(format!("{}", e));
                                    warn!("failed to start session {}, {}", session_id, e);
                                    reply.send(Value::Error(e));
                                }
                                Ok(()) => {
                                    cluster.send_cmd(&(client, session_id));
                                    reply.send(Value::from(uuid_string(session_id)));
                                }
                            }
                        }
                    }
                }
            },
        }
    }
}
