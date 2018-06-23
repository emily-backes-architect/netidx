use futures::prelude::*;
use futures::sync::oneshot;
use std::{sync::{Arc, Mutex}, collections::VecDeque, mem};
use error::*;

struct PipeInner<T> {
  read_waiters: Vec<oneshot::Sender<()>>,
  write_waiters: Vec<oneshot::Sender<()>>,
  buffer: VecDeque<T>,
  closed: bool,
  limit: usize,
}

#[derive(Clone)]
struct Pipe<T>(Arc<Mutex<PipeInner<T>>>);

struct Sentinal<T>(Pipe<T>);

impl Drop for Sentinal<T> {
  fn drop(&mut self) {
    let mut t = self.0.0.lock().unwrap();
    t.closed = true;
    for s in t.read_waiters.drain(0..) { let _ = s.send(()); }
    for s in t.write_waiters.drain(0..) { let _ = s.send(()); }
  }
}

#[derive(Clone)]
pub(crate) struct Writer<T>(Pipe<T>, Arc<Sentinal<T>>);

impl Writer<T> {
  pub(crate) fn write(&self, v: T) {
    let mut t = self.0.0.lock().unwrap();
    t.buffer.push_back(v);
  }

  pub(crate) fn write_many<V: IntoIterator<Item=T>>(&self, batch: V) {
    let mut t = self.0.0.lock().unwrap();
    t.buffer.extend(batch);
  }

  #[async]
  pub(crate) fn flush(self) -> Result<()> {
    let wait = {
      let mut t = self.0.0.lock().unwrap();
      if t.closed { bail!("pipe is closed") }
      for s in t.read_waiters.drain(0..) { let _ = s.send(()) }
      if t.buffer.len() <= t.limit { None }
      else {
        let (tx, rx) = oneshot::channel();
        t.write_waiters.push(tx);
        Some(rx)
      }
    };
    match wait {
      Some(wait) => Ok(await!(wait)?),
      None => Ok(())
    }
  }
}

#[derive(Clone)]
pub(crate) struct Reader<T>(Arc<Mutex<PipeInner<T>>>);

impl Reader<T> {
  #[async]
  fn read_raw<V, F>(self, f: F) -> Result<V>
  where F: Fn(&mut VecDeque<T>) -> Option<V> {
    loop {
      let wait = {
        let mut t = self.0.0.lock().unwrap();
        if t.buffer.len() == 0 && t.closed { bail!("pipe is closed") }
        match f(t.buffer) {
          Some(v) => {
            if t.buffer.len() <= t.buffer.limit {
              for s in t.write_waiters.drain(0..) { let _ = s.send(()); }
            }
            return Ok(v)
          },
          None => {
            let (tx, rx) = oneshot::channel();
            t.read_waiters.push(tx);
            for s in t.write_waiters.drain(0..) { let _ = s.send(()); }
            rx
          }
        }
      };
      await!(wait)?
    }
  }

  #[async]
  pub(crate) fn read(self) -> Result<T> {
    Ok(await!(self.read_raw(|buf| buf.pop_front()))?)
  }

  #[async]
  pub(crate) fn read_all(self) -> Result<VecDeque<T>> {
    Ok(await!(self.read_raw(|buf| {
      if buf.len() == 0 { None }
      else {
        let mut new = VecDeque::new();
        mem::swap(&mut new, buf);
        Some(new)
      }
    })))
  }
}

pub(crate) fn pipe() -> (Writer<T>, Reader<T>) {
  let i = Arc::new(Mutex::new(Pipe {
    waiters: Vec::new(),
    buffer: VecDeque::new()
  }));
  (Writer(i.clone()), Reader(i))
}
