use super::{
    auth::{PMap, UserDb},
    config::Config,
};
use crate::{
    channel::K5CtxWrap,
    chars::Chars,
    os::{
        local_auth::{AuthServer, Credential},
        Mapper,
    },
    protocol::resolver::{Auth, PublisherId},
};
use anyhow::{bail, Result};
use cross_krb5::{K5Ctx, ServerCtx};
use fxhash::FxHashMap;
use netidx_core::pack::Pack;
use parking_lot::{RwLock, RwLockReadGuard};
use std::{collections::HashMap, sync::Arc};
use tokio::task;

pub(super) struct LocalAuth(AuthServer);

impl LocalAuth {
    pub(super) async fn new(socket_path: &str) -> Result<Self> {
        Ok(Self(AuthServer::start(socket_path).await?))
    }

    pub(super) fn authenticate(&self, mut token: &[u8]) -> Result<Credential> {
        if token.len() < 10 {
            bail!("token short")
        }
        let cred = <Credential as Pack>::decode(&mut token)?;
        if !self.0.validate(&cred) {
            bail!("invalid token")
        }
        Ok(cred)
    }
}

pub(super) trait SecDataCommon {
    fn secret(&self) -> u128;
}

pub(super) struct SecCtxData<S: 'static> {
    pub(super) users: UserDb,
    pub(super) pmap: PMap,
    data: FxHashMap<PublisherId, S>,
}

impl<S: 'static + SecDataCommon> SecCtxData<S> {
    pub(super) fn new(cfg: &Config) -> Result<Self> {
        let mut users = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(&cfg.perms, &mut users, cfg.root(), &cfg.children)?;
        Ok(Self { users, pmap, data: HashMap::default() })
    }

    pub(super) fn remove(&mut self, id: &PublisherId) {
        self.data.remove(&id);
    }

    pub(super) fn insert(&mut self, id: PublisherId, data: S) {
        self.remove(&id);
        self.data.insert(id, data);
    }

    pub(super) fn secret(&self, id: &PublisherId) -> Option<u128> {
        self.data.get(id).map(|d| d.secret())
    }
}

#[derive(Debug, Clone)]
pub(super) struct K5SecData {
    pub(super) secret: u128,
    pub(super) ctx: K5CtxWrap<ServerCtx>,
}

impl SecDataCommon for K5SecData {
    fn secret(&self) -> u128 {
        self.secret
    }
}

impl SecCtxData<K5SecData> {
    pub(super) fn get(&self, id: &PublisherId) -> Option<&K5SecData> {
        self.data.get(id).and_then(|r| {
            match task::block_in_place(|| r.ctx.lock().ttl()) {
                Ok(ttl) if ttl.as_secs() > 0 => Some(r),
                _ => None,
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct LocalSecData {
    pub(super) user: Chars,
    pub(super) secret: u128,
}

impl SecDataCommon for LocalSecData {
    fn secret(&self) -> u128 {
        self.secret
    }
}

impl SecCtxData<LocalSecData> {
    pub(super) fn get(&self, id: &PublisherId) -> Option<&LocalSecData> {
        self.data.get(id)
    }
}

pub(super) enum SecCtxDataReadGuard<'a> {
    Anonymous,
    Krb5(RwLockReadGuard<'a, SecCtxData<K5SecData>>),
    Local(RwLockReadGuard<'a, SecCtxData<LocalSecData>>),
}

impl<'a> SecCtxDataReadGuard<'a> {
    pub(super) fn pmap(&'a self) -> Option<&'a PMap> {
        match self {
            SecCtxDataReadGuard::Anonymous => None,
            SecCtxDataReadGuard::Krb5(r) => Some(&r.pmap),
            SecCtxDataReadGuard::Local(r) => Some(&r.pmap),
        }
    }
}

#[derive(Clone)]
pub(super) enum SecCtx {
    Anonymous,
    Krb5(Arc<(Chars, RwLock<SecCtxData<K5SecData>>)>),
    Local(Arc<(LocalAuth, RwLock<SecCtxData<LocalSecData>>)>),
}

impl SecCtx {
    pub(super) async fn new(cfg: &Config) -> Result<Self> {
        let t = match &cfg.auth {
            Auth::Anonymous => SecCtx::Anonymous,
            Auth::Local { path } => {
                let auth = LocalAuth::new(&path).await?;
                let store = RwLock::new(SecCtxData::new(cfg)?);
                SecCtx::Local(Arc::new((auth, store)))
            }
            Auth::Krb5 { spn } => {
                let store = RwLock::new(SecCtxData::new(cfg)?);
                SecCtx::Krb5(Arc::new((spn.clone(), store)))
            }
        };
        Ok(t)
    }

    pub(super) fn read(&self) -> SecCtxDataReadGuard {
        match self {
            SecCtx::Anonymous => SecCtxDataReadGuard::Anonymous,
            SecCtx::Krb5(a) => SecCtxDataReadGuard::Krb5(a.1.read()),
            SecCtx::Local(a) => SecCtxDataReadGuard::Local(a.1.read()),
        }
    }

    pub(super) fn remove(&self, id: &PublisherId) {
        match self {
            SecCtx::Krb5(a) => a.1.write().remove(id),
            SecCtx::Local(a) => a.1.write().remove(id),
            SecCtx::Anonymous => (),
        }
    }
}