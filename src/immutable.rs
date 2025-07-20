use std::sync::PoisonError;

use bytes::Bytes;
use nebz::NonEmptyBz;
use oblux::U63;

use crate::{
    Get, GetError, NodeHash, Sealed,
    kvstore::KVStore,
    node::{ArlockNode, NodeError, ndb::NodeDb},
};

#[derive(Debug, Clone)]
pub struct ImmutableTree<DB> {
    root: ArlockNode,
    hash: NodeHash,
    ndb: NodeDb<DB>,
    version: U63,
}

#[bon::bon]
impl<DB> ImmutableTree<DB> {
    #[builder]
    pub(crate) fn new(
        root: ArlockNode,
        ndb: NodeDb<DB>,
        version: U63,
    ) -> Result<Self, PoisonError<()>> {
        let hash = root
            .read()
            .map_err(|_| PoisonError::new(()))?
            .hash()
            .cloned()
            .expect("root must be hashed");

        Ok(Self {
            root,
            hash,
            ndb,
            version,
        })
    }
}

impl<DB> ImmutableTree<DB> {
    pub fn hash(&self) -> NodeHash {
        self.hash
    }

    pub fn version(&self) -> U63 {
        self.version
    }

    pub(crate) fn set_version(&mut self, version: U63) {
        self.version = version;
    }

    fn root(&self) -> &ArlockNode {
        &self.root
    }
}

impl<DB> Get for ImmutableTree<DB>
where
    DB: KVStore,
{
    type Error = GetError;

    type Value = Bytes;

    fn get<K>(
        &self,
        key: NonEmptyBz<K>,
    ) -> Result<(U63, Option<NonEmptyBz<Self::Value>>), Self::Error>
    where
        K: AsRef<[u8]>,
    {
        self.root()
            .read()
            .map_err(NodeError::from)?
            .get(&self.ndb, key)
            .map_err(From::from)
    }
}

impl<DB> Sealed for ImmutableTree<DB> {}
