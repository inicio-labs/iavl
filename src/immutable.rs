use std::borrow::Cow;

use bon::Builder;

use crate::{
    NodeHash,
    kvstore::KVStore,
    node::{ArlockNode, NodeError, db::NodeDb},
    types::{NonEmptyBz, U63},
};

#[derive(Debug, Clone, Builder)]
pub struct ImmutableTree<DB> {
    root: ArlockNode,
    hash: NodeHash,
    ndb: NodeDb<DB>,
    version: U63,
}

impl<DB> ImmutableTree<DB> {
    pub fn hash(&self) -> NodeHash {
        self.hash
    }

    pub fn version(&self) -> U63 {
        self.version
    }

    fn root(&self) -> &ArlockNode {
        &self.root
    }
}

impl<DB> ImmutableTree<DB>
where
    DB: KVStore,
{
    pub fn get<K>(&self, key: NonEmptyBz<K>) -> Result<(U63, Option<NonEmptyBz>), NodeError>
    where
        K: AsRef<[u8]>,
    {
        self.root()
            .read()?
            .get(&self.ndb, key)
            .map(|(idx, val)| (idx, val.map(Cow::into_owned)))
    }
}
