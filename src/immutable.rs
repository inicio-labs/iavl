use std::{borrow::Cow, sync::PoisonError};

use crate::{
    NodeHash,
    kvstore::KVStore,
    node::{ArlockNode, Node, NodeError, db::NodeDb},
    types::{NonEmptyBz, U63},
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
