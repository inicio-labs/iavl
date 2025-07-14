pub mod db;
pub mod info;

mod error;
mod inner;
mod kind;
mod leaf;

pub use self::{
    error::NodeError,
    inner::{Child, InnerNode, InnerNodeError},
    kind::{DeserializedNode, DraftedNode, HashedNode, SavedNode},
    leaf::LeafNode,
};

use std::{
    borrow::Cow,
    sync::{Arc, RwLock},
};

use crate::{
    NodeHash, NodeKey,
    kvstore::KVStore,
    types::{NonEmptyBz, U7, U63},
};

use self::{db::NodeDb, error::Result};

pub type ArlockNode = Arc<RwLock<Node>>;

#[derive(Debug)]
pub enum Node {
    Drafted(DraftedNode),
    Saved(SavedNode),
}

impl Node {
    pub fn key(&self) -> &NonEmptyBz {
        match self {
            Self::Drafted(drafted) => drafted.key(),
            Self::Saved(saved) => saved.key(),
        }
    }

    pub fn height(&self) -> U7 {
        match self {
            Self::Drafted(drafted) => drafted.height(),
            Self::Saved(saved) => saved.height(),
        }
    }

    pub fn size(&self) -> U63 {
        match self {
            Self::Drafted(drafted) => drafted.size(),
            Self::Saved(saved) => saved.size(),
        }
    }

    pub fn hash(&self) -> Option<&NodeHash> {
        match self {
            Self::Saved(saved) => Some(saved.hash()),
            _ => None,
        }
    }

    pub fn value(&self) -> Option<&NonEmptyBz> {
        match self {
            Self::Drafted(DraftedNode::Leaf(leaf)) => Some(leaf.value()),
            Self::Saved(SavedNode::Leaf(leaf)) => Some(leaf.value()),
            _ => None,
        }
    }

    pub fn as_drafted(&self) -> Option<&DraftedNode> {
        match self {
            Self::Drafted(drafted) => Some(drafted),
            _ => None,
        }
    }

    pub fn as_saved(&self) -> Option<&SavedNode> {
        match self {
            Self::Saved(saved) => Some(saved),
            _ => None,
        }
    }

    pub fn left(&self) -> Option<&Child> {
        match self {
            Self::Drafted(drafted) => drafted.left(),
            Self::Saved(saved) => saved.left(),
        }
    }

    pub fn right(&self) -> Option<&Child> {
        match self {
            Self::Drafted(drafted) => drafted.right(),
            Self::Saved(saved) => saved.right(),
        }
    }

    pub fn left_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Drafted(drafted) => drafted.left_mut(),
            Self::Saved(saved) => saved.left_mut(),
        }
    }

    pub fn right_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Drafted(drafted) => drafted.right_mut(),
            Self::Saved(saved) => saved.right_mut(),
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::Drafted(DraftedNode::Leaf(_)) | Self::Saved(SavedNode::Leaf(_)),
        )
    }

    pub fn get<DB, K>(
        &self,
        ndb: &NodeDb<DB>,
        key: NonEmptyBz<K>,
    ) -> Result<(U63, Option<Cow<'_, NonEmptyBz>>), NodeError>
    where
        K: AsRef<[u8]>,
        DB: KVStore,
    {
        // leaf node check
        if let Some(value) = self.value() {
            if key.as_non_empty_slice() == self.key().as_non_empty_slice() {
                return Ok((U63::MIN, Some(Cow::Borrowed(value))));
            }

            return Ok((U63::MIN, None));
        }

        // unwrap is safe because self is inner node
        if key.as_non_empty_slice() < self.key().as_non_empty_slice() {
            return self
                .left()
                .map(|left| left.fetch_full(ndb))
                .transpose()?
                .unwrap()
                .read()?
                .get(ndb, key)
                .map(|(i, v)| (i, v.map(Cow::into_owned).map(Cow::Owned)));
        }

        // unwrap is safe because self is inner node
        let right = self
            .right()
            .map(|right| right.fetch_full(ndb))
            .transpose()?
            .unwrap();
        let right = right.read()?;
        let right_size = right.size().get();

        right.get(ndb, key).map(|(i, v)| {
            (
                // direct subtraction is safe because parent's size always exceeds that of the child
                i.get()
                    .checked_add(self.size().get() - right_size)
                    .and_then(U63::new)
                    .unwrap(),
                v.map(Cow::into_owned).map(Cow::Owned),
            )
        })
    }
}

impl From<Node> for ArlockNode {
    fn from(node: Node) -> Self {
        Arc::new(RwLock::new(node))
    }
}
