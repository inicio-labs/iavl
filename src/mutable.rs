mod error;

use bytes::Bytes;
use nebz::NonEmptyBz;
use oblux::{U7, U31, U63};

pub use self::error::MutableTreeError;

use core::{cmp, mem, ops::Deref};

use crate::{
    NodeHash, Sealed,
    kvstore::{KVIterator, KVStore, MutKVStore},
};

use super::{
    Get, GetError,
    immutable::ImmutableTree,
    node::{ArlockNode, ndb::NodeDb},
    node::{
        Child, DeserializedNode, DraftedNode, InnerNode, LeafNode, Node, NodeError, SavedNode,
        ndb::FetchedNode,
    },
};

use self::error::{MutableTreeErrorKind, Result};

pub struct MutableTree<DB> {
    root: Option<ArlockNode>,
    last_saved: Option<ImmutableTree<DB>>,
    version: U63,
    ndb: NodeDb<DB>,
    size: U63,
}

impl<DB> MutableTree<DB> {
    const EMPTY_ROOT_HASH: NodeHash = [
        0xE3, 0xB0, 0xC4, 0x42, 0x98, 0xFC, 0x1C, 0x14, 0x9A, 0xFB, 0xF4, 0xC8, 0x99, 0x6F, 0xB9,
        0x24, 0x27, 0xAE, 0x41, 0xE4, 0x64, 0x9B, 0x93, 0x4C, 0xA4, 0x95, 0x99, 0x1B, 0x78, 0x52,
        0xB8, 0x55,
    ];

    pub fn new(db: DB) -> Self {
        Self::with_ndb(NodeDb::builder().db(db).build())
    }

    pub fn last_saved(&self) -> Option<&ImmutableTree<DB>> {
        self.last_saved.as_ref()
    }

    pub fn version(&self) -> U63 {
        self.version
    }

    pub fn size(&self) -> U63 {
        self.size
    }

    pub fn saved_hash(&self) -> NodeHash {
        self.last_saved()
            .map(ImmutableTree::hash)
            .unwrap_or(Self::EMPTY_ROOT_HASH)
    }

    fn with_ndb(ndb: NodeDb<DB>) -> Self {
        Self {
            root: None,
            last_saved: None,
            version: U63::MIN,
            ndb,
            size: U63::MIN,
        }
    }

    fn root(&self) -> Option<&ArlockNode> {
        self.root.as_ref()
    }
}

impl<DB> MutableTree<DB>
where
    DB: KVStore + KVIterator + Clone,
{
    pub fn load_latest_version(db: DB) -> Result<Self> {
        let ndb = NodeDb::builder().db(db).build();

        let Some((latest_root_nk, latest_root_node)) = ndb
            .fetch_latest_root_node()
            .map_err(MutableTreeErrorKind::from)?
        else {
            return Ok(Self::with_ndb(ndb));
        };

        let root = match latest_root_node {
            FetchedNode::EmptyRoot => return Ok(Self::with_ndb(ndb)),
            FetchedNode::Deserialized(denode) => denode
                .into_saved_checked(&latest_root_nk)
                .map_err(MutableTreeErrorKind::from)?,
            FetchedNode::ReferenceRoot(nk) => match ndb
                .fetch_one_node(&nk)
                .map_err(MutableTreeErrorKind::from)?
            {
                Some(node) => match node {
                    FetchedNode::Deserialized(denode) => denode
                        .into_saved_checked(&nk)
                        .map_err(MutableTreeErrorKind::from)?,
                    _ => Err(MutableTreeErrorKind::ConflictingRoot)?,
                },
                None => return Ok(Self::with_ndb(ndb)),
            },
        };

        let root = ArlockNode::from(root);

        let last_saved = ImmutableTree::builder()
            .root(root.clone())
            .ndb(ndb.clone())
            .version(*latest_root_nk.version())
            .build()
            .map_err(MutableTreeErrorKind::from)?;

        let size = last_saved.size();

        Ok(Self {
            root: Some(root),
            last_saved: Some(last_saved),
            version: *latest_root_nk.version(),
            ndb,
            size,
        })
    }
}

impl<DB> MutableTree<DB>
where
    DB: MutKVStore + KVStore + Clone,
{
    /// Inserts/updates the node with given key-value pair.
    ///
    /// Returns [`true`] if an existing key is updated.
    pub fn insert(&mut self, key: NonEmptyBz<Bytes>, value: NonEmptyBz<Bytes>) -> Result<bool> {
        let Some(root) = self.root.take() else {
            let leaf = LeafNode::builder().key(key).value(value).build();
            self.root = Some(leaf.into());
            self.size = U63::ONE;
            return Ok(false);
        };

        let (new_root, updated) = recursive_insert(&root, &self.ndb, key, value)?;

        self.root = Some(new_root.into());

        if !updated {
            self.size = self
                .size
                .get()
                .checked_add(1)
                .and_then(U63::new)
                .ok_or(MutableTreeErrorKind::Overflow)?;
        }

        Ok(updated)
    }

    /// Removes the node with given key-value pair.
    ///
    /// Returns [`false`] when `key` is not found.
    pub fn remove<K>(&mut self, key: NonEmptyBz<K>) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let Some(root) = self.root.take() else {
            return Ok(false);
        };

        let (new_root, removed) = recursive_remove(root, &self.ndb, key)?;

        self.root = new_root;

        if removed {
            // unwrap is safe here because original size must be positive for a key to be removed
            self.size = self.size.get().checked_sub(1).and_then(U63::new).unwrap();
        }

        Ok(removed)
    }

    pub fn save(&mut self) -> Result<U63> {
        let working_version = self
            .version()
            .get()
            .checked_add(1)
            .and_then(U63::new)
            .ok_or(MutableTreeErrorKind::Overflow)?;

        let Some(root) = self.root.take() else {
            self.ndb
                .save_overwriting_empty_root(working_version)
                .map_err(MutableTreeErrorKind::from)?;
            self.version = working_version;

            if let Some(tree) = self.last_saved.as_mut() {
                tree.set_version(working_version)
            }

            return Ok(working_version);
        };

        match root.read().map_err(MutableTreeErrorKind::from)?.deref() {
            Node::Saved(saved) => {
                self.ndb
                    .save_overwriting_reference_root(working_version, &saved.node_key())
                    .map_err(MutableTreeErrorKind::from)?;
            }
            Node::Drafted(drafted) => {
                // TODO: devise a strategy to avoid creating new `DraftedNode` from `&DraftedNode`.
                let drafted = drafted.into();
                let mut nonce = U31::MIN;
                let new_root: ArlockNode =
                    recursive_make_saved_nodes(drafted, &self.ndb, working_version, &mut nonce)?
                        .into();

                let new_last_saved = ImmutableTree::builder()
                    .root(new_root.clone())
                    .ndb(self.ndb.clone()) // TODO: devise a strategy to avoid `ndb`'s clone
                    .version(working_version)
                    .build()
                    .map_err(MutableTreeErrorKind::from)?;

                self.root = Some(new_root);
                self.last_saved = Some(new_last_saved);
                self.version = working_version;

                return Ok(working_version);
            }
        };

        // TODO: devise a strategy to avoid these repetitive lines
        let new_last_saved = ImmutableTree::builder()
            .root(root.clone())
            .ndb(self.ndb.clone()) // TODO: devise a strategy to avoid `ndb`'s clone
            .version(working_version)
            .build()
            .map_err(MutableTreeErrorKind::from)?;

        self.root = Some(root);
        self.last_saved = Some(new_last_saved);
        self.version = working_version;

        Ok(working_version)
    }

    /// `root` must be of Saved type.
    #[allow(dead_code)]
    pub(crate) fn with_saved_root(
        ndb: NodeDb<DB>,
        root: ArlockNode,
    ) -> Result<Self, MutableTreeErrorKind> {
        let version = root
            .read()?
            .as_saved()
            .map(|sn| save_new_root_node_checked(sn, &ndb).map(|_| sn.version()))
            .transpose()?
            .ok_or(MutableTreeErrorKind::MissingNodeKey)?;

        let last_saved = ImmutableTree::builder()
            .root(root.clone())
            .ndb(ndb.clone())
            .version(version)
            .build()?;

        let size = last_saved.size();

        Ok(Self {
            root: Some(root),
            ndb,
            version,
            last_saved: Some(last_saved),
            size,
        })
    }
}

impl<DB> Get for MutableTree<DB>
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
        let Some(root) = self.root() else {
            return Ok((U63::MIN, None));
        };

        root.read()
            .map_err(NodeError::from)?
            .get(&self.ndb, key)
            .map_err(From::from)
    }
}

impl<DB> Sealed for MutableTree<DB> {}

fn recursive_remove<DB, K>(
    node: ArlockNode,
    ndb: &NodeDb<DB>,
    key: NonEmptyBz<K>,
) -> Result<(Option<ArlockNode>, bool), MutableTreeErrorKind>
where
    DB: KVStore,
    K: AsRef<[u8]>,
{
    {
        let gnode = node.read()?;
        if gnode.is_leaf() {
            if gnode.key().as_ref_slice() == key.as_ref_slice() {
                return Ok((None, true));
            }

            mem::drop(gnode);

            return Ok((Some(node), false));
        }
    }

    // unwraps are safe because inner node must contain children
    let (left, right) = {
        let mut gnode_mut = node.write()?;

        let left = gnode_mut
            .left_mut()
            .map(Child::extract)
            .transpose()?
            .map(|c| c.fetch_full(ndb))
            .transpose()?
            .unwrap();

        let right = gnode_mut
            .right_mut()
            .map(Child::extract)
            .transpose()?
            .map(|c| c.fetch_full(ndb))
            .transpose()?
            .unwrap();

        (left, right)
    };

    let gnode = node.read()?;

    let (new_left, new_right, removed) = {
        if key.as_ref_slice() < gnode.key().as_ref_slice() {
            let (new_left, removed) = recursive_remove(left, ndb, key)?;
            (new_left, Some(right), removed)
        } else {
            let (new_right, removed) = recursive_remove(right, ndb, key)?;
            (Some(left), new_right, removed)
        }
    };

    if !removed {
        mem::drop(gnode);

        return Ok((Some(node), false));
    }

    match (new_left, new_right) {
        (None, None) => unreachable!(),
        (left @ Some(_), None) => Ok((left, true)),
        (None, right @ Some(_)) => Ok((right, true)),
        (Some(left), Some(right)) => {
            let (left_height, left_size) = {
                let gleft = left.read()?;
                (gleft.height(), gleft.size())
            };

            let (right_height, right_size) = {
                let gright = right.read()?;
                (gright.height(), gright.size())
            };

            let height = cmp::max(left_height, right_height)
                .get()
                .checked_add(1)
                .and_then(U7::new)
                .unwrap();

            let size = left_size
                .get()
                .checked_add(right_size.get())
                .and_then(U63::new)
                .unwrap();

            let mut inner = InnerNode::builder()
                .key(gnode.key().cloned())
                .height(height)
                .size(size)
                .left(Child::Full(left))
                .right(Child::Full(right))
                .build();

            inner.make_balanced(ndb)?;

            Ok((Some(inner.into()), true))
        }
    }
}

fn save_new_root_node_checked<DB>(
    saved_root_node: &SavedNode,
    ndb: &NodeDb<DB>,
) -> Result<(), MutableTreeErrorKind>
where
    DB: MutKVStore + KVStore,
{
    let Some(existing) = ndb.save_non_overwririting_one_node(saved_root_node)? else {
        return Ok(());
    };

    match (saved_root_node, existing) {
        (
            SavedNode::Inner(root),
            FetchedNode::Deserialized(DeserializedNode::Inner(deserialized_drafted, hash)),
        ) => {
            if root.hash() != &hash {
                return Err(MutableTreeErrorKind::ConflictingRoot);
            }

            let deserialized_hashed = deserialized_drafted.to_hashed(*root.version())?;

            root.hash()
                .eq(deserialized_hashed.hash())
                .then_some(())
                .ok_or(MutableTreeErrorKind::ConflictingRoot)
        }
        (
            SavedNode::Leaf(root),
            FetchedNode::Deserialized(DeserializedNode::Leaf(deserialized_drafted)),
        ) => deserialized_drafted
            .to_hashed(*root.version())
            .hash()
            .eq(root.hash())
            .then_some(())
            .ok_or(MutableTreeErrorKind::ConflictingRoot),
        _ => Err(MutableTreeErrorKind::ConflictingRoot),
    }
}

fn recursive_insert<DB>(
    node: &ArlockNode,
    ndb: &NodeDb<DB>,
    key: NonEmptyBz<Bytes>,
    value: NonEmptyBz<Bytes>,
) -> Result<(DraftedNode, bool), MutableTreeErrorKind>
where
    DB: KVStore,
{
    {
        let gnode = node.read()?;
        if gnode.is_leaf() {
            return handle_leaf_insert_case(node, gnode.key(), key, value).map(|node| {
                let updated = matches!(node, DraftedNode::Leaf(_));
                (node, updated)
            });
        }
    }

    // unwraps are safe because inner node must contain children
    let (left, right) = {
        let mut gnode_mut = node.write()?;

        let left = gnode_mut
            .left_mut()
            .map(Child::extract)
            .transpose()?
            .map(|c| c.fetch_full(ndb))
            .transpose()?
            .unwrap();

        let right = gnode_mut
            .right_mut()
            .map(Child::extract)
            .transpose()?
            .map(|c| c.fetch_full(ndb))
            .transpose()?
            .unwrap();

        (left, right)
    };

    let gnode = node.read()?;

    let (left, right, updated) = if key.as_ref() < gnode.key() {
        let (new_left, updated) = recursive_insert(&left, ndb, key, value)?;
        (new_left.into(), right, updated)
    } else {
        let (new_right, updated) = recursive_insert(&right, ndb, key, value)?;
        (left, new_right.into(), updated)
    };

    let height = cmp::max(left.read()?.height(), right.read()?.height())
        .get()
        .checked_add(1)
        .and_then(U7::new)
        .unwrap();

    let size = left
        .read()?
        .size()
        .get()
        .checked_add(right.read()?.size().get())
        .and_then(U63::new)
        .unwrap();

    let mut inner = InnerNode::builder()
        .key(gnode.key().cloned())
        .height(height)
        .size(size)
        .left(Child::Full(left))
        .right(Child::Full(right))
        .build();

    if updated {
        return Ok((inner.into(), true));
    }

    inner.make_balanced(ndb)?;

    Ok((inner.into(), updated))
}

// TODO: make this efficient by tracking the exact reference count,
// `Arc::into_inner` should work with root.
fn recursive_make_saved_nodes<DB>(
    drafted: DraftedNode,
    ndb: &NodeDb<DB>,
    version: U63,
    nonce: &mut U31,
) -> Result<SavedNode, MutableTreeErrorKind>
where
    DB: MutKVStore + KVStore,
{
    *nonce = nonce
        .get()
        .checked_add(1)
        .and_then(U31::new)
        .ok_or(MutableTreeErrorKind::Overflow)?;

    let this_nonce = *nonce;

    let mut save_arlock_node = |node: &mut ArlockNode| -> Result<_, MutableTreeErrorKind> {
        let mut gnode_mut = node.write()?;

        if let Node::Drafted(drafted) = gnode_mut.deref() {
            *gnode_mut = recursive_make_saved_nodes(drafted.into(), ndb, version, nonce)?.into();
        }

        Ok(())
    };

    let saved = match drafted {
        DraftedNode::Leaf(leaf) => leaf.to_hashed(version).into_saved(this_nonce).into(),
        DraftedNode::Inner(mut inner) => {
            match inner.left_mut() {
                Child::Full(full) => save_arlock_node(full)?,
                Child::Part(_) => (),
            }

            match inner.right_mut() {
                Child::Full(full) => save_arlock_node(full)?,
                Child::Part(_) => (),
            }

            // unwraps are safe because children must have been saved
            inner
                .to_hashed(version)
                .unwrap()
                .into_saved(this_nonce)
                .unwrap()
                .into()
        }
    };

    // TODO: remove this assert after save behaviour fully controlled
    assert!(ndb.save_non_overwririting_one_node(&saved)?.is_none());

    Ok(saved)
}

fn handle_leaf_insert_case(
    node: &ArlockNode,
    existing_key: NonEmptyBz<&Bytes>,
    new_key: NonEmptyBz<Bytes>,
    new_value: NonEmptyBz<Bytes>,
) -> Result<DraftedNode, MutableTreeErrorKind> {
    let new_leaf = LeafNode::builder().key(new_key).value(new_value).build();

    if new_leaf.key().as_ref() == existing_key {
        return Ok(new_leaf.into());
    }

    let (inner_key, left, right) = if new_leaf.key().as_ref() < existing_key {
        (
            existing_key.cloned(),
            ArlockNode::from(new_leaf),
            node.clone(),
        )
    } else {
        (
            new_leaf.key().clone(),
            node.clone(),
            ArlockNode::from(new_leaf),
        )
    };

    let inner = InnerNode::builder()
        .key(inner_key)
        .height(U7::ONE)
        .size(U63::TWO)
        .left(left.into())
        .right(right.into())
        .build();

    Ok(inner.into())
}
