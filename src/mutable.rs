mod error;

pub use self::error::MutableTreeError;

use core::{mem, ops::Deref};

use std::borrow::Cow;

use crate::{
    kvstore::{KVStore, MutKVStore},
    node::{Child, DeserializedNode, DraftedNode, InnerNode, LeafNode, Node, SavedNode},
    types::{NonEmptyBz, U7, U31, U63},
};

use super::{
    immutable::ImmutableTree,
    node::{ArlockNode, db::NodeDb},
};

use self::error::Result;

pub struct MutableTree<DB> {
    root: Option<ArlockNode>,
    ndb: NodeDb<DB>,
    last_saved: Option<ImmutableTree<DB>>,
}

impl<DB> MutableTree<DB> {
    pub fn new(ndb: NodeDb<DB>) -> Self {
        Self {
            root: None,
            ndb,
            last_saved: None,
        }
    }
}

impl<DB> MutableTree<DB>
where
    DB: MutKVStore + KVStore + Clone,
{
    /// `root` must be of Saved type.
    pub fn with_saved_root(ndb: NodeDb<DB>, root: ArlockNode) -> Result<Self> {
        let version = root
            .read()?
            .as_saved()
            .map(|sn| save_new_root_node_checked(sn, &ndb).map(|_| sn.version()))
            .transpose()?
            .ok_or(MutableTreeError::MissingNodeKey)?;

        let last_saved = ImmutableTree::builder()
            .root(root.clone())
            .ndb(ndb.clone())
            .version(version)
            .build();

        Ok(Self {
            root: Some(root),
            ndb,
            last_saved: Some(last_saved),
        })
    }
}

impl<DB> MutableTree<DB> {
    pub fn last_saved(&self) -> Option<&ImmutableTree<DB>> {
        self.last_saved.as_ref()
    }

    fn root(&self) -> Option<&ArlockNode> {
        self.root.as_ref()
    }
}

impl<DB> MutableTree<DB>
where
    DB: KVStore,
{
    pub fn get(&self, key: &NonEmptyBz) -> Result<(U63, Option<NonEmptyBz>)> {
        let Some(root) = self.root() else {
            return Ok((U63::MIN, None));
        };

        root.read()?
            .get(&self.ndb, key)
            .map(|(idx, val)| (idx, val.map(Cow::into_owned)))
            .map_err(From::from)
    }
}

impl<DB> MutableTree<DB>
where
    DB: MutKVStore + KVStore + Clone,
{
    /// inserts/updates the node with given key-value pair, and returns the old root along with a
    /// bool that is `true` if an existing key is updated. Returns None if root was empty.
    pub fn insert(
        &mut self,
        key: NonEmptyBz,
        value: NonEmptyBz,
    ) -> Result<Option<(ArlockNode, bool)>> {
        let Some(root) = self.root.take() else {
            let leaf = LeafNode::builder().key(key).value(value).build();
            self.root = Some(leaf.into());
            return Ok(None);
        };

        let (new_root, updated) = recursive_insert(&root, &self.ndb, key, value)?;

        self.root = Some(new_root.into());

        Ok(Some((root, updated)))
    }

    pub fn remove(&mut self, key: &NonEmptyBz) -> Result<bool> {
        let Some(root) = self.root.take() else {
            return Ok(false);
        };

        let (new_root, removed) = recursive_remove(root, &self.ndb, key)?;

        self.root = new_root;

        Ok(removed)
    }

    // TODO: ascertain whether version ought to be bumped up when no changes done since last save
    pub fn save(&mut self) -> Result<Option<ImmutableTree<DB>>> {
        let Some(root) = self.root.take() else {
            return Ok(None);
        };

        let drafted = match root.read()?.deref() {
            Node::Drafted(drafted) => drafted.into(),
            Node::Saved(_) => return Ok(None),
        };

        let version = self
            .last_saved()
            .map(ImmutableTree::version)
            .unwrap_or(U63::MIN)
            .get()
            .checked_add(1)
            .and_then(U63::new)
            .ok_or(MutableTreeError::Overflow)?;

        let mut nonce = U31::MIN;
        let new_root: ArlockNode =
            recursive_make_saved_nodes(drafted, &self.ndb, version, &mut nonce)?.into();

        let new_last_saved = ImmutableTree::builder()
            .root(new_root.clone())
            .ndb(self.ndb.clone())
            .version(version)
            .build();

        self.root = Some(new_root);

        Ok(self.last_saved.replace(new_last_saved))
    }
}

fn recursive_remove<DB>(
    node: ArlockNode,
    ndb: &NodeDb<DB>,
    key: &NonEmptyBz,
) -> Result<(Option<ArlockNode>, bool)>
where
    DB: KVStore,
{
    {
        let gnode = node.read()?;
        if gnode.is_leaf() {
            if gnode.key() == key {
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
        if key < gnode.key() {
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

            let height = core::cmp::max(left_height, right_height)
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
                .key(gnode.key().clone())
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

fn save_new_root_node_checked<DB>(saved_root_node: &SavedNode, ndb: &NodeDb<DB>) -> Result<()>
where
    DB: MutKVStore + KVStore,
{
    let maybe_existing = ndb.save_non_overwririting_one_node(saved_root_node)?;

    match (saved_root_node, maybe_existing) {
        (_, None) => Ok(()),
        (SavedNode::Inner(root), Some(DeserializedNode::Inner(deserialized_drafted, hash))) => {
            if root.hash() != &hash {
                return Err(MutableTreeError::ConflictingRoot);
            }

            let deserialized_hashed = deserialized_drafted.into_hashed(*root.version())?;

            root.hash()
                .eq(deserialized_hashed.hash())
                .then_some(())
                .ok_or(MutableTreeError::ConflictingRoot)
        }
        (SavedNode::Leaf(root), Some(DeserializedNode::Leaf(deserialized_drafted))) => {
            deserialized_drafted
                .into_hashed(*root.version())
                .hash()
                .eq(root.hash())
                .then_some(())
                .ok_or(MutableTreeError::ConflictingRoot)
        }
        _ => Err(MutableTreeError::ConflictingRoot),
    }
}

fn recursive_insert<DB>(
    node: &ArlockNode,
    ndb: &NodeDb<DB>,
    key: NonEmptyBz,
    value: NonEmptyBz,
) -> Result<(DraftedNode, bool)>
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

    let (left, right, updated) = if &key < gnode.key() {
        let (new_left, updated) = recursive_insert(&left, ndb, key, value)?;
        (new_left.into(), right, updated)
    } else {
        let (new_right, updated) = recursive_insert(&right, ndb, key, value)?;
        (left, new_right.into(), updated)
    };

    let height = core::cmp::max(left.read()?.height(), right.read()?.height())
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
        .key(gnode.key().clone())
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
) -> Result<SavedNode>
where
    DB: MutKVStore + KVStore,
{
    *nonce = nonce
        .get()
        .checked_add(1)
        .and_then(U31::new)
        .ok_or(MutableTreeError::Overflow)?;

    let this_nonce = *nonce;

    let mut save_arlock_node = |node: &mut ArlockNode| -> Result<_> {
        let mut gnode = node.write()?;

        if let Node::Drafted(drafted) = gnode.deref() {
            *gnode = recursive_make_saved_nodes(drafted.into(), ndb, version, nonce)?.into();
        }

        Ok(())
    };

    let saved = match drafted {
        DraftedNode::Leaf(leaf) => leaf.into_hashed(version).into_saved(this_nonce).into(),
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
                .into_hashed(version)
                .unwrap()
                .into_saved(this_nonce)
                .unwrap()
                .into()
        }
    };

    assert!(ndb.save_non_overwririting_one_node(&saved)?.is_none());

    Ok(saved)
}

fn handle_leaf_insert_case(
    node: &ArlockNode,
    existing_key: &NonEmptyBz,
    new_key: NonEmptyBz,
    new_value: NonEmptyBz,
) -> Result<DraftedNode> {
    let new_leaf = LeafNode::builder().key(new_key).value(new_value).build();

    if new_leaf.key() == existing_key {
        return Ok(new_leaf.into());
    }

    let (inner_key, left, right) = if new_leaf.key() < existing_key {
        (
            existing_key.clone(),
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
