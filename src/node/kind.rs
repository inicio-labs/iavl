use core::num::NonZeroUsize;

use std::io::{Read, Write};

use bytes::Bytes;
use integer_encoding::VarIntReader;
use nebz::NonEmptyBz;
use oblux::{U7, U63};

use crate::{
    NodeHashPair, NodeKey, NodeKeyPair,
    encoding::{self, DeserializationError, SerializationError},
};

use super::{
    ArlockNode, InnerNode, InnerNodeError, LeafNode, Node, NodeHash,
    info::{Drafted, Saved},
    inner::Child,
};

#[derive(Debug, Clone)]
pub(crate) enum DraftedNode {
    Inner(InnerNode<Drafted>),
    Leaf(LeafNode<Drafted>),
}

#[derive(Debug, Clone)]
pub(crate) enum SavedNode {
    Inner(InnerNode<Saved<NodeHashPair, NodeKeyPair>>),
    Leaf(LeafNode<Saved>),
}

#[derive(Debug)]
pub(crate) enum DeserializedNode {
    Inner(InnerNode<Drafted>, NodeHash),
    Leaf(LeafNode<Drafted>),
}

impl DeserializedNode {
    pub fn deserialize<R>(mut reader: R) -> Result<Self, DeserializationError>
    where
        R: Read,
    {
        let height = reader
            .read_varint::<i8>()
            .map(U7::from_signed)?
            .ok_or(DeserializationError::InvalidInteger)?;

        let size = reader
            .read_varint::<i64>()
            .map(U63::from_signed)?
            .ok_or(DeserializationError::InvalidInteger)?;

        let key = encoding::deserialize_bytes(&mut reader)?;

        if height.get() == 0 {
            let value = encoding::deserialize_bytes(&mut reader)?;

            let node = LeafNode::builder().key(key).value(value).build();

            return Ok(Self::Leaf(node));
        }

        let node_hash = encoding::deserialize_hash(&mut reader)?;

        if reader.read_varint::<u8>()? != 0 {
            return Err(DeserializationError::InvalidMode);
        }

        let left = NodeKey::deserialize(&mut reader).map(Child::Part)?;
        let right = NodeKey::deserialize(&mut reader).map(Child::Part)?;

        let inner_node = InnerNode::builder()
            .key(key)
            .height(height)
            .size(size)
            .left(left)
            .right(right)
            .build();

        Ok(Self::Inner(inner_node, node_hash))
    }

    pub fn into_saved_checked(self, nk: &NodeKey) -> Result<SavedNode, InnerNodeError> {
        match self {
            DeserializedNode::Inner(inner, hash) => {
                let hashed = inner.to_hashed(*nk.version())?;

                if hashed.hash() != &hash {
                    return Err(InnerNodeError::Other("inconsistent hash".into()));
                }

                hashed.into_saved(*nk.nonce()).map(SavedNode::Inner)
            }
            DeserializedNode::Leaf(leaf) => {
                let saved_leaf = leaf.to_hashed(*nk.version()).into_saved(*nk.nonce());

                Ok(SavedNode::Leaf(saved_leaf))
            }
        }
    }
}

impl DraftedNode {
    pub fn key(&self) -> NonEmptyBz<&Bytes> {
        match self {
            Self::Inner(inner) => inner.key().as_ref(),
            Self::Leaf(leaf) => leaf.key().as_ref(),
        }
    }

    pub fn height(&self) -> U7 {
        match self {
            Self::Inner(inner) => inner.height(),
            Self::Leaf(_) => LeafNode::<()>::HEIGHT,
        }
    }

    pub fn size(&self) -> U63 {
        match self {
            Self::Inner(inner) => inner.size(),
            Self::Leaf(_) => LeafNode::<()>::SIZE,
        }
    }

    pub fn left(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner) => Some(inner.left()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner) => Some(inner.right()),
            Self::Leaf(_) => None,
        }
    }

    pub fn left_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner) => Some(inner.left_mut()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner) => Some(inner.right_mut()),
            Self::Leaf(_) => None,
        }
    }
}

impl SavedNode {
    pub fn key(&self) -> NonEmptyBz<&Bytes> {
        match self {
            Self::Inner(inner) => inner.key().as_ref(),
            Self::Leaf(leaf) => leaf.key().as_ref(),
        }
    }

    pub fn height(&self) -> U7 {
        match self {
            Self::Inner(inner) => inner.height(),
            Self::Leaf(_) => LeafNode::<()>::HEIGHT,
        }
    }

    pub fn hash(&self) -> &NodeHash {
        match self {
            Self::Inner(inner) => inner.hash(),
            Self::Leaf(leaf) => leaf.hash(),
        }
    }

    pub fn node_key(&self) -> NodeKey {
        match self {
            Self::Inner(inner) => inner.node_key(),
            Self::Leaf(leaf) => leaf.node_key(),
        }
    }

    pub fn version(&self) -> U63 {
        match self {
            Self::Inner(inner) => *inner.version(),
            Self::Leaf(leaf) => *leaf.version(),
        }
    }

    pub fn size(&self) -> U63 {
        match self {
            Self::Inner(inner) => inner.size(),
            Self::Leaf(_) => LeafNode::<()>::SIZE,
        }
    }

    pub fn left(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner) => Some(inner.left()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner) => Some(inner.right()),
            Self::Leaf(_) => None,
        }
    }

    pub fn left_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner) => Some(inner.left_mut()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner) => Some(inner.right_mut()),
            Self::Leaf(_) => None,
        }
    }

    pub fn serialize<W>(&self, writer: W) -> Result<NonZeroUsize, SerializationError>
    where
        W: Write,
    {
        match self {
            Self::Inner(inner) => inner.serialize(writer),
            Self::Leaf(leaf) => leaf.serialize(writer),
        }
    }
}

impl From<LeafNode<Drafted>> for DraftedNode {
    fn from(node: LeafNode<Drafted>) -> Self {
        Self::Leaf(node)
    }
}

impl From<InnerNode<Drafted>> for DraftedNode {
    fn from(node: InnerNode<Drafted>) -> Self {
        Self::Inner(node)
    }
}

impl From<&DraftedNode> for DraftedNode {
    fn from(node: &DraftedNode) -> Self {
        match node {
            Self::Inner(inner_node) => Self::Inner(inner_node.into()),
            Self::Leaf(leaf_node) => Self::Leaf(leaf_node.into()),
        }
    }
}

impl From<&SavedNode> for DraftedNode {
    fn from(saved: &SavedNode) -> Self {
        match saved {
            SavedNode::Inner(inner) => Self::Inner(inner.into()),
            SavedNode::Leaf(leaf) => Self::Leaf(leaf.into()),
        }
    }
}

impl From<&Node> for DraftedNode {
    fn from(node: &Node) -> Self {
        match node {
            Node::Drafted(drafted) => drafted.into(),
            Node::Saved(saved) => saved.into(),
        }
    }
}

impl From<InnerNode<Saved<NodeHashPair, NodeKeyPair>>> for SavedNode {
    fn from(node: InnerNode<Saved<NodeHashPair, NodeKeyPair>>) -> Self {
        Self::Inner(node)
    }
}

impl From<LeafNode<Saved>> for SavedNode {
    fn from(node: LeafNode<Saved>) -> Self {
        Self::Leaf(node)
    }
}

impl From<DraftedNode> for Node {
    fn from(node: DraftedNode) -> Self {
        Self::Drafted(node)
    }
}

impl From<SavedNode> for Node {
    fn from(node: SavedNode) -> Self {
        Self::Saved(node)
    }
}

impl From<LeafNode<Drafted>> for Node {
    fn from(node: LeafNode<Drafted>) -> Self {
        DraftedNode::from(node).into()
    }
}

impl From<InnerNode<Drafted>> for Node {
    fn from(node: InnerNode<Drafted>) -> Self {
        DraftedNode::from(node).into()
    }
}

impl From<LeafNode<Saved>> for Node {
    fn from(node: LeafNode<Saved>) -> Self {
        SavedNode::from(node).into()
    }
}

impl From<InnerNode<Saved<NodeHashPair, NodeKeyPair>>> for Node {
    fn from(node: InnerNode<Saved<NodeHashPair, NodeKeyPair>>) -> Self {
        SavedNode::from(node).into()
    }
}

impl From<DraftedNode> for ArlockNode {
    fn from(node: DraftedNode) -> Self {
        Node::from(node).into()
    }
}

impl From<SavedNode> for ArlockNode {
    fn from(node: SavedNode) -> Self {
        Node::from(node).into()
    }
}

impl From<LeafNode<Drafted>> for ArlockNode {
    fn from(node: LeafNode<Drafted>) -> Self {
        Node::from(node).into()
    }
}

impl From<InnerNode<Drafted>> for ArlockNode {
    fn from(node: InnerNode<Drafted>) -> Self {
        Node::from(node).into()
    }
}

impl From<LeafNode<Saved>> for ArlockNode {
    fn from(node: LeafNode<Saved>) -> Self {
        Node::from(node).into()
    }
}

impl From<InnerNode<Saved<NodeHashPair, NodeKeyPair>>> for ArlockNode {
    fn from(node: InnerNode<Saved<NodeHashPair, NodeKeyPair>>) -> Self {
        Node::from(node).into()
    }
}
