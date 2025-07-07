use core::num::NonZeroUsize;

use std::io::{Read, Write};

use integer_encoding::VarIntReader;

use crate::{
    NodeHashPair, NodeKey, NodeKeyPair,
    encoding::{self, DeserializationError, SerializationError},
    types::{NonEmptyBz, U7, U31, U63},
};

use super::{
    ArlockNode, InnerNode, LeafNode, Node, NodeHash,
    info::{Drafted, Hashed, Saved},
    inner::Child,
};

#[derive(Debug, Clone)]
pub enum DraftedNode {
    Inner(InnerNode<Drafted>),
    Leaf(LeafNode<Drafted>),
}

#[derive(Debug, Clone)]
pub enum HashedNode {
    Inner(InnerNode<Hashed<NodeHashPair>>),
    Leaf(LeafNode<Hashed>),
}

#[derive(Debug, Clone)]
pub enum SavedNode {
    Inner(InnerNode<Saved<NodeHashPair, NodeKeyPair>>),
    Leaf(LeafNode<Saved>),
}

#[derive(Debug)]
pub enum DeserializedNode {
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
}

impl DraftedNode {
    pub fn key(&self) -> &NonEmptyBz {
        match self {
            Self::Inner(inner) => inner.key(),
            Self::Leaf(leaf) => leaf.key(),
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

    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf(_))
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
            Self::Inner(inner_node) => Some(inner_node.right_mut()),
            Self::Leaf(_) => None,
        }
    }
}

impl HashedNode {
    pub fn key(&self) -> &NonEmptyBz {
        match self {
            Self::Inner(inner_node) => inner_node.key(),
            Self::Leaf(leaf_node) => leaf_node.key(),
        }
    }

    pub fn height(&self) -> U7 {
        match self {
            Self::Inner(inner_node) => inner_node.height(),
            Self::Leaf(_) => LeafNode::<()>::HEIGHT,
        }
    }

    pub fn size(&self) -> U63 {
        match self {
            Self::Inner(inner_node) => inner_node.size(),
            Self::Leaf(_) => LeafNode::<()>::SIZE,
        }
    }

    pub fn hash(&self) -> &NodeHash {
        match self {
            Self::Inner(inner_node) => inner_node.hash(),
            Self::Leaf(leaf_node) => leaf_node.hash(),
        }
    }

    pub fn version(&self) -> U63 {
        match self {
            Self::Inner(inner_node) => *inner_node.version(),
            Self::Leaf(leaf_node) => *leaf_node.version(),
        }
    }

    pub fn left(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.left()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.right()),
            Self::Leaf(_) => None,
        }
    }

    pub fn left_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.left_mut()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.right_mut()),
            Self::Leaf(_) => None,
        }
    }
}

impl SavedNode {
    pub fn key(&self) -> &NonEmptyBz {
        match self {
            Self::Inner(inner_node) => inner_node.key(),
            Self::Leaf(leaf_node) => leaf_node.key(),
        }
    }

    pub fn height(&self) -> U7 {
        match self {
            Self::Inner(inner_node) => inner_node.height(),
            Self::Leaf(_) => LeafNode::<()>::HEIGHT,
        }
    }

    pub fn hash(&self) -> &NodeHash {
        match self {
            Self::Inner(inner_node) => inner_node.hash(),
            Self::Leaf(leaf_node) => leaf_node.hash(),
        }
    }

    pub fn node_key(&self) -> NodeKey {
        match self {
            SavedNode::Inner(inner_node) => inner_node.node_key(),
            SavedNode::Leaf(leaf_node) => leaf_node.node_key(),
        }
    }

    pub fn version(&self) -> U63 {
        match self {
            Self::Inner(inner_node) => *inner_node.version(),
            Self::Leaf(leaf_node) => *leaf_node.version(),
        }
    }

    pub fn nonce(&self) -> U31 {
        match self {
            Self::Inner(inner_node) => *inner_node.nonce(),
            Self::Leaf(leaf_node) => *leaf_node.nonce(),
        }
    }

    pub fn size(&self) -> U63 {
        match self {
            Self::Inner(inner_node) => inner_node.size(),
            Self::Leaf(_) => LeafNode::<()>::SIZE,
        }
    }

    pub fn left(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.left()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right(&self) -> Option<&Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.right()),
            Self::Leaf(_) => None,
        }
    }

    pub fn left_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.left_mut()),
            Self::Leaf(_) => None,
        }
    }

    pub fn right_mut(&mut self) -> Option<&mut Child> {
        match self {
            Self::Inner(inner_node) => Some(inner_node.right_mut()),
            Self::Leaf(_) => None,
        }
    }

    pub fn serialize<W>(&self, writer: W) -> Result<NonZeroUsize, SerializationError>
    where
        W: Write,
    {
        match self {
            Self::Inner(inner_node) => inner_node.serialize(writer),
            Self::Leaf(leaf_node) => leaf_node.serialize(writer),
        }
    }
}

impl From<DeserializedNode> for DraftedNode {
    fn from(node: DeserializedNode) -> Self {
        match node {
            DeserializedNode::Inner(inner_node, _) => Self::Inner(inner_node),
            DeserializedNode::Leaf(leaf_node) => Self::Leaf(leaf_node),
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
            DraftedNode::Inner(inner_node) => Self::Inner(inner_node.into()),
            DraftedNode::Leaf(leaf_node) => Self::Leaf(leaf_node.into()),
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

impl From<DeserializedNode> for Node {
    fn from(node: DeserializedNode) -> Self {
        Self::Drafted(node.into())
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

impl From<DeserializedNode> for ArlockNode {
    fn from(node: DeserializedNode) -> Self {
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
