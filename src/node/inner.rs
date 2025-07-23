mod balance;
mod error;

pub use self::error::InnerNodeError;

use core::{mem, num::NonZeroUsize};

use std::{
	io::Write,
	sync::{PoisonError, RwLockReadGuard},
};

use bytes::Bytes;
use integer_encoding::VarIntWriter;
use nebz::NonEmptyBz;
use oblux::{U7, U63};
use sha2::{Digest, Sha256};

use crate::{
	NodeHashPair, NodeKey, NodeKeyPair,
	encoding::{self, SerializationError},
	kvstore::KVStore,
};

use super::{
	ArlockNode, Node, SavedNode,
	info::{Drafted, Drafter, Hashed, Hasher, Saved, Saver},
	ndb::{FetchedNode, NodeDb},
};

use self::error::Result;

const LEGACY_MODE: u8 = 0;

#[derive(Debug, Clone)]
pub(crate) struct InnerNode<INFO> {
	info: INFO,
	height: U7,
	size: U63,
	left: Child,
	right: Child,
}

#[derive(Debug, Clone)]
pub(crate) enum Child {
	Full(ArlockNode),
	Part(NodeKey),
}

impl<INFO> InnerNode<INFO> {
	pub fn height(&self) -> U7 {
		self.height
	}

	pub fn size(&self) -> U63 {
		self.size
	}

	pub fn left(&self) -> &Child {
		&self.left
	}

	pub fn right(&self) -> &Child {
		&self.right
	}

	pub fn left_mut(&mut self) -> &mut Child {
		&mut self.left
	}

	pub fn right_mut(&mut self) -> &mut Child {
		&mut self.right
	}
}

#[bon::bon]
impl InnerNode<Drafted> {
	#[builder]
	pub fn new(key: NonEmptyBz<Bytes>, height: U7, size: U63, left: Child, right: Child) -> Self {
		Self { info: Drafted::new(key), height, size, left, right }
	}
}

impl InnerNode<Drafted> {
	pub fn to_hashed(&self, version: U63) -> Result<InnerNode<Hashed<NodeHashPair>>> {
		let left = self
			.left()
			.as_full()
			.ok_or("inner node's children must be hashed".into())
			.map_err(InnerNodeError::IntoHashed)?;

		let right = self
			.right()
			.as_full()
			.ok_or("inner node's children must be hashed".into())
			.map_err(InnerNodeError::IntoHashed)?;

		let (hash, left_hash, right_hash) = {
			let mut hasher = Sha256::new();

			// unwrap calls are safe because write on Sha256's hasher is infallible
			hasher.write_varint(self.height.to_signed()).unwrap();
			hasher.write_varint(self.size.to_signed()).unwrap();
			hasher.write_varint(version.to_signed()).unwrap();

			let left_hash = *left
				.read()?
				.hash()
				.inspect(|&h| {
					encoding::serialize_hash(h, &mut hasher).unwrap();
				})
				.ok_or("inner node's children must be hashed".into())
				.map_err(InnerNodeError::IntoHashed)?;

			let right_hash = *right
				.read()?
				.hash()
				.inspect(|&h| {
					encoding::serialize_hash(h, &mut hasher).unwrap();
				})
				.ok_or("inner node's children must be hashed".into())
				.map_err(InnerNodeError::IntoHashed)?;

			(hasher.finalize(), left_hash, right_hash)
		};

		let inner_node = InnerNode {
			info: self.info.clone().into_hashed(version, hash.into(), (left_hash, right_hash)),
			height: self.height,
			size: self.size,
			left: self.left.clone(),
			right: self.right.clone(),
		};

		Ok(inner_node)
	}
}

impl<K, VERSION, HASH, HAUX> InnerNode<Drafter<K, Hasher<VERSION, HASH, HAUX>>> {
	#[allow(clippy::type_complexity)]
	pub fn into_saved<NONCE>(
		self,
		nonce: NONCE,
	) -> Result<InnerNode<Drafter<K, Hasher<VERSION, HASH, HAUX, Saver<NONCE, NodeKeyPair>>>>> {
		let left_nk = self
			.left()
			.node_key()?
			.ok_or("child must yield node key".into())
			.map_err(InnerNodeError::IntoSaved)?;

		let right_nk = self
			.right()
			.node_key()?
			.ok_or("child must yield node key".into())
			.map_err(InnerNodeError::IntoSaved)?;

		let inner_node = InnerNode {
			info: self.info.into_saved(nonce, (left_nk, right_nk)),
			height: self.height,
			size: self.size,
			left: self.left,
			right: self.right,
		};

		Ok(inner_node)
	}
}

impl<K, HAUX> InnerNode<Saved<HAUX, NodeKeyPair, K>>
where
	K: AsRef<[u8]>,
{
	pub fn serialize<W>(&self, mut writer: W) -> Result<NonZeroUsize, SerializationError>
	where
		W: Write,
	{
		let height_bytes_len = writer.write_varint(self.height().to_signed())?;
		let size_bytes_len = writer.write_varint(self.size().to_signed())?;

		let key_bytes_len = encoding::serialize_nebz(self.key().as_ref(), &mut writer)?;
		let hash_bytes_len = encoding::serialize_hash(self.hash(), &mut writer)?;

		// TODO: ascertain whether zig-zag encoding is needed
		let legacy_mode_bytes_len = writer.write_varint(LEGACY_MODE)?;

		let (left_nk, right_nk) = self.children_nk_pair();
		let left_nk_bytes_len = left_nk.serialize(&mut writer)?;
		let right_nk_bytes_len = right_nk.serialize(&mut writer)?;

		// direct addition won't overflow
		key_bytes_len
			.checked_add(
				height_bytes_len
					+ size_bytes_len
					+ hash_bytes_len.get()
					+ legacy_mode_bytes_len
					+ left_nk_bytes_len.get()
					+ right_nk_bytes_len.get(),
			)
			.ok_or(SerializationError::Overflow)
	}
}

impl<K, HAUX, SAUX> InnerNode<Saved<HAUX, SAUX, K>>
where
	K: AsRef<[u8]>,
{
	pub fn node_key(&self) -> NodeKey {
		NodeKey::new(*self.version(), *self.nonce())
	}
}

impl<K, VERSION, HASH, HAUX, STATUS> InnerNode<Drafter<K, Hasher<VERSION, HASH, HAUX, STATUS>>> {
	pub fn version(&self) -> &VERSION {
		self.info.version()
	}

	pub fn hash(&self) -> &HASH {
		self.info.hash()
	}
}

impl<K, STAGE> InnerNode<Drafter<K, STAGE>> {
	pub fn key(&self) -> &K {
		self.info.key()
	}
}

impl<K, VERSION, HASH, HAUX, NONCE, SAUX>
	InnerNode<Drafter<K, Hasher<VERSION, HASH, HAUX, Saver<NONCE, SAUX>>>>
{
	pub fn nonce(&self) -> &NONCE {
		self.info.nonce()
	}
}

impl<K, VERSION, HASH, HAUX, NONCE>
	InnerNode<Drafter<K, Hasher<VERSION, HASH, HAUX, Saver<NONCE, NodeKeyPair>>>>
{
	pub fn children_nk_pair(&self) -> &NodeKeyPair {
		self.info.saux()
	}
}

impl Child {
	pub fn node_key(&self) -> Result<Option<NodeKey>, PoisonError<RwLockReadGuard<Node>>> {
		match self {
			Self::Full(node) => Ok(node.read()?.as_saved().map(SavedNode::node_key)),
			Self::Part(nk) => Ok(Some(nk.clone())),
		}
	}

	pub fn as_full(&self) -> Option<&ArlockNode> {
		match self {
			Self::Full(node) => Some(node),
			Self::Part(_) => None,
		}
	}

	pub fn fetch_full<DB>(&self, ndb: &NodeDb<DB>) -> Result<ArlockNode>
	where
		DB: KVStore,
	{
		let nk = match self {
			Child::Full(full) => return Ok(full.clone()),
			Child::Part(nk) => nk,
		};

		ndb.fetch_one_node(nk)?
			.map(|node| match node {
				FetchedNode::Deserialized(denode) => denode.into_saved_checked(nk),
				FetchedNode::EmptyRoot | FetchedNode::ReferenceRoot(_) => {
					Err(InnerNodeError::InvalidChild)
				},
			})
			.transpose()?
			.map(From::from)
			.ok_or(InnerNodeError::ChildNotFound)
	}

	pub fn extract(&mut self) -> Result<Self> {
		let replacement = match self {
			Self::Part(nk) => Self::Part(nk.clone()),
			Self::Full(full) => full
				.read()?
				.as_saved()
				.map(|sn| Child::Part(sn.node_key()))
				.unwrap_or_else(|| Self::Full(full.clone())),
		};

		Ok(mem::replace(self, replacement))
	}
}

impl<STAGE> From<&InnerNode<Drafter<NonEmptyBz<Bytes>, STAGE>>> for InnerNode<Drafted> {
	fn from(saved: &InnerNode<Drafter<NonEmptyBz<Bytes>, STAGE>>) -> Self {
		Self::builder()
			.key(saved.key().clone())
			.height(saved.height())
			.size(saved.size())
			.left(saved.left().clone())
			.right(saved.right().clone())
			.build()
	}
}

impl From<NodeKey> for Child {
	fn from(nk: NodeKey) -> Self {
		Self::Part(nk)
	}
}

impl From<ArlockNode> for Child {
	fn from(node: ArlockNode) -> Self {
		Self::Full(node)
	}
}
