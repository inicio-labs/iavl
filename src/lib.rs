pub mod kvstore;

mod encoding;
mod immutable;
mod mutable;
mod node;

pub use self::{
	immutable::ImmutableTree,
	mutable::{MutableTree, MutableTreeError},
};

use core::num::NonZeroUsize;

use std::io::{Read, Write};

use integer_encoding::{VarIntReader, VarIntWriter};
use nebz::NonEmptyBz;
use oblux::{U31, U63};

use self::{
	encoding::{DeserializationError, SerializationError},
	node::NodeError,
	sealed::Sealed,
};

const SHA256_HASH_LEN: NonZeroUsize = NonZeroUsize::new(32).unwrap();

type NodeHash<const N: usize = { SHA256_HASH_LEN.get() }> = [u8; N];

type NodeHashPair = (NodeHash, NodeHash);

type NodeKeyPair = (NodeKey, NodeKey);

pub trait Get: Sealed {
	type Error;

	type Value: AsRef<[u8]>;

	#[allow(clippy::type_complexity)]
	fn get<K>(&self, key: NonEmptyBz<K>) -> Result<(U63, Option<Self::Value>), Self::Error>
	where
		K: AsRef<[u8]>;
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct GetError(#[from] NodeError);

/// NodeKey represents a key of node in the DB
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeKey<V = U63, N = U31> {
	/// version of the IAVL that this node was first added in
	version: V,

	/// local nonce for the same version   
	nonce: N,
}

impl<V, N> NodeKey<V, N> {
	const fn new(version: V, nonce: N) -> Self {
		Self { version, nonce }
	}

	const fn version(&self) -> &V {
		&self.version
	}

	const fn nonce(&self) -> &N {
		&self.nonce
	}
}

impl NodeKey {
	fn deserialize<R>(mut reader: R) -> Result<Self, DeserializationError>
	where
		R: Read,
	{
		let version = reader
			.read_varint::<i64>()
			.map(U63::from_signed)?
			.ok_or(DeserializationError::InvalidInteger)?;

		let nonce = reader
			.read_varint::<i32>()
			.map(U31::from_signed)?
			.ok_or(DeserializationError::InvalidInteger)?;

		Ok(NodeKey::new(version, nonce))
	}

	fn serialize<W>(&self, mut writer: W) -> Result<NonZeroUsize, SerializationError>
	where
		W: Write,
	{
		writer
			.write_varint(self.version().to_signed())
			.and_then(|vlen| {
				writer.write_varint(self.nonce().to_signed()).map(|nlen| vlen + nlen) // direct addition won't overflow
			})
			.map(NonZeroUsize::new)
			.transpose()
			.unwrap() // unwrap is safe here as vlen + nlen > 0
			.map_err(From::from)
	}
}

mod sealed {
	pub trait Sealed {}
}
