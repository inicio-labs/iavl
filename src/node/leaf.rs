use core::num::NonZeroUsize;

use std::io::Write;

use bytes::Bytes;
use integer_encoding::VarIntWriter;
use nebz::NonEmptyBz;
use oblux::{U7, U63};
use sha2::{Digest, Sha256};

use crate::encoding::{self, SerializationError};

use super::{
	NodeKey,
	info::{Drafted, Drafter, Hashed, Hasher, Saved, Saver},
};

type SavedLeafNode<K, V, VERSION, HASH, HAUX, NONCE> =
	LeafNode<Drafter<K, Hasher<VERSION, HASH, HAUX, Saver<NONCE>>>, V>;

#[derive(Debug, Clone)]
pub(crate) struct LeafNode<INFO, V = NonEmptyBz<Bytes>> {
	info: INFO,
	value: V,
}

impl<INFO, V> LeafNode<INFO, V> {
	pub const HEIGHT: U7 = U7::MIN;

	pub const SIZE: U63 = U63::ONE;

	const SUBTREE_HEIGHT_VARINT_ENCODED: [u8; 1] = [0];

	const SIZE_VARINT_ENCODED: [u8; 1] = [2];

	const SUBTREE_HEIGHT_AND_SIZE_VARINT_ENCODED: [u8; 2] = [
		Self::SUBTREE_HEIGHT_VARINT_ENCODED[0],
		Self::SIZE_VARINT_ENCODED[0],
	];

	pub fn value(&self) -> &V {
		&self.value
	}
}

#[bon::bon]
impl LeafNode<Drafted> {
	#[builder]
	pub fn new(key: NonEmptyBz<Bytes>, value: NonEmptyBz<Bytes>) -> Self {
		Self { info: Drafted::new(key), value }
	}
}

impl LeafNode<Drafted> {
	pub fn to_hashed(&self, version: U63) -> LeafNode<Hashed> {
		let mut hasher = Sha256::new();

		hasher.update(Self::SUBTREE_HEIGHT_AND_SIZE_VARINT_ENCODED);

		// unwrap calls are safe because write on Sha256's hasher is infallible

		hasher.write_varint(version.to_signed()).unwrap();

		hasher.write_varint(self.key().len().get()).unwrap();
		hasher.update(self.key().get());

		encoding::serialize_hash(&Sha256::digest(self.value().get()).into(), &mut hasher).unwrap();

		LeafNode {
			info: self.info.clone().into_hashed(version, hasher.finalize().into(), ()),
			value: self.value.clone(),
		}
	}
}

impl<K, V, STAGE> LeafNode<Drafter<NonEmptyBz<K>, STAGE>, NonEmptyBz<V>>
where
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	pub fn serialize<W>(&self, mut writer: W) -> Result<NonZeroUsize, SerializationError>
	where
		W: Write,
	{
		writer.write_all(&Self::SUBTREE_HEIGHT_AND_SIZE_VARINT_ENCODED)?;

		let key_bytes_len = encoding::serialize_nebz(self.key().as_ref(), &mut writer)?;
		let value_bytes_len = encoding::serialize_nebz(self.value().as_ref(), writer)?;

		Self::SUBTREE_HEIGHT_AND_SIZE_VARINT_ENCODED
			.len()
			.checked_add(key_bytes_len.get())
			.and_then(|len| len.checked_add(value_bytes_len.get()))
			.and_then(NonZeroUsize::new)
			.ok_or(SerializationError::Overflow)
	}
}

impl<K, V, VERSION, HASH, HAUX> LeafNode<Drafter<K, Hasher<VERSION, HASH, HAUX>>, V> {
	pub fn into_saved<NONCE>(
		self,
		nonce: NONCE,
	) -> SavedLeafNode<K, V, VERSION, HASH, HAUX, NONCE> {
		LeafNode { info: self.info.into_saved(nonce, ()), value: self.value }
	}
}

impl<HAUX, SAUX> LeafNode<Saved<HAUX, SAUX>> {
	pub fn node_key(&self) -> NodeKey {
		NodeKey::new(*self.version(), *self.nonce())
	}
}

impl<K, V, STAGE> LeafNode<Drafter<K, STAGE>, V> {
	pub fn key(&self) -> &K {
		self.info.key()
	}
}

impl<K, V, VERSION, HASH, HAUX, STATUS>
	LeafNode<Drafter<K, Hasher<VERSION, HASH, HAUX, STATUS>>, V>
{
	pub fn version(&self) -> &VERSION {
		self.info.version()
	}

	pub fn hash(&self) -> &HASH {
		self.info.hash()
	}
}

impl<K, V, VERSION, HASH, HAUX, NONCE, SAUX>
	LeafNode<Drafter<K, Hasher<VERSION, HASH, HAUX, Saver<NONCE, SAUX>>>, V>
{
	pub fn nonce(&self) -> &NONCE {
		self.info.nonce()
	}
}

impl<STAGE> From<&LeafNode<Drafter<NonEmptyBz<Bytes>, STAGE>>> for LeafNode<Drafted> {
	fn from(saved: &LeafNode<Drafter<NonEmptyBz<Bytes>, STAGE>>) -> Self {
		Self::builder().key(saved.key().clone()).value(saved.value().clone()).build()
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	mod utils {
		use nebz::NonEmptyBz;
		use oblux::{U31, U63};

		use crate::node::{
			info::{Drafted, Saved},
			leaf::LeafNode,
		};

		pub fn draft_leaf<K, V>(key: K, value: V) -> LeafNode<Drafted>
		where
			K: AsRef<[u8]>,
			V: AsRef<[u8]>,
		{
			LeafNode::builder()
				.key(NonEmptyBz::new(key.as_ref()).map(From::from).unwrap())
				.value(NonEmptyBz::new(value.as_ref()).map(From::from).unwrap())
				.build()
		}

		pub fn saved_leaf<K, V>(key: K, value: V, version: u64, nonce: u32) -> LeafNode<Saved>
		where
			K: AsRef<[u8]>,
			V: AsRef<[u8]>,
		{
			draft_leaf(key, value)
				.to_hashed(U63::new(version).unwrap())
				.into_saved(U31::new(nonce).unwrap())
		}
	}

	#[rstest]
	#[case::draft(utils::draft_leaf("key", "value"), "0002036b65790576616c7565")]
	#[case::saved(
		utils::saved_leaf("hello", "world", 20, 17),
		"00020568656c6c6f05776f726c64"
	)]
	fn serialize_works_with_infallible_writer<K, V, STAGE, HEX>(
		#[case] node: LeafNode<Drafter<NonEmptyBz<K>, STAGE>, NonEmptyBz<V>>,
		#[case] hex_serialized: HEX,
	) where
		K: AsRef<[u8]>,
		V: AsRef<[u8]>,
		HEX: AsRef<[u8]>,
	{
		// Arrange
		let expected_serialized = const_hex::decode(hex_serialized).unwrap();

		// Act
		let mut serialized = vec![];
		let used = node.serialize(&mut serialized).unwrap();

		// Assert
		assert_eq!(expected_serialized, serialized);
		assert_eq!(expected_serialized.len(), used.get());
	}
}
