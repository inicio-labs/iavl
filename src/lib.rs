pub mod immutable;
pub mod kvstore;
pub mod mutable;
pub mod node;
pub mod types;

mod encoding;
mod traversal;

use core::num::NonZeroUsize;

use std::io::{Read, Write};

use integer_encoding::{VarIntReader, VarIntWriter};

use self::{
    encoding::{DeserializationError, SerializationError},
    types::{U31, U63},
};

pub const SHA256_HASH_LEN: NonZeroUsize = NonZeroUsize::new(32).unwrap();

pub type NodeHash<const N: usize = { SHA256_HASH_LEN.get() }> = [u8; N];

pub type NodeHashPair = (NodeHash, NodeHash);

pub type NodeKeyPair = (NodeKey, NodeKey);

/// NodeKey represents a key of node in the DB
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeKey<V = U63, N = U31> {
    /// version of the IAVL that this node was first added in
    version: V,

    /// local nonce for the same version   
    nonce: N,
}

impl<V, N> NodeKey<V, N> {
    pub const fn new(version: V, nonce: N) -> Self {
        Self { version, nonce }
    }

    pub const fn version(&self) -> &V {
        &self.version
    }

    pub const fn nonce(&self) -> &N {
        &self.nonce
    }
}

impl NodeKey {
    pub fn deserialize<R>(mut reader: R) -> Result<Self, DeserializationError>
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

    pub fn serialize<W>(&self, mut writer: W) -> Result<NonZeroUsize, SerializationError>
    where
        W: Write,
    {
        writer
            .write_varint(self.version().to_signed())
            .and_then(|vlen| {
                writer
                    .write_varint(self.nonce().to_signed())
                    .map(|nlen| vlen + nlen) // direct addition won't overflow
            })
            .map(NonZeroUsize::new)
            .transpose()
            .unwrap() // unwrap is safe here as vlen + nlen > 0
            .map_err(From::from)
    }
}
