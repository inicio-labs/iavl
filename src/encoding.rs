mod error;

pub use self::error::{DeserializationError, SerializationError};

use core::num::NonZeroUsize;

use std::io::{self, Read, Write};

use bytes::{BufMut, Bytes, BytesMut};
use integer_encoding::{VarIntReader, VarIntWriter};
use nebz::NonEmptyBz;

use super::{NodeHash, NodeKey, SHA256_HASH_LEN};

pub const NODE_DB_KEY_LEN: usize = size_of::<u8>() + size_of::<u64>() + size_of::<u32>();

pub fn deserialize_hash<R>(mut reader: R) -> Result<NodeHash, DeserializationError>
where
    R: Read,
{
    let len: usize = reader.read_varint::<u64>()?.try_into()?;

    if len != SHA256_HASH_LEN.get() {
        return Err(DeserializationError::PrefixLengthMismatch);
    }

    let mut hash = NodeHash::default();

    reader
        .read_exact(&mut hash)
        .map(|_| hash)
        .map_err(From::from)
}

pub fn deserialize_bytes<R>(mut reader: R) -> Result<NonEmptyBz<Bytes>, DeserializationError>
where
    R: Read,
{
    reader
        .read_varint::<u64>()
        .map_err(From::from)
        .and_then(|len| {
            if len == 0 {
                return Err(DeserializationError::ZeroPrefixLength);
            }

            let mut buf = BytesMut::with_capacity(len.try_into()?).writer();

            // unwrap is safe because len > 0
            io::copy(&mut reader.by_ref().take(len), &mut buf)?
                .eq(&len)
                .then(|| NonEmptyBz::new(buf.into_inner().freeze()).unwrap())
                .ok_or(DeserializationError::PrefixLengthMismatch)
        })
}

pub fn serialize_hash<W>(
    hash: &NodeHash<{ SHA256_HASH_LEN.get() }>,
    mut writer: W,
) -> io::Result<NonZeroUsize>
where
    W: Write,
{
    let sha256_hash_len_bytes = writer.write_varint(SHA256_HASH_LEN.get())?;
    writer.write_all(hash)?;

    // TODO: devise strategy to avoid this unwrap.
    // unwrap is safe
    Ok(SHA256_HASH_LEN.checked_add(sha256_hash_len_bytes).unwrap())
}

pub fn serialize_nebz<W, BZ>(
    bz: NonEmptyBz<BZ>,
    mut writer: W,
) -> Result<NonZeroUsize, SerializationError>
where
    W: Write,
    BZ: AsRef<[u8]>,
{
    let prefix_len_bytes = writer.write_varint(bz.len().get())?;
    writer.write_all(bz.get().as_ref())?;

    bz.len()
        .checked_add(prefix_len_bytes)
        .ok_or(SerializationError::Overflow)
}

pub const fn make_ndb_key<const KEY_PREFIX_BYTE: u8>(nk: &NodeKey) -> [u8; NODE_DB_KEY_LEN] {
    let mut key = [0; NODE_DB_KEY_LEN];
    key[0] = KEY_PREFIX_BYTE;

    let version_be_bytes = nk.version().get().to_be_bytes();
    let mut i = 0;
    while i < size_of::<u64>() {
        key[i + 1] = version_be_bytes[i];
        i += 1;
    }

    let nonce_be_bytes = nk.nonce().get().to_be_bytes();
    let mut i = 0;
    while i < size_of::<u32>() {
        key[i + 1 + size_of::<u64>()] = nonce_be_bytes[i];
        i += 1;
    }

    key
}
