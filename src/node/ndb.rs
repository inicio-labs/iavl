mod error;

pub use self::error::NodeDbError;

use bon::Builder;
use bytes::{Buf, BufMut, BytesMut};
use nebz::NonEmptyBz;
use oblux::{U31, U63};

use crate::{
    encoding::{self, DeserializationError, NODE_DB_KEY_LEN},
    kvstore::{KVIterator, KVStore, MutKVStore},
};

use super::{DeserializedNode, NodeKey, kind::SavedNode};

use self::error::Result;

const NODE_DB_KEY_PREFIX: u8 = b's';

#[derive(Debug, Clone, Builder)]
pub(crate) struct NodeDb<DB> {
    db: DB,
}

pub(crate) enum FetchedNode {
    EmptyRoot,
    ReferenceRoot(Option<DeserializedNode>),
    Deserialized(DeserializedNode),
}

impl<DB> NodeDb<DB> {
    // the serialized bytes of a node cannot start with byte value `0xFF` as it exceeds U7::MAX
    const EMPTY_ROOT_MARKER: u8 = u8::MAX;

    const NEW_ROOT_NONCE: U31 = U31::ONE;
}

impl<DB> NodeDb<DB>
where
    DB: KVStore,
{
    pub fn fetch_one_node(&self, nk: &NodeKey) -> Result<Option<FetchedNode>> {
        let fetch_ndb_value = |key: NonEmptyBz<&[u8]>| {
            self.db
                .get(key)
                .map_err(From::from)
                .map_err(NodeDbError::Store)
        };

        let key = encoding::make_ndb_key::<NODE_DB_KEY_PREFIX>(nk);

        let Some(ndb_value) = fetch_ndb_value(NonEmptyBz::from_owned_array(key).as_ref_slice())?
        else {
            return Ok(None);
        };

        let node = match ndb_value.split_first() {
            (Self::EMPTY_ROOT_MARKER, _) => FetchedNode::EmptyRoot,
            (NODE_DB_KEY_PREFIX, mut version_nonce_bz) => {
                // check if valid version
                version_nonce_bz
                    .try_get_u64()
                    .ok()
                    .and_then(U63::new)
                    .ok_or(DeserializationError::InvalidInteger)?;

                // check if valid nonce
                version_nonce_bz
                    .try_get_u32()
                    .ok()
                    .and_then(U31::new)
                    .ok_or(DeserializationError::InvalidInteger)?;

                let original = fetch_ndb_value(ndb_value.as_ref_slice())?
                    .map(NonEmptyBz::into_inner)
                    .as_deref()
                    .map(DeserializedNode::deserialize)
                    .transpose()?;

                FetchedNode::ReferenceRoot(original)
            }
            _ => DeserializedNode::deserialize(ndb_value.into_inner().as_ref())
                .map(FetchedNode::Deserialized)?,
        };

        Ok(Some(node))
    }
}

impl<DB> NodeDb<DB>
where
    DB: MutKVStore,
{
    /// Overwrites serialized bytes of `node` against `node`'s [`NodeKey`].
    ///
    /// Returns true if the same [`NodeKey`] of `node` already existed.
    pub fn save_overwriting_one_node(&self, node: &SavedNode) -> Result<bool> {
        let serialized = {
            let mut serialized = BytesMut::new().writer();

            node.serialize(&mut serialized)?;

            NonEmptyBz::new(serialized.into_inner().freeze())
                .ok_or(NodeDbError::Other("serialized must be non-empty".into()))?
        };

        let ndb_key = {
            let ndb_key_array = encoding::make_ndb_key::<NODE_DB_KEY_PREFIX>(&node.node_key());
            NonEmptyBz::from_owned_array(ndb_key_array)
        };

        self.db
            .insert(ndb_key, serialized)
            .map_err(From::from)
            .map_err(NodeDbError::Store)
    }

    /// Overwrites empty root marker against [`NodeKey`] with `version` and nonce [`U31::ONE`].
    ///
    /// Returns true if the same [`NodeKey`] with `version` and nonce [`U31::ONE`] already existed.
    pub fn save_overwriting_empty_root(&self, version: U63) -> Result<bool> {
        let ndb_key = root_ndb_key(version);
        let marker_value = NonEmptyBz::from_owned_array(Self::EMPTY_ROOT_MARKER.to_be_bytes());

        self.db
            .insert(ndb_key, marker_value)
            .map_err(From::from)
            .map_err(NodeDbError::Store)
    }

    /// Overwrites original node-db key in node-db key format `s<version><nonce>`
    /// against [`NodeKey`] with `version` and nonce [`U31::ONE`].
    ///
    /// Returns true if the same [`NodeKey`] with `version` and nonce [`U31::ONE`] already existed.
    pub fn save_overwriting_reference_root(
        &self,
        version: U63,
        original_nk: &NodeKey,
    ) -> Result<bool> {
        let original_root_ndb_key =
            NonEmptyBz::from_owned_array(encoding::make_ndb_key::<NODE_DB_KEY_PREFIX>(original_nk));

        self.db
            .insert(root_ndb_key(version), original_root_ndb_key)
            .map_err(From::from)
            .map_err(NodeDbError::Store)
    }
}

impl<DB> NodeDb<DB>
where
    DB: MutKVStore + KVStore,
{
    pub fn save_non_overwririting_one_node(&self, node: &SavedNode) -> Result<Option<FetchedNode>> {
        let nk = node.node_key();
        if let existing @ Some(_) = self.fetch_one_node(&nk)? {
            return Ok(existing);
        }

        // TODO: remove this assert when fully certain about key conflict behavior
        assert!(
            !self.save_overwriting_one_node(node)?,
            "key conflict must not occur",
        );

        Ok(None)
    }
}

impl<DB> NodeDb<DB>
where
    DB: KVIterator,
{
    pub fn latest_version(&self) -> Result<Option<U63>> {
        let Some((ndb_key_bz, _)) = self
            .db
            .iter(NonEmptyBz::from_owned_array([NODE_DB_KEY_PREFIX]).as_ref_slice()..)
            .map_err(From::from)
            .map_err(NodeDbError::Store)?
            .next_back()
            .transpose()
            .map_err(From::from)
            .map_err(NodeDbError::Store)?
        else {
            return Ok(None);
        };

        ndb_key_bz
            .split_first()
            .1
            .try_get_u64()
            .ok()
            .and_then(U63::new)
            .ok_or(DeserializationError::InvalidInteger)
            .map(Some)
            .map_err(From::from)
    }
}

fn root_ndb_key(version: U63) -> NonEmptyBz<[u8; NODE_DB_KEY_LEN]> {
    let nk = NodeKey::new(version, NodeDb::<()>::NEW_ROOT_NONCE);
    let ndb_key_array = encoding::make_ndb_key::<NODE_DB_KEY_PREFIX>(&nk);
    NonEmptyBz::from_owned_array(ndb_key_array)
}
