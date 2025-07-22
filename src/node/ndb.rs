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
    ReferenceRoot(NodeKey),
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
        let ndb_key = encoding::make_ndb_key::<NODE_DB_KEY_PREFIX>(nk);

        self.db
            .get(NonEmptyBz::from_owned_array(ndb_key))
            .map_err(From::from)
            .map_err(NodeDbError::Store)?
            .map(make_fetched_node)
            .transpose()
            .map_err(From::from)
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
    pub fn fetch_latest_root_node(&self) -> Result<Option<(NodeKey, FetchedNode)>> {
        let (root_ndb_key_bz, root_ndb_value_bz) = {
            let Some((ndb_key_bz_max_version_max_nonce, _)) = self
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

            // TODO: ascertain if block expression can return early when retrieved nonce is 1

            let prefix = ndb_key_bz_max_version_max_nonce
                .get()
                .get(..9)
                .map(|bz| {
                    let mut prefix = [0; 9];
                    prefix.copy_from_slice(bz);

                    NonEmptyBz::from_owned_array(prefix)
                })
                .ok_or(DeserializationError::InvalidInteger)?;

            self.db
                .iter(prefix.as_ref_slice()..)
                .map_err(From::from)
                .map_err(NodeDbError::Store)?
                .next()
                .transpose()
                .map_err(From::from)
                .map_err(NodeDbError::Store)?
                .ok_or("must contain max version min nonce ndb key".into())
                .map_err(NodeDbError::Store)?
        };

        let nk = {
            let (_, mut version_nonce_bz) = root_ndb_key_bz.split_first();

            let version = version_nonce_bz
                .try_get_u64()
                .ok()
                .and_then(U63::new)
                .ok_or(DeserializationError::InvalidInteger)?;

            let nonce = version_nonce_bz
                .try_get_u32()
                .ok()
                .and_then(U31::new)
                .ok_or(DeserializationError::InvalidInteger)?;

            NodeKey::new(version, nonce)
        };

        Ok(Some((nk, make_fetched_node(root_ndb_value_bz)?)))
    }
}

fn make_fetched_node<BZ>(ndb_value_bz: NonEmptyBz<BZ>) -> Result<FetchedNode, DeserializationError>
where
    BZ: AsRef<[u8]>,
{
    match ndb_value_bz.split_first() {
        (NodeDb::<()>::EMPTY_ROOT_MARKER, _) => Ok(FetchedNode::EmptyRoot),
        (NODE_DB_KEY_PREFIX, mut version_nonce_bz) => {
            // check if valid version
            let version = version_nonce_bz
                .try_get_u64()
                .ok()
                .and_then(U63::new)
                .ok_or(DeserializationError::InvalidInteger)?;

            // check if valid nonce
            let nonce = version_nonce_bz
                .try_get_u32()
                .ok()
                .and_then(U31::new)
                .ok_or(DeserializationError::InvalidInteger)?;

            Ok(FetchedNode::ReferenceRoot(NodeKey::new(version, nonce)))
        }
        _ => DeserializedNode::deserialize(ndb_value_bz.get().as_ref())
            .map(FetchedNode::Deserialized),
    }
}

fn root_ndb_key(version: U63) -> NonEmptyBz<[u8; NODE_DB_KEY_LEN]> {
    let nk = NodeKey::new(version, NodeDb::<()>::NEW_ROOT_NONCE);
    let ndb_key_array = encoding::make_ndb_key::<NODE_DB_KEY_PREFIX>(&nk);
    NonEmptyBz::from_owned_array(ndb_key_array)
}
