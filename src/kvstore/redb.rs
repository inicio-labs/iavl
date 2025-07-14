mod error;

pub use self::error::RedbStoreError;

use core::cmp::Ordering;

use std::sync::Arc;

use bytes::Bytes;
use redb::{Database, Key, TableDefinition, TypeName, Value};

use crate::types::NonEmptyBz;

use super::{KVStore, MutKVStore};

#[derive(Clone)]
pub struct RedbStore {
    db: Arc<Database>,
    table: TableDefinition<'static, NonEmptyBz<&'static [u8]>, NonEmptyBz>,
}

impl RedbStore {
    pub fn new(db: Arc<Database>, table_name: &'static str) -> Self {
        let table = TableDefinition::new(table_name);
        Self { db, table }
    }
}

impl KVStore for RedbStore {
    type Error = RedbStoreError;

    fn get<K>(&self, key: NonEmptyBz<K>) -> Result<Option<NonEmptyBz>, Self::Error>
    where
        K: AsRef<[u8]>,
    {
        let value = self
            .db
            .begin_read()?
            .open_table(self.table)?
            .get(key.as_non_empty_slice())?
            .map(|v| v.value());

        Ok(value)
    }
}

impl MutKVStore for RedbStore {
    type Error = RedbStoreError;

    fn insert<K, V>(&self, key: NonEmptyBz<K>, value: NonEmptyBz<V>) -> Result<bool, Self::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let write_tx = self.db.begin_write()?;

        // unwrap is safe here because `value` is non-empty.
        let value = NonEmptyBz::new(Bytes::copy_from_slice(value.get().as_ref())).unwrap();

        let updated = write_tx
            .open_table(self.table)?
            .insert(key.as_non_empty_slice(), value)?
            .is_some();

        write_tx.commit()?;

        Ok(updated)
    }

    fn remove<K>(&self, key: NonEmptyBz<K>) -> Result<bool, Self::Error>
    where
        K: AsRef<[u8]>,
    {
        let write_tx = self.db.begin_write()?;

        let removed = write_tx
            .open_table(self.table)?
            .remove(key.as_non_empty_slice())?
            .is_some();

        write_tx.commit()?;

        Ok(removed)
    }
}

impl Key for NonEmptyBz<&[u8]> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl Value for NonEmptyBz<&[u8]> {
    type SelfType<'a>
        = NonEmptyBz<&'a [u8]>
    where
        Self: 'a;

    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(bz: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        NonEmptyBz::new(bz).expect("bz must not be empty")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.get()
    }

    fn type_name() -> TypeName {
        TypeName::new("iavl::NonEmptyBz<&[u8]>")
    }
}

impl Value for NonEmptyBz {
    type SelfType<'a> = Self;

    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(bz: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::new(Bytes::copy_from_slice(bz)).expect("bz must not be empty")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.get().as_ref()
    }

    fn type_name() -> redb::TypeName {
        TypeName::new("iavl::NonEmptyBz<Bytes>")
    }
}
