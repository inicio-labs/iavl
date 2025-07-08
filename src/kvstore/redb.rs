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
    table: TableDefinition<'static, NonEmptyBz, NonEmptyBz>,
}

impl RedbStore {
    pub fn new(db: Arc<Database>, table_name: &'static str) -> Self {
        let table = TableDefinition::new(table_name);
        Self { db, table }
    }
}

impl KVStore for RedbStore {
    type Error = RedbStoreError;

    fn get(&self, key: &NonEmptyBz) -> Result<Option<NonEmptyBz>, Self::Error> {
        self.db
            .begin_read()?
            .open_table(self.table)?
            .get(key)
            .map(|v| v.map(|v| v.value()))
            .map_err(From::from)
    }
}

impl MutKVStore for RedbStore {
    type Error = RedbStoreError;

    fn insert(&self, key: &NonEmptyBz, value: &NonEmptyBz) -> Result<bool, Self::Error> {
        let write_tx = self.db.begin_write()?;

        let updated = write_tx
            .open_table(self.table)?
            .insert(key, value)?
            .is_some();

        write_tx.commit()?;

        Ok(updated)
    }

    fn remove(&self, key: &NonEmptyBz) -> Result<bool, Self::Error> {
        let write_tx = self.db.begin_write()?;

        let removed = write_tx.open_table(self.table)?.remove(key)?.is_some();

        write_tx.commit()?;

        Ok(removed)
    }
}

impl Key for NonEmptyBz {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl Value for NonEmptyBz {
    type SelfType<'a> = NonEmptyBz;

    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(bz: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::new(Bytes::copy_from_slice(bz)).expect("data must not be empty")
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
