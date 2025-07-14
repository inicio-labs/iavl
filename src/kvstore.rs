#[cfg(feature = "redb")]
pub mod redb;

use core::error::Error;

use crate::types::NonEmptyBz;

pub trait MutKVStore {
    type Error: Error + Send + Sync + 'static;

    fn insert<K, V>(&self, key: NonEmptyBz<K>, value: NonEmptyBz<V>) -> Result<bool, Self::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    fn remove<K>(&self, key: NonEmptyBz<K>) -> Result<bool, Self::Error>
    where
        K: AsRef<[u8]>;
}

pub trait KVStore {
    type Error: Error + Send + Sync + 'static;

    fn get<K>(&self, key: NonEmptyBz<K>) -> Result<Option<NonEmptyBz>, Self::Error>
    where
        K: AsRef<[u8]>;

    fn has<K>(&self, key: NonEmptyBz) -> Result<bool, Self::Error>
    where
        K: AsRef<[u8]>,
    {
        self.get(key).map(|v| v.is_some())
    }
}

pub trait KVIterator {
    type Error: Error + Send + Sync + 'static;

    fn next(&mut self) -> Result<Option<(NonEmptyBz, NonEmptyBz)>, Self::Error>;
}
