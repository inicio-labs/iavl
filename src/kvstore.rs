use core::error::Error;

use crate::types::NonEmptyBz;

pub trait MutKVStore {
    type Error: Error + Send + Sync + 'static;

    fn insert(&self, key: &NonEmptyBz, value: &NonEmptyBz) -> Result<bool, Self::Error>;

    fn remove(&self, key: &NonEmptyBz) -> Result<(), Self::Error>;
}

pub trait KVStore {
    type Error: Error + Send + Sync + 'static;

    fn get(&self, key: &NonEmptyBz) -> Result<Option<NonEmptyBz>, Self::Error>;

    fn has(&self, key: &NonEmptyBz) -> Result<bool, Self::Error> {
        self.get(key).map(|v| v.is_some())
    }
}

pub trait KVIterator {
    type Error: Error + Send + Sync + 'static;

    fn next(&mut self) -> Result<Option<(NonEmptyBz, NonEmptyBz)>, Self::Error>;
}
