#[cfg(feature = "redb")]
pub mod redb;

use core::{error::Error, ops::RangeBounds};

use bytes::Bytes;
use nebz::NonEmptyBz;

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

    fn get<K>(&self, key: NonEmptyBz<K>) -> Result<Option<NonEmptyBz<Bytes>>, Self::Error>
    where
        K: AsRef<[u8]>;

    fn has<K>(&self, key: NonEmptyBz<K>) -> Result<bool, Self::Error>
    where
        K: AsRef<[u8]>,
    {
        self.get(key).map(|v| v.is_some())
    }
}

pub trait KVIterator {
    type Error: Error + Send + Sync + 'static;

    type FetchError: Error + Send + Sync + 'static;

    // TODO: reconsider the clippy lint when `type_alias_impl_trait` lands in stable.
    // https://doc.rust-lang.org/beta/unstable-book/language-features/type-alias-impl-trait.html
    #[allow(clippy::type_complexity)]
    fn iter<'a, KR>(
        &self,
        range: KR,
    ) -> Result<
        impl DoubleEndedIterator<
            Item = Result<(NonEmptyBz<Bytes>, NonEmptyBz<Bytes>), Self::FetchError>,
        >,
        Self::Error,
    >
    where
        KR: RangeBounds<NonEmptyBz<&'a [u8]>>;
}
