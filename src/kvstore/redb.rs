mod error;

pub use self::error::RedbStoreError;

use core::{cmp::Ordering, ops::RangeBounds};

use std::sync::Arc;

use bytes::Bytes;
use nebz::NonEmptyBz;
use redb::{Database, TableDefinition, TypeName};

use super::{KVIterator, KVStore, MutKVStore};

#[derive(Clone)]
pub struct RedbStore {
	db: Arc<Database>,
	table: TableDefinition<'static, Key, Value>,
}

#[derive(Debug)]
struct Key;

#[derive(Debug)]
struct Value;

impl RedbStore {
	pub fn new(db: Arc<Database>, table_name: &'static str) -> Result<Self, RedbStoreError> {
		let table = TableDefinition::new(table_name);

		let write_tx = db.begin_write()?;
		write_tx.open_table(table)?;
		write_tx.commit()?;

		Ok(Self { db, table })
	}
}

impl KVStore for RedbStore {
	type Error = RedbStoreError;

	fn get<K>(&self, key: NonEmptyBz<K>) -> Result<Option<NonEmptyBz<Bytes>>, Self::Error>
	where
		K: AsRef<[u8]>,
	{
		let value = self
			.db
			.begin_read()?
			.open_table(self.table)?
			.get(key.as_ref_slice())?
			.map(|v| v.value().into());

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

		let updated = write_tx
			.open_table(self.table)?
			.insert(key.as_ref_slice(), value.as_ref_slice())?
			.is_some();

		write_tx.commit()?;

		Ok(updated)
	}

	fn remove<K>(&self, key: NonEmptyBz<K>) -> Result<bool, Self::Error>
	where
		K: AsRef<[u8]>,
	{
		let write_tx = self.db.begin_write()?;

		let removed = write_tx.open_table(self.table)?.remove(key.as_ref_slice())?.is_some();

		write_tx.commit()?;

		Ok(removed)
	}
}

impl KVIterator for RedbStore {
	type Error = RedbStoreError;

	type FetchError = RedbStoreError;

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
		KR: RangeBounds<NonEmptyBz<&'a [u8]>>,
	{
		let iter = self
			.db
			.begin_read()?
			.open_table(self.table)?
			.range(range)?
			.map(|kv| kv.map(|(k, v)| (k.value().into(), v.value().into())))
			.map(|kv| kv.map_err(From::from));

		Ok(iter)
	}
}

impl redb::Key for Key {
	fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
		data1.cmp(data2)
	}
}

impl redb::Value for Key {
	type SelfType<'a>
		= NonEmptyBz<&'a [u8]>
	where
		Self: 'a;

	type AsBytes<'a> = &'a [u8];

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
		TypeName::new("Key")
	}
}

impl redb::Value for Value {
	type SelfType<'a>
		= NonEmptyBz<&'a [u8]>
	where
		Self: 'a;

	type AsBytes<'a> = &'a [u8];

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
		TypeName::new("Value")
	}
}
