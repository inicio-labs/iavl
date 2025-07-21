pub mod utils;

use std::sync::Arc;

use iavl::{MutableTree, kvstore::redb::RedbStore};
use redb::{Database, backends::InMemoryBackend};

pub struct TestContext {
    pub tree: MutableTree<RedbStore>,
}

impl TestContext {
    pub fn new() -> Self {
        Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .map(Arc::new)
            .map(|db| RedbStore::new(db, "test").unwrap())
            .map(MutableTree::new)
            .map(|tree| Self { tree })
            .expect("database error")
    }
}
