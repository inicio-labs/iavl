#[derive(Debug, thiserror::Error)]
pub enum RedbStoreError {
    #[error("database error: {0}")]
    Database(#[from] redb::Error),

    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
}
