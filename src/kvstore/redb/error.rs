#[derive(Debug, thiserror::Error)]
pub enum RedbStoreError {
    #[error("database error: {0}")]
    Database(Box<redb::Error>),

    #[error("transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),

    #[error("table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("empty value error: value must not be empty")]
    EmptyValue,
}

impl From<redb::Error> for RedbStoreError {
    fn from(err: redb::Error) -> Self {
        Self::Database(Box::new(err))
    }
}

impl From<redb::TransactionError> for RedbStoreError {
    fn from(err: redb::TransactionError) -> Self {
        Self::Transaction(Box::new(err))
    }
}
