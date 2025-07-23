use std::sync::PoisonError;

use crate::node::{InnerNodeError, NodeError, ndb::NodeDbError};

pub type Result<T, E = MutableTreeError> = core::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct MutableTreeError(#[from] MutableTreeErrorKind);

#[derive(Debug, thiserror::Error)]
pub(crate) enum MutableTreeErrorKind {
	#[error("node db error: {0}")]
	NodeDb(#[from] NodeDbError),

	#[error("node error: {0}")]
	Node(#[from] NodeError),

	#[error("missing node key error")]
	MissingNodeKey,

	#[error("conflicting root error")]
	ConflictingRoot,

	#[error("inner node error: {0}")]
	InnerNode(#[from] InnerNodeError),

	#[error("poisoned lock error")]
	PoisonedLock,

	#[error("overflow error")]
	Overflow,
}

impl<T> From<PoisonError<T>> for MutableTreeErrorKind {
	fn from(_err: PoisonError<T>) -> Self {
		Self::PoisonedLock
	}
}
