use std::{borrow::Cow, sync::PoisonError};

use super::inner::InnerNodeError;

pub type Result<T, E = NodeError> = core::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
	#[error("poisoned lock error: lock must not be poisoned")]
	PoisonedLock,

	#[error("inner node error: {0}")]
	Inner(#[from] InnerNodeError),

	#[error("deserialization error: {0}")]
	Deserialization(#[from] crate::encoding::DeserializationError),

	#[error("serialization error: {0}")]
	Serialization(#[from] crate::encoding::SerializationError),

	#[error("other error: {0}")]
	Other(Cow<'static, str>),
}

impl<T> From<PoisonError<T>> for NodeError {
	fn from(_err: PoisonError<T>) -> Self {
		Self::PoisonedLock
	}
}
