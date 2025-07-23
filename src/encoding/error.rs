use core::num::TryFromIntError;

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
	#[error("io error: {0}")]
	Io(#[from] io::Error),

	#[error("overflow error")]
	Overflow,
}

#[derive(Debug, thiserror::Error)]
pub enum DeserializationError {
	#[error("io error: {0}")]
	Io(#[from] io::Error),

	#[error("invalid integer error")]
	InvalidInteger,

	#[error("zero prefix length error")]
	ZeroPrefixLength,

	#[error("prefix length mismatch error")]
	PrefixLengthMismatch,

	#[error("invalid mode")]
	InvalidMode,
}

impl From<TryFromIntError> for DeserializationError {
	fn from(_err: TryFromIntError) -> Self {
		Self::InvalidInteger
	}
}
