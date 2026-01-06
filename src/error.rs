use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StorageError {
    KeyNotFound { key: String },
    KeyAlreadyExists { key: String },
    Internal { message: String },
}

impl StorageError {
    pub fn not_found(key: impl Into<String>) -> Self {
        Self::KeyNotFound { key: key.into() }
    }

    pub fn already_exists(key: impl Into<String>) -> Self {
        Self::KeyAlreadyExists { key: key.into() }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn key(&self) -> Option<&str> {
        match self {
            Self::KeyNotFound { key } => Some(key),
            Self::KeyAlreadyExists { key } => Some(key),
            Self::Internal { .. } => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::KeyNotFound { .. })
    }

    pub fn key_already_exists(&self) -> bool {
        matches!(self, Self::KeyAlreadyExists { .. })
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::KeyNotFound { key } => {
                write!(f, "key not found: {}", key)
            }
            StorageError::KeyAlreadyExists { key } => {
                write!(f, "key already exists: {}", key)
            }
            StorageError::Internal { message } => {
                write!(f, "storage error: {}", message)
            }
        }
    }
}

impl Error for StorageError {}

pub type StorageResult<T> = Result<T, StorageError>;
