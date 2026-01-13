use std::error::Error;
use std::fmt;

use crate::types::{DecodeError, KeyValidationError};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalError {
    TypeMismatch {
        left: &'static str,
        right: &'static str,
    },
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch { left, right } => {
                write!(f, "cannot compare {} with {}", left, right)
            }
        }
    }
}

impl std::error::Error for EvalError {}

pub type EvalResult = Result<bool, EvalError>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TableError {
    InvalidKey(KeyValidationError),
    ItemNotFound,
    ItemAlreadyExists,
    IndexNotFound { name: String },
    ConditionFailed,
    ConditionError(String),
    UpdateError(String),
    Storage(String),
    Encoding(String),
}

impl TableError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::ItemNotFound)
    }
    pub fn item_already_exists(&self) -> bool {
        matches!(self, Self::ItemAlreadyExists)
    }
    pub fn is_invalid_key(&self) -> bool {
        matches!(self, Self::InvalidKey(_))
    }
    pub fn is_condition_failed(&self) -> bool {
        matches!(self, Self::ConditionFailed)
    }
    pub fn is_index_not_found(&self) -> bool {
        matches!(self, Self::IndexNotFound { .. })
    }
    pub fn index_not_found(name: impl Into<String>) -> Self {
        Self::IndexNotFound { name: name.into() }
    }
    pub fn is_update_error(&self) -> bool {
        matches!(self, Self::UpdateError { .. })
    }
    pub fn update_error(msg: impl Into<String>) -> Self {
        Self::UpdateError(msg.into())
    }
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableError::InvalidKey(e) => write!(f, "invalid key: {}", e),
            TableError::ItemNotFound => write!(f, "item not found"),
            TableError::ItemAlreadyExists => write!(f, "item already exists"),
            TableError::IndexNotFound { name } => write!(f, "index not found: {}", name),
            TableError::ConditionFailed => write!(f, "condition check failed"),
            TableError::ConditionError(msg) => write!(f, "condition error: {}", msg),
            TableError::UpdateError(msg) => write!(f, "update error: {}", msg),
            TableError::Storage(msg) => write!(f, "storage error: {}", msg),
            TableError::Encoding(msg) => write!(f, "encoding error: {}", msg),
        }
    }
}

impl Error for TableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TableError::InvalidKey(e) => Some(e),
            _ => None,
        }
    }
}

impl From<KeyValidationError> for TableError {
    fn from(e: KeyValidationError) -> Self {
        Self::InvalidKey(e)
    }
}

impl From<StorageError> for TableError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<DecodeError> for TableError {
    fn from(e: DecodeError) -> Self {
        Self::Encoding(e.to_string())
    }
}

impl From<EvalError> for TableError {
    fn from(e: EvalError) -> Self {
        Self::ConditionError(e.to_string())
    }
}

pub type TableResult<T> = Result<T, TableError>;
