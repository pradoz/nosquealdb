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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_format() {
        assert_eq!(
            StorageError::not_found("user:123").to_string(),
            "key not found: user:123",
        );
        assert_eq!(
            StorageError::already_exists("user:456").to_string(),
            "key already exists: user:456",
        );
        assert_eq!(
            StorageError::internal("disk full").to_string(),
            "storage error: disk full",
        );
    }

    #[test]
    fn error_compare() {
        let not_found_a = StorageError::not_found("a");
        let not_found_a_same = StorageError::not_found("a");
        let not_found_b = StorageError::not_found("b");
        let already_exists_a = StorageError::already_exists("a");

        assert_eq!(not_found_a, not_found_a_same);
        assert_ne!(not_found_a, not_found_b);
        assert_ne!(not_found_a, already_exists_a);
    }

    #[test]
    fn key_extraction() {
        assert_eq!(StorageError::not_found("a").key(), Some("a"));
        assert_eq!(StorageError::already_exists("b").key(), Some("b"));
        assert_eq!(StorageError::internal("lalala").key(), None);
    }

    #[test]
    fn type_check_methods() {
        let not_found = StorageError::not_found("k");
        let exists = StorageError::already_exists("k");
        let internal = StorageError::internal("m");

        assert!(not_found.is_not_found());
        assert!(!not_found.key_already_exists());

        assert!(!exists.is_not_found());
        assert!(exists.key_already_exists());

        assert!(!internal.is_not_found());
        assert!(!internal.key_already_exists());
    }
}
