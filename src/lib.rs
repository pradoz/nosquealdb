pub mod error;
pub mod storage;

pub use error::{StorageError, StorageResult};
pub use storage::{MemoryStorage, Storage, StorageExt};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn public_api_is_accessible() {
        let mut storage = MemoryStorage::new();

        let _: StorageResult<()> = storage.put("a", vec![1, 2, 3]);
        let _: StorageResult<Option<Vec<u8>>> = storage.get("a");
        let _: StorageResult<bool> = storage.exists("a");
        let _: StorageResult<()> = storage.delete("a");

        let _: StorageResult<()> = storage.put_if_not_exists("a", vec![42]);
        let _: StorageResult<Vec<u8>> = storage.get_or_error("a").or(Ok(vec![]));

        let _err = StorageError::not_found("foo");
        let _err = StorageError::already_exists("bar");
        let _err = StorageError::internal("disk space");
    }
}
