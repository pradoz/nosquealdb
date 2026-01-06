use crate::error::{StorageError, StorageResult};

pub trait Storage {
    fn put(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()>;

    fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>>;

    fn delete(&mut self, key: &str) -> StorageResult<()>;

    fn exists(&self, key: &str) -> StorageResult<bool>;

    // TODO: scan/paginate for total item count?
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait StorageExt: Storage {
    fn put_if_not_exists(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()>;

    fn get_or_error(&self, key: &str) -> StorageResult<Vec<u8>>;

    fn update(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()>;

    // TODO: batch operations
    fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Vec<u8>>>>;

    fn delete_and_get_old(&mut self, key: &str) -> StorageResult<Option<Vec<u8>>>;
}

impl<T: Storage> StorageExt for T {
    fn put_if_not_exists(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()> {
        if self.exists(key)? {
            return Err(StorageError::already_exists(key));
        }
        self.put(key, value)
    }

    fn get_or_error(&self, key: &str) -> StorageResult<Vec<u8>> {
        self.get(key)?.ok_or_else(|| StorageError::not_found(key))
    }

    fn update(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()> {
        if !self.exists(key)? {
            return Err(StorageError::not_found(key));
        }
        self.put(key, value)
    }

    // TODO: batch operations
    fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    fn delete_and_get_old(&mut self, key: &str) -> StorageResult<Option<Vec<u8>>> {
        let value = self.get(key)?;
        self.delete(key)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct FailingStorage {
        data: HashMap<String, Vec<u8>>,
        fail_on_key: Option<String>,
    }

    impl FailingStorage {
        fn new() -> Self {
            Self {
                data: HashMap::new(),
                fail_on_key: None,
            }
        }

        fn fail_on(&mut self, key: &str) {
            self.fail_on_key = Some(key.to_string());
        }

        fn check_fail(&self, key: &str) -> StorageResult<()> {
            if self.fail_on_key.as_deref() == Some(key) {
                return Err(StorageError::internal("simulated failure"));
            }
            Ok(())
        }
    }

    impl Storage for FailingStorage {
        fn put(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()> {
            self.check_fail(key)?;
            self.data.insert(key.to_string(), value);
            Ok(())
        }

        fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>> {
            self.check_fail(key)?;
            Ok(self.data.get(key).cloned())
        }

        fn delete(&mut self, key: &str) -> StorageResult<()> {
            self.check_fail(key)?;
            self.data.remove(key);
            Ok(())
        }

        fn exists(&self, key: &str) -> StorageResult<bool> {
            self.check_fail(key)?;
            Ok(self.data.contains_key(key))
        }

        fn len(&self) -> usize {
            self.data.len()
        }
    }

    #[test]
    fn put_if_not_exists_returns_error_on_duplicate() {
        let mut storage = FailingStorage::new();
        storage.put("key", vec![1]).unwrap();

        let result = storage.put_if_not_exists("key", vec![2]);

        assert!(result.unwrap_err().key_already_exists());
        // value is preserved
        assert_eq!(storage.get("key").unwrap(), Some(vec![1]));
    }

    #[test]
    fn update_returns_error_on_missing() {
        let mut storage = FailingStorage::new();

        let result = storage.update("missing", vec![1]);

        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn get_or_error_returns_error_on_missing() {
        let storage = FailingStorage::new();

        let result = storage.get_or_error("missing");

        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn storage_ext_propagates_underlying_errors() {
        let mut storage = FailingStorage::new();
        storage.fail_on("boom");

        assert!(storage.put_if_not_exists("boom", vec![]).is_err());
        assert!(storage.get_or_error("boom").is_err());
        assert!(storage.update("boom", vec![]).is_err());
        assert!(storage.get_many(&["boom"]).is_err());
        assert!(storage.delete_and_get_old("boom").is_err());
    }
}
