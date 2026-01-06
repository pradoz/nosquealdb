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

    struct MockStorage {
        data: HashMap<String, Vec<u8>>,
        fail_on_key: Option<String>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self { data: HashMap::new(), fail_on_key: None }
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

    impl Storage for MockStorage {
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
    fn is_empty() {
        let mut storage = MockStorage::new();
        assert!(storage.is_empty());

        let _ = storage.put("foo", vec![42]);
        assert!(!storage.is_empty());
    }

    #[test]
    fn put_if_not_exists() {
        let mut storage = MockStorage::new();

        // success
        let result = storage.put_if_not_exists("foo", vec![1,2]);
        assert!(result.is_ok());
        assert_eq!(storage.get("foo").unwrap(), Some(vec![1,2]));

        // fails, already exists
        let result = storage.put_if_not_exists("foo", vec![1,2]);
        assert!(result.is_err());
        assert!(result.unwrap_err().key_already_exists());

        // value should be unmodified
        assert_eq!(storage.get("foo").unwrap(), Some(vec![1,2]));
    }

    #[test]
    fn get_or_error() {
        let mut storage = MockStorage::new();
        let _ = storage.put("bar", vec![42]);

        let result = storage.get_or_error("bar");
        assert_eq!(result.unwrap(), vec![42]);

        let result = storage.get_or_error("lala");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn update() {
        let mut storage = MockStorage::new();

        let key = "key";
        let value = vec![42];
        let new_value = vec![67];

        // modify existing key
        storage.put(key, value).unwrap();
        let result = storage.update(key, new_value.clone());
        assert!(result.is_ok());
        assert_eq!(storage.get(key).unwrap(), Some(new_value));

        // fails when key is missing
        let result = storage.update("notfound", vec![1]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn get_many() {
        let mut storage = MockStorage::new();

        // empty should work
        let result = storage.get_many(&[]).unwrap();
        assert!(result.is_empty());

        // TODO: batch operations
        storage.put("foo", vec![1]).unwrap();
        storage.put("bar", vec![2]).unwrap();
        storage.put("baz", vec![3]).unwrap();
        assert_eq!(storage.len(), 3);

        let result = storage.get_many(&["foo", "bar", "baz", "notfound"]).unwrap();
        assert_eq!(result[0], Some(vec![1]));
        assert_eq!(result[1], Some(vec![2]));
        assert_eq!(result[2], Some(vec![3]));
        assert_eq!(result[3], None);
    }

    #[test]
    fn delete() {
        let mut storage = MockStorage::new();

        // missing key
        // TODO: figure out how to handle these
        let _ = storage.delete("notfound");

        let _ = storage.put("a", vec![1]);
        let result = storage.delete("a");
        assert!(result.is_ok());
    }

    #[test]
    fn delete_and_get_old() {
        let mut storage = MockStorage::new();

        // missing key
        let result = storage.delete_and_get_old("notfound");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let _ = storage.put("a", vec![1]);
        let result = storage.delete_and_get_old("a");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(vec![1]));
    }

    #[test]
    fn propagate_errors() {
        let mut storage = MockStorage::new();
        let k = "boom";

        storage.fail_on(k);

        assert!(storage.put(k, vec![1]).is_err());
        assert!(storage.get(k).is_err());
        assert!(storage.exists(k).is_err());
        assert!(storage.delete(k).is_err());
    }
}
