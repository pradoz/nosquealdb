use std::collections::HashMap;

use super::traits::Storage;
use crate::error::StorageResult;

/// NOT thread-safe
/// TODO: wrap in `Arc<RwLock<MemoryStorage>>` or use a concurrent implementation
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: HashMap<String, Vec<u8>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: HashMap::with_capacity(capacity),
        }
    }

    pub fn clear(&mut self) {
        self.data.clear()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.data.keys().map(|s| s.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.data.iter().map(|(k, v)| (k.as_str(), v.as_slice()))
    }

    pub fn keys_with_prefix<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a str> {
        self.data
            .keys()
            .filter(move |k| k.starts_with(prefix))
            .map(|s| s.as_str())
    }

    pub fn count_with_prefix(&self, prefix: &str) -> usize {
        self.keys_with_prefix(prefix).count()
    }

    // TODO: count bytes instead of values
    pub fn total_value_bytes(&self) -> usize {
        self.data.values().map(|v| v.len()).sum()
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn put(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()> {
        self.data.insert(key.to_string(), value);
        Ok(())
    }

    fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &str) -> StorageResult<()> {
        self.data.remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> StorageResult<bool> {
        Ok(self.data.contains_key(key))
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

impl IntoIterator for MemoryStorage {
    type Item = (String, Vec<u8>);
    type IntoIter = std::collections::hash_map::IntoIter<String, Vec<u8>>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl FromIterator<(String, Vec<u8>)> for MemoryStorage {
    fn from_iter<T: IntoIterator<Item = (String, Vec<u8>)>>(iter: T) -> Self {
        Self {
            data: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageExt;

    fn val(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    mod construction {
        use super::*;

        #[test]
        fn new_is_empty() {
            let storage = MemoryStorage::new();
            assert!(storage.is_empty());
            assert_eq!(storage.len(), 0);
        }

        #[test]
        fn default_is_empty() {
            let storage = MemoryStorage::default();
            assert!(storage.is_empty());
            assert_eq!(storage.len(), 0);
        }

        #[test]
        fn with_capacity() {
            let storage = MemoryStorage::with_capacity(10);
            assert!(storage.is_empty());
            assert_eq!(storage.len(), 0);
        }
    }

    mod operations {
        use super::*;

        #[test]
        fn put_and_get() {
            let mut storage = MemoryStorage::new();

            storage.put("key", val("banana")).unwrap();
            assert_eq!(storage.len(), 1);
            assert_eq!(storage.get("key").unwrap(), Some(val("banana")));

            // not found
            assert_eq!(storage.get("notfound").unwrap(), None);
        }

        #[test]
        fn put_overwrite() {
            let mut storage = MemoryStorage::new();

            storage.put("key", val("banana")).unwrap();
            storage.put("key", val("orange")).unwrap();
            assert_eq!(storage.len(), 1);
            assert_eq!(storage.get("key").unwrap(), Some(val("orange")));
        }

        #[test]
        fn delete() {
            let mut storage = MemoryStorage::new();

            // not found
            assert!(storage.delete("notfound").is_ok());

            // normal case
            assert!(!storage.exists("foo").unwrap());

            storage.put("foo", val("bar")).unwrap();
            assert!(storage.exists("foo").unwrap());

            storage.delete("foo").unwrap();
            assert!(!storage.exists("foo").unwrap());
        }
    }

    mod utility {
        use super::*;

        #[test]
        fn clear_removes_data() {
            let mut storage = MemoryStorage::new();

            storage.put("a", val("1")).unwrap();
            storage.put("b", val("2")).unwrap();
            assert!(!storage.is_empty());

            storage.clear();
            assert!(storage.is_empty());
        }

        #[test]
        fn keys_returns_all_keys() {
            let mut storage = MemoryStorage::new();
            storage.put("a", val("1")).unwrap();
            storage.put("b", val("2")).unwrap();
            storage.put("c", val("3")).unwrap();

            let mut keys: Vec<_> = storage.keys().collect();
            keys.sort();

            assert_eq!(keys, vec!["a", "b", "c"]);
        }

        #[test]
        fn iter_returns_all_pairs() {
            let mut storage = MemoryStorage::new();
            storage.put("a", vec![1]).unwrap();
            storage.put("b", vec![2]).unwrap();

            let mut pairs: Vec<_> = storage.iter().collect();
            pairs.sort_by_key(|(k, _)| *k);

            assert_eq!(pairs, vec![("a", &[1][..]), ("b", &[2][..])]);
        }

        #[test]
        fn keys_with_prefix_filter() {
            let mut storage = MemoryStorage::new();
            storage.put("user:1", val("1")).unwrap();
            storage.put("user:2", val("2")).unwrap();
            storage.put("banana:1", val("3")).unwrap();

            let mut user_keys: Vec<_> = storage.keys_with_prefix("user:").collect();
            user_keys.sort();

            assert_eq!(user_keys, vec!["user:1", "user:2"]);
        }

        #[test]
        fn keys_with_prefix_count() {
            let mut storage = MemoryStorage::new();
            storage.put("user:1", val("1")).unwrap();
            storage.put("user:2", val("2")).unwrap();
            storage.put("banana:1", val("3")).unwrap();

            assert_eq!(storage.count_with_prefix("user:"), 2);
            assert_eq!(storage.count_with_prefix("banana:"), 1);
            assert_eq!(storage.count_with_prefix("notfound:"), 0);
        }

        #[test]
        fn total_value_bytes() {
            let mut storage = MemoryStorage::new();
            storage.put("a", vec![1, 2]).unwrap();
            storage.put("b", vec![4]).unwrap();

            assert_eq!(storage.total_value_bytes(), 3);
        }

        #[test]
        fn into_iterator() {
            let mut storage = MemoryStorage::new();
            storage.put("a", vec![1]).unwrap();
            storage.put("b", vec![2]).unwrap();

            let mut pairs: Vec<_> = storage.into_iter().collect();
            pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

            assert_eq!(
                pairs,
                vec![("a".to_string(), vec![1]), ("b".to_string(), vec![2])]
            );
        }
    }

    mod storage_ext_integration {
        use super::*;

        #[test]
        fn put_if_not_exists() {
            let mut storage = MemoryStorage::new();

            assert!(storage.put_if_not_exists("key", val("first")).is_ok());
            assert!(storage.put_if_not_exists("key", val("second")).is_err());

            assert_eq!(storage.get("key").unwrap(), Some(val("first")));
        }

        #[test]
        fn get_or_error() {
            let mut storage = MemoryStorage::new();
            storage.put("key", val("value")).unwrap();

            assert_eq!(storage.get_or_error("key").unwrap(), val("value"));
            assert!(storage.get_or_error("missing").is_err());
        }

        #[test]
        fn update() {
            let mut storage = MemoryStorage::new();
            storage.put("key", val("original")).unwrap();

            assert!(storage.update("key", val("updated")).is_ok());
            assert_eq!(storage.get("key").unwrap(), Some(val("updated")));

            assert!(storage.update("missing", val("value")).is_err());
        }

        #[test]
        fn get_many() {
            let mut storage = MemoryStorage::new();
            storage.put("a", vec![1]).unwrap();
            storage.put("c", vec![3]).unwrap();

            let results = storage.get_many(&["a", "b", "c"]).unwrap();

            assert_eq!(results[0], Some(vec![1]));
            assert_eq!(results[1], None);
            assert_eq!(results[2], Some(vec![3]));
        }

        #[test]
        fn delete_and_get_old() {
            let mut storage = MemoryStorage::new();
            storage.put("key", val("value")).unwrap();

            let old = storage.delete_and_get_old("key").unwrap();
            assert_eq!(old, Some(val("value")));
            assert!(!storage.exists("key").unwrap());

            let missing = storage.delete_and_get_old("missing").unwrap();
            assert_eq!(missing, None);
        }
    }
}
