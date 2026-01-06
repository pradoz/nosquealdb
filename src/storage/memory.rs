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
