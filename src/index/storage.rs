use std::collections::HashMap;

#[derive(Debug)]
pub struct IndexStorage<V> {
    /// primary data store: index_storage_key -> value
    data: HashMap<String, V>,
    /// reverse index: table_storage_key -> index_storage_key
    reverse_index: HashMap<String, String>,
}

impl<V> IndexStorage<V> {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            reverse_index: HashMap::new(),
        }
    }

    pub fn put(&mut self, table_key: String, index_key: String, value: V) -> Option<V> {
        let old = self.remove_by_table_key(&table_key);

        self.reverse_index.insert(table_key, index_key.clone());
        self.data.insert(index_key, value);

        old
    }

    pub fn get(&self, index_key: &str) -> Option<&V> {
        self.data.get(index_key)
    }

    pub fn remove_by_table_key(&mut self, table_key: &str) -> Option<V> {
        if let Some(index_key) = self.reverse_index.remove(table_key) {
            self.data.remove(&index_key)
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.reverse_index.clear();
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.data.values()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.data.iter()
    }

    #[cfg(test)]
    pub fn reverse_index_len(&self) -> usize {
        self.reverse_index.len()
    }
}

impl<V> Default for IndexStorage<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let mut storage: IndexStorage<String> = IndexStorage::new();
        storage.put("table_key_1".into(), "index_key_1".into(), "value1".into());
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.get("index_key_1"), Some(&"value1".to_string()));
    }

    #[test]
    fn put_overwrite_returns_old() {
        let mut storage: IndexStorage<String> = IndexStorage::new();

        storage.put("table_key_1".into(), "index_key_1".into(), "value1".into());
        let old = storage.put("table_key_1".into(), "index_key_2".into(), "value2".into());

        assert_eq!(old, Some("value1".to_string()));
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.get("index_key_1"), None);
        assert_eq!(storage.get("index_key_2"), Some(&"value2".to_string()));
    }

    #[test]
    fn remove_by_table_key() {
        let mut storage: IndexStorage<String> = IndexStorage::new();

        storage.put("table_key_1".into(), "index_key_1".into(), "value1".into());
        storage.put("table_key_2".into(), "index_key_2".into(), "value2".into());
        assert_eq!(storage.len(), 2);

        // remove nonexistent
        let removed = storage.remove_by_table_key("nonexistent");
        assert_eq!(storage.len(), 2);
        assert_eq!(removed, None);
        // remove actual key
        let removed = storage.remove_by_table_key("table_key_2");
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.reverse_index_len(), 1);
        assert_eq!(removed, Some("value2".to_string()));
    }

    #[test]
    fn clear() {
        let mut storage: IndexStorage<String> = IndexStorage::new();
        storage.put("t1".into(), "i1".into(), "v1".into());
        storage.put("t2".into(), "i2".into(), "v2".into());
        assert_eq!(storage.len(), 2);

        storage.clear();
        assert!(storage.is_empty());
        assert_eq!(storage.len(), 0);
        assert_eq!(storage.reverse_index_len(), 0);
    }

    #[test]
    fn values_iter() {
        let mut storage: IndexStorage<i32> = IndexStorage::new();
        storage.put("t1".into(), "i1".into(), 1);
        storage.put("t2".into(), "i2".into(), 2);
        storage.put("t3".into(), "i3".into(), 3);
        assert_eq!(storage.len(), 3);

        let sum: i32 = storage.values().sum();
        assert_eq!(sum, 6);
    }
}
