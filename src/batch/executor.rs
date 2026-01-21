use super::types::{BatchGetResult, BatchWriteItem, BatchWriteResult};
use crate::error::TableResult;
use crate::types::{Item, KeySchema, PrimaryKey};

pub struct BatchExecutor;

impl BatchExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute_put<F>(
        &self,
        items: Vec<Item>,
        schema: &KeySchema,
        mut put_item: F,
    ) -> TableResult<BatchWriteResult>
    where
        F: FnMut(Item) -> TableResult<()>,
    {
        let mut result = BatchWriteResult::new();

        for item in items {
            if item.validate_key(schema).is_err() {
                result.unprocessed_items.push(BatchWriteItem::put(item));
                continue;
            }
            match put_item(item.clone()) {
                Ok(()) => result.processed_count += 1,
                Err(_) => result.unprocessed_items.push(BatchWriteItem::put(item)),
            }
        }

        Ok(result)
    }

    pub fn execute_delete<F>(
        &self,
        keys: Vec<PrimaryKey>,
        mut delete_item: F,
    ) -> TableResult<BatchWriteResult>
    where
        F: FnMut(&PrimaryKey) -> TableResult<()>,
    {
        let mut result = BatchWriteResult::new();

        for key in keys {
            match delete_item(&key) {
                Ok(()) => result.processed_count += 1,
                Err(_) => result.unprocessed_items.push(BatchWriteItem::delete(key)),
            }
        }

        Ok(result)
    }

    pub fn execute_get<F>(
        &self,
        keys: Vec<PrimaryKey>,
        mut get_item: F,
    ) -> TableResult<BatchGetResult>
    where
        F: FnMut(&PrimaryKey) -> TableResult<Option<Item>>,
    {
        let mut result = BatchGetResult::new();

        for key in keys {
            match get_item(&key) {
                Ok(Some(item)) => result.items.push(item),
                Ok(None) => result.not_found_keys.push(key),
                Err(_) => result.unprocessed_keys.push(key),
            }
        }

        Ok(result)
    }
}

impl Default for BatchExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TableError;
    use crate::types::KeyType;
    use std::collections::HashMap;

    struct MockStorage {
        items: HashMap<String, Item>,
        schema: KeySchema,
        fail_on_write: bool,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                items: HashMap::new(),
                schema: KeySchema::simple("pk", KeyType::S),
                fail_on_write: false,
            }
        }
        fn with_item(mut self, pk: &str, item: Item) -> Self {
            self.items.insert(format!("S:{}", pk), item);
            self
        }
        fn with_fail_on_write(mut self) -> Self {
            self.fail_on_write = true;
            self
        }
        fn put(&mut self, item: Item) -> TableResult<()> {
            if self.fail_on_write {
                return Err(TableError::Storage("simulated failure".into()));
            }
            if let Some(key) = item.extract_key(&self.schema) {
                self.items.insert(key.to_storage_key(), item);
            }
            Ok(())
        }
        fn delete(&mut self, key: &PrimaryKey) -> TableResult<()> {
            if self.fail_on_write {
                return Err(TableError::Storage("simulated failure".into()));
            }
            self.items.remove(&key.to_storage_key());
            Ok(())
        }
        fn get(&self, key: &PrimaryKey) -> TableResult<Option<Item>> {
            Ok(self.items.get(&key.to_storage_key()).cloned())
        }
    }

    #[test]
    fn put() {
        let executor = BatchExecutor::new();
        let mut storage = MockStorage::new();

        let items = vec![
            Item::new().with_s("pk", "test0"),
            Item::new().with_s("pk", "test1"),
        ];

        let result = executor
            .execute_put(items, &storage.schema.clone(), |item| storage.put(item))
            .unwrap();
        assert!(result.is_complete());
        assert!(!result.has_unprocessed());
        assert_eq!(result.processed_count, 2);
        assert_eq!(result.unprocessed_count(), 0);
    }

    #[test]
    fn put_with_invalid_key() {
        let executor = BatchExecutor::new();
        let mut storage = MockStorage::new();

        let items = vec![
            Item::new().with_s("pk", "good"),
            Item::new().with_s("somethingelse", "bad"),
        ];

        let result = executor
            .execute_put(items, &storage.schema.clone(), |item| storage.put(item))
            .unwrap();
        assert!(!result.is_complete());
        assert!(result.has_unprocessed());
        assert_eq!(result.processed_count, 1);
        assert_eq!(result.unprocessed_count(), 1);
    }

    #[test]
    fn put_with_failures() {
        let executor = BatchExecutor::new();
        let mut storage = MockStorage::new();

        let items = vec![
            Item::new().with_s("pk", "test0"),
            Item::new().with_s("not-the-pk", "test1"),
        ];

        let result = executor
            .execute_put(items, &storage.schema.clone(), |item| storage.put(item))
            .unwrap();
        assert!(!result.is_complete());
        assert!(result.has_unprocessed());
        assert_eq!(result.processed_count, 1);
        assert_eq!(result.unprocessed_count(), 1);
    }

    #[test]
    fn delete() {
        let executor = BatchExecutor::new();
        let mut storage = MockStorage::new()
            .with_item("test0", Item::new().with_s("pk", "test0"))
            .with_item("test1", Item::new().with_s("pk", "test1"));

        let keys = vec![PrimaryKey::simple("test0"), PrimaryKey::simple("test1")];

        let result = executor
            .execute_delete(keys, |key| storage.delete(key))
            .unwrap();
        assert!(result.is_complete());
        assert!(!result.has_unprocessed());
        assert_eq!(result.processed_count, 2);
        assert_eq!(result.unprocessed_count(), 0);
    }

    #[test]
    fn delete_with_failures() {
        let executor = BatchExecutor::new();
        let mut storage = MockStorage::new().with_fail_on_write();

        let keys = vec![PrimaryKey::simple("test0"), PrimaryKey::simple("test1")];

        let result = executor
            .execute_delete(keys, |key| storage.delete(key))
            .unwrap();
        assert!(!result.is_complete());
        assert!(result.has_unprocessed());
        assert_eq!(result.processed_count, 0);
        assert_eq!(result.unprocessed_count(), 2);
    }

    #[test]
    fn get_batch() {
        let executor = BatchExecutor::new();
        let storage = MockStorage::new()
            .with_item(
                "test0",
                Item::new().with_s("pk", "test0").with_n("value", 0),
            )
            .with_item(
                "test1",
                Item::new().with_s("pk", "test1").with_n("value", 1),
            );

        let keys = vec![PrimaryKey::simple("test0"), PrimaryKey::simple("test1")];

        let result = executor.execute_get(keys, |key| storage.get(key)).unwrap();
        assert!(result.is_complete());
        assert!(result.not_found_keys.is_empty());
        assert_eq!(result.found_count(), 2);
    }

    #[test]
    fn get_batch_some_not_found() {
        let executor = BatchExecutor::new();
        let storage = MockStorage::new()
            .with_item(
                "test0",
                Item::new().with_s("pk", "test0").with_n("value", 0),
            )
            .with_item(
                "test1",
                Item::new().with_s("pk", "test1").with_n("value", 1),
            );

        let keys = vec![
            PrimaryKey::simple("test0"),
            PrimaryKey::simple("nonexistent"),
        ];

        let result = executor.execute_get(keys, |key| storage.get(key)).unwrap();
        assert!(result.is_complete());
        assert!(!result.not_found_keys.is_empty());
        assert_eq!(result.found_count(), 1);
        assert_eq!(result.not_found_keys.len(), 1);
    }
}
