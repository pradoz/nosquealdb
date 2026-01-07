use std::collections::BTreeMap;

use crate::error::{TableError, TableResult};
use crate::storage::{MemoryStorage, Storage};
use crate::types::{AttributeValue, Item, KeySchema, PrimaryKey, decode, encode};

#[derive(Debug)]
pub struct Table {
    name: String,
    schema: KeySchema,
    storage: MemoryStorage,
}

impl Table {
    pub fn new(name: impl Into<String>, schema: KeySchema) -> Self {
        Self {
            name: name.into(),
            schema,
            storage: MemoryStorage::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &KeySchema {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn clear(&mut self) {
        self.storage.clear();
    }

    // internal helpers
    fn encode_item(&self, item: &Item) -> TableResult<Vec<u8>> {
        let map: BTreeMap<String, AttributeValue> = item
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let av = AttributeValue::M(map);
        Ok(encode(&av))
    }

    fn decode_item(&self, data: &[u8]) -> TableResult<Item> {
        let av = decode(data)?;
        match av {
            AttributeValue::M(map) => Ok(Item::from(map)),
            _ => Err(TableError::Encoding("expected map type".into())),
        }
    }

    pub fn put_item(&mut self, item: Item) -> TableResult<Option<Item>> {
        let _ = item.validate_key(&self.schema);

        let pk = item.extract_key(&self.schema).ok_or_else(|| {
            TableError::InvalidKey(crate::types::KeyValidationError::MissingAttribute {
                name: self.schema.pk_name().to_string(),
            })
        })?;

        let storage_key = pk.to_storage_key();
        let encoded = self.encode_item(&item)?;
        let old_item = self.get_item_by_storage_key(&storage_key)?;

        self.storage.put(&storage_key, encoded)?;

        Ok(old_item)
    }

    pub fn put_item_if_not_exists(&mut self, item: Item) -> TableResult<()> {
        let _ = item.validate_key(&self.schema());

        let pk = item.extract_key(&self.schema).ok_or_else(|| {
            TableError::InvalidKey(crate::types::KeyValidationError::MissingAttribute {
                name: self.schema.pk_name().to_string(),
            })
        })?;

        let storage_key = pk.to_storage_key();
        if self.storage.exists(&storage_key)? {
            return Err(TableError::ItemAlreadyExists);
        }

        let encoded = self.encode_item(&item)?;
        self.storage.put(&storage_key, encoded)?;

        Ok(())
    }

    pub fn get_item(&self, key: &PrimaryKey) -> TableResult<Option<Item>> {
        let storage_key = key.to_storage_key();
        self.get_item_by_storage_key(&storage_key)
    }

    pub fn delete_item(&mut self, key: &PrimaryKey) -> TableResult<Option<Item>> {
        let storage_key = key.to_storage_key();
        let old_item = self.get_item_by_storage_key(&storage_key)?;

        self.storage.delete(&storage_key)?;

        Ok(old_item)
    }

    pub fn scan(&self) -> TableResult<Vec<Item>> {
        let mut items = Vec::new();

        for (_, value) in self.storage.iter() {
            let item = self.decode_item(value)?;
            items.push(item);
        }

        Ok(items)
    }

    pub fn scan_limit(&self, limit: usize) -> TableResult<Vec<Item>> {
        let mut items = Vec::with_capacity(limit.min(self.storage.len()));

        for (_, value) in self.storage.iter().take(limit) {
            let item = self.decode_item(value)?;
            items.push(item);
        }

        Ok(items)
    }

    fn get_item_by_storage_key(&self, storage_key: &str) -> TableResult<Option<Item>> {
        match self.storage.get(storage_key)? {
            Some(data) => Ok(Some(self.decode_item(&data)?)),
            None => Ok(None),
        }
    }
}

pub struct TableBuilder {
    name: String,
    schema: KeySchema,
    initial_capacity: Option<usize>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>, schema: KeySchema) -> Self {
        Self {
            name: name.into(),
            schema,
            initial_capacity: None,
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.initial_capacity = Some(capacity);
        self
    }

    pub fn build(self) -> Table {
        let mut table = Table::new(self.name, self.schema);
        if let Some(cap) = self.initial_capacity {
            table.storage = MemoryStorage::with_capacity(cap);
        }
        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;

    fn simple_table() -> Table {
        Table::new("users", KeySchema::simple("user_id", KeyType::S))
    }

    fn composite_table() -> Table {
        Table::new(
            "orders",
            KeySchema::composite("user_id", KeyType::S, "order_id", KeyType::S),
        )
    }

    mod put_and_get {
        use super::*;

        #[test]
        fn simple_key() {
            let mut table = simple_table();

            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Bob")
                .with_n("count", 42);

            table.put_item(item.clone()).unwrap();

            let key = PrimaryKey::simple("user123");
            let retrieved = table.get_item(&key).unwrap().unwrap();

            assert_eq!(retrieved.get("user_id"), item.get("user_id"));
            assert_eq!(retrieved.get("name"), item.get("name"));
            assert_eq!(retrieved.get("count"), item.get("count"));
        }

        #[test]
        fn composite_key() {
            let mut table = composite_table();

            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("order_id", "order456")
                .with_n("total", 67);

            table.put_item(item.clone()).unwrap();

            let key = PrimaryKey::composite("user123", "order456");
            let retrieved = table.get_item(&key).unwrap().unwrap();

            assert_eq!(retrieved.get("user_id"), item.get("user_id"));
            assert_eq!(retrieved.get("order_id"), item.get("order_id"));
            assert_eq!(retrieved.get("total"), item.get("total"));
        }

        #[test]
        fn nonexistent_returns_none() {
            let table = simple_table();
            let key = PrimaryKey::simple("user123");
            let result = table.get_item(&key);
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[test]
        fn replace_existing_and_return_old() {
            let mut table = simple_table();
            let test_key_name = "user123";

            let item1 = Item::new()
                .with_s("user_id", test_key_name)
                .with_s("name", "Alice");
            let item2 = Item::new()
                .with_s("user_id", test_key_name)
                .with_s("name", "Bob");

            let old = table.put_item(item1).unwrap();
            assert!(old.is_none());

            let old = table.put_item(item2).unwrap();
            assert!(old.is_some());
            assert_eq!(
                old.unwrap().get("name"),
                Some(&AttributeValue::S("Alice".into()))
            );

            let key = PrimaryKey::simple(test_key_name);
            let curr = table.get_item(&key).unwrap().unwrap();
            assert_eq!(curr.get("name"), Some(&AttributeValue::S("Bob".into())));
        }
    }

    mod conditional {
        use super::*;

        #[test]
        fn put_if_not_exists() {
            let mut table = simple_table();

            let item1 = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice");
            let item2 = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Bob");

            // doesn't exist yet, should succeed
            assert!(table.put_item_if_not_exists(item1).is_ok());
            assert_eq!(table.len(), 1);

            // alreadys exists, should fail
            assert!(table.put_item_if_not_exists(item2).is_err());
            assert_eq!(table.len(), 1);

            // initial put is preserved
            let key = PrimaryKey::simple("user123");
            let item = table.get_item(&key).unwrap().unwrap();
            assert_eq!(item.get("name"), Some(&AttributeValue::S("Alice".into())))
        }
    }

    mod delete {
        use super::*;

        #[test]
        fn existing_returns_item() {
            let mut table = simple_table();

            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice");
            table.put_item(item).unwrap();

            let key = PrimaryKey::simple("user123");
            let deleted = table.delete_item(&key).unwrap();
            assert!(deleted.is_some());
            assert_eq!(
                deleted.unwrap().get("name"),
                Some(&AttributeValue::S("Alice".into()))
            );
            assert!(table.is_empty());
        }

        #[test]
        fn nonexistent_returns_none() {
            let mut table = simple_table();
            let key = PrimaryKey::simple("nonexistent");
            let deleted = table.delete_item(&key);
            assert!(deleted.is_ok());
            assert!(deleted.unwrap().is_none());
        }
    }

    mod validation {
        use super::*;

        #[test]
        fn put_rejects_missing_partition_key() {
            let mut table = simple_table();
            let item = Item::new().with_s("name", "Alice");

            let result = table.put_item(item);
            assert!(result.unwrap_err().is_invalid_key());
        }

        #[test]
        fn put_rejects_missing_sort_key() {
            let mut table = composite_table();
            let item = Item::new().with_s("user_id", "user123");

            let result = table.put_item(item);
            assert!(result.unwrap_err().is_invalid_key());
        }

        #[test]
        fn put_rejects_wrong_key_type() {
            let mut table = simple_table();
            let item = Item::new().with_n("user_id", 123).with_s("name", "Alice");

            let result = table.put_item(item);
            assert!(result.unwrap_err().is_invalid_key());
        }
    }

    mod scan {
        use super::*;

        #[test]
        fn empty_table() {
            let table = simple_table();
            let items = table.scan().unwrap();
            assert!(items.is_empty());
        }

        #[test]
        fn returns_all_items() {
            let mut table = simple_table();
            let total_items = 10;

            for i in 0..total_items {
                let item = Item::new()
                    .with_s("user_id", format!("user{}", i))
                    .with_n("index", i);
                table.put_item(item).unwrap();
            }

            let items = table.scan().unwrap();
            assert!(!items.is_empty());
            assert_eq!(items.len(), table.len());
            assert_eq!(items.len(), total_items);
        }

        #[test]
        fn returns_limited_items() {
            let mut table = simple_table();
            let total_items = 10;
            let limit = 5;

            for i in 0..total_items {
                let item = Item::new()
                    .with_s("user_id", format!("user{}", i))
                    .with_n("index", i);
                table.put_item(item).unwrap();
            }

            let items = table.scan_limit(limit).unwrap();
            assert!(!items.is_empty());
            assert_eq!(items.len(), limit);
        }
    }
}
