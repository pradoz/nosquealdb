use std::collections::BTreeMap;

use crate::condition::{Condition, evaluate};
use crate::error::{TableError, TableResult};
use crate::index::{GlobalSecondaryIndex, GsiBuilder, LocalSecondaryIndex, LsiBuilder};
use crate::query::{KeyCondition, QueryExecutor, QueryOptions, QueryResult};
use crate::storage::{MemoryStorage, Storage};
use crate::types::{
    AttributeValue, Item, KeyAttribute, KeySchema, KeyValidationError, PrimaryKey, ReturnValue,
    WriteResult, decode, encode,
};
use crate::update::{UpdateExecutor, UpdateExpression};

#[derive(Debug)]
pub struct Table {
    name: String,
    schema: KeySchema,
    storage: MemoryStorage,
    gsis: BTreeMap<String, GlobalSecondaryIndex>,
    lsis: BTreeMap<String, LocalSecondaryIndex>,
}

impl Table {
    pub fn new(name: impl Into<String>, schema: KeySchema) -> Self {
        Self {
            name: name.into(),
            schema,
            storage: MemoryStorage::new(),
            gsis: BTreeMap::new(),
            lsis: BTreeMap::new(),
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
        for gsi in self.gsis.values_mut() {
            *gsi = GlobalSecondaryIndex::new(
                gsi.name(),
                gsi.schema().clone(),
                gsi.projection().clone(),
                self.schema.clone(),
            );
        }
        for lsi in self.lsis.values_mut() {
            *lsi = LocalSecondaryIndex::new(
                lsi.name(),
                KeyAttribute::new(lsi.sort_key_name(), lsi.sort_key_type()),
                lsi.projection().clone(),
                self.schema.clone(),
            );
        }
    }

    // index management
    pub fn add_gsi(&mut self, builder: GsiBuilder) {
        let gsi = builder.build(self.schema.clone());
        let name = gsi.name().to_string();

        let mut gsi = gsi;
        for item in self.scan().unwrap_or_default() {
            if let Some(pk) = item.extract_key(&self.schema) {
                gsi.put(pk, &item);
            }
        }

        self.gsis.insert(name, gsi);
    }

    pub fn gsi(&self, name: &str) -> Option<&GlobalSecondaryIndex> {
        self.gsis.get(name)
    }

    pub fn gsi_names(&self) -> impl Iterator<Item = &str> {
        self.gsis.keys().map(|s| s.as_str())
    }

    pub fn add_lsi(&mut self, builder: LsiBuilder) {
        let lsi = builder.build(self.schema.clone());
        let name = lsi.name().to_string();

        let mut lsi = lsi;
        for item in self.scan().unwrap_or_default() {
            if let Some(pk) = item.extract_key(&self.schema) {
                lsi.put(&pk, &item);
            }
        }

        self.lsis.insert(name, lsi);
    }

    pub fn lsi(&self, name: &str) -> Option<&LocalSecondaryIndex> {
        self.lsis.get(name)
    }

    pub fn lsi_names(&self) -> impl Iterator<Item = &str> {
        self.lsis.keys().map(|s| s.as_str())
    }

    // operations
    pub fn put_item(&mut self, item: Item) -> TableResult<Option<Item>> {
        let result = self.put_item_with_return(item, ReturnValue::AllOld)?;
        Ok(result.attributes)
    }

    pub fn put_item_with_return(
        &mut self,
        item: Item,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        self.put_item_internal(item, None, return_value)
    }

    pub fn put_item_with_condition(
        &mut self,
        item: Item,
        condition: Condition,
    ) -> TableResult<Option<Item>> {
        let result =
            self.put_item_with_condition_and_return(item, condition, ReturnValue::AllOld)?;
        Ok(result.attributes)
    }

    pub fn put_item_with_condition_and_return(
        &mut self,
        item: Item,
        condition: Condition,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        self.put_item_internal(item, Some(condition), return_value)
    }

    fn put_item_internal(
        &mut self,
        item: Item,
        condition: Option<Condition>,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        let _ = item.validate_key(&self.schema)?;

        let pk = item.extract_key(&self.schema).ok_or_else(|| {
            TableError::InvalidKey(KeyValidationError::MissingAttribute {
                name: self.schema.pk_name().to_string(),
            })
        })?;

        let storage_key = pk.to_storage_key();
        let old_item = self.get_item_by_storage_key(&storage_key)?;

        if let Some(cond) = condition {
            let check_item = old_item.clone().unwrap_or_default();
            if !evaluate(&cond, &check_item)? {
                return Err(TableError::ConditionFailed);
            }
        }

        let was_update = old_item.is_some();
        let encoded = self.encode_item(&item)?;
        self.storage.put(&storage_key, encoded)?;
        self.update_indexes_on_put(&pk, &item);

        let attributes = match return_value {
            ReturnValue::None => None,
            ReturnValue::AllOld => old_item,
            ReturnValue::AllNew => Some(item),
        };

        Ok(WriteResult {
            attributes,
            was_update,
        })
    }

    pub fn put_item_if_not_exists(&mut self, item: Item) -> TableResult<()> {
        let _ = item.validate_key(&self.schema())?;

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
        self.update_indexes_on_put(&pk, &item);

        Ok(())
    }

    pub fn get_item(&self, key: &PrimaryKey) -> TableResult<Option<Item>> {
        let storage_key = key.to_storage_key();
        self.get_item_by_storage_key(&storage_key)
    }

    pub fn delete_item(&mut self, key: &PrimaryKey) -> TableResult<Option<Item>> {
        let result = self.delete_item_with_return(key, ReturnValue::AllOld)?;
        Ok(result.attributes)
    }

    pub fn delete_item_with_return(
        &mut self,
        key: &PrimaryKey,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        self.delete_item_internal(key, None, return_value)
    }

    pub fn delete_item_with_condition(
        &mut self,
        key: &PrimaryKey,
        condition: Condition,
    ) -> TableResult<Option<Item>> {
        let result =
            self.delete_item_with_condition_and_return(key, condition, ReturnValue::AllOld)?;
        Ok(result.attributes)
    }

    pub fn delete_item_with_condition_and_return(
        &mut self,
        key: &PrimaryKey,
        condition: Condition,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        self.delete_item_internal(key, Some(condition), return_value)
    }

    fn delete_item_internal(
        &mut self,
        key: &PrimaryKey,
        condition: Option<Condition>,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        let storage_key = key.to_storage_key();
        let old_item = self.get_item_by_storage_key(&storage_key)?;

        if let Some(cond) = condition {
            let check_item = old_item.clone().unwrap_or_default();
            if !evaluate(&cond, &check_item)? {
                return Err(TableError::ConditionFailed);
            }
        }

        let was_update = old_item.is_some();

        self.storage.delete(&storage_key)?;

        if was_update {
            self.update_indexes_on_delete(key);
        }

        let attributes = match return_value {
            ReturnValue::None => None,
            ReturnValue::AllOld => old_item,
            ReturnValue::AllNew => None, // Delete has no "new" item
        };

        Ok(WriteResult {
            attributes,
            was_update,
        })
    }

    pub fn query(&self, condition: KeyCondition) -> TableResult<QueryResult> {
        self.query_with_options(condition, QueryOptions::new())
    }

    pub fn query_with_options(
        &self,
        condition: KeyCondition,
        options: QueryOptions,
    ) -> TableResult<QueryResult> {
        let executor = QueryExecutor::new(&self.schema);
        executor.validate_condition(&condition)?;

        let items = self.iter_with_keys()?;
        executor.execute(items.into_iter(), &condition, &options)
    }

    pub fn query_with_filter(
        &self,
        key_condition: KeyCondition,
        filter: Condition,
    ) -> TableResult<QueryResult> {
        self.query_with_filter_and_options(key_condition, filter, QueryOptions::new())
    }

    pub fn query_with_filter_and_options(
        &self,
        key_condition: KeyCondition,
        filter: Condition,
        options: QueryOptions,
    ) -> TableResult<QueryResult> {
        let mut result = self.query_with_options(key_condition, options)?;
        let filtered: Vec<Item> = result
            .items
            .into_iter()
            .filter(|item| evaluate(&filter, item).unwrap_or(false))
            .collect();
        let count = filtered.len();
        result.items = filtered;
        result.count = count;

        Ok(result)
    }

    pub fn query_gsi(&self, index_name: &str, condition: KeyCondition) -> TableResult<QueryResult> {
        self.query_gsi_with_options(index_name, condition, QueryOptions::new())
    }

    pub fn query_gsi_with_options(
        &self,
        index_name: &str,
        condition: KeyCondition,
        options: QueryOptions,
    ) -> TableResult<QueryResult> {
        let gsi = self
            .gsis
            .get(index_name)
            .ok_or_else(|| TableError::index_not_found(index_name))?;

        gsi.query_with_options(condition, options)
    }

    pub fn query_gsi_with_filter(
        &self,
        index_name: &str,
        key_condition: KeyCondition,
        filter: Condition,
    ) -> TableResult<QueryResult> {
        let mut result = self.query_gsi(index_name, key_condition)?;

        let filtered: Vec<Item> = result
            .items
            .into_iter()
            .filter(|item| evaluate(&filter, item).unwrap_or(false))
            .collect();

        let count = filtered.len();
        result.items = filtered;
        result.count = count;

        Ok(result)
    }

    pub fn query_lsi(&self, index_name: &str, condition: KeyCondition) -> TableResult<QueryResult> {
        self.query_lsi_with_options(index_name, condition, QueryOptions::new())
    }

    pub fn query_lsi_with_options(
        &self,
        index_name: &str,
        condition: KeyCondition,
        options: QueryOptions,
    ) -> TableResult<QueryResult> {
        let lsi = self
            .lsis
            .get(index_name)
            .ok_or_else(|| TableError::index_not_found(index_name))?;

        lsi.query_with_options(condition, options)
    }

    pub fn update_item(
        &mut self,
        key: &PrimaryKey,
        expression: UpdateExpression,
    ) -> TableResult<Option<Item>> {
        let result = self.update_item_with_return(key, expression, ReturnValue::AllNew)?;
        Ok(result.attributes)
    }

    pub fn update_item_with_return(
        &mut self,
        key: &PrimaryKey,
        expression: UpdateExpression,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        self.update_item_internal(key, expression, None, return_value)
    }

    pub fn update_item_with_condition(
        &mut self,
        key: &PrimaryKey,
        expression: UpdateExpression,
        condition: Condition,
    ) -> TableResult<Option<Item>> {
        let result = self.update_item_with_condition_and_return(
            key,
            expression,
            condition,
            ReturnValue::AllNew,
        )?;
        Ok(result.attributes)
    }

    pub fn update_item_with_condition_and_return(
        &mut self,
        key: &PrimaryKey,
        expression: UpdateExpression,
        condition: Condition,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        self.update_item_internal(key, expression, Some(condition), return_value)
    }

    fn update_item_internal(
        &mut self,
        key: &PrimaryKey,
        expression: UpdateExpression,
        condition: Option<Condition>,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
        let storage_key = key.to_storage_key();
        let old_item = self
            .get_item_by_storage_key(&storage_key)?
            .ok_or(TableError::ItemNotFound)?;

        if let Some(cond) = condition {
            if !evaluate(&cond, &old_item)? {
                return Err(TableError::ConditionFailed);
            }
        }

        let executor = UpdateExecutor::new();
        let new_item = executor.execute(old_item.clone(), &expression)?;

        // failure checks
        let new_key = new_item.extract_key(&self.schema).ok_or_else(|| {
            TableError::UpdateError("update removed key attributes".to_string())
        })  ?;

        if &new_key != key {
            return Err(TableError::UpdateError("cannot modify key attributes".to_string()));
        }

        // save updated item
        let encoded = self.encode_item(&new_item)?;
        self.storage.put(&storage_key, encoded)?;
        self.update_indexes_on_put(key, &new_item);

        let attributes = match return_value {
            ReturnValue::AllNew => Some(new_item),
            ReturnValue::AllOld => Some(old_item),
            ReturnValue::None => None,
        };

        Ok(WriteResult {
            attributes,
            was_update: true,
        })
    }

    pub fn scan(&self) -> TableResult<Vec<Item>> {
        let mut items = Vec::new();

        for (_, value) in self.storage.iter() {
            let item = self.decode_item(value)?;
            items.push(item);
        }

        Ok(items)
    }

    pub fn scan_with_filter(&self, filter: Condition) -> TableResult<Vec<Item>> {
        let items = self.scan()?;

        Ok(items
            .into_iter()
            .filter(|item| evaluate(&filter, item).unwrap_or(false))
            .collect())
    }

    pub fn scan_limit(&self, limit: usize) -> TableResult<Vec<Item>> {
        let mut items = Vec::with_capacity(limit.min(self.storage.len()));

        for (_, value) in self.storage.iter().take(limit) {
            let item = self.decode_item(value)?;
            items.push(item);
        }

        Ok(items)
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

    fn get_item_by_storage_key(&self, storage_key: &str) -> TableResult<Option<Item>> {
        match self.storage.get(storage_key)? {
            Some(data) => Ok(Some(self.decode_item(&data)?)),
            None => Ok(None),
        }
    }

    /// TODO: performance: this allocates a Vec for all items. For large tables,
    /// consider returning an iterator that decodes lazily to reduce memory pressure
    fn iter_with_keys(&self) -> TableResult<Vec<(PrimaryKey, Item)>> {
        let mut result = Vec::new();
        for (_, value) in self.storage.iter() {
            let item = self.decode_item(value)?;
            if let Some(pk) = item.extract_key(&self.schema) {
                result.push((pk, item));
            }
        }

        Ok(result)
    }

    fn update_indexes_on_put(&mut self, pk: &PrimaryKey, item: &Item) {
        for gsi in self.gsis.values_mut() {
            gsi.put(pk.clone(), item);
        }
        for lsi in self.lsis.values_mut() {
            lsi.put(pk, item);
        }
    }

    fn update_indexes_on_delete(&mut self, pk: &PrimaryKey) {
        for gsi in self.gsis.values_mut() {
            gsi.delete(pk);
        }
        for lsi in self.lsis.values_mut() {
            lsi.delete(pk);
        }
    }
}

pub struct TableBuilder {
    name: String,
    schema: KeySchema,
    initial_capacity: Option<usize>,
    gsi_builders: Vec<GsiBuilder>,
    lsi_builders: Vec<LsiBuilder>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>, schema: KeySchema) -> Self {
        Self {
            name: name.into(),
            schema,
            initial_capacity: None,
            gsi_builders: Vec::new(),
            lsi_builders: Vec::new(),
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.initial_capacity = Some(capacity);
        self
    }

    pub fn with_gsi(mut self, builder: GsiBuilder) -> Self {
        self.gsi_builders.push(builder);
        self
    }

    pub fn with_lsi(mut self, builder: LsiBuilder) -> Self {
        self.lsi_builders.push(builder);
        self
    }

    pub fn build(self) -> Table {
        let mut table = Table::new(self.name, self.schema);
        if let Some(cap) = self.initial_capacity {
            table.storage = MemoryStorage::with_capacity(cap);
        }
        for gsi_builder in self.gsi_builders {
            table.add_gsi(gsi_builder);
        }
        for lsi_builder in self.lsi_builders {
            table.add_lsi(lsi_builder);
        }
        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::attr;
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

    mod update_item {
        use super::*;
        use crate::update::UpdateExpression;

        #[test]
        fn simple() {
            let mut table = simple_table();
            let update_expr = UpdateExpression::new();

            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice")
                .with_n("count", 42);
            table.put_item(item.clone()).unwrap();

            let key = PrimaryKey::simple("user123");
            let result = table.update_item(&key, update_expr.set("name", "Bob").add("count", 5i32)).unwrap().unwrap();

            assert_eq!(result.get("name"), Some(&AttributeValue::S("Bob".into())));
            assert_eq!(result.get("count"), Some(&AttributeValue::N("47".into())));
        }

        #[test]
        fn nonexistent_fails() {
            let mut table = simple_table();
            let key = PrimaryKey::simple("nonexistent");
            let result = table.update_item(&key, UpdateExpression::new().set("name", "Bob"));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_not_found());
        }

        #[test]
        fn modify_key_fails() {
            let mut table = simple_table();
            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice")
                .with_n("count", 42);
            table.put_item(item.clone()).unwrap();

            let key = PrimaryKey::simple("user123");
            let result = table.update_item(&key, UpdateExpression::new().set("user_id", "fail"));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_update_error());
        }
    }

    mod return_values {
        use super::*;

        #[test]
        fn put_return_none() {
            let mut table = simple_table();
            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice");

            let result = table.put_item_with_return(item, ReturnValue::None).unwrap();
            assert!(result.attributes.is_none());
            assert!(!result.was_update);
        }

        #[test]
        fn put_return_all_old() {
            let mut table = simple_table();

            // on create
            let item1 = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice");
            let result = table
                .put_item_with_return(item1, ReturnValue::AllOld)
                .unwrap();
            assert!(result.attributes.is_none());
            assert!(!result.was_update);

            // on overwrite
            let item2 = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Bob");
            let result = table
                .put_item_with_return(item2, ReturnValue::AllOld)
                .unwrap();
            assert!(result.was_update);
            let old = result.attributes.unwrap();
            assert_eq!(old.get("name"), Some(&AttributeValue::S("Alice".into())));
        }

        #[test]
        fn put_return_all_new() {
            let mut table = simple_table();
            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice");

            let result = table
                .put_item_with_return(item.clone(), ReturnValue::AllNew)
                .unwrap();
            assert!(!result.was_update);

            let new = result.attributes.unwrap();
            assert_eq!(new.get("name"), Some(&AttributeValue::S("Alice".into())));
        }

        #[test]
        fn delete_return_none() {
            let mut table = simple_table();
            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user123")
                        .with_s("name", "Alice"),
                )
                .unwrap();

            let result = table
                .delete_item_with_return(&PrimaryKey::simple("user123"), ReturnValue::None)
                .unwrap();
            assert!(result.attributes.is_none());
            assert!(result.was_update);
        }

        #[test]
        fn delete_return_all_old() {
            let mut table = simple_table();
            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user123")
                        .with_s("name", "Alice"),
                )
                .unwrap();

            let result = table
                .delete_item_with_return(&PrimaryKey::simple("user123"), ReturnValue::AllOld)
                .unwrap();

            assert!(result.was_update);
            let old = result.attributes.unwrap();
            assert_eq!(old.get("name"), Some(&AttributeValue::S("Alice".into())));
        }
    }

    mod indexes {
        use super::*;
        use crate::query::KeyCondition;

        fn composite_table_with_indexes() -> Table {
            let schema = KeySchema::composite("user_id", KeyType::S, "order_id", KeyType::S);

            TableBuilder::new("orders", schema)
                .with_gsi(GsiBuilder::new(
                    "orders-by-date",
                    KeySchema::composite("order_date", KeyType::S, "user_id", KeyType::S),
                ))
                .with_lsi(LsiBuilder::new("orders-by-status", "status", KeyType::S))
                .build()
        }

        fn sample_order(user: &str, order: &str, date: &str, status: &str, amount: i32) -> Item {
            Item::new()
                .with_s("user_id", user)
                .with_s("order_id", order)
                .with_s("order_date", date)
                .with_s("status", status)
                .with_n("amount", amount)
        }

        #[test]
        fn updated_on_delete() {
            let mut table = composite_table_with_indexes();

            table
                .put_item(sample_order(
                    "user1",
                    "order001",
                    "2026-01-08",
                    "pending",
                    100,
                ))
                .unwrap();

            let result = table
                .query_gsi("orders-by-date", KeyCondition::pk("2026-01-08"))
                .unwrap();
            assert_eq!(result.count, 1);

            table
                .delete_item(&PrimaryKey::composite("user1", "order001"))
                .unwrap();

            let result = table
                .query_gsi("orders-by-date", KeyCondition::pk("2026-01-08"))
                .unwrap();
            assert_eq!(result.count, 0);
        }

        #[test]
        fn updated_on_item_update() {
            let mut table = composite_table_with_indexes();

            table
                .put_item(sample_order(
                    "user1",
                    "order001",
                    "2026-01-08",
                    "pending",
                    100,
                ))
                .unwrap();

            table
                .put_item(sample_order(
                    "user1",
                    "order001",
                    "2026-01-20",
                    "shipped",
                    150,
                ))
                .unwrap();

            let result = table
                .query_gsi("orders-by-date", KeyCondition::pk("2026-01-08"))
                .unwrap();
            assert_eq!(result.count, 0);

            let result = table
                .query_gsi("orders-by-date", KeyCondition::pk("2026-01-20"))
                .unwrap();
            assert_eq!(result.count, 1);
        }

        #[test]
        fn sparse_only_indexes_items_with_attributes() {
            let mut table = composite_table_with_indexes();

            table
                .put_item(sample_order(
                    "user1",
                    "order001",
                    "2026-01-08",
                    "pending",
                    100,
                ))
                .unwrap();

            // item without order_date: should not be in GSI
            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user1")
                        .with_s("order_id", "order002")
                        .with_s("status", "pending")
                        .with_n("amount", 200),
                )
                .unwrap();

            let result = table
                .query_gsi("orders-by-date", KeyCondition::pk("2026-01-08"))
                .unwrap();
            assert_eq!(result.count, 1);

            // should exist in the table
            assert_eq!(table.len(), 2);
        }

        mod gsi {
            use super::*;

            #[test]
            fn automatically_indexed_on_put() {
                let mut table = composite_table_with_indexes();

                table
                    .put_item(sample_order(
                        "user1",
                        "order001",
                        "2026-01-08",
                        "pending",
                        100,
                    ))
                    .unwrap();
                table
                    .put_item(sample_order(
                        "user1",
                        "order002",
                        "2026-01-08",
                        "shipped",
                        200,
                    ))
                    .unwrap();
                table
                    .put_item(sample_order(
                        "user2",
                        "order003",
                        "2026-01-08",
                        "pending",
                        300,
                    ))
                    .unwrap();

                let result = table
                    .query_gsi("orders-by-date", KeyCondition::pk("2026-01-08"))
                    .unwrap();

                assert_eq!(result.count, 3);
            }

            #[test]
            fn query_with_sort_key_condition() {
                let mut table = composite_table_with_indexes();

                table
                    .put_item(sample_order(
                        "user1",
                        "order001",
                        "2026-01-08",
                        "pending",
                        100,
                    ))
                    .unwrap();
                table
                    .put_item(sample_order(
                        "user2",
                        "order002",
                        "2026-01-08",
                        "shipped",
                        200,
                    ))
                    .unwrap();
                table
                    .put_item(sample_order(
                        "user3",
                        "order003",
                        "2026-01-08",
                        "pending",
                        300,
                    ))
                    .unwrap();

                // query GSI with sort key condition
                let result = table
                    .query_gsi(
                        "orders-by-date",
                        KeyCondition::pk("2026-01-08").sk_begins_with("user1"),
                    )
                    .unwrap();

                assert_eq!(result.count, 1);
            }
        }

        mod lsi {
            use super::*;

            #[test]
            fn automatically_indexed_on_put() {
                let mut table = composite_table_with_indexes();

                table
                    .put_item(sample_order(
                        "user1",
                        "order001",
                        "2026-01-08",
                        "pending",
                        100,
                    ))
                    .unwrap();
                table
                    .put_item(sample_order(
                        "user1",
                        "order002",
                        "2026-01-16",
                        "shipped",
                        200,
                    ))
                    .unwrap();
                table
                    .put_item(sample_order(
                        "user1",
                        "order003",
                        "2026-01-17",
                        "pending",
                        300,
                    ))
                    .unwrap();

                // query LSI: same partition key, different sort key
                let result = table
                    .query_lsi(
                        "orders-by-status",
                        KeyCondition::pk("user1").sk_eq("pending"),
                    )
                    .unwrap();

                assert_eq!(result.count, 2);
            }
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

        #[test]
        fn put_with_condition() {
            let mut table = simple_table();

            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Alice");

            // doesn't exist yet, should succeed
            let result = table.put_item_with_condition(item.clone(), attr("user_id").not_exists());
            assert!(result.is_ok());
            assert_eq!(table.len(), 1);

            // alreadys exists, should fail
            let result = table.put_item_with_condition(item.clone(), attr("user_id").not_exists());
            assert!(result.is_err());
            assert!(result.unwrap_err().is_condition_failed());
            assert_eq!(table.len(), 1);

            // initial put is preserved
            let key = PrimaryKey::simple("user123");
            let item = table.get_item(&key).unwrap().unwrap();
            assert_eq!(item.get("name"), Some(&AttributeValue::S("Alice".into())))
        }

        #[test]
        fn put_with_optimistic_locking() {
            let mut table = simple_table();

            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user123")
                        .with_n("version", 1),
                )
                .unwrap();

            let item = Item::new()
                .with_s("user_id", "user123")
                .with_n("version", 2)
                .with_s("name", "Alice");

            let result = table.put_item_with_condition(item.clone(), attr("version").eq(1i32));
            assert!(result.is_ok());

            let key = PrimaryKey::simple("user123");
            let stored = table.get_item(&key).unwrap().unwrap();
            assert_eq!(stored.get("version"), Some(&AttributeValue::N("2".into())));
        }

        #[test]
        fn delete_with_condition() {
            let mut table = simple_table();

            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user123")
                        .with_s("status", "inactive"),
                )
                .unwrap();

            let key = PrimaryKey::simple("user123");

            // wrong condition, should fail
            let result = table.delete_item_with_condition(&key, attr("status").eq("active"));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_condition_failed());
            assert_eq!(table.len(), 1);

            // item exists, should succeed
            let result = table.delete_item_with_condition(&key, attr("status").eq("inactive"));
            assert!(result.is_ok());
            assert!(table.is_empty());
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

    mod query {
        use super::*;

        #[test]
        fn with_filter() {
            let mut table = composite_table();

            for i in 1..=5 {
                let status = if i % 2 == 0 { "shipped" } else { "pending" };
                table
                    .put_item(
                        Item::new()
                            .with_s("user_id", "user1")
                            .with_s("order_id", format!("order#{:03}", i))
                            .with_s("status", status)
                            .with_n("amount", i * 100),
                    )
                    .unwrap();
            }

            let result = table
                .query_with_filter(KeyCondition::pk("user1"), attr("status").eq("pending"))
                .unwrap();
            assert_eq!(result.count, 3);

            let result = table
                .query_with_filter(
                    KeyCondition::pk("user1"),
                    attr("status").eq("pending").and(attr("amount").ge(300i32)),
                )
                .unwrap();
            assert_eq!(result.count, 2);
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

        #[test]
        fn with_filter() {
            let mut table = simple_table();
            let total_items = 10;

            for i in 0..total_items {
                let status = if i % 2 == 0 { "active" } else { "inactive" };
                table
                    .put_item(
                        Item::new()
                            .with_s("user_id", format!("user{}", i))
                            .with_s("status", status),
                    )
                    .unwrap();
            }

            let items = table.scan_with_filter(attr("status").eq("active")).unwrap();
            assert_eq!(items.len(), 5);
        }
    }
}
