use std::collections::BTreeMap;

use super::request::{
    DeleteRequest, GetRequest, PutRequest, QueryRequest, ScanRequest, UpdateRequest,
};
use crate::batch::{
    BatchExecutor, BatchGetRequest, BatchGetResult, BatchWriteItem, BatchWriteRequest,
    BatchWriteResult,
};
use crate::condition::{Condition, evaluate};
use crate::error::{TableError, TableResult, TransactionCancelReason};
use crate::index::{GlobalSecondaryIndex, GsiBuilder, LocalSecondaryIndex, LsiBuilder};
use crate::query::{KeyCondition, QueryExecutor, QueryOptions, QueryResult};
use crate::storage::{MemoryStorage, Storage};
use crate::transaction::{
    TransactGetRequest, TransactGetResult, TransactWriteItem, TransactWriteRequest,
    TransactionExecutor, TransactionFailureReason,
};
use crate::types::{
    AttributeValue, Item, KeySchema, KeyValidationError, PrimaryKey, ReturnValue, WriteResult,
    decode, encode,
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
            gsi.clear();
        }
        for lsi in self.lsis.values_mut() {
            lsi.clear();
        }
    }

    // index management
    pub fn add_gsi(&mut self, builder: GsiBuilder) {
        let gsi = builder.build(self.schema.clone());
        let name = gsi.name().to_string();

        let mut gsi = gsi;
        for item in self.scan_all().unwrap_or_default() {
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
        for item in self.scan_all().unwrap_or_default() {
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

    // public API operations
    pub fn put(&mut self, request: impl Into<PutRequest>) -> TableResult<WriteResult> {
        let request = request.into();

        if request.if_not_exists {
            return self.put_if_not_exists_internal(request.item, request.return_value);
        }

        self.put_internal(request.item, request.condition, request.return_value)
    }

    pub fn get(&self, request: impl Into<GetRequest>) -> TableResult<Option<Item>> {
        let request = request.into();
        let storage_key = request.key.to_storage_key();
        let item = self.get_item_by_storage_key(&storage_key)?;

        // TODO: apply projection if it exists
        // if let (Some(item), Some(projection)) = (&item, &request.projection) {
        //     return Ok(Some(project_item(item, projection)));
        // }

        Ok(item)
    }

    pub fn update(&mut self, request: UpdateRequest) -> TableResult<WriteResult> {
        self.update_internal(
            &request.key,
            request.expression,
            request.condition,
            request.return_value,
        )
    }

    pub fn delete(&mut self, request: impl Into<DeleteRequest>) -> TableResult<WriteResult> {
        let request = request.into();
        self.delete_internal(&request.key, request.condition, request.return_value)
    }

    pub fn query(&mut self, request: impl Into<QueryRequest>) -> TableResult<QueryResult> {
        let request = request.into();
        self.query_internal(request.key_condition, request.filter, request.options)
    }

    pub fn query_gsi(
        &self,
        index_name: &str,
        request: impl Into<QueryRequest>,
    ) -> TableResult<QueryResult> {
        let request = request.into();
        let gsi = self
            .gsis
            .get(index_name)
            .ok_or_else(|| TableError::index_not_found(index_name))?;

        let mut result = gsi.query_with_options(request.key_condition, request.options)?;

        if let Some(filter) = request.filter {
            let filtered: Vec<Item> = result
                .items
                .into_iter()
                .filter(|item| evaluate(&filter, item).unwrap_or(false))
                .collect();
            result.count = filtered.len();
            result.items = filtered;
        }
        Ok(result)
    }

    pub fn query_lsi(
        &self,
        index_name: &str,
        request: impl Into<QueryRequest>,
    ) -> TableResult<QueryResult> {
        let request = request.into();
        let lsi = self
            .lsis
            .get(index_name)
            .ok_or_else(|| TableError::index_not_found(index_name))?;

        let mut result = lsi.query_with_options(request.key_condition, request.options)?;

        if let Some(filter) = request.filter {
            let filtered: Vec<Item> = result
                .items
                .into_iter()
                .filter(|item| evaluate(&filter, item).unwrap_or(false))
                .collect();
            result.count = filtered.len();
            result.items = filtered;
        }
        Ok(result)
    }

    pub fn scan(&self, request: ScanRequest) -> TableResult<Vec<Item>> {
        let mut items = Vec::new();
        let limit = request.limit.unwrap_or(usize::MAX);

        for (_, value) in self.storage.iter() {
            if items.len() >= limit {
                break;
            }

            let item = self.decode_item(value)?;
            if let Some(ref filter) = request.filter {
                if !evaluate(filter, &item).unwrap_or(false) {
                    continue;
                }
            }

            items.push(item);
        }

        Ok(items)
    }

    // convenience methods
    pub fn put_item(&mut self, item: Item) -> TableResult<()> {
        self.put(PutRequest::new(item))?;
        Ok(())
    }

    pub fn get_item(&self, key: &PrimaryKey) -> TableResult<Option<Item>> {
        self.get(GetRequest::new(key.clone()))
    }

    pub fn delete_item(&mut self, key: &PrimaryKey) -> TableResult<Option<Item>> {
        let result = self.delete(DeleteRequest::new(key.clone()).return_old())?;
        Ok(result.attributes)
    }

    pub fn update_item(
        &mut self,
        key: &PrimaryKey,
        expression: UpdateExpression,
    ) -> TableResult<Option<Item>> {
        let result = self.update(UpdateRequest::new(key.clone(), expression))?;
        Ok(result.attributes)
    }

    pub fn scan_all(&self) -> TableResult<Vec<Item>> {
        self.scan(ScanRequest::new())
    }

    pub fn transact_write(&mut self, request: impl Into<TransactWriteRequest>) -> TableResult<()> {
        let request = request.into();
        if request.is_empty() {
            return Ok(());
        }

        // validate all operations
        let executor = TransactionExecutor::new();
        let validation =
            executor.validate_write(&request.items, &self.schema, |key| self.get_item(key));

        if let Err(failure) = validation {
            return Err(self.convert_failure_to_error(failure));
        }

        // apply all operations
        for item in request.items {
            self.apply_transact_write_item(item)?;
        }

        Ok(())
    }

    pub fn transact_get(
        &self,
        request: impl Into<TransactGetRequest>,
    ) -> TableResult<TransactGetResult> {
        let request = request.into();
        let executor = TransactionExecutor::new();
        executor.execute_get(&request.items, |key| self.get_item(key))
    }

    pub fn batch_write(
        &mut self,
        request: impl Into<BatchWriteRequest>,
    ) -> TableResult<BatchWriteResult> {
        let request: BatchWriteRequest = request.into();

        if request.is_empty() {
            return Ok(BatchWriteResult::new());
        }

        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        for item in request.items {
            match item {
                BatchWriteItem::Put { item } => puts.push(item),
                BatchWriteItem::Delete { key } => deletes.push(key),
            }
        }

        let schema = self.schema.clone();
        let executor = BatchExecutor::new();
        let mut write_result = executor.execute_put(puts, &schema, |item| self.put_item(item))?;
        let delete_result =
            executor.execute_delete(deletes, |key| self.delete_item(key).map(|_| ()))?;

        // merge results
        write_result.processed_count += delete_result.processed_count;
        write_result
            .unprocessed_items
            .extend(delete_result.unprocessed_items);

        Ok(write_result)
    }

    pub fn batch_get(&self, request: impl Into<BatchGetRequest>) -> TableResult<BatchGetResult> {
        let request: BatchGetRequest = request.into();

        if request.is_empty() {
            return Ok(BatchGetResult::new());
        }

        let executor = BatchExecutor::new();
        executor.execute_get(request.keys, |key| self.get_item(key))
    }

    // batch convenience methods
    pub fn put_items(&mut self, items: Vec<Item>) -> TableResult<BatchWriteResult> {
        self.batch_write(items)
    }
    pub fn delete_items(&mut self, keys: Vec<PrimaryKey>) -> TableResult<BatchWriteResult> {
        let request = BatchWriteRequest::new().delete_many(keys);
        self.batch_write(request)
    }
    pub fn get_items(&mut self, keys: Vec<PrimaryKey>) -> TableResult<BatchGetResult> {
        self.batch_get(keys)
    }

    // internal operations
    fn put_internal(
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

    fn put_if_not_exists_internal(
        &mut self,
        item: Item,
        return_value: ReturnValue,
    ) -> TableResult<WriteResult> {
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

        let attributes = match return_value {
            ReturnValue::None => None,
            ReturnValue::AllOld => None,
            ReturnValue::AllNew => Some(item),
        };

        Ok(WriteResult {
            attributes,
            was_update: false,
        })
    }

    fn delete_internal(
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
            ReturnValue::AllNew => None, // delete has no "new" item
        };

        Ok(WriteResult {
            attributes,
            was_update,
        })
    }

    fn update_internal(
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
        let new_key = new_item
            .extract_key(&self.schema)
            .ok_or_else(|| TableError::UpdateError("update removed key attributes".to_string()))?;

        if &new_key != key {
            return Err(TableError::UpdateError(
                "cannot modify key attributes".to_string(),
            ));
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

    fn query_internal(
        &self,
        key_condition: KeyCondition,
        filter: Option<Condition>,
        options: QueryOptions,
    ) -> TableResult<QueryResult> {
        let executor = QueryExecutor::new(&self.schema);
        executor.validate_condition(&key_condition)?;

        let items = self.iter_with_keys()?;
        let mut result = executor.execute(items.into_iter(), &key_condition, &options)?;

        if let Some(filter) = filter {
            let filtered: Vec<Item> = result
                .items
                .into_iter()
                .filter(|item| evaluate(&filter, item).unwrap_or(false))
                .collect();
            result.count = filtered.len();
            result.items = filtered;
        }

        Ok(result)
    }

    fn apply_transact_write_item(&mut self, item: TransactWriteItem) -> TableResult<()> {
        match item {
            TransactWriteItem::Put { item, .. } => {
                self.put_item(item)?;
            }
            TransactWriteItem::Update {
                key, expression, ..
            } => {
                self.update_item(&key, expression)?;
            }
            TransactWriteItem::Delete { key, .. } => {
                self.delete_item(&key)?;
            }
            TransactWriteItem::ConditionCheck { .. } => {
                // condition is already checked during validation
            }
        }
        Ok(())
    }

    fn convert_failure_to_error(&self, failure: TransactionFailureReason) -> TableError {
        let reason = match failure {
            TransactionFailureReason::ConditionCheckFailed { index } => {
                TransactionCancelReason::ConditionCheckFailed { index }
            }
            TransactionFailureReason::ItemNotFound { index } => {
                TransactionCancelReason::ItemNotFound { index }
            }
            TransactionFailureReason::KeyModification { index } => {
                TransactionCancelReason::ValidationError {
                    index,
                    message: "cannot modify key attributes".to_string(),
                }
            }
            TransactionFailureReason::DuplicateItem { index } => {
                TransactionCancelReason::DuplicateItem { index }
            }
            TransactionFailureReason::InvalidKey { index, message } => {
                TransactionCancelReason::ValidationError { index, message }
            }
        };

        TableError::transaction_canceled(vec![reason])
    }

    // non-operation utilities
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

            let result = table.put(PutRequest::new(item1).return_old()).unwrap();
            assert!(result.attributes.is_none());
            assert!(!result.was_update);

            let result = table.put(PutRequest::new(item2).return_old()).unwrap();
            assert!(result.attributes.is_some());
            assert!(result.was_update);
            assert_eq!(
                result.attributes.unwrap().get("name"),
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
            let result = table
                .update_item(&key, update_expr.set("name", "Bob").add("count", 5i32))
                .unwrap()
                .unwrap();

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

            let result = table.put(PutRequest::new(item)).unwrap();
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
            let result = table.put(PutRequest::new(item1).return_old()).unwrap();
            assert!(result.attributes.is_none());
            assert!(!result.was_update);

            // on overwrite
            let item2 = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Bob");
            let result = table.put(PutRequest::new(item2).return_old()).unwrap();
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

            let result = table.put(PutRequest::new(item).return_new()).unwrap();
            assert!(!result.was_update);

            let new = result.attributes.unwrap();
            assert_eq!(new.get("name"), Some(&AttributeValue::S("Alice".into())));
        }

        #[test]
        fn delete_return_old() {
            let mut table = simple_table();
            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user123")
                        .with_s("name", "Alice"),
                )
                .unwrap();

            let result = table
                .delete(DeleteRequest::new(PrimaryKey::simple("user123")).return_old())
                .unwrap();

            assert!(result.was_update);
            assert_eq!(
                result.attributes.unwrap().get("name"),
                Some(&AttributeValue::S("Alice".into()))
            );
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
            assert!(table.put(PutRequest::new(item1).if_not_exists()).is_ok());
            assert_eq!(table.len(), 1);

            // alreadys exists, should fail
            assert!(table.put(PutRequest::new(item2).if_not_exists()).is_err());
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
            let result =
                table.put(PutRequest::new(item.clone()).condition(attr("user_id").not_exists()));
            assert!(result.is_ok());
            assert_eq!(table.len(), 1);

            // alreadys exists, should fail
            let result =
                table.put(PutRequest::new(item.clone()).condition(attr("user_id").not_exists()));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_condition_failed());
            assert_eq!(table.len(), 1);

            // initial put is preserved
            let key = PrimaryKey::simple("user123");
            let item = table.get_item(&key).unwrap().unwrap();
            assert_eq!(item.get("name"), Some(&AttributeValue::S("Alice".into())))
        }

        #[test]
        fn delete_with_condition() {
            let mut table = simple_table();

            let _ = table.put(PutRequest::new(
                Item::new()
                    .with_s("user_id", "user123")
                    .with_s("status", "inactive"),
            ));

            let key = PrimaryKey::simple("user123");

            // wrong condition, should fail
            let result = table
                .delete(DeleteRequest::new(key.clone()).condition(attr("status").eq("active")));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_condition_failed());
            assert_eq!(table.len(), 1);

            // item exists, should succeed
            let result = table
                .delete(DeleteRequest::new(key.clone()).condition(attr("status").eq("inactive")));
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

    mod query_and_scan {
        use super::*;
        use crate::query::KeyCondition;

        #[test]
        fn query_with_request_builder() {
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

            // query
            let result = table
                .query(QueryRequest::new(KeyCondition::pk("user1")))
                .unwrap();
            assert_eq!(result.count, 5);

            // with filter
            let result = table
                .query(
                    QueryRequest::new(KeyCondition::pk("user1"))
                        .filter(attr("status").eq("pending")),
                )
                .unwrap();
            assert_eq!(result.count, 3);

            // with limit and reverse
            let result = table
                .query(
                    QueryRequest::new(KeyCondition::pk("user1"))
                        .limit(2)
                        .reverse(),
                )
                .unwrap();
            assert_eq!(result.count, 2);
            assert_eq!(
                result.items[0].get("order_id").unwrap().as_s(),
                Some("order#005")
            );
        }

        #[test]
        fn scan_with_request_builder() {
            let mut table = simple_table();

            for i in 0..10 {
                let status = if i % 2 == 0 { "active" } else { "inactive" };
                table
                    .put_item(
                        Item::new()
                            .with_s("user_id", format!("user{}", i))
                            .with_s("status", status),
                    )
                    .unwrap();
            }

            // scan all
            let items = table.scan(ScanRequest::new()).unwrap();
            assert_eq!(items.len(), 10);

            // with filter
            let items = table
                .scan(ScanRequest::new().filter(attr("status").eq("active")))
                .unwrap();
            assert_eq!(items.len(), 5);

            // with limit
            let items = table.scan(ScanRequest::new().limit(3)).unwrap();
            assert_eq!(items.len(), 3);
        }
    }

    mod transactions {
        use super::*;
        use crate::transaction::TransactGetItem;

        #[test]
        fn empty() {
            let mut table = simple_table();
            let result = table.transact_write(TransactWriteRequest::new());
            assert!(result.is_ok());
        }

        #[test]
        fn write() {
            let mut table = simple_table();

            // single write
            let result = table.transact_write(
                TransactWriteRequest::new().put(
                    Item::new()
                        .with_s("user_id", "user1")
                        .with_s("name", "Alice"),
                ),
            );
            assert!(result.is_ok());
            assert_eq!(table.len(), 1);

            // multiple writes
            let result = table.transact_write(
                TransactWriteRequest::new()
                    .put(Item::new().with_s("user_id", "user2").with_s("name", "Bob"))
                    .put(
                        Item::new()
                            .with_s("user_id", "user3")
                            .with_s("name", "John"),
                    ),
            );
            assert!(result.is_ok());
            assert_eq!(table.len(), 3);
        }

        #[test]
        fn get() {
            let mut table = simple_table();
            table
                .put_item(Item::new().with_s("user_id", "user1").with_n("value", 1))
                .unwrap();
            table
                .put_item(Item::new().with_s("user_id", "user2").with_n("value", 2))
                .unwrap();

            let result = table
                .transact_get(
                    TransactGetRequest::new()
                        .get(PrimaryKey::simple("user1"))
                        .get(PrimaryKey::simple("user2")),
                )
                .unwrap();

            assert_eq!(result.len(), 2);
            assert_eq!(result.found_count(), 2);
            assert_eq!(
                result.get(0).unwrap().get("value"),
                Some(&AttributeValue::N("1".into()))
            );

            // missing item should not fail and return accurate results
            let result = table
                .transact_get(
                    TransactGetRequest::new()
                        .get(PrimaryKey::simple("user1"))
                        .get(PrimaryKey::simple("missing")),
                )
                .unwrap();

            assert_eq!(result.len(), 2);
            assert_eq!(result.found_count(), 1);
            assert!(result.get(0).is_some());
            assert!(result.get(1).is_none());
        }

        #[test]
        fn mixed_operations() {
            let mut table = simple_table();
            table
                .put_item(Item::new().with_s("user_id", "user1").with_n("count", 100))
                .unwrap();

            // write + update
            let result = table.transact_write(
                TransactWriteRequest::new()
                    .put(Item::new().with_s("user_id", "user2").with_n("count", 200))
                    .update(
                        PrimaryKey::simple("user1"),
                        UpdateExpression::new().add("count", 42i32),
                    ),
            );
            assert!(result.is_ok());
            assert_eq!(table.len(), 2);

            // get
            let item = table
                .get_item(&PrimaryKey::simple("user1"))
                .unwrap()
                .unwrap();
            assert_eq!(item.get("count"), Some(&AttributeValue::N("142".into())));
        }

        #[test]
        fn reject_duplicate_keys() {
            let mut table = simple_table();

            let result = table.transact_write(
                TransactWriteRequest::new()
                    .put(Item::new().with_s("user_id", "foo"))
                    .put(Item::new().with_s("user_id", "foo")),
            );
            assert!(result.is_err());
            assert!(result.unwrap_err().is_transaction_canceled());
            assert!(table.is_empty());
        }

        #[test]
        fn condition_check_failure_rolls_back() {
            let mut table = simple_table();
            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user1")
                        .with_s("status", "inactive"),
                )
                .unwrap();

            let result = table.transact_write(
                TransactWriteRequest::new()
                    .put(Item::new().with_s("user_id", "user2"))
                    .condition_check(PrimaryKey::simple("user1"), attr("status").eq("active")),
            );
            assert!(result.is_err());
            assert!(result.unwrap_err().is_transaction_canceled());

            // transaction failed, user2 should not be created
            table
                .get_item(&PrimaryKey::simple("user1"))
                .unwrap()
                .unwrap();
            assert_eq!(table.len(), 1);
            assert!(
                table
                    .get_item(&PrimaryKey::simple("user2"))
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn from_vec() {
            let mut table = simple_table();

            let items = vec![
                TransactWriteItem::put(Item::new().with_s("user_id", "user1")),
                TransactWriteItem::put(Item::new().with_s("user_id", "user2")),
            ];

            let result = table.transact_write(items);
            assert!(result.is_ok());
            assert_eq!(table.len(), 2);

            let items = vec![
                TransactGetItem::get(PrimaryKey::simple("user1")),
                TransactGetItem::get(PrimaryKey::simple("user2")),
            ];
            let result = table.transact_get(items).unwrap();
            assert_eq!(result.found_count(), 2);
        }
    }

    mod batch {
        use super::*;

        #[test]
        fn empty_batch() {
            let mut table = simple_table();

            // write
            let result = table.batch_write(BatchWriteRequest::new()).unwrap();
            assert!(result.is_complete());
            assert_eq!(result.processed_count, 0);

            // read
            let result = table.batch_get(BatchGetRequest::new()).unwrap();
            assert!(result.is_complete());
            assert_eq!(result.found_count(), 0);
        }

        #[test]
        fn multiple_writes() {
            let mut table = simple_table();

            let result = table
                .batch_write(
                    BatchWriteRequest::new()
                        .put(Item::new().with_s("user_id", "user0"))
                        .put(Item::new().with_s("user_id", "user1"))
                        .put(Item::new().with_s("user_id", "user2"))
                        .delete(PrimaryKey::simple("user2")),
                )
                .unwrap();
            assert!(result.is_complete());
            assert_eq!(result.processed_count, 4);
            assert_eq!(table.len(), 2);
        }

        #[test]
        fn from_vec_items() {
            let mut table = simple_table();

            // put
            let items = vec![
                Item::new().with_s("user_id", "user0"),
                Item::new().with_s("user_id", "user1"),
            ];
            let result = table.put_items(items).unwrap();
            assert!(result.is_complete());
            assert_eq!(result.processed_count, 2);
            assert_eq!(table.len(), 2);

            // get
            let keys = vec![PrimaryKey::simple("user0"), PrimaryKey::simple("user1")];
            let result = table.get_items(keys.clone()).unwrap();
            assert!(result.is_complete());
            assert_eq!(result.found_count(), 2);

            // delete
            let result = table.delete_items(keys.clone()).unwrap();
            assert!(result.is_complete());
            assert!(table.is_empty());
            assert_eq!(result.processed_count, 2);
        }

        #[test]
        fn updates_indexes() {
            let mut table = TableBuilder::new(
                "test",
                KeySchema::composite("pk", KeyType::S, "sk", KeyType::S),
            )
            .with_gsi(GsiBuilder::new(
                "by-status",
                KeySchema::simple("status", KeyType::S),
            ))
            .build();

            table
                .batch_write(
                    BatchWriteRequest::new()
                        .put(
                            Item::new()
                                .with_s("pk", "user1")
                                .with_s("sk", "order1")
                                .with_s("status", "pending"),
                        )
                        .put(
                            Item::new()
                                .with_s("pk", "user1")
                                .with_s("sk", "order2")
                                .with_s("status", "pending"),
                        ),
                )
                .unwrap();

            let result = table
                .query_gsi("by-status", KeyCondition::pk("pending"))
                .unwrap();
            assert_eq!(result.count, 2);
        }
    }
}
