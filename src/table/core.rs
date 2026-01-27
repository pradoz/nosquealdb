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
        for (pk, item) in self.iter_with_keys() {
            gsi.put(pk, &item);
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
        for (pk, item) in self.iter_with_keys() {
            lsi.put(&pk, &item);
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

        for (_, item) in self.iter_with_keys() {
            if items.len() >= limit {
                break;
            }

            if let Some(ref filter) = request.filter
                && !evaluate(filter, &item).unwrap_or(false)
            {
                continue;
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
        item.validate_key(&self.schema)?;

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
        item.validate_key(&self.schema)?;

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

        if let Some(cond) = condition
            && !evaluate(&cond, &old_item)?
        {
            return Err(TableError::ConditionFailed);
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

        let items = self.iter_with_keys();
        let mut result = executor.execute(items, &key_condition, &options)?;

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

    fn iter_with_keys(&self) -> impl Iterator<Item = (PrimaryKey, Item)> + '_ {
        self.storage.iter().filter_map(|(_, value)| {
            let item = self.decode_item(value).ok()?;
            let pk = item.extract_key(&self.schema)?;
            Some((pk, item))
        })
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

    mod update_errors {
        use super::*;
        use crate::update::UpdateExpression;

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
            table
                .put_item(
                    Item::new()
                        .with_s("user_id", "user123")
                        .with_s("name", "Alice"),
                )
                .unwrap();

            let key = PrimaryKey::simple("user123");
            let result = table.update_item(&key, UpdateExpression::new().set("user_id", "fail"));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_update_error());
        }
    }

    mod iter_with_keys {
        use super::*;

        #[test]
        fn lazily_consumable() {
            let mut table = simple_table();

            for i in 0..100 {
                table
                    .put_item(Item::new().with_s("user_id", format!("user{}", i)))
                    .unwrap();
            }

            let chunk: Vec<_> = table.iter_with_keys().take(10).collect();
            assert_eq!(chunk.len(), 10);

            let all: Vec<_> = table.iter_with_keys().collect();
            assert_eq!(all.len(), 100);
        }

        #[test]
        fn skips_invalid_items() {
            let table = simple_table();
            let items: Vec<_> = table.iter_with_keys().collect();
            assert!(items.is_empty());
        }
    }
}
