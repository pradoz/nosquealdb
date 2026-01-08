use std::collections::HashMap;

use crate::error::TableResult;
use crate::query::{KeyCondition, QueryExecutor, QueryOptions, QueryResult};
use crate::types::{Item, KeyAttribute, KeySchema, KeyType, KeyValue, PrimaryKey};
use crate::utils::base64_encode;

use super::projection::Projection;

/// Local Secondary Index - same partition key as table, different sort key.
#[derive(Debug)]
pub struct LocalSecondaryIndex {
    name: String,
    sort_key: KeyAttribute,
    projection: Projection,
    table_schema: KeySchema,
    data: HashMap<String, Item>,
}

impl LocalSecondaryIndex {
    pub fn new(
        name: impl Into<String>,
        sort_key: KeyAttribute,
        projection: Projection,
        table_schema: KeySchema,
    ) -> Self {
        Self {
            name: name.into(),
            sort_key,
            projection,
            table_schema,
            data: HashMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn sort_key_name(&self) -> &str {
        &self.sort_key.name
    }
    pub fn sort_key_type(&self) -> KeyType {
        self.sort_key.key_type
    }
    pub fn projection(&self) -> &Projection {
        &self.projection
    }
    pub fn schema(&self) -> KeySchema {
        KeySchema {
            partition_key: self.table_schema.partition_key.clone(),
            sort_key: Some(self.sort_key.clone()),
        }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn put(&mut self, table_key: &PrimaryKey, item: &Item) -> Option<Item> {
        let old = self.remove_by_table_key(table_key);

        if let Some(lsi_sk) = self.extract_lsi_sort_key(item) {
            let storage_key = self.make_storage_key(&table_key.pk, &lsi_sk, table_key);
            let projected = self
                .projection
                .project_item(item, &self.table_schema, &self.schema());
            self.data.insert(storage_key, projected);
        }

        old
    }

    pub fn delete(&mut self, table_key: &PrimaryKey) -> Option<Item> {
        self.remove_by_table_key(table_key)
    }

    pub fn query(&self, condition: KeyCondition) -> TableResult<QueryResult> {
        self.query_with_options(condition, QueryOptions::new())
    }

    pub fn query_with_options(
        &self,
        condition: KeyCondition,
        options: QueryOptions,
    ) -> TableResult<QueryResult> {
        let schema = self.schema();
        let executor = QueryExecutor::new(&schema);
        executor.validate_condition(&condition)?;

        let items = self.data.values().filter_map(|item| {
            let pk = self.extract_pk_from_item(item)?;
            let sk = self.extract_lsi_sort_key(item)?;
            Some((PrimaryKey { pk, sk: Some(sk) }, item.clone()))
        });

        executor.execute(items, &condition, &options)
    }

    fn extract_pk_from_item(&self, item: &Item) -> Option<KeyValue> {
        let attr = item.get(self.table_schema.pk_name())?;
        KeyValue::from_attribute_with_type(attr, self.table_schema.partition_key.key_type)
    }

    fn extract_lsi_sort_key(&self, item: &Item) -> Option<KeyValue> {
        let attr = item.get(&self.sort_key.name)?;
        KeyValue::from_attribute_with_type(attr, self.sort_key.key_type)
    }

    fn make_storage_key(&self, pk: &KeyValue, lsi_sk: &KeyValue, table_key: &PrimaryKey) -> String {
        format!(
            "{}#{}#{}",
            pk_to_string(pk),
            pk_to_string(lsi_sk),
            table_key.to_storage_key()
        )
    }

    fn remove_by_table_key(&mut self, table_key: &PrimaryKey) -> Option<Item> {
        let suffix = table_key.to_storage_key();
        let to_remove = self.data.keys().find(|k| k.ends_with(&suffix)).cloned();
        to_remove.and_then(|k| self.data.remove(&k))
    }
}

fn pk_to_string(kv: &KeyValue) -> String {
    match kv {
        KeyValue::S(s) => format!("S:{}", s),
        KeyValue::N(n) => format!("N:{}", n),
        KeyValue::B(b) => format!("B:{}", base64_encode(b)),
    }
}

pub struct LsiBuilder {
    name: String,
    sort_key: KeyAttribute,
    projection: Projection,
}

impl LsiBuilder {
    pub fn new(
        name: impl Into<String>,
        sort_key_name: impl Into<String>,
        sort_key_type: KeyType,
    ) -> Self {
        Self {
            name: name.into(),
            sort_key: KeyAttribute::new(sort_key_name, sort_key_type),
            projection: Projection::All,
        }
    }

    pub fn projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }

    pub fn keys_only(mut self) -> Self {
        self.projection = Projection::KeysOnly;
        self
    }

    pub fn include<I, S>(mut self, attrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Projection::include(attrs);
        self
    }

    pub fn build(self, table_schema: KeySchema) -> LocalSecondaryIndex {
        LocalSecondaryIndex::new(self.name, self.sort_key, self.projection, table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::KeyCondition;

    fn table_schema() -> KeySchema {
        KeySchema::composite("user_id", KeyType::S, "order_id", KeyType::S)
    }
    fn create_lsi() -> LocalSecondaryIndex {
        LocalSecondaryIndex::new(
            "orders-by-date",
            KeyAttribute::new("order_date", KeyType::S),
            Projection::All,
            table_schema(),
        )
    }
    fn sample_order(user: &str, order: &str, date: &str, amount: i32) -> Item {
        Item::new()
            .with_s("user_id", user)
            .with_s("order_id", order)
            .with_s("order_date", date)
            .with_n("amount", amount)
    }

    #[test]
    fn put_indexes_item() {
        let mut lsi = create_lsi();
        let table_key = PrimaryKey::composite("user1", "order001");
        let item = sample_order("user1", "order001", "2026-01-08", 100);

        lsi.put(&table_key, &item);
        assert_eq!(lsi.len(), 1);
    }

    #[test]
    fn sparse_index_skips_items_without_sort_key() {
        let mut lsi = create_lsi();

        // item without order_date
        let item = Item::new()
            .with_s("user_id", "user1")
            .with_s("order_id", "order001")
            .with_n("amount", 100);
        let table_key = PrimaryKey::composite("user1", "order001");

        lsi.put(&table_key, &item);

        assert!(lsi.is_empty());
    }

    #[test]
    fn query_same_partition_different_sort() {
        let mut lsi = create_lsi();

        lsi.put(
            &PrimaryKey::composite("user1", "order001"),
            &sample_order("user1", "order001", "2026-01-09", 100),
        );
        lsi.put(
            &PrimaryKey::composite("user1", "order002"),
            &sample_order("user1", "order002", "2026-01-10", 200),
        );
        lsi.put(
            &PrimaryKey::composite("user1", "order003"),
            &sample_order("user1", "order003", "2026-01-20", 300),
        );
        lsi.put(
            &PrimaryKey::composite("user2", "order004"),
            &sample_order("user2", "order004", "2026-01-08", 400),
        );

        let result = lsi.query(KeyCondition::pk("user1")).unwrap();
        assert_eq!(result.count, 3);

        assert_eq!(
            result.items[0].get("order_date").unwrap().as_s(),
            Some("2026-01-09")
        );
        assert_eq!(
            result.items[2].get("order_date").unwrap().as_s(),
            Some("2026-01-20")
        );
    }

    #[test]
    fn query_with_sort_key_condition() {
        let mut lsi = create_lsi();

        lsi.put(
            &PrimaryKey::composite("user1", "order001"),
            &sample_order("user1", "order002", "2026-01-08", 200),
        );
        lsi.put(
            &PrimaryKey::composite("user1", "order002"),
            &sample_order("user1", "order001", "2026-01-10", 100),
        );
        lsi.put(
            &PrimaryKey::composite("user1", "order003"),
            &sample_order("user1", "order003", "2026-01-20", 300),
        );

        let result = lsi
            .query(KeyCondition::pk("user1").sk_gt("2026-01-10"))
            .unwrap();

        assert_eq!(result.count, 1);
    }

    #[test]
    fn delete_removes_from_index() {
        let mut lsi = create_lsi();

        let table_key = PrimaryKey::composite("user1", "order001");
        lsi.put(
            &table_key,
            &sample_order("user1", "order001", "2026-01-08", 100),
        );
        assert_eq!(lsi.len(), 1);

        lsi.delete(&table_key);
        assert!(lsi.is_empty());
    }

    #[test]
    fn update_replaces_index_entry() {
        let mut lsi = create_lsi();

        let table_key = PrimaryKey::composite("user1", "order001");

        lsi.put(
            &table_key,
            &sample_order("user1", "order001", "2026-01-08", 100),
        );

        // update with different date
        lsi.put(
            &table_key,
            &sample_order("user1", "order001", "2026-01-20", 150),
        );
        assert_eq!(lsi.len(), 1);

        let result = lsi.query(KeyCondition::pk("user1")).unwrap();
        assert_eq!(
            result.items[0].get("order_date").unwrap().as_s(),
            Some("2026-01-20")
        );
    }
}
