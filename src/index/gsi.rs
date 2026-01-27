use crate::error::TableResult;
use crate::query::{KeyCondition, QueryExecutor, QueryOptions, QueryResult};
use crate::types::{Item, KeySchema, KeyValue, PrimaryKey};

use super::projection::Projection;
use super::storage::IndexStorage;

#[derive(Debug)]
pub struct GlobalSecondaryIndex {
    name: String,
    schema: KeySchema,
    projection: Projection,
    table_schema: KeySchema,
    storage: IndexStorage<(PrimaryKey, Item)>,
}

impl GlobalSecondaryIndex {
    pub fn new(
        name: impl Into<String>,
        schema: KeySchema,
        projection: Projection,
        table_schema: KeySchema,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            projection,
            table_schema,
            storage: IndexStorage::new(),
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn schema(&self) -> &KeySchema {
        &self.schema
    }

    #[inline]
    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn put(&mut self, table_key: PrimaryKey, item: &Item) -> Option<Item> {
        // if an item doesn't have index keys, it's a sparse index - item just isn't indexed
        let index_key = match self.extract_index_key(item) {
            Some(k) => k,
            None => {
                return self
                    .storage
                    .remove_by_table_key(&table_key.to_storage_key())
                    .map(|(_, item)| item);
            }
        };

        let storage_key = self.make_storage_key(&index_key, &table_key);
        let table_storage_key = table_key.to_storage_key();
        let projected = self
            .projection
            .project_item(item, &self.table_schema, &self.schema);

        self.storage
            .put(table_storage_key, storage_key, (table_key, projected))
            .map(|(_, item)| item)
    }

    pub fn delete(&mut self, table_key: &PrimaryKey) -> Option<Item> {
        self.storage
            .remove_by_table_key(&table_key.to_storage_key())
            .map(|(_, item)| item)
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

        let items = self.storage.values().filter_map(|(_, item)| {
            self.extract_index_key(item)
                .map(|index_key| (index_key, item.clone()))
        });

        executor.execute(items, &condition, &options)
    }

    pub fn scan(&self) -> Vec<&Item> {
        self.storage.values().map(|(_, item)| item).collect()
    }

    pub fn clear(&mut self) {
        self.storage.clear();
    }

    fn extract_index_key(&self, item: &Item) -> Option<PrimaryKey> {
        let pk_attr = item.get(self.schema.pk_name())?;
        let pk = KeyValue::from_attribute_with_type(pk_attr, self.schema.partition_key.key_type)?;

        let sk = if let Some(sk_def) = &self.schema.sort_key {
            let sk_attr = item.get(&sk_def.name)?;
            Some(KeyValue::from_attribute_with_type(
                sk_attr,
                sk_def.key_type,
            )?)
        } else {
            None
        };

        Some(PrimaryKey { pk, sk })
    }

    #[inline]
    fn make_storage_key(&self, index_key: &PrimaryKey, table_key: &PrimaryKey) -> String {
        format!(
            "{}|{}",
            index_key.to_storage_key(),
            table_key.to_storage_key()
        )
    }
}

pub struct GsiBuilder {
    name: String,
    schema: KeySchema,
    projection: Projection,
}

impl GsiBuilder {
    pub fn new(name: impl Into<String>, schema: KeySchema) -> Self {
        Self {
            name: name.into(),
            schema,
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

    pub fn build(self, table_schema: KeySchema) -> GlobalSecondaryIndex {
        GlobalSecondaryIndex::new(self.name, self.schema, self.projection, table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::KeyCondition;
    use crate::types::KeyType;

    fn table_schema() -> KeySchema {
        KeySchema::composite("user_id", KeyType::S, "order_id", KeyType::S)
    }

    fn create_gsi() -> GlobalSecondaryIndex {
        // GSI on order_date (PK) and user_id (SK)
        let schema = KeySchema::composite("order_date", KeyType::S, "user_id", KeyType::S);
        GlobalSecondaryIndex::new("orders-by-date", schema, Projection::All, table_schema())
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
        let mut gsi = create_gsi();
        let item = sample_order("user1", "order001", "2026-01-07", 42);

        let table_key = PrimaryKey::composite("user1", "order001");
        gsi.put(table_key, &item);

        assert_eq!(gsi.len(), 1);
    }

    #[test]
    fn sparse_index_skips_items_without_key() {
        let mut gsi = create_gsi();

        // no 'order_date'; should not be indexed
        let item = Item::new()
            .with_s("user_id", "user1")
            .with_s("order_id", "order001")
            .with_n("amount", 100);

        let table_key = PrimaryKey::composite("user1", "order001");
        gsi.put(table_key, &item);

        assert!(gsi.is_empty());
    }

    #[test]
    fn query_by_index_partition_key() {
        let mut gsi = create_gsi();

        gsi.put(
            PrimaryKey::composite("user1", "order001"),
            &sample_order("user1", "order001", "2026-01-07", 100),
        );
        gsi.put(
            PrimaryKey::composite("user1", "order002"),
            &sample_order("user1", "order002", "2026-01-07", 200),
        );
        gsi.put(
            PrimaryKey::composite("user2", "order003"),
            &sample_order("user2", "order003", "2026-01-07", 300),
        );
        gsi.put(
            PrimaryKey::composite("user1", "order004"),
            &sample_order("user1", "order004", "2026-01-31", 400),
        );

        // query orders on 2026-01-07
        let result = gsi.query(KeyCondition::pk("2026-01-07")).unwrap();

        assert_eq!(result.count, 3);
    }

    #[test]
    fn query_by_index_with_sort_key() {
        let mut gsi = create_gsi();

        gsi.put(
            PrimaryKey::composite("user1", "order001"),
            &sample_order("user1", "order001", "2026-01-07", 100),
        );
        gsi.put(
            PrimaryKey::composite("user2", "order002"),
            &sample_order("user2", "order002", "2026-01-07", 200),
        );
        gsi.put(
            PrimaryKey::composite("user3", "order003"),
            &sample_order("user3", "order003", "2026-01-07", 300),
        );

        // query orders on 2026-01-07 for user2
        let result = gsi
            .query(KeyCondition::pk("2026-01-07").sk_eq("user2"))
            .unwrap();

        assert_eq!(result.count, 1);
        assert_eq!(
            result.items[0].get("order_id").unwrap().as_s(),
            Some("order002")
        );
    }

    #[test]
    fn delete_removes_from_index() {
        let mut gsi = create_gsi();

        let table_key = PrimaryKey::composite("user1", "order001");
        gsi.put(
            table_key.clone(),
            &sample_order("user1", "order001", "2026-01-07", 100),
        );

        assert_eq!(gsi.len(), 1);
        gsi.delete(&table_key);
        assert!(gsi.is_empty());
    }

    #[test]
    fn update_replaces_index_entry() {
        let mut gsi = create_gsi();

        let table_key = PrimaryKey::composite("user1", "order001");

        gsi.put(
            table_key.clone(),
            &sample_order("user1", "order001", "2026-01-07", 100),
        );

        // update
        gsi.put(
            table_key,
            &sample_order("user1", "order001", "2026-01-31", 150),
        );

        assert_eq!(gsi.len(), 1);

        // should find under new date
        let result = gsi.query(KeyCondition::pk("2026-01-31")).unwrap();
        assert_eq!(result.count, 1);

        // should not find under old date
        let result = gsi.query(KeyCondition::pk("2026-01-07")).unwrap();
        assert_eq!(result.count, 0);
    }

    #[test]
    fn projection_keys_only() {
        let schema = KeySchema::composite("order_date", KeyType::S, "user_id", KeyType::S);
        let mut gsi = GlobalSecondaryIndex::new(
            "orders-by-date",
            schema,
            Projection::KeysOnly,
            table_schema(),
        );

        let table_key = PrimaryKey::composite("user1", "order001");
        gsi.put(
            table_key,
            &sample_order("user1", "order001", "2026-01-07", 100),
        );

        let result = gsi.query(KeyCondition::pk("2026-01-07")).unwrap();

        let item = &result.items[0];

        // should have table keys and index keys
        assert!(item.contains("user_id"));
        assert!(item.contains("order_id"));
        assert!(item.contains("order_date"));

        // should not have non-key attributes
        assert!(!item.contains("amount"));
    }

    #[test]
    fn clear() {
        let mut gsi = create_gsi();

        for i in 0..10 {
            let table_key = PrimaryKey::composite("user1", format!("order{:03}", i));
            gsi.put(
                table_key,
                &sample_order("user1", &format!("order{:03}", i), "2026-01-22", i * 100),
            );
        }
        assert_eq!(gsi.len(), 10);

        gsi.clear();
        assert_eq!(gsi.len(), 0);
    }
}
