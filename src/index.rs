use crate::error::TableResult;
use crate::query::{KeyCondition, QueryExecutor, QueryOptions, QueryResult};
use crate::types::{Item, KeyAttribute, KeySchema, KeyType, KeyValue, PrimaryKey};

mod projection;
mod storage;

pub use projection::Projection;
pub use storage::IndexStorage;

#[derive(Debug, Clone)]
pub enum IndexKind {
    // independent partition key
    Global { schema: KeySchema },
    // same partition key as table, different sort key
    Local { sort_key: KeyAttribute },
}

impl IndexKind {
    pub fn global(schema: KeySchema) -> Self {
        Self::Global { schema }
    }

    pub fn local(key_name: impl Into<String>, key_type: KeyType) -> Self {
        Self::Local {
            sort_key: KeyAttribute::new(key_name, key_type),
        }
    }
}

#[derive(Debug)]
pub struct SecondaryIndex {
    name: String,
    kind: IndexKind,
    projection: Projection,
    table_schema: KeySchema,
    storage: IndexStorage<(PrimaryKey, Item)>,
}

impl SecondaryIndex {
    pub fn new(
        name: impl Into<String>,
        kind: IndexKind,
        projection: Projection,
        table_schema: KeySchema,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
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
    pub fn projection(&self) -> &Projection {
        &self.projection
    }
    #[inline]
    pub fn is_global(&self) -> bool {
        matches!(self.kind, IndexKind::Global { .. })
    }
    #[inline]
    pub fn is_local(&self) -> bool {
        matches!(self.kind, IndexKind::Local { .. })
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.storage.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }
    pub fn clear(&mut self) {
        self.storage.clear();
    }

    pub fn schema(&self) -> KeySchema {
        match &self.kind {
            IndexKind::Global { schema } => schema.clone(),
            IndexKind::Local { sort_key } => KeySchema {
                partition_key: self.table_schema.partition_key.clone(),
                sort_key: Some(sort_key.clone()),
            },
        }
    }

    pub fn put(&mut self, table_key: PrimaryKey, item: &Item) -> Option<Item> {
        let index_key = match self.extract_key_index(item) {
            Some(k) => k,
            None => {
                // sparse index: item w/o index key attrs should not be indexed
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
            .project_item(item, &self.table_schema, &self.schema());

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
        let schema = self.schema();
        let executor = QueryExecutor::new(&schema);
        executor.validate_condition(&condition)?;

        let items = self.storage.values().filter_map(|(_, item)| {
            self.extract_key_index(item)
                .map(|index_key| (index_key, item.clone()))
        });

        executor.execute(items, condition, options)
    }

    #[inline]
    fn make_storage_key(&self, index_key: &PrimaryKey, table_key: &PrimaryKey) -> String {
        format!(
            "{}|{}",
            index_key.to_storage_key(),
            table_key.to_storage_key()
        )
    }

    fn extract_key_index(&self, item: &Item) -> Option<PrimaryKey> {
        match &self.kind {
            IndexKind::Global { schema } => {
                let pk_attr = item.get(schema.pk_name())?;
                let pk =
                    KeyValue::from_attribute_with_type(pk_attr, schema.partition_key.key_type)?;

                let sk = if let Some(sk_def) = &schema.sort_key {
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
            IndexKind::Local { sort_key } => {
                let pk_attr = item.get(self.table_schema.pk_name())?;
                let pk = KeyValue::from_attribute_with_type(
                    pk_attr,
                    self.table_schema.partition_key.key_type,
                )?;

                let sk_attr = item.get(&sort_key.name)?;
                let sk = KeyValue::from_attribute_with_type(sk_attr, sort_key.key_type)?;

                Some(PrimaryKey { pk, sk: Some(sk) })
            }
        }
    }
}

#[derive(Debug)]
pub struct IndexBuilder {
    name: String,
    kind: IndexKind,
    projection: Projection,
}

impl IndexBuilder {
    pub fn global(name: impl Into<String>, schema: KeySchema) -> Self {
        Self {
            name: name.into(),
            kind: IndexKind::global(schema),
            projection: Projection::All,
        }
    }

    pub fn local(name: impl Into<String>, key_name: impl Into<String>, key_type: KeyType) -> Self {
        Self {
            name: name.into(),
            kind: IndexKind::local(key_name, key_type),
            projection: Projection::All,
        }
    }

    pub fn projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }
    pub fn keys_only(mut self, projection: Projection) -> Self {
        self.projection = Projection::KeysOnly;
        self
    }
    pub fn include(mut self, attrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Projection::include(attrs);
        self
    }

    pub fn build(self, table_schema: KeySchema) -> SecondaryIndex {
        SecondaryIndex::new(self.name, self.kind, self.projection, table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_schema() -> KeySchema {
        KeySchema::composite("user_id", KeyType::S, "order_id", KeyType::S)
    }

    fn make_item(user: &str, order: &str, status: &str) -> Item {
        Item::new()
            .with_s("user_id", user)
            .with_s("order_id", order)
            .with_s("status", status)
            .with_s("date", "2026-01-26")
    }

    mod global {
        use super::*;

        #[test]
        fn put_and_query() {
            let gsi_schema = KeySchema::simple("status", KeyType::S);
            let mut index = IndexBuilder::global("by-status", gsi_schema).build(table_schema());

            let table_key = PrimaryKey::composite("user1", "order1");
            index.put(table_key, &make_item("user1", "order1", "pending"));

            let result = index.query(KeyCondition::pk("pending")).unwrap();
            assert_eq!(result.count, 1);
        }

        #[test]
        fn sparse_index() {
            let gsi_schema = KeySchema::simple("status", KeyType::S);
            let mut index = IndexBuilder::global("by-status", gsi_schema).build(table_schema());

            let table_key = PrimaryKey::composite("user1", "order1");
            index.put(
                table_key,
                // no status attr
                Item::new()
                    .with_s("user_id", "user1")
                    .with_s("order_id", "order1"),
            );

            assert!(index.is_empty());
        }
    }

    mod local {
        use super::*;

        #[test]
        fn put_and_query() {
            let mut index = IndexBuilder::local("by-date", "date", KeyType::S).build(table_schema());

            let table_key = PrimaryKey::composite("user1", "order1");
            index.put(table_key, &make_item("user1", "order1", "pending"));

            let result = index.query(KeyCondition::pk("user1").sk_eq("2026-01-26")).unwrap();
            assert_eq!(result.count, 1);
        }
    }

    mod projection {
        use super::*;

        #[test]
        fn keys_only() {
            let gsi_schema = KeySchema::simple("status", KeyType::S);
            let mut index = IndexBuilder::global("by-status", gsi_schema).build(table_schema());

            let table_key = PrimaryKey::composite("user1", "order1");
            index.put(table_key, &make_item("user1", "order1", "pending"));

            let result = index.query(KeyCondition::pk("pending")).unwrap();
            let item = &result.items[0];

            assert!(item.contains("user_id"));
            assert!(item.contains("order_id"));
            assert!(item.contains("status"));
            assert!(item.contains("date"));
        }
    }
}
