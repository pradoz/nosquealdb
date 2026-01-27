use crate::error::{TableError, TableResult};
use crate::types::{Item, KeySchema, KeyValidationError, KeyValue, PrimaryKey};
use crate::utils::compare_key_values;

use super::condition::KeyCondition;

use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub items: Vec<Item>,
    pub scanned_count: usize, // before filtering
    pub count: usize,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            scanned_count: 0,
            count: 0,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct QueryOptions {
    pub limit: Option<usize>,
    pub scan_forward: bool,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self {
            limit: None,
            scan_forward: true,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn forward(mut self) -> Self {
        self.scan_forward = true;
        self
    }

    pub fn reverse(mut self) -> Self {
        self.scan_forward = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SortableItem {
    sk: Option<KeyValue>,
    storage_key: String,
    sequence: usize,
    item: Item,
}

impl SortableItem {
    #[inline]
    fn new(pk: &PrimaryKey, item: Item, sequence: usize) -> Self {
        Self {
            sk: pk.sk.clone(),
            storage_key: pk.to_storage_key(),
            sequence,
            item,
        }
    }
}

impl PartialOrd for SortableItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.sk, &other.sk) {
            (Some(a), Some(b)) => {
                let key_cmp = compare_key_values(a, b);
                if key_cmp == Ordering::Equal {
                    self.storage_key
                        .cmp(&other.storage_key)
                        .then(self.sequence.cmp(&other.sequence))
                } else {
                    key_cmp
                }
            }
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => self
                .storage_key
                .cmp(&other.storage_key)
                .then(self.sequence.cmp(&other.sequence)),
        }
    }
}

pub struct QueryExecutor<'a> {
    schema: &'a KeySchema,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(schema: &'a KeySchema) -> Self {
        Self { schema }
    }

    /// TODO(performance): use a bounded heap for ascneding queries with limit
    pub fn execute(
        &self,
        items: impl Iterator<Item = (PrimaryKey, Item)>,
        condition: &KeyCondition,
        options: &QueryOptions,
    ) -> TableResult<QueryResult> {
        let mut scanned = 0usize;
        let mut sequence = 0usize;

        let _ = options.limit.unwrap_or(64).min(1024);
        let mut matching: BTreeMap<SortableItem, ()> = BTreeMap::new();

        for (pk, item) in items {
            scanned += 1;

            if pk.pk != condition.partition_key {
                continue;
            }

            if let Some(sk_op) = &condition.sort_key {
                match &pk.sk {
                    Some(sk) if sk_op.matches(sk) => {}
                    _ => continue,
                }
            }

            let sortable = SortableItem::new(&pk, item, sequence);
            sequence += 1;
            matching.insert(sortable, ());
        }

        // extract items in sorted order
        let items = Self::extract_ordered_items(matching, options);
        let count = items.len();

        Ok(QueryResult {
            items,
            scanned_count: scanned,
            count,
        })
    }

    #[inline]
    fn extract_ordered_items(
        matching: BTreeMap<SortableItem, ()>,
        options: &QueryOptions,
    ) -> Vec<Item> {
        let limit = options.limit.unwrap_or(usize::MAX);

        if options.scan_forward {
            matching.into_keys().take(limit).map(|s| s.item).collect()
        } else {
            matching
                .into_keys()
                .rev()
                .take(limit)
                .map(|s| s.item)
                .collect()
        }
    }

    pub fn validate_condition(&self, condition: &KeyCondition) -> TableResult<()> {
        if !self
            .schema
            .partition_key
            .key_type
            .matches(&condition.partition_key)
        {
            return Err(TableError::InvalidKey(KeyValidationError::TypeMismatch {
                name: self.schema.partition_key.name.clone(),
                expected: self.schema.partition_key.key_type.as_str(),
                actual: condition.partition_key.type_name(),
            }));
        }

        if let Some(sk_op) = &condition.sort_key {
            match &self.schema.sort_key {
                Some(sk_def) => {
                    let sk_value = sk_op.value();
                    if !sk_def.key_type.matches(sk_value) {
                        return Err(TableError::InvalidKey(KeyValidationError::TypeMismatch {
                            name: sk_def.name.clone(),
                            expected: sk_def.key_type.as_str(),
                            actual: sk_value.type_name(),
                        }));
                    }
                }
                None => {
                    return Err(TableError::InvalidKey(
                        KeyValidationError::MissingAttribute {
                            name: "sort_key".to_string(),
                        },
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;

    fn make_item(pk: &str, sk: &str, data: &str) -> (PrimaryKey, Item) {
        let key = PrimaryKey::composite(pk, sk);
        let item = Item::new()
            .with_s("pk", pk)
            .with_s("sk", sk)
            .with_s("data", data);
        (key, item)
    }

    fn test_items() -> Vec<(PrimaryKey, Item)> {
        vec![
            make_item("user1", "order#001", "first"),
            make_item("user1", "order#002", "second"),
            make_item("user1", "order#003", "third"),
            make_item("user1", "profile", "user1 profile"),
            make_item("user2", "order#001", "user2 first"),
            make_item("user2", "order#002", "user2 second"),
        ]
    }

    fn schema() -> KeySchema {
        KeySchema::composite("pk", KeyType::S, "sk", KeyType::S)
    }

    #[test]
    fn query_empty_result() {
        let schema = schema();
        let executor = QueryExecutor::new(&schema);
        let result = executor
            .execute(
                test_items().into_iter(),
                &KeyCondition::pk("nonexistent"),
                &QueryOptions::new(),
            )
            .unwrap();
        assert!(result.items.is_empty());
        assert_eq!(result.count, 0);
        assert_eq!(result.scanned_count, 6);
    }

    #[test]
    fn query_with_sort_key_prefix() {
        let schema = schema();
        let executor = QueryExecutor::new(&schema);
        let result = executor
            .execute(
                test_items().into_iter(),
                &KeyCondition::pk("user1").sk_begins_with("order"),
                &QueryOptions::new(),
            )
            .unwrap();
        assert_eq!(result.count, 3);
    }

    #[test]
    fn query_with_sk_between() {
        let schema = schema();
        let executor = QueryExecutor::new(&schema);
        let result = executor
            .execute(
                test_items().into_iter(),
                &KeyCondition::pk("user1").sk_between("order#002", "order#003"),
                &QueryOptions::new(),
            )
            .unwrap();
        assert_eq!(result.count, 2);
    }

    #[test]
    fn query_with_limit_forward() {
        let schema = schema();
        let executor = QueryExecutor::new(&schema);
        let result = executor
            .execute(
                test_items().into_iter(),
                &KeyCondition::pk("user1").sk_begins_with("order"),
                &QueryOptions::new().with_limit(2),
            )
            .unwrap();
        assert_eq!(result.count, 2);
        assert_eq!(result.items[0].get("sk").unwrap().as_s(), Some("order#001"));
        assert_eq!(result.items[1].get("sk").unwrap().as_s(), Some("order#002"));
    }

    #[test]
    fn query_with_limit_reverse() {
        let schema = schema();
        let executor = QueryExecutor::new(&schema);
        let result = executor
            .execute(
                test_items().into_iter(),
                &KeyCondition::pk("user1").sk_begins_with("order"),
                &QueryOptions::new().with_limit(2).reverse(),
            )
            .unwrap();
        assert_eq!(result.count, 2);
        assert_eq!(result.items[0].get("sk").unwrap().as_s(), Some("order#003"));
        assert_eq!(result.items[1].get("sk").unwrap().as_s(), Some("order#002"));
    }

    #[test]
    fn numeric_sort_keys() {
        let schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::N);
        let items: Vec<(PrimaryKey, Item)> = vec![100, -4, -100, 50, 0]
            .into_iter()
            .map(|n| {
                let key = PrimaryKey::composite("user1", KeyValue::N(n.to_string()));
                let item = Item::new()
                    .with_s("pk", "user1")
                    .with_n("sk", n)
                    .with_n("value", n);
                (key, item)
            })
            .collect();

        let executor = QueryExecutor::new(&schema);
        let result = executor
            .execute(
                items.into_iter(),
                &KeyCondition::pk("user1"),
                &QueryOptions::new(),
            )
            .unwrap();

        let sks: Vec<&str> = result
            .items
            .iter()
            .map(|i| i.get("sk").unwrap().as_n().unwrap())
            .collect();
        assert_eq!(sks, vec!["-100", "-4", "0", "50", "100"]);
    }
}
