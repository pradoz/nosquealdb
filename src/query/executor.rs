use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::error::{TableError, TableResult};
use crate::types::{Item, KeySchema, KeyValidationError, KeyValue, PrimaryKey};

use super::condition::{KeyCondition, SortKeyOp};

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
struct SortableKey {
    sk: Option<KeyValue>,
    unique_suffix: String,
}

impl SortableKey {
    fn new(pk: &PrimaryKey) -> Self {
        Self {
            sk: pk.sk.clone(),
            unique_suffix: pk.to_storage_key(),
        }
    }
}

impl PartialOrd for SortableKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.sk, &other.sk) {
            // unique suffix should differentiate items with same SK but different PK
            (Some(a), Some(b)) => {
                let key_cmp = compare_key_values(a, b);
                if key_cmp == Ordering::Equal {
                    self.unique_suffix.cmp(&other.unique_suffix)
                } else {
                    key_cmp
                }
            }
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => self.unique_suffix.cmp(&other.unique_suffix),
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

    /// TODO(performance): current implementation collects all matches before sorting.
    /// For large result sets, consider:
    /// - early termination for limited queries
    /// - streaming results/lazy eval
    pub fn execute(
        &self,
        items: impl Iterator<Item = (PrimaryKey, Item)>,
        condition: &KeyCondition,
        options: &QueryOptions,
    ) -> TableResult<QueryResult> {
        let mut matching: BTreeMap<SortableKey, Item> = BTreeMap::new();
        let mut scanned = 0;

        for (pk, item) in items {
            scanned += 1;

            if pk.pk != condition.partition_key {
                continue;
            }

            if let Some(sk_op) = &condition.sort_key {
                if let Some(sk) = &pk.sk {
                    if !sk_op.matches(sk) {
                        continue;
                    }
                } else {
                    continue; // item has no sort key but requires one
                }
            }

            let sortable_key = SortableKey::new(&pk);
            matching.insert(sortable_key, item);
        }

        // extract items in sorted order
        let items: Vec<Item> = if options.scan_forward {
            if let Some(limit) = options.limit {
                matching.into_values().take(limit).collect()
            } else {
                matching.into_values().collect()
            }
        } else {
            // reverse order
            if let Some(limit) = options.limit {
                matching.into_values().rev().take(limit).collect()
            } else {
                matching.into_values().rev().collect()
            }
        };

        let count = items.len();
        Ok(QueryResult {
            items,
            scanned_count: scanned,
            count: count,
        })
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
                    let sk_value = get_sk_value_from_op(sk_op);
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

fn compare_key_values(a: &KeyValue, b: &KeyValue) -> Ordering {
    match (a, b) {
        (KeyValue::S(a), KeyValue::S(b)) => a.cmp(b),
        (KeyValue::N(a), KeyValue::N(b)) => {
            let x: f64 = a.parse().unwrap_or(f64::NAN);
            let y: f64 = b.parse().unwrap_or(f64::NAN);
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
        (KeyValue::B(a), KeyValue::B(b)) => a.cmp(b),
        _ => a.type_name().cmp(b.type_name()),
    }
}

fn get_sk_value_from_op(op: &SortKeyOp) -> &KeyValue {
    match op {
        SortKeyOp::Eq(v)
        | SortKeyOp::Lt(v)
        | SortKeyOp::Le(v)
        | SortKeyOp::Gt(v)
        | SortKeyOp::Ge(v)
        | SortKeyOp::BeginsWith(v) => v,
        SortKeyOp::Between { low, .. } => low,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;

    struct TestFixture {
        schema: KeySchema,
        items: Vec<(PrimaryKey, Item)>,
        opts: QueryOptions,
    }

    impl TestFixture {
        fn new() -> Self {
            Self {
                schema: KeySchema::composite("pk", KeyType::S, "sk", KeyType::S),
                items: vec![
                    make_item("user1", "order#001", "first"),
                    make_item("user1", "order#002", "second"),
                    make_item("user1", "order#003", "third"),
                    make_item("user1", "profile", "user1 profile"),
                    make_item("user2", "order#001", "user2 first"),
                    make_item("user2", "order#002", "user2 second"),
                ],
                opts: QueryOptions::new(),
            }
        }

        fn execute(&self, condition: KeyCondition) -> QueryResult {
            let executor = QueryExecutor::new(&self.schema);
            executor
                .execute(self.items.clone().into_iter(), &condition, &self.opts)
                .unwrap()
        }

        fn execute_with_opts(&self, condition: KeyCondition, opts: QueryOptions) -> QueryResult {
            let executor = QueryExecutor::new(&self.schema);
            executor
                .execute(self.items.clone().into_iter(), &condition, &opts)
                .unwrap()
        }
    }

    fn make_item(pk: &str, sk: &str, data: &str) -> (PrimaryKey, Item) {
        let key = PrimaryKey::composite(pk, sk);
        let item = Item::new()
            .with_s("pk", pk)
            .with_s("sk", sk)
            .with_s("data", data);
        (key, item)
    }

    #[test]
    fn query_no_results() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("nonexistent");
        let result = f.execute(cond);
        assert_eq!(result.count, 0);
        assert_eq!(result.scanned_count, 6);
    }

    #[test]
    fn query_by_partition_key() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1");
        let result = f.execute(cond);
        assert_eq!(result.count, 4);
        assert_eq!(result.scanned_count, 6);
    }

    #[test]
    fn query_with_sk_begins_with() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1").sk_begins_with("order");
        let result = f.execute(cond);
        assert_eq!(result.count, 3);
    }

    #[test]
    fn query_with_sk_between() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1").sk_between("order#002", "order#003");
        let result = f.execute(cond);
        assert_eq!(result.count, 2);
    }

    #[test]
    fn query_reverse_order() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1").sk_begins_with("order");
        let opts = QueryOptions::new().reverse();
        let result = f.execute_with_opts(cond, opts);
        assert_eq!(result.count, 3);
        assert_eq!(result.items[0].get("sk").unwrap().as_s(), Some("order#003"));
        assert_eq!(result.items[2].get("sk").unwrap().as_s(), Some("order#001"));
    }

    #[test]
    fn query_with_limit() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1").sk_begins_with("order");
        let opts = QueryOptions::new().with_limit(1);
        let result = f.execute_with_opts(cond, opts);
        assert_eq!(result.count, 1);
    }

    #[test]
    fn query_forward_is_sorted() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1").sk_begins_with("order");
        let result = f.execute(cond);

        assert_eq!(result.count, 3);
        assert_eq!(result.items[0].get("sk").unwrap().as_s(), Some("order#001"));
        assert_eq!(result.items[1].get("sk").unwrap().as_s(), Some("order#002"));
        assert_eq!(result.items[2].get("sk").unwrap().as_s(), Some("order#003"));
    }

    #[test]
    fn query_limit_with_reverse() {
        let f = TestFixture::new();
        let cond = KeyCondition::pk("user1").sk_begins_with("order");
        let opts = QueryOptions::new().with_limit(2).reverse();
        let result = f.execute_with_opts(cond, opts);

        assert_eq!(result.count, 2);
        assert_eq!(result.items[0].get("sk").unwrap().as_s(), Some("order#003"));
        assert_eq!(result.items[1].get("sk").unwrap().as_s(), Some("order#002"));
    }

    mod numeric_sort_keys {
        use super::*;

        fn make_numeric_item(pk: &str, sk: i32, data: &str) -> (PrimaryKey, Item) {
            let key = PrimaryKey::composite(pk, KeyValue::N(sk.to_string()));
            let item = Item::new()
                .with_s("pk", pk)
                .with_n("sk", sk)
                .with_s("data", data);
            (key, item)
        }

        #[test]
        fn sorted_integers() {
            let schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::N);
            let items = vec![
                make_numeric_item("user1", 100, "onehundred"),
                make_numeric_item("user1", -4, "negativefour"),
                make_numeric_item("user1", -100, "negativeonehundred"),
                make_numeric_item("user1", 50, "50"),
                make_numeric_item("user1", 0, "zero"),
            ];

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

    mod sortable_key {
        use super::*;

        #[test]
        fn with_none_as_sort_key() {
            let pk1 = PrimaryKey::simple("user1");
            let pk2 = PrimaryKey::simple("user2");
            let pk3 = PrimaryKey::composite("user2", "order1");

            let sk1 = SortableKey::new(&pk1);
            let sk2 = SortableKey::new(&pk2);
            let sk3 = SortableKey::new(&pk3);

            // None < Some
            assert!(sk1 < sk3);
            assert!(sk2 < sk3);
            // None ? None --> use unique suffix
            assert_ne!(sk1, sk2);
        }

        #[test]
        fn with_same_sort_key() {
            let pk1 = PrimaryKey::composite("user1", "order1");
            let pk2 = PrimaryKey::composite("user2", "order1");

            let sk1 = SortableKey::new(&pk1);
            let sk2 = SortableKey::new(&pk2);

            // should use unique suffix
            assert_ne!(sk1, sk2);

            // order should be consistent
            let cmp1 = sk1.cmp(&sk2);
            let cmp2 = sk2.cmp(&sk1);
            assert_eq!(cmp1, cmp2.reverse());
        }
    }
}
