use std::cmp::Ordering;

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

pub struct QueryExecutor<'a> {
    schema: &'a KeySchema,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(schema: &'a KeySchema) -> Self {
        Self { schema }
    }

    /// TODO: performance : current implementation collects all matches before sorting.
    /// For large result sets, consider:
    /// - a BTreeMap keyed by sort key during collection to avoid post-sort
    /// - early termination for limited queries when data is pre-sorted
    /// - streaming results instead of collecting into Vec
    pub fn execute(
        &self,
        items: impl Iterator<Item = (PrimaryKey, Item)>,
        condition: &KeyCondition,
        options: &QueryOptions,
    ) -> TableResult<QueryResult> {
        let mut matching: Vec<(PrimaryKey, Item)> = Vec::new();
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
            matching.push((pk, item));
        }

        matching.sort_by(|(a, _), (b, _)| match (&a.sk, &b.sk) {
            (Some(x), Some(y)) => compare_key_values(x, y),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        });

        if !options.scan_forward {
            matching.reverse();
        }

        let items: Vec<Item> = if let Some(limit) = options.limit {
            matching
                .into_iter()
                .take(limit)
                .map(|(_, item)| item)
                .collect()
        } else {
            matching.into_iter().map(|(_, item)| item).collect()
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
}
