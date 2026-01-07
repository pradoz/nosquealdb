use crate::types::KeyValue;
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKeyOp {
    Eq(KeyValue),
    Lt(KeyValue),
    Le(KeyValue),
    Gt(KeyValue),
    Ge(KeyValue),
    Between { low: KeyValue, high: KeyValue },
    BeginsWith(KeyValue),
}

impl SortKeyOp {
    pub fn eq(value: impl Into<KeyValue>) -> Self {
        Self::Eq(value.into())
    }
    pub fn lt(value: impl Into<KeyValue>) -> Self {
        Self::Lt(value.into())
    }
    pub fn le(value: impl Into<KeyValue>) -> Self {
        Self::Le(value.into())
    }
    pub fn gt(value: impl Into<KeyValue>) -> Self {
        Self::Gt(value.into())
    }
    pub fn ge(value: impl Into<KeyValue>) -> Self {
        Self::Ge(value.into())
    }
    pub fn between(low: impl Into<KeyValue>, high: impl Into<KeyValue>) -> Self {
        Self::Between {
            low: low.into(),
            high: high.into(),
        }
    }
    pub fn begins_with(prefix: impl Into<KeyValue>) -> Self {
        Self::BeginsWith(prefix.into())
    }

    pub fn matches(&self, value: &KeyValue) -> bool {
        match self {
            SortKeyOp::Eq(target) => compare_keys(value, target) == Ordering::Equal,
            SortKeyOp::Lt(target) => compare_keys(value, target) == Ordering::Less,
            SortKeyOp::Le(target) => compare_keys(value, target) != Ordering::Greater,
            SortKeyOp::Gt(target) => compare_keys(value, target) == Ordering::Greater,
            SortKeyOp::Ge(target) => compare_keys(value, target) != Ordering::Less,
            SortKeyOp::Between { low, high } => {
                compare_keys(value, low) != Ordering::Less
                    && compare_keys(value, high) != Ordering::Greater
            }
            SortKeyOp::BeginsWith(prefix) => key_begins_with(value, prefix),
        }
    }
}

fn compare_keys(a: &KeyValue, b: &KeyValue) -> Ordering {
    match (a, b) {
        (KeyValue::S(a), KeyValue::S(b)) => a.cmp(b),
        (KeyValue::N(a), KeyValue::N(b)) => compare_numeric_strings(a, b),
        (KeyValue::B(a), KeyValue::B(b)) => a.cmp(b),
        // compare by type name
        _ => a.type_name().cmp(b.type_name()),
    }
}

fn compare_numeric_strings(a: &str, b: &str) -> Ordering {
    // TODO: arbitrary precision
    let x: f64 = a.parse().unwrap_or(f64::NAN);
    let y: f64 = b.parse().unwrap_or(f64::NAN);
    x.partial_cmp(&y).unwrap_or(Ordering::Equal)
}

fn key_begins_with(value: &KeyValue, prefix: &KeyValue) -> bool {
    match (value, prefix) {
        (KeyValue::S(v), KeyValue::S(p)) => v.starts_with(p),
        (KeyValue::B(v), KeyValue::B(p)) => v.starts_with(p),
        // no prefix matching for numeric values
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct KeyCondition {
    pub partition_key: KeyValue,
    pub sort_key: Option<SortKeyOp>,
}

impl KeyCondition {
    pub fn pk(pk: impl Into<KeyValue>) -> Self {
        Self {
            partition_key: pk.into(),
            sort_key: None,
        }
    }

    pub fn pk_sk(pk: impl Into<KeyValue>, sk_op: SortKeyOp) -> Self {
        Self {
            partition_key: pk.into(),
            sort_key: Some(sk_op),
        }
    }

    pub fn sk_eq(mut self, value: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::eq(value));
        self
    }
    pub fn sk_lt(mut self, value: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::lt(value));
        self
    }
    pub fn sk_le(mut self, value: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::le(value));
        self
    }
    pub fn sk_gt(mut self, value: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::gt(value));
        self
    }
    pub fn sk_ge(mut self, value: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::ge(value));
        self
    }
    pub fn sk_between(mut self, low: impl Into<KeyValue>, high: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::between(low, high));
        self
    }
    pub fn sk_begins_with(mut self, prefix: impl Into<KeyValue>) -> Self {
        self.sort_key = Some(SortKeyOp::begins_with(prefix));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod sort_key_ops {
        use super::*;

        #[test]
        fn equal() {
            let op = SortKeyOp::eq("foo");
            assert!(op.matches(&KeyValue::S("foo".into())));
            assert!(!op.matches(&KeyValue::S("bar".into())));
        }

        #[test]
        fn less_than() {
            let op = SortKeyOp::lt("b");
            assert!(op.matches(&KeyValue::S("a".into())));
            assert!(!op.matches(&KeyValue::S("b".into())));
            assert!(!op.matches(&KeyValue::S("c".into())));
        }

        #[test]
        fn less_than_or_equal() {
            let op = SortKeyOp::le("b");
            assert!(op.matches(&KeyValue::S("a".into())));
            assert!(op.matches(&KeyValue::S("b".into())));
            assert!(!op.matches(&KeyValue::S("c".into())));
        }

        #[test]
        fn greater_than() {
            let op = SortKeyOp::gt("b");
            assert!(!op.matches(&KeyValue::S("a".into())));
            assert!(!op.matches(&KeyValue::S("b".into())));
            assert!(op.matches(&KeyValue::S("c".into())));
        }

        #[test]
        fn greater_than_or_equal() {
            let op = SortKeyOp::ge("b");
            assert!(!op.matches(&KeyValue::S("a".into())));
            assert!(op.matches(&KeyValue::S("b".into())));
            assert!(op.matches(&KeyValue::S("c".into())));
        }

        #[test]
        fn between_inclusive() {
            let op = SortKeyOp::between("b", "d");
            assert!(!op.matches(&KeyValue::S("a".into())));
            assert!(op.matches(&KeyValue::S("b".into())));
            assert!(op.matches(&KeyValue::S("c".into())));
            assert!(op.matches(&KeyValue::S("d".into())));
            assert!(!op.matches(&KeyValue::S("e".into())));
        }

        #[test]
        fn begins_with_string() {
            let op = SortKeyOp::begins_with("foo");
            assert!(op.matches(&KeyValue::S("foobar".into())));
            assert!(!op.matches(&KeyValue::S("boobaz".into())));
        }

        #[test]
        fn begins_with_binary() {
            let op = SortKeyOp::begins_with(vec![0x04, 0x05]);
            assert!(op.matches(&KeyValue::B(vec![0x04, 0x05].into())));
            assert!(op.matches(&KeyValue::B(vec![0x04, 0x05, 0xFF].into())));
            assert!(!op.matches(&KeyValue::B(vec![0x05, 0x04].into())));
            assert!(!op.matches(&KeyValue::B(vec![0x05].into())));
        }

        #[test]
        fn numeric_comparison() {
            let op = SortKeyOp::lt(KeyValue::N("100".into()));
            assert!(op.matches(&KeyValue::N("99".into())));
            assert!(op.matches(&KeyValue::N("0".into())));
            assert!(op.matches(&KeyValue::N("-5".into())));
            assert!(!op.matches(&KeyValue::N("100".into())));
            assert!(!op.matches(&KeyValue::N("1000".into())));
        }

        #[test]
        fn numeric_decimal() {
            let op = SortKeyOp::ge(KeyValue::N("4.2".into()));
            assert!(op.matches(&KeyValue::N("4.2".into())));
            assert!(op.matches(&KeyValue::N("5.0".into())));
            // TODO: arbitrary precision
            assert!(op.matches(&KeyValue::N("4.200".into())));
            assert!(!op.matches(&KeyValue::N("-6.7".into())));
            assert!(!op.matches(&KeyValue::N("4".into())));
        }
    }

    mod key_condition {
        use super::*;

        #[test]
        fn pk_only() {
            let cond = KeyCondition::pk("user123");
            assert_eq!(cond.partition_key, KeyValue::S("user123".into()));
            assert!(cond.sort_key.is_none());
        }

        #[test]
        fn pk_with_sk_builder() {
            let cond = KeyCondition::pk("user123").sk_begins_with("order");
            assert_eq!(cond.partition_key, KeyValue::S("user123".into()));
            assert!(matches!(cond.sort_key, Some(SortKeyOp::BeginsWith(_))));
        }
    }
}
