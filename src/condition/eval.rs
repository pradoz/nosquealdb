use super::expression::{AttrType, CompareOp, Condition};
use crate::error::EvalResult;
use crate::types::{AttributeValue, Item};
use crate::utils::{compare_values, numbers_equal};

pub fn evaluate(condition: &Condition, item: &Item) -> EvalResult {
    match condition {
        Condition::Compare { path, op, value } => {
            let attr = path.resolve(item);
            match attr {
                Some(a) => eval_compare(a, op, value),
                None => Ok(matches!(op, CompareOp::Ne)),
            }
        }
        Condition::Between { path, low, high } => match path.resolve(item) {
            Some(a) => {
                let ge_low = compare_values(a, low)?.is_ge();
                let le_high = compare_values(a, high)?.is_le();
                Ok(ge_low && le_high)
            }
            None => Ok(false),
        },
        Condition::AttributeExists(path) => Ok(path.resolve(item).is_some()),
        Condition::AttributeNotExists(path) => Ok(path.resolve(item).is_none()),
        Condition::BeginsWith { path, prefix } => {
            let attr = path.resolve(item);
            match (attr, prefix) {
                (Some(AttributeValue::S(s)), AttributeValue::S(p)) => Ok(s.starts_with(p)),
                (Some(AttributeValue::B(b)), AttributeValue::B(p)) => Ok(b.starts_with(p)),
                _ => Ok(false),
            }
        }
        Condition::Contains { path, operand } => {
            let attr = path.resolve(item);
            eval_contains(attr, operand)
        }
        Condition::AttributeType {
            path,
            attribute_type,
        } => {
            let attr = path.resolve(item);
            match attr {
                Some(a) => Ok(matches_type(a, *attribute_type)),
                None => Ok(false),
            }
        }
        Condition::Size { path, op, value } => {
            let attr = path.resolve(item);
            match attr {
                Some(a) => {
                    let size = get_size(a);
                    Ok(eval_size_compare(size, op, *value))
                }
                None => Ok(false),
            }
        }
        Condition::And(left, right) => {
            let left_result = evaluate(left, item)?;
            if !left_result {
                return Ok(false); // short-circuit
            }
            evaluate(right, item)
        }
        Condition::Or(left, right) => {
            let left_result = evaluate(left, item)?;
            if left_result {
                return Ok(true); // short-circuit
            }
            evaluate(right, item)
        }
        Condition::Not(inner) => evaluate(inner, item).map(|r| !r),
    }
}

fn eval_compare(attr: &AttributeValue, op: &CompareOp, value: &AttributeValue) -> EvalResult {
    match op {
        CompareOp::Eq => Ok(values_equal(attr, value)),
        CompareOp::Ne => Ok(!values_equal(attr, value)),
        CompareOp::Lt => compare_values(attr, value).map(|ord| ord.is_lt()),
        CompareOp::Le => compare_values(attr, value).map(|ord| ord.is_le()),
        CompareOp::Gt => compare_values(attr, value).map(|ord| ord.is_gt()),
        CompareOp::Ge => compare_values(attr, value).map(|ord| ord.is_ge()),
    }
}

fn eval_contains(attr: Option<&AttributeValue>, operand: &AttributeValue) -> EvalResult {
    match attr {
        Some(AttributeValue::S(s)) => {
            if let AttributeValue::S(substr) = operand {
                Ok(s.contains(substr))
            } else {
                Ok(false)
            }
        }
        Some(AttributeValue::B(b)) => {
            if let AttributeValue::B(subbytes) = operand {
                Ok(contains_bytes(b, subbytes))
            } else {
                Ok(false)
            }
        }
        Some(AttributeValue::L(l)) => Ok(l.iter().any(|v| values_equal(v, operand))),
        Some(AttributeValue::Ss(set)) => {
            if let AttributeValue::S(s) = operand {
                Ok(set.contains(s))
            } else {
                Ok(false)
            }
        }
        Some(AttributeValue::Ns(set)) => {
            if let AttributeValue::N(n) = operand {
                Ok(set.iter().any(|v| numbers_equal(v, n)))
            } else {
                Ok(false)
            }
        }
        Some(AttributeValue::Bs(set)) => {
            if let AttributeValue::B(b) = operand {
                Ok(set.contains(b))
            } else {
                Ok(false)
            }
        }
        _ => Ok(false),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn matches_type(attr: &AttributeValue, expected: AttrType) -> bool {
    matches!(
        (attr, expected),
        (AttributeValue::S(_), AttrType::String)
            | (AttributeValue::N(_), AttrType::Number)
            | (AttributeValue::B(_), AttrType::Binary)
            | (AttributeValue::Bool(_), AttrType::Boolean)
            | (AttributeValue::Null, AttrType::Null)
            | (AttributeValue::M(_), AttrType::Map)
            | (AttributeValue::L(_), AttrType::List)
            | (AttributeValue::Ss(_), AttrType::StringSet)
            | (AttributeValue::Ns(_), AttrType::NumberSet)
            | (AttributeValue::Bs(_), AttrType::BinarySet)
    )
}

fn get_size(attr: &AttributeValue) -> usize {
    match attr {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => n.len(),
        AttributeValue::B(b) => b.len(),
        AttributeValue::Bool(_) => 1,
        AttributeValue::Null => 0,
        AttributeValue::M(map) => map.len(),
        AttributeValue::L(list) => list.len(),
        AttributeValue::Ss(set) => set.len(),
        AttributeValue::Ns(set) => set.len(),
        AttributeValue::Bs(set) => set.len(),
    }
}

fn eval_size_compare(size: usize, op: &CompareOp, value: usize) -> bool {
    match op {
        CompareOp::Eq => size == value,
        CompareOp::Ne => size != value,
        CompareOp::Lt => size < value,
        CompareOp::Le => size <= value,
        CompareOp::Gt => size > value,
        CompareOp::Ge => size >= value,
    }
}

fn values_equal(a: &AttributeValue, b: &AttributeValue) -> bool {
    match (a, b) {
        (AttributeValue::S(a), AttributeValue::S(b)) => a == b,
        (AttributeValue::N(a), AttributeValue::N(b)) => numbers_equal(a, b),
        (AttributeValue::B(a), AttributeValue::B(b)) => a == b,
        (AttributeValue::Bool(a), AttributeValue::Bool(b)) => a == b,
        (AttributeValue::Null, AttributeValue::Null) => true,
        (AttributeValue::L(a), AttributeValue::L(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_equal(x, y))
        }
        (AttributeValue::M(a), AttributeValue::M(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).map(|bv| values_equal(v, bv)).unwrap_or(false))
        }
        (AttributeValue::Ss(a), AttributeValue::Ss(b)) => a == b,
        (AttributeValue::Ns(a), AttributeValue::Ns(b)) => a == b,
        (AttributeValue::Bs(a), AttributeValue::Bs(b)) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::expression::attr;

    fn test_item() -> Item {
        Item::new()
            .with_s("name", "Alice")
            .with_n("id", 42)
            .with_s("status", "active")
            .with_bool("verified", true)
            .with_l(
                "tags",
                vec![
                    AttributeValue::S("rust".into()),
                    AttributeValue::S("db".into()),
                ],
            )
            .with(
                "scores",
                AttributeValue::Ns(["85", "90", "95"].into_iter().map(String::from).collect()),
            )
    }

    mod equality {
        use super::*;

        #[test]
        fn not_equals() {
            let item = test_item();
            assert!(evaluate(&attr("id").ne(67i32), &item).unwrap());
            assert!(!evaluate(&attr("id").ne(42i32), &item).unwrap());
        }

        #[test]
        fn string() {
            let item = test_item();
            assert!(evaluate(&attr("name").eq("Alice"), &item).unwrap());
            assert!(!evaluate(&attr("name").eq("Bob"), &item).unwrap());
        }

        #[test]
        fn number() {
            let item = test_item();
            assert!(evaluate(&attr("id").eq(42i32), &item).unwrap());
            assert!(!evaluate(&attr("id").eq(67i32), &item).unwrap());
        }

        #[test]
        fn bool() {
            let item = test_item();
            assert!(evaluate(&attr("verified").eq(true), &item).unwrap());
            assert!(!evaluate(&attr("verified").eq(false), &item).unwrap());
        }

        #[test]
        fn missing_attribute() {
            let item = test_item();
            assert!(!evaluate(&attr("missing").eq("AAAA"), &item).unwrap());
            assert!(evaluate(&attr("missing").ne("AAAA"), &item).unwrap());
        }
    }

    mod comparison {
        use super::*;

        #[test]
        fn string_comparison() {
            let item = test_item();
            assert!(evaluate(&attr("name").lt("Bob"), &item).unwrap());
            assert!(evaluate(&attr("name").gt("Aaron"), &item).unwrap());
            assert!(evaluate(&attr("name").eq("Alice"), &item).unwrap());
            assert!(evaluate(&attr("name").le("Alice"), &item).unwrap());
            assert!(evaluate(&attr("name").ge("Alice"), &item).unwrap());
        }

        #[test]
        fn numeric_comparison() {
            let item = test_item();
            assert!(evaluate(&attr("id").gt(-1i32), &item).unwrap());
            assert!(evaluate(&attr("id").lt(50i32), &item).unwrap());
            assert!(evaluate(&attr("id").ge(-1i32), &item).unwrap());
            assert!(evaluate(&attr("id").le(50i32), &item).unwrap());
            assert!(evaluate(&attr("id").ge(42i32), &item).unwrap());
            assert!(evaluate(&attr("id").le(42i32), &item).unwrap());
            assert!(evaluate(&attr("id").eq(42i32), &item).unwrap());
            assert!(evaluate(&attr("id").ne(67i32), &item).unwrap());
        }

        #[test]
        fn between() {
            let item = test_item();
            assert!(evaluate(&attr("id").between(-1i32, 67i32), &item).unwrap());
            assert!(!evaluate(&attr("id").between(0i32, 1i32), &item).unwrap());
            assert!(evaluate(&attr("name").between("Aaron", "Bob"), &item).unwrap());
            assert!(!evaluate(&attr("name").between("foo", "bar"), &item).unwrap());
        }
    }

    mod functions {
        use super::*;

        #[test]
        fn attribute_existence() {
            let item = test_item();
            assert!(evaluate(&attr("name").exists(), &item).unwrap());
            assert!(!evaluate(&attr("nonexistent").exists(), &item).unwrap());

            assert!(!evaluate(&attr("name").not_exists(), &item).unwrap());
            assert!(evaluate(&attr("nonexistent").not_exists(), &item).unwrap());
        }

        #[test]
        fn begins_with() {
            let item = test_item();
            assert!(evaluate(&attr("name").begins_with("A"), &item).unwrap());
            assert!(evaluate(&attr("name").begins_with("Al"), &item).unwrap());
            assert!(!evaluate(&attr("name").begins_with("xyAl"), &item).unwrap());
        }

        #[test]
        fn contains() {
            let item = test_item();
            assert!(evaluate(&attr("name").contains("li"), &item).unwrap());
            assert!(evaluate(&attr("name").contains("ce"), &item).unwrap());
            assert!(!evaluate(&attr("name").contains("xce"), &item).unwrap());
            assert!(!evaluate(&attr("name").contains("Alz"), &item).unwrap());
        }

        #[test]
        fn attr_type_check() {
            let item = test_item();
            assert!(evaluate(&attr("name").is_type(AttrType::String), &item).unwrap());
            assert!(!evaluate(&attr("id").is_type(AttrType::String), &item).unwrap());
            assert!(evaluate(&attr("id").is_type(AttrType::Number), &item).unwrap());
            assert!(evaluate(&attr("verified").is_type(AttrType::Boolean), &item).unwrap());
            assert!(evaluate(&attr("tags").is_type(AttrType::List), &item).unwrap());
            assert!(evaluate(&attr("scores").is_type(AttrType::NumberSet), &item).unwrap());
        }

        #[test]
        fn size_check() {
            let item = test_item();
            assert!(evaluate(&attr("name").size_eq(5), &item).unwrap());
            assert!(evaluate(&attr("name").size_gt(3), &item).unwrap());
            assert!(evaluate(&attr("name").size_lt(9), &item).unwrap());
            assert!(evaluate(&attr("tags").size_eq(2), &item).unwrap());
        }
    }

    mod logical {
        use super::*;

        #[test]
        fn not() {
            let item = test_item();
            assert!(evaluate(&attr("name").eq("Bob").not(), &item).unwrap());
            assert!(!evaluate(&attr("name").eq("Alice").not(), &item).unwrap());
        }

        #[test]
        fn and() {
            let item = test_item();
            let cond = attr("name").eq("Alice").and(attr("status").eq("active"));
            assert!(evaluate(&cond, &item).unwrap());

            let cond = attr("name").eq("Alice").and(attr("status").eq("inactive"));
            assert!(!evaluate(&cond, &item).unwrap());

            let cond = attr("name").eq("Bob").and(attr("status").eq("inactive"));
            assert!(!evaluate(&cond, &item).unwrap());

            let cond = attr("name").eq("Bob").and(attr("status").eq("active"));
            assert!(!evaluate(&cond, &item).unwrap());
        }

        #[test]
        fn or() {
            let item = test_item();
            let cond = attr("name").eq("Alice").or(attr("status").eq("active"));
            assert!(evaluate(&cond, &item).unwrap());

            let cond = attr("name").eq("Alice").or(attr("status").eq("inactive"));
            assert!(evaluate(&cond, &item).unwrap());

            let cond = attr("name").eq("Bob").or(attr("status").eq("active"));
            assert!(evaluate(&cond, &item).unwrap());

            let cond = attr("name").eq("Bob").or(attr("status").eq("inactive"));
            assert!(!evaluate(&cond, &item).unwrap());
        }

        #[test]
        fn complex() {
            let item = test_item();
            let cond = attr("name")
                .eq("Alice")
                .and(attr("id").ge(67i32))
                .not()
                .or(attr("status").eq("admin"));
            assert!(evaluate(&cond, &item).unwrap());
        }
    }

    mod nested_paths {
        use super::*;
        use crate::condition::expression::ConditionBuilder;
        use crate::condition::path::AttributePath;
        use std::collections::BTreeMap;

        #[test]
        fn map() {
            let mut address = BTreeMap::new();
            address.insert("state".to_string(), AttributeValue::S("California".into()));

            let item = Item::new().with("country", AttributeValue::M(address));

            let path = AttributePath::new("country").key("state");
            assert!(
                evaluate(&ConditionBuilder::new(path.clone()).eq("California"), &item).unwrap()
            );
            assert!(!evaluate(&ConditionBuilder::new(path).eq("Florida"), &item).unwrap());
        }

        #[test]
        fn list() {
            let item = Item::new().with(
                "items",
                AttributeValue::L(vec![
                    AttributeValue::S("first".into()),
                    AttributeValue::S("second".into()),
                ]),
            );

            let path = AttributePath::new("items").index(0);
            let cond = ConditionBuilder::new(path).eq("first");
            assert!(evaluate(&cond, &item).unwrap());
        }

        #[test]
        fn deeply() {
            let inner = AttributeValue::M(
                [("name".to_string(), AttributeValue::S("nested".into()))]
                    .into_iter()
                    .collect(),
            );
            let list = AttributeValue::L(vec![inner]);
            let outer = AttributeValue::M([("items".to_string(), list)].into_iter().collect());

            let item = Item::new().with("data", outer);

            let path = AttributePath::new("data").key("items").index(0).key("name");
            let cond = ConditionBuilder::new(path).eq("nested");
            assert!(evaluate(&cond, &item).unwrap());
        }
    }
}
