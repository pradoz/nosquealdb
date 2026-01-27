use std::collections::BTreeMap;

use crate::KeyValidationError;
use crate::condition::{AttributePath, PathSegment};
use crate::error::{TableError, TableResult};
use crate::types::{AttributeValue, Item};
use crate::utils::add_numeric_strings;

use super::expression::{UpdateAction, UpdateExpression};

pub struct UpdateExecutor;

impl UpdateExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute(&self, mut item: Item, expression: &UpdateExpression) -> TableResult<Item> {
        for a in expression.actions() {
            item = self.apply_action(item, a)?;
        }
        Ok(item)
    }

    fn apply_action(&self, mut item: Item, action: &UpdateAction) -> TableResult<Item> {
        match action {
            UpdateAction::Set { path, value } => {
                self.set_path(&mut item, path, value.clone())?;
            }
            UpdateAction::SetIfNotExists { path, value } => {
                if path.resolve(&item).is_none() {
                    self.set_path(&mut item, path, value.clone())?;
                }
            }
            UpdateAction::Remove { path } => {
                self.remove_path(&mut item, path)?;
            }
            UpdateAction::Add { path, value } => {
                self.add_to_path(&mut item, path, value)?;
            }
            UpdateAction::Delete { path, value } => {
                self.delete_from_path(&mut item, path, value)?;
            }
        }
        Ok(item)
    }

    fn set_path(
        &self,
        item: &mut Item,
        path: &AttributePath,
        value: AttributeValue,
    ) -> TableResult<()> {
        let segments = path.segments();
        if segments.is_empty() {
            return Err(TableError::InvalidKey(
                KeyValidationError::MissingAttribute {
                    name: "path".to_string(),
                },
            ));
        }

        if segments.len() == 1
            && let PathSegment::Key(key) = &segments[0]
        {
            item.set(key.clone(), value);
            return Ok(());
        }

        // navigate and set
        self.set_nested(item, segments, value)
    }

    fn set_nested(
        &self,
        item: &mut Item,
        segments: &[PathSegment],
        value: AttributeValue,
    ) -> TableResult<()> {
        let root = match &segments[0] {
            PathSegment::Key(k) => k.clone(),
            PathSegment::Index(_) => {
                return Err(TableError::update_error(
                    "path must start with attribute name",
                ));
            }
        };

        if segments.len() == 1 {
            item.set(root, value);
            return Ok(());
        }

        // get or create root
        let mut current = item
            .remove(&root)
            .unwrap_or(AttributeValue::M(BTreeMap::new()));

        // navigate to parent of target and set value
        current = Self::set_at_path(current, &segments[1..], value)?;

        item.set(root, current);
        Ok(())
    }

    fn set_at_path(
        mut current: AttributeValue,
        segments: &[PathSegment],
        value: AttributeValue,
    ) -> TableResult<AttributeValue> {
        if segments.is_empty() {
            return Ok(value);
        }

        match (&mut current, &segments[0]) {
            (AttributeValue::M(map), PathSegment::Key(key)) => {
                if segments.len() == 1 {
                    map.insert(key.clone(), value);
                } else {
                    let child = map
                        .remove(key)
                        .unwrap_or(AttributeValue::M(BTreeMap::new()));
                    let updated = Self::set_at_path(child, &segments[1..], value)?;
                    map.insert(key.clone(), updated);
                }
            }
            (AttributeValue::L(list), PathSegment::Index(idx)) => {
                if segments.len() == 1 {
                    if *idx < list.len() {
                        list[*idx] = value;
                    } else if *idx == list.len() {
                        list.push(value);
                    } else {
                        return Err(TableError::update_error("list index out of bounds"));
                    }
                } else {
                    if *idx >= list.len() {
                        return Err(TableError::update_error("list index out of bounds"));
                    }
                    let child = std::mem::replace(&mut list[*idx], AttributeValue::Null);
                    list[*idx] = Self::set_at_path(child, &segments[1..], value)?;
                }
            }
            (_, PathSegment::Key(key)) => {
                let mut map = BTreeMap::new();
                if segments.len() == 1 {
                    map.insert(key.clone(), value);
                } else {
                    let child = AttributeValue::M(BTreeMap::new());
                    let updated = Self::set_at_path(child, &segments[1..], value)?;
                    map.insert(key.clone(), updated);
                }
                current = AttributeValue::M(map);
            }
            (_, PathSegment::Index(_)) => {
                return Err(TableError::update_error("cannot index into non-list type"));
            }
        }

        Ok(current)
    }

    fn remove_path(&self, item: &mut Item, path: &AttributePath) -> TableResult<()> {
        let segments = path.segments();
        if segments.is_empty() {
            return Ok(());
        }

        if segments.len() == 1
            && let PathSegment::Key(k) = &segments[0]
        {
            item.remove(k);
            return Ok(());
        }

        // nested removal
        let root = match &segments[0] {
            PathSegment::Key(k) => k.clone(),
            PathSegment::Index(_) => return Ok(()),
        };

        if let Some(current) = item.remove(&root) {
            if let Some(updated) = Self::remove_at_path(current, &segments[1..])? {
                item.set(root, updated);
            } else {
                // TODO: put it back if removal doesn't happen?
            }
        }
        Ok(())
    }

    fn remove_at_path(
        mut current: AttributeValue,
        segments: &[PathSegment],
    ) -> TableResult<Option<AttributeValue>> {
        if segments.is_empty() {
            return Ok(None);
        }

        match (&mut current, &segments[0]) {
            (AttributeValue::M(map), PathSegment::Key(k)) => {
                if segments.len() == 1 {
                    map.remove(k);
                } else if let Some(child) = map.remove(k)
                    && let Some(updated) = Self::remove_at_path(child, &segments[1..])?
                {
                    map.insert(k.clone(), updated);
                }
            }
            (AttributeValue::L(list), PathSegment::Index(idx)) => {
                if *idx < list.len() {
                    if segments.len() == 1 {
                        list.remove(*idx);
                    } else {
                        let child = std::mem::replace(&mut list[*idx], AttributeValue::Null);
                        if let Some(updated) = Self::remove_at_path(child, &segments[1..])? {
                            list[*idx] = updated;
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(Some(current))
    }

    fn add_to_path(
        &self,
        item: &mut Item,
        path: &AttributePath,
        value: &AttributeValue,
    ) -> TableResult<()> {
        let current = path.resolve(item).cloned();

        let new_value = match (current, value) {
            // ADD - increment number
            (Some(AttributeValue::N(n)), AttributeValue::N(delta)) => {
                let result = add_numeric_strings(&n, delta)?;
                AttributeValue::N(result)
            }
            (None, AttributeValue::N(_)) => value.clone(),
            // ADD to string set
            (Some(AttributeValue::Ss(mut set)), AttributeValue::Ss(to_add)) => {
                set.extend(to_add.iter().cloned());
                AttributeValue::Ss(set)
            }
            (None, AttributeValue::Ss(_)) => value.clone(),
            // ADD to number set
            (Some(AttributeValue::Ns(mut set)), AttributeValue::Ns(to_add)) => {
                set.extend(to_add.iter().cloned());
                AttributeValue::Ns(set)
            }
            (None, AttributeValue::Ns(_)) => value.clone(),
            // ADD to binary set
            (Some(AttributeValue::Bs(mut set)), AttributeValue::Bs(to_add)) => {
                set.extend(to_add.iter().cloned());
                AttributeValue::Bs(set)
            }
            (None, AttributeValue::Bs(_)) => value.clone(),
            _ => {
                return Err(TableError::update_error(
                    "ADD requires number or set types with matching operand",
                ));
            }
        };

        self.set_path(item, path, new_value)
    }

    fn delete_from_path(
        &self,
        item: &mut Item,
        path: &AttributePath,
        value: &AttributeValue,
    ) -> TableResult<()> {
        let current = path.resolve(item).cloned();

        let new_value = match (current, value) {
            // DELETE from string set
            (Some(AttributeValue::Ss(mut set)), AttributeValue::Ss(to_remove)) => {
                for s in to_remove {
                    set.remove(s);
                }
                AttributeValue::Ss(set)
            }
            // DELETE from number set
            (Some(AttributeValue::Ns(mut set)), AttributeValue::Ns(to_remove)) => {
                for n in to_remove {
                    set.remove(n);
                }
                AttributeValue::Ns(set)
            }
            // DELETE from binary set
            (Some(AttributeValue::Bs(mut set)), AttributeValue::Bs(to_remove)) => {
                for b in to_remove {
                    set.remove(b);
                }
                AttributeValue::Bs(set)
            }
            (None, _) => return Ok(()),
            _ => {
                return Err(TableError::update_error(
                    "DELETE requires set types with matching operand",
                ));
            }
        };
        self.set_path(item, path, new_value)
    }
}

impl Default for UpdateExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update_expr() -> UpdateExpression {
        UpdateExpression::new()
    }

    fn test_item() -> Item {
        Item::new()
            .with_s("pk", "test")
            .with_s("name", "Alice")
            .with_n("count", 10)
    }

    mod set {
        use super::*;
        use crate::condition::AttributePath;

        #[test]
        fn new_attribute() {
            let executor = UpdateExecutor::new();
            let item = test_item();
            let result = executor
                .execute(item, &update_expr().set("email", "alice@example.com"))
                .unwrap();
            assert_eq!(
                result.get("email"),
                Some(&AttributeValue::S("alice@example.com".into()))
            );
            // original preserved
            assert_eq!(result.get("pk"), Some(&AttributeValue::S("test".into())));
            assert_eq!(result.get("name"), Some(&AttributeValue::S("Alice".into())));
            assert_eq!(result.get("count"), Some(&AttributeValue::N("10".into())));
        }

        #[test]
        fn overwrites_existing() {
            let executor = UpdateExecutor::new();
            let item = test_item();
            let result = executor
                .execute(item, &update_expr().set("name", "Bob"))
                .unwrap();
            assert_eq!(result.get("name"), Some(&AttributeValue::S("Bob".into())));
        }

        #[test]
        fn if_not_exists() {
            let executor = UpdateExecutor::new();
            let item = test_item();

            // set if it does not exist
            let result = executor
                .execute(
                    item.clone(),
                    &update_expr().set_if_not_exists("email", "test@email.com"),
                )
                .unwrap();
            assert_eq!(
                result.get("email"),
                Some(&AttributeValue::S("test@email.com".into()))
            );

            // should skip existing
            let result = executor
                .execute(
                    item.clone(),
                    &update_expr().set_if_not_exists("name", "Bob"),
                )
                .unwrap();
            assert_eq!(result.get("name"), Some(&AttributeValue::S("Alice".into())));
        }

        #[test]
        fn nested() {
            let executor = UpdateExecutor::new();
            let mut map = BTreeMap::new();
            map.insert("city".to_string(), AttributeValue::S("Newton Falls".into()));
            let item = Item::new().with_s("pk", "test").with_m("address", map);

            let path = AttributePath::new("address").key("zip");
            let result = executor
                .execute(item.clone(), &update_expr().set(path, "44444"))
                .unwrap();

            let address = result.get("address").unwrap().as_m().unwrap();
            assert_eq!(
                address.get("city"),
                Some(&AttributeValue::S("Newton Falls".into()))
            );
            assert_eq!(address.get("zip"), Some(&AttributeValue::S("44444".into())));
        }
    }

    mod add {
        use super::*;

        #[test]
        fn to_nonexistent() {
            let executor = UpdateExecutor::new();
            let item = test_item();

            // should create the attribute
            let result = executor
                .execute(item.clone(), &update_expr().add("new_count_attr", 5i32))
                .unwrap();
            assert_eq!(
                result.get("new_count_attr"),
                Some(&AttributeValue::N("5".into()))
            );
        }

        #[test]
        fn to_number() {
            let executor = UpdateExecutor::new();
            let item = test_item();

            // positive number should add
            let result = executor
                .execute(item.clone(), &update_expr().add("count", 5i32))
                .unwrap();
            assert_eq!(result.get("count"), Some(&AttributeValue::N("15".into())));

            // negative number should subtract
            let result = executor
                .execute(item.clone(), &update_expr().add("count", -3i32))
                .unwrap();
            assert_eq!(result.get("count"), Some(&AttributeValue::N("7".into())));
        }

        #[test]
        fn to_string_set() {
            let executor = UpdateExecutor::new();
            let item = Item::new().with_s("pk", "test").with(
                "tags",
                AttributeValue::Ss(["a", "b"].into_iter().map(String::from).collect()),
            );

            let result = executor
                .execute(
                    item,
                    &update_expr().add(
                        "tags",
                        AttributeValue::Ss(["c", "d"].into_iter().map(String::from).collect()),
                    ),
                )
                .unwrap();

            let tags = result.get("tags").unwrap().as_ss().unwrap();
            assert!(tags.contains("a"));
            assert!(tags.contains("b"));
            assert!(tags.contains("c"));
            assert!(tags.contains("d"));
        }

        #[test]
        fn wrong_type_fails() {
            let executor = UpdateExecutor::new();
            let item = test_item();
            let result = executor.execute(item.clone(), &update_expr().add("name", 42i32));
            assert!(result.is_err());
        }
    }

    mod remove {
        use super::*;

        #[test]
        fn existing_attribute() {
            let executor = UpdateExecutor::new();
            let item = test_item();
            let result = executor
                .execute(item.clone(), &update_expr().remove("name"))
                .unwrap();
            assert!(result.get("name").is_none());
            assert_eq!(result.get("pk"), Some(&AttributeValue::S("test".into())));
            assert_eq!(result.get("count"), Some(&AttributeValue::N("10".into())));
        }

        #[test]
        fn non_existent_attribute() {
            let executor = UpdateExecutor::new();
            let item = test_item();

            // should be a noop
            let result = executor
                .execute(item.clone(), &update_expr().remove("nonexistent"))
                .unwrap();
            assert_eq!(result.len(), item.len());
        }

        #[test]
        fn nested() {
            let executor = UpdateExecutor::new();
            let mut inner = BTreeMap::new();
            inner.insert("a".to_string(), AttributeValue::N("1".into()));
            inner.insert("b".to_string(), AttributeValue::N("2".into()));

            let item = Item::new().with_s("pk", "test").with_m("data", inner);

            let path = AttributePath::new("data").key("a");
            let result = executor.execute(item, &update_expr().remove(path)).unwrap();

            let data = result.get("data").unwrap().as_m().unwrap();
            assert!(data.get("a").is_none());
            assert_eq!(data.get("b"), Some(&AttributeValue::N("2".into())));
        }
    }

    mod delete {
        use super::*;

        #[test]
        fn from_nonexistent() {
            let executor = UpdateExecutor::new();
            let item = test_item();

            // should perform a noop
            let result = executor
                .execute(item.clone(), &update_expr().delete("nonexistent", "abc"))
                .unwrap();
            assert_eq!(result.len(), item.len());
        }

        #[test]
        fn from_string_set() {
            let executor = UpdateExecutor::new();
            let item = Item::new().with_s("pk", "test").with(
                "tags",
                AttributeValue::Ss(["a", "b", "c"].into_iter().map(String::from).collect()),
            );

            let result = executor
                .execute(
                    item,
                    &update_expr().delete(
                        "tags",
                        AttributeValue::Ss(["b"].into_iter().map(String::from).collect()),
                    ),
                )
                .unwrap();

            let tags = result.get("tags").unwrap().as_ss().unwrap();
            assert!(tags.contains("a"));
            assert!(!tags.contains("b"));
            assert!(tags.contains("c"));
        }
    }
}
