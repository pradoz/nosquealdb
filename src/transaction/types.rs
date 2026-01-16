use crate::condition::Condition;
use crate::types::{Item, PrimaryKey};
use crate::update::UpdateExpression;

#[derive(Debug, Clone)]
pub enum TransactWriteItem {
    Put {
        item: Item,
        condition: Option<Condition>,
    },
    Update {
        key: PrimaryKey,
        expression: UpdateExpression,
        condition: Option<Condition>,
    },
    Delete {
        key: PrimaryKey,
        condition: Option<Condition>,
    },
    ConditionCheck {
        key: PrimaryKey,
        condition: Condition,
    },
}

impl TransactWriteItem {
    pub fn put(item: Item) -> Self {
        Self::Put {
            item,
            condition: None,
        }
    }
    pub fn put_with_condition(item: Item, condition: Condition) -> Self {
        Self::Put {
            item,
            condition: Some(condition),
        }
    }

    pub fn update(key: impl Into<PrimaryKey>, expression: UpdateExpression) -> Self {
        Self::Update {
            key: key.into(),
            expression,
            condition: None,
        }
    }
    pub fn update_with_condition(
        key: impl Into<PrimaryKey>,
        expression: UpdateExpression,
        condition: Condition,
    ) -> Self {
        Self::Update {
            key: key.into(),
            expression,
            condition: Some(condition),
        }
    }

    pub fn delete(key: impl Into<PrimaryKey>) -> Self {
        Self::Delete {
            key: key.into(),
            condition: None,
        }
    }
    pub fn delete_with_condition(key: impl Into<PrimaryKey>, condition: Condition) -> Self {
        Self::Delete {
            key: key.into(),
            condition: Some(condition),
        }
    }

    pub fn condition_check(key: impl Into<PrimaryKey>, condition: Condition) -> Self {
        Self::ConditionCheck {
            key: key.into(),
            condition: condition,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactGetItem {
    Get { key: PrimaryKey },
}

impl TransactGetItem {
    pub fn get(key: impl Into<PrimaryKey>) -> Self {
        Self::Get { key: key.into() }
    }
}

#[derive(Debug, Clone)]
pub struct TransactGetResult {
    pub items: Vec<Option<Item>>,
}

impl TransactGetResult {
    pub fn new(items: Vec<Option<Item>>) -> Self {
        Self { items }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn found_count(&self) -> usize {
        self.items.iter().filter(|i| i.is_some()).count()
    }

    pub fn get(&self, index: usize) -> Option<&Item> {
        self.items.get(index).and_then(|i| i.as_ref())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::condition::attr;

    #[test]
    fn write_item_put() {
        let item = Item::new().with_s("pk", "test");

        let put = TransactWriteItem::put(item.clone());
        assert!(matches!(
            put,
            TransactWriteItem::Put {
                condition: None,
                ..
            }
        ));
        let put_cond = TransactWriteItem::put_with_condition(item.clone(), attr("pk").not_exists());
        assert!(matches!(
            put_cond,
            TransactWriteItem::Put {
                condition: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn write_item_update() {
        let key = PrimaryKey::simple("pk");

        let update =
            TransactWriteItem::update(key.clone(), UpdateExpression::new().set("name", "Alice"));
        assert!(matches!(
            update,
            TransactWriteItem::Update {
                condition: None,
                ..
            }
        ));
        let update_cond = TransactWriteItem::update_with_condition(
            key.clone(),
            UpdateExpression::new().set("name", "Alice"),
            attr("pk").not_exists(),
        );
        assert!(matches!(
            update_cond,
            TransactWriteItem::Update {
                condition: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn write_item_delete() {
        let delete = TransactWriteItem::delete(PrimaryKey::simple("test"));
        assert!(matches!(
            delete,
            TransactWriteItem::Delete {
                condition: None,
                ..
            }
        ));
    }

    #[test]
    fn condition_check() {
        let check = TransactWriteItem::condition_check(
            PrimaryKey::simple("test"),
            attr("status").eq("active"),
        );
        assert!(matches!(check, TransactWriteItem::ConditionCheck { .. }));
    }

    #[test]
    fn get_result() {
        let result = TransactGetResult::new(vec![
            Some(Item::new().with_s("pk", "item0")),
            None,
            Some(Item::new().with_s("pk", "item2")),
        ]);

        assert_eq!(result.len(), 3);
        assert_eq!(result.found_count(), 2);
        assert!(result.get(0).is_some());
        assert!(result.get(1).is_none());
        assert!(result.get(2).is_some());
        assert!(result.get(42).is_none()); // out of bounds
    }
}
