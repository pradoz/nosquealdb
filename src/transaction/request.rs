use super::types::{TransactGetItem, TransactWriteItem};

use crate::condition::Condition;
use crate::types::{Item, PrimaryKey};
use crate::update::UpdateExpression;

#[derive(Debug, Clone, Default)]
pub struct TransactWriteRequest {
    pub(crate) items: Vec<TransactWriteItem>,
}

impl TransactWriteRequest {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn put(mut self, item: Item) -> Self {
        self.items.push(TransactWriteItem::put(item));
        self
    }
    pub fn put_with_condition(mut self, item: Item, condition: Condition) -> Self {
        self.items
            .push(TransactWriteItem::put_with_condition(item, condition));
        self
    }

    pub fn update(mut self, key: impl Into<PrimaryKey>, expression: UpdateExpression) -> Self {
        self.items.push(TransactWriteItem::update(key, expression));
        self
    }
    pub fn update_with_condition(
        mut self,
        key: impl Into<PrimaryKey>,
        expression: UpdateExpression,
        condition: Condition,
    ) -> Self {
        self.items.push(TransactWriteItem::update_with_condition(
            key, expression, condition,
        ));
        self
    }

    pub fn delete(mut self, key: impl Into<PrimaryKey>) -> Self {
        self.items.push(TransactWriteItem::delete(key));
        self
    }
    pub fn delete_with_condition(
        mut self,
        key: impl Into<PrimaryKey>,
        condition: Condition,
    ) -> Self {
        self.items
            .push(TransactWriteItem::delete_with_condition(key, condition));
        self
    }

    pub fn condition_check(mut self, key: impl Into<PrimaryKey>, condition: Condition) -> Self {
        self.items
            .push(TransactWriteItem::condition_check(key, condition));
        self
    }

    pub fn with_item(mut self, item: TransactWriteItem) -> Self {
        self.items.push(item);
        self
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl From<Vec<TransactWriteItem>> for TransactWriteRequest {
    fn from(items: Vec<TransactWriteItem>) -> Self {
        Self { items }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransactGetRequest {
    pub(crate) items: Vec<TransactGetItem>,
}

impl TransactGetRequest {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn get(mut self, key: impl Into<PrimaryKey>) -> Self {
        self.items.push(TransactGetItem::get(key));
        self
    }

    pub fn with_item(mut self, item: TransactGetItem) -> Self {
        self.items.push(item);
        self
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl From<Vec<TransactGetItem>> for TransactGetRequest {
    fn from(items: Vec<TransactGetItem>) -> Self {
        Self { items }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::attr;

    #[test]
    fn write_builder() {
        let request = TransactWriteRequest::new()
            .put(Item::new().with_s("pk", "test1"))
            .put_with_condition(Item::new().with_s("pk", "test2"), attr("pk").not_exists())
            .update(
                PrimaryKey::simple("test3"),
                UpdateExpression::new().set("name", "bob"),
            )
            .delete(PrimaryKey::simple("test4"))
            .condition_check(PrimaryKey::simple("test5"), attr("status").eq("active"));
        assert!(!request.is_empty());
        assert_eq!(request.len(), 5);
    }

    #[test]
    fn get_builder() {
        let request = TransactGetRequest::new()
            .get(PrimaryKey::simple("test0"))
            .get(PrimaryKey::simple("test1"))
            .get(PrimaryKey::simple("test2"));
        assert!(!request.is_empty());
        assert_eq!(request.len(), 3);
    }

    #[test]
    fn from_vec() {
        let items = vec![
            TransactWriteItem::put(Item::new().with_s("pk", "test1")),
            TransactWriteItem::put(Item::new().with_s("pk", "test2")),
            TransactWriteItem::delete(PrimaryKey::simple("test2")),
        ];
        let request: TransactWriteRequest = items.into();
        assert_eq!(request.len(), 3);

        let items = vec![
            TransactGetItem::get(PrimaryKey::simple("test0")),
            TransactGetItem::get(PrimaryKey::simple("test1")),
            TransactGetItem::get(PrimaryKey::simple("test2")),
        ];
        let request: TransactGetRequest = items.into();
        assert_eq!(request.len(), 3);
    }
}
