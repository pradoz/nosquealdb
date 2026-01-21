use crate::types::{Item, PrimaryKey};

pub const MAX_BATCH_WRITE_ITEMS: usize = 25;
pub const MAX_BATCH_GET_ITEMS: usize = 100;

#[derive(Debug, Clone, PartialEq)]
pub enum BatchWriteItem {
    Put { item: Item },
    Delete { key: PrimaryKey },
}

impl BatchWriteItem {
    pub fn put(item: Item) -> Self {
        Self::Put { item }
    }
    pub fn delete(key: impl Into<PrimaryKey>) -> Self {
        Self::Delete { key: key.into() }
    }

    pub fn is_put(&self) -> bool {
        matches!(self, Self::Put { .. })
    }
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }
}

#[derive(Debug, Clone, Default)]
pub struct BatchWriteResult {
    pub processed_count: usize,
    pub unprocessed_items: Vec<BatchWriteItem>,
}

impl BatchWriteResult {
    pub fn new() -> Self {
        Self {
            processed_count: 0,
            unprocessed_items: Vec::new(),
        }
    }
    pub fn is_complete(&self) -> bool {
        self.unprocessed_items.is_empty()
    }
    pub fn has_unprocessed(&self) -> bool {
        !self.unprocessed_items.is_empty()
    }
    pub fn unprocessed_count(&self) -> usize {
        self.unprocessed_items.len()
    }
}

#[derive(Debug, Clone, Default)]
pub struct BatchGetResult {
    pub items: Vec<Item>,
    pub not_found_keys: Vec<PrimaryKey>,
    pub unprocessed_keys: Vec<PrimaryKey>,
}

impl BatchGetResult {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            not_found_keys: Vec::new(),
            unprocessed_keys: Vec::new(),
        }
    }
    pub fn is_complete(&self) -> bool {
        self.unprocessed_keys.is_empty()
    }
    pub fn has_unprocessed(&self) -> bool {
        !self.unprocessed_keys.is_empty()
    }
    pub fn found_count(&self) -> usize {
        self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_item() {
        let item = Item::new().with_s("pk", "test");

        let put = BatchWriteItem::put(item);
        assert!(put.is_put());
        assert!(!put.is_delete());

        let delete = BatchWriteItem::delete(PrimaryKey::simple("test"));
        assert!(!delete.is_put());
        assert!(delete.is_delete());
    }

    #[test]
    fn write_result() {
        let mut result = BatchWriteResult::new();
        assert!(result.is_complete());
        assert!(!result.has_unprocessed());
        assert_eq!(result.unprocessed_count(), 0);

        result
            .unprocessed_items
            .push(BatchWriteItem::put(Item::new().with_s("pk", "test")));
        assert!(!result.is_complete());
        assert!(result.has_unprocessed());
        assert_eq!(result.unprocessed_count(), 1);
    }

    #[test]
    fn get_result() {
        let mut result = BatchGetResult::new();
        assert!(result.is_complete());
        assert_eq!(result.found_count(), 0);

        result.items.push(Item::new().with_s("pk", "test"));
        assert_eq!(result.found_count(), 1);

        result.unprocessed_keys.push(PrimaryKey::simple("test"));
        assert!(!result.is_complete());
        assert!(result.has_unprocessed());
    }
}
