use super::types::{BatchWriteItem, MAX_BATCH_GET_ITEMS, MAX_BATCH_WRITE_ITEMS};
use crate::types::{Item, PrimaryKey};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchWriteRequest {
    pub(crate) items: Vec<BatchWriteItem>,
}

impl BatchWriteRequest {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn put(mut self, item: Item) -> Self {
        self.items.push(BatchWriteItem::put(item));
        self
    }
    pub fn put_many(mut self, items: impl IntoIterator<Item = Item>) -> Self {
        for item in items {
            self.items.push(BatchWriteItem::put(item));
        }
        self
    }

    pub fn delete(mut self, key: PrimaryKey) -> Self {
        self.items.push(BatchWriteItem::delete(key));
        self
    }
    pub fn delete_many(mut self, keys: impl IntoIterator<Item = PrimaryKey>) -> Self {
        for key in keys {
            self.items.push(BatchWriteItem::delete(key));
        }
        self
    }

    pub fn with_item(mut self, item: BatchWriteItem) -> Self {
        self.items.push(item);
        self
    }

    pub fn exceeds_limit(&self) -> bool {
        self.items.len() > MAX_BATCH_WRITE_ITEMS
    }

    pub fn into_chunks(self) -> Vec<BatchWriteRequest> {
        self.items
            .chunks(MAX_BATCH_WRITE_ITEMS)
            .map(|chunk| BatchWriteRequest {
                items: chunk.to_vec(),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl From<Vec<BatchWriteItem>> for BatchWriteRequest {
    fn from(items: Vec<BatchWriteItem>) -> Self {
        Self { items }
    }
}

impl From<Vec<Item>> for BatchWriteRequest {
    fn from(items: Vec<Item>) -> Self {
        Self {
            items: items.into_iter().map(BatchWriteItem::put).collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BatchGetRequest {
    pub(crate) keys: Vec<PrimaryKey>,
}

impl BatchGetRequest {
    pub fn new() -> Self {
        Self { keys: Vec::new() }
    }

    pub fn get(mut self, key: impl Into<PrimaryKey>) -> Self {
        self.keys.push(key.into());
        self
    }
    pub fn get_many(mut self, keys: impl IntoIterator<Item = PrimaryKey>) -> Self {
        self.keys.extend(keys);
        self
    }

    pub fn exceeds_limit(&self) -> bool {
        self.keys.len() > MAX_BATCH_GET_ITEMS
    }

    pub fn into_chunks(self) -> Vec<BatchGetRequest> {
        self.keys
            .chunks(MAX_BATCH_GET_ITEMS)
            .map(|chunk| BatchGetRequest {
                keys: chunk.to_vec(),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl From<Vec<PrimaryKey>> for BatchGetRequest {
    fn from(keys: Vec<PrimaryKey>) -> Self {
        Self { keys }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_builder() {
        let req = BatchWriteRequest::new()
            .put(Item::new().with_s("pk", "test1"))
            .put(Item::new().with_s("pk", "test2"))
            .delete(PrimaryKey::simple("item3"));
        assert!(!req.is_empty());
        assert_eq!(req.len(), 3);
        assert!(!req.exceeds_limit());
    }

    #[test]
    fn write_many() {
        let items = vec![
            Item::new().with_s("pk", "test1"),
            Item::new().with_s("pk", "test2"),
        ];

        let req = BatchWriteRequest::new().put_many(items);
        assert!(!req.is_empty());
        assert_eq!(req.len(), 2);
        assert!(!req.exceeds_limit());
    }

    #[test]
    fn write_chunks() {
        let mut req = BatchWriteRequest::new();

        for i in 0..(MAX_BATCH_WRITE_ITEMS + 1) {
            req = req.put(Item::new().with_s("pk", format!("test{}", i)));
        }
        assert!(req.exceeds_limit());

        let chunks = req.into_chunks();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), MAX_BATCH_WRITE_ITEMS);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn write_from_items() {
        let items = vec![
            Item::new().with_s("pk", "test1"),
            Item::new().with_s("pk", "test2"),
        ];

        let req: BatchWriteRequest = items.into();
        assert!(!req.is_empty());
        assert_eq!(req.len(), 2);
        assert!(!req.exceeds_limit());
    }

    #[test]
    fn get_builder() {
        let req = BatchGetRequest::new()
            .get(PrimaryKey::simple("test1"))
            .get(PrimaryKey::simple("test2"))
            .get(PrimaryKey::simple("test3"));
        assert!(!req.is_empty());
        assert_eq!(req.len(), 3);
        assert!(!req.exceeds_limit());
    }

    #[test]
    fn get_many() {
        let keys = vec![PrimaryKey::simple("test1"), PrimaryKey::simple("test2")];

        let req = BatchGetRequest::new().get_many(keys);
        assert!(!req.is_empty());
        assert_eq!(req.len(), 2);
        assert!(!req.exceeds_limit());
    }

    #[test]
    fn get_chunks() {
        let mut req = BatchGetRequest::new();

        for i in 0..(MAX_BATCH_GET_ITEMS + 1) {
            req = req.get(PrimaryKey::simple(format!("test{}", i)));
        }
        assert!(req.exceeds_limit());

        let chunks = req.into_chunks();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), MAX_BATCH_GET_ITEMS);
        assert_eq!(chunks[1].len(), 1);
    }
}
