use crate::condition::Condition;
use crate::query::{KeyCondition, QueryOptions};
use crate::types::{Item, PrimaryKey, ReturnValue};
use crate::update::UpdateExpression;

#[derive(Debug, Clone)]
pub struct PutRequest {
    pub(crate) item: Item,
    pub(crate) condition: Option<Condition>,
    pub(crate) return_value: ReturnValue,
    pub(crate) if_not_exists: bool,
}

impl PutRequest {
    pub fn new(item: Item) -> Self {
        Self {
            item,
            condition: None,
            return_value: ReturnValue::None,
            if_not_exists: false,
        }
    }

    pub fn condition(mut self, condition: Condition) -> Self {
        self.condition = Some(condition);
        self
    }

    pub fn condition_if(mut self, condition: Option<Condition>) -> Self {
        self.condition = condition;
        self
    }

    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    pub fn return_old(mut self) -> Self {
        self.return_value = ReturnValue::AllOld;
        self
    }

    pub fn return_new(mut self) -> Self {
        self.return_value = ReturnValue::AllNew;
        self
    }

    pub fn return_value(mut self, rv: ReturnValue) -> Self {
        self.return_value = rv;
        self
    }
}

impl From<Item> for PutRequest {
    fn from(item: Item) -> Self {
        Self::new(item)
    }
}

#[derive(Debug, Clone)]
pub struct UpdateRequest {
    pub(crate) key: PrimaryKey,
    pub(crate) expression: UpdateExpression,
    pub(crate) condition: Option<Condition>,
    pub(crate) return_value: ReturnValue,
}

impl UpdateRequest {
    pub fn new(key: impl Into<PrimaryKey>, expression: UpdateExpression) -> Self {
        Self {
            key: key.into(),
            expression,
            condition: None,
            return_value: ReturnValue::AllNew,
        }
    }

    pub fn condition(mut self, condition: Condition) -> Self {
        self.condition = Some(condition);
        self
    }

    pub fn condition_if(mut self, condition: Option<Condition>) -> Self {
        self.condition = condition;
        self
    }

    pub fn return_none(mut self) -> Self {
        self.return_value = ReturnValue::None;
        self
    }

    pub fn return_old(mut self) -> Self {
        self.return_value = ReturnValue::AllOld;
        self
    }

    pub fn return_new(mut self) -> Self {
        self.return_value = ReturnValue::AllNew;
        self
    }

    pub fn return_value(mut self, rv: ReturnValue) -> Self {
        self.return_value = rv;
        self
    }
}

#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub(crate) key: PrimaryKey,
    pub(crate) condition: Option<Condition>,
    pub(crate) return_value: ReturnValue,
}

impl DeleteRequest {
    pub fn new(key: impl Into<PrimaryKey>) -> Self {
        Self {
            key: key.into(),
            condition: None,
            return_value: ReturnValue::None,
        }
    }

    pub fn condition(mut self, condition: Condition) -> Self {
        self.condition = Some(condition);
        self
    }

    pub fn condition_if(mut self, condition: Option<Condition>) -> Self {
        self.condition = condition;
        self
    }

    pub fn return_old(mut self) -> Self {
        self.return_value = ReturnValue::AllOld;
        self
    }

    pub fn return_new(mut self) -> Self {
        self.return_value = ReturnValue::AllNew;
        self
    }

    pub fn return_value(mut self, rv: ReturnValue) -> Self {
        self.return_value = rv;
        self
    }
}

impl From<PrimaryKey> for DeleteRequest {
    fn from(key: PrimaryKey) -> Self {
        Self::new(key)
    }
}

#[derive(Debug, Clone)]
pub struct GetRequest {
    pub(crate) key: PrimaryKey,
    pub(crate) projection: Option<Vec<String>>,
}

impl GetRequest {
    pub fn new(key: impl Into<PrimaryKey>) -> Self {
        Self {
            key: key.into(),
            projection: None,
        }
    }

    pub fn project<I, S>(mut self, attrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(attrs.into_iter().map(Into::into).collect());
        self
    }
}

impl From<PrimaryKey> for GetRequest {
    fn from(key: PrimaryKey) -> Self {
        Self::new(key)
    }
}

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub(crate) key_condition: KeyCondition,
    pub(crate) filter: Option<Condition>,
    pub(crate) options: QueryOptions,
}

impl QueryRequest {
    pub fn new(key_condition: KeyCondition) -> Self {
        Self {
            key_condition,
            filter: None,
            options: QueryOptions::new(),
        }
    }

    pub fn filter(mut self, filter: Condition) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn filter_if(mut self, filter: Option<Condition>) -> Self {
        self.filter = filter;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.options = self.options.with_limit(limit);
        self
    }

    pub fn reverse(mut self) -> Self {
        self.options = self.options.reverse();
        self
    }

    pub fn forward(mut self) -> Self {
        self.options = self.options.forward();
        self
    }

    pub fn options(mut self, options: QueryOptions) -> Self {
        self.options = options;
        self
    }
}

impl From<KeyCondition> for QueryRequest {
    fn from(key_condition: KeyCondition) -> Self {
        Self::new(key_condition)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    pub(crate) filter: Option<Condition>,
    pub(crate) limit: Option<usize>,
}

impl ScanRequest {
    pub fn new() -> Self {
        Self {
            filter: None,
            limit: None,
        }
    }

    pub fn filter(mut self, filter: Condition) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn filter_if(mut self, filter: Option<Condition>) -> Self {
        self.filter = filter;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::attr;

    #[test]
    fn put() {
        let item = Item::new().with_s("pk", "test");

        let req = PutRequest::new(item.clone());
        assert!(req.condition.is_none());
        assert!(!req.if_not_exists);
        assert_eq!(req.return_value, ReturnValue::None);

        let req = PutRequest::new(item.clone())
            .condition(attr("version").eq(1i32))
            .return_old()
            .if_not_exists();
        assert!(req.condition.is_some());
        assert!(req.if_not_exists);
        assert_eq!(req.return_value, ReturnValue::AllOld);
    }

    #[test]
    fn update() {
        let key = PrimaryKey::simple("test");
        let expr = UpdateExpression::new().set("name", "Alice");

        let req = UpdateRequest::new(key.clone(), expr.clone());
        assert!(req.condition.is_none());
        assert_eq!(req.return_value, ReturnValue::AllNew);

        let req = UpdateRequest::new(key.clone(), expr.clone())
            .condition(attr("status").eq("active"))
            .return_old();
        assert!(req.condition.is_some());
        assert_eq!(req.return_value, ReturnValue::AllOld);
    }

    #[test]
    fn delete() {
        let key = PrimaryKey::simple("test");

        let req = DeleteRequest::new(key.clone());
        assert!(req.condition.is_none());
        assert_eq!(req.return_value, ReturnValue::None);

        let req = DeleteRequest::new(key.clone())
            .condition(attr("locked").eq(false))
            .return_old();
        assert!(req.condition.is_some());
        assert_eq!(req.return_value, ReturnValue::AllOld);
    }

    #[test]
    fn query() {
        let cond = KeyCondition::pk("user1");

        let req = QueryRequest::new(cond.clone());
        assert!(req.filter.is_none());
        assert!(req.options.limit.is_none());
        assert!(req.options.scan_forward);

        let req = QueryRequest::new(cond.clone())
            .filter(attr("status").eq("active"))
            .limit(10)
            .reverse();
        assert!(req.filter.is_some());
        assert!(req.options.limit.is_some());
        assert_eq!(req.options.limit, Some(10));
        assert!(!req.options.scan_forward);
    }

    #[test]
    fn scan() {
        let req = ScanRequest::new();
        assert!(req.limit.is_none());
        assert!(req.filter.is_none());

        let req = ScanRequest::new()
            .filter(attr("status").eq("active"))
            .limit(5);
        assert!(req.limit.is_some());
        assert_eq!(req.limit, Some(5));
        assert!(req.filter.is_some());
    }
}
