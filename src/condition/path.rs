use std::str;

use crate::types::{AttributeValue, Item};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributePath {
    segments: Vec<PathSegment>,
}

impl AttributePath {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            segments: vec![PathSegment::Key(name.into())],
        }
    }

    pub fn empty() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn key(mut self, name: impl Into<String>) -> Self {
        self.segments.push(PathSegment::Key(name.into()));
        self
    }

    pub fn index(mut self, i: usize) -> Self {
        self.segments.push(PathSegment::Index(i));
        self
    }

    pub fn root(&self) -> Option<&str> {
        match self.segments.first() {
            Some(PathSegment::Key(k)) => Some(k),
            _ => None,
        }
    }

    pub fn is_simple(&self) -> bool {
        self.segments.len() == 1 && matches!(self.segments.first(), Some(PathSegment::Key(_)))
    }

    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }

    pub fn resolve<'a>(&self, item: &'a Item) -> Option<&'a AttributeValue> {
        if self.segments.is_empty() {
            return None;
        }

        let root_key = match &self.segments[0] {
            PathSegment::Key(k) => k,
            PathSegment::Index(_) => return None,
        };

        let mut current = item.get(root_key)?;
        for segment in &self.segments[1..] {
            current = match (segment, current) {
                (PathSegment::Key(k), AttributeValue::M(map)) => map.get(k)?,
                (PathSegment::Index(i), AttributeValue::L(list)) => list.get(*i)?,
                _ => return None,
            };
        }

        Some(current)
    }
}

// TODO: do we need these?
// currently using a cleaner API than legacy string format
impl From<&str> for AttributePath {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for AttributePath {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod resolve {
        use super::*;
        use std::collections::BTreeMap;

        #[test]
        fn simple() {
            let item = Item::new().with_s("pk", "id");
            let path = AttributePath::new("pk");
            let value = path.resolve(&item);
            assert_eq!(value, Some(&AttributeValue::S("id".into())));
        }

        #[test]
        fn nested() {
            let mut map = BTreeMap::new();
            map.insert("city".to_string(), AttributeValue::S("Newton Falls".into()));
            map.insert("zip".to_string(), AttributeValue::N("44444".into()));

            let item = Item::new().with_s("name", "zach").with_m("address", map);

            let path = AttributePath::new("address").key("city");
            let value = path.resolve(&item);
            assert_eq!(value, Some(&AttributeValue::S("Newton Falls".into())));
            let path = AttributePath::new("address").key("zip");
            let value = path.resolve(&item);
            assert_eq!(value, Some(&AttributeValue::N("44444".into())));
        }

        #[test]
        fn deeply_nested() {
            let mut inner = BTreeMap::new();
            let mut outer = BTreeMap::new();
            inner.insert("value".to_string(), AttributeValue::N("42".into()));

            let list = AttributeValue::L(vec![AttributeValue::M(inner)]);
            outer.insert("items".to_string(), list);

            let item = Item::new().with_m("data", outer);
            let path = AttributePath::new("data")
                .key("items")
                .index(0)
                .key("value");
            let value = path.resolve(&item);
            assert_eq!(value, Some(&AttributeValue::N("42".into())));
        }

        #[test]
        fn with_index() {
            let item = Item::new().with_l(
                "tags",
                vec![
                    AttributeValue::S("rust".into()),
                    AttributeValue::S("database".into()),
                ],
            );

            let path = AttributePath::new("tags").index(0);
            let value = path.resolve(&item);
            assert_eq!(value, Some(&AttributeValue::S("rust".into())));
            let path = AttributePath::new("tags").index(1);
            let value = path.resolve(&item);
            assert_eq!(value, Some(&AttributeValue::S("database".into())));
        }

        #[test]
        fn missing_returns_none() {
            let item = Item::new().with_s("username", "zach");
            assert!(AttributePath::new("missing").resolve(&item).is_none());
        }

        #[test]
        fn wrong_type_returns_none() {
            let item = Item::new().with_s("username", "zach");

            // index on string
            assert!(
                AttributePath::new("username")
                    .index(0)
                    .resolve(&item)
                    .is_none()
            );
            // key lookup on string
            assert!(
                AttributePath::new("username")
                    .key("nested")
                    .resolve(&item)
                    .is_none()
            );
        }

        #[test]
        fn out_of_bounds_returns_none() {
            let item = Item::new().with_l("list", vec![AttributeValue::N("0".into())]);
            assert!(AttributePath::new("list").index(1).resolve(&item).is_none());
        }
    }
}
