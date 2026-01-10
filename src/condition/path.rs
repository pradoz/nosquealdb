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
        Self { segments: Vec::new() }
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


mod tests {
    use super::*;

    #[test]
    fn simple() {
    }

    #[test]
    fn nested() {
    }

    #[test]
    fn with_index() {
    }

    mod resolve {
        use super::*;
        use std::collections::BTreeMap;

        #[test]
        fn simple() {
        }

        #[test]
        fn nested() {
        }

        #[test]
        fn deeply_nested() {
        }

        #[test]
        fn with_index() {
        }

        #[test]
        fn missing_returns_none() {
        }

        #[test]
        fn wrong_type_returns_none() {
        }

        #[test]
        fn out_of_bounds_returns_none() {
        }
    }
}
