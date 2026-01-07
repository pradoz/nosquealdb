use std::collections::BTreeMap;

use super::{AttributeValue, KeySchema, KeyType, KeyValue, PrimaryKey};

#[derive(Debug, Clone)]
pub struct Item {
    attributes: BTreeMap<String, AttributeValue>,
}

impl Item {
    pub fn new() -> Self {
        Self {
            attributes: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    pub fn set(&mut self, key: impl Into<String>, value: AttributeValue) -> Option<AttributeValue> {
        self.attributes.insert(key.into(), value)
    }

    pub fn remove(&mut self, key: &str) -> Option<AttributeValue> {
        self.attributes.remove(key)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.attributes.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.attributes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.attributes.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.attributes.keys().map(|k| k.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &AttributeValue)> {
        self.attributes.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn into_inner(self) -> BTreeMap<String, AttributeValue> {
        self.attributes
    }

    pub fn extract_key(&self, schema: &KeySchema) -> Option<PrimaryKey> {
        let pk_attr = self.get(schema.pk_name())?;
        let pk = extract_key_value(pk_attr, schema.partition_key.key_type)?;

        let sk = if let Some(sk_attr_def) = &schema.sort_key {
            let sk_attr = self.get(&sk_attr_def.name)?;
            Some(extract_key_value(sk_attr, sk_attr_def.key_type)?)
        } else {
            None
        };

        Some(PrimaryKey { pk, sk })
    }

    pub fn validate_key(&self, schema: &KeySchema) -> Result<(), KeyValidationError> {
        validate_key_attribute(
            &self.attributes,
            &schema.partition_key.name,
            schema.partition_key.key_type,
            true,
        )?;

        if let Some(sk_def) = &schema.sort_key {
            validate_key_attribute(&self.attributes, &sk_def.name, sk_def.key_type, true)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValidationError {
    MissingAttribute {
        name: String,
    },
    TypeMismatch {
        name: String,
        expected: &'static str,
        actual: &'static str,
    },
}

impl std::fmt::Display for KeyValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAttribute { name } => {
                write!(f, "missing required key attribute: {}", name)
            }
            Self::TypeMismatch {
                name,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "key attribute '{}' has wrong type. expected: {}, got: {}",
                    name, expected, actual
                )
            }
        }
    }
}

impl std::error::Error for KeyValidationError {}

fn extract_key_value(attr: &AttributeValue, expected_type: KeyType) -> Option<KeyValue> {
    match (attr, expected_type) {
        (AttributeValue::S(s), KeyType::S) => Some(KeyValue::S(s.clone())),
        (AttributeValue::N(n), KeyType::N) => Some(KeyValue::N(n.clone())),
        (AttributeValue::B(b), KeyType::B) => Some(KeyValue::B(b.clone())),
        _ => None,
    }
}

fn validate_key_attribute(
    attributes: &BTreeMap<String, AttributeValue>,
    name: &str,
    expected_type: KeyType,
    required: bool,
) -> Result<(), KeyValidationError> {
    match attributes.get(name) {
        None if required => Err(KeyValidationError::MissingAttribute {
            name: name.to_string(),
        }),
        None => Ok(()),
        Some(attr) => {
            if expected_type.matches_attribute(attr) {
                Ok(())
            } else {
                Err(KeyValidationError::TypeMismatch {
                    name: name.to_string(),
                    expected: expected_type.as_str(),
                    actual: attr.type_name(),
                })
            }
        }
    }
}

// builder method... because rust ergonomics!
impl Item {
    pub fn with(mut self, name: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        self.attributes.insert(name.into(), value.into());
        self
    }

    pub fn with_s(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.with(name.into(), AttributeValue::S(value.into()))
    }

    pub fn with_n<N: ToString>(self, name: impl Into<String>, value: N) -> Self {
        self.with(name.into(), AttributeValue::N(value.to_string()))
    }

    pub fn with_b(self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.with(name.into(), AttributeValue::B(value.into()))
    }

    pub fn with_bool(self, name: impl Into<String>, value: bool) -> Self {
        self.with(name, AttributeValue::Bool(value))
    }

    pub fn with_null(self, name: impl Into<String>) -> Self {
        self.with(name, AttributeValue::Null)
    }
}

impl From<BTreeMap<String, AttributeValue>> for Item {
    fn from(map: BTreeMap<String, AttributeValue>) -> Self {
        Self { attributes: map }
    }
}

impl<const N: usize> From<[(&str, AttributeValue); N]> for Item {
    fn from(arr: [(&str, AttributeValue); N]) -> Self {
        Self {
            attributes: arr.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }
}

impl IntoIterator for Item {
    type Item = (String, AttributeValue);
    type IntoIter = std::collections::btree_map::IntoIter<String, AttributeValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.attributes.into_iter()
    }
}

impl<'a> IntoIterator for &'a Item {
    type Item = (&'a String, &'a AttributeValue);
    type IntoIter = std::collections::btree_map::Iter<'a, String, AttributeValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.attributes.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_schema() -> KeySchema {
        KeySchema::simple("user_id", KeyType::S)
    }

    fn composite_schema() -> KeySchema {
        KeySchema::composite("user_id", KeyType::S, "order_id", KeyType::S)
    }

    #[test]
    fn item_builder() {
        let item = Item::new()
            .with_s("name", "Username")
            .with_n("id", 42)
            .with_bool("active", true);

        assert_eq!(
            item.get("name"),
            Some(&AttributeValue::S("Username".into()))
        );
        assert_eq!(item.get("id"), Some(&AttributeValue::N("42".into())));
        assert_eq!(item.get("active"), Some(&AttributeValue::Bool(true)));
        assert_eq!(item.get("banana"), None);
    }

    mod extract_key {
        use super::*;

        #[test]
        fn simple() {
            let schema = simple_schema();
            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("name", "Username");

            let key = item.extract_key(&schema).unwrap();
            assert_eq!(key.pk, KeyValue::S("user123".into()));
            assert!(key.sk.is_none());
        }

        #[test]
        fn composite() {
            let schema = composite_schema();
            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("order_id", "order456")
                .with_n("total", 99);

            let key = item.extract_key(&schema).unwrap();
            assert_eq!(key.pk, KeyValue::S("user123".into()));
            assert_eq!(key.sk, Some(KeyValue::S("order456".into())));
        }

        #[test]
        fn missing_pk_is_none() {
            let schema = simple_schema();
            let item = Item::new().with_s("name", "Username");
            assert!(item.extract_key(&schema).is_none());
        }

        #[test]
        fn wrong_type_is_none() {
            let schema = simple_schema();
            let item = Item::new().with_n("user_id", 123);
            assert!(item.extract_key(&schema).is_none());
        }
    }
    mod validate_key {
        use super::*;

        #[test]
        fn simple() {
            let schema = simple_schema();
            let item = Item::new().with_s("user_id", "user123");
            assert!(item.validate_key(&schema).is_ok());
        }

        #[test]
        fn composite() {
            let schema = composite_schema();
            let item = Item::new()
                .with_s("user_id", "user123")
                .with_s("order_id", "order456");
            assert!(item.validate_key(&schema).is_ok());
        }

        #[test]
        fn missing_attribute() {
            let schema = composite_schema();
            let item = Item::new().with_s("user_id", "user123");
            let err = item.validate_key(&schema).unwrap_err();
            assert!(
                matches!(err, KeyValidationError::MissingAttribute { name } if name == "order_id")
            );
        }

        #[test]
        fn type_mismatch() {
            let schema = simple_schema();
            let item = Item::new().with_n("user_id", 123);
            let err = item.validate_key(&schema).unwrap_err();
            assert!(
                matches!(err, KeyValidationError::TypeMismatch { name, expected: "S", actual: "N" } if name == "user_id")
            );
        }
    }
}
