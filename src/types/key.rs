use super::AttributeValue;
use crate::utils::base64_encode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValue {
    S(String),
    N(String),
    B(Vec<u8>),
}

impl KeyValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            KeyValue::S(_) => "S",
            KeyValue::N(_) => "N",
            KeyValue::B(_) => "B",
        }
    }

    pub fn as_s(&self) -> Option<&str> {
        match self {
            KeyValue::S(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_n(&self) -> Option<&str> {
        match self {
            KeyValue::N(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_b(&self) -> Option<&[u8]> {
        match self {
            KeyValue::B(b) => Some(b),
            _ => None,
        }
    }

    pub fn to_attribute_value(&self) -> AttributeValue {
        match self {
            Self::S(s) => AttributeValue::S(s.clone()),
            Self::N(n) => AttributeValue::N(n.clone()),
            Self::B(b) => AttributeValue::B(b.clone()),
        }
    }

    pub fn from_attribute_value(av: &AttributeValue) -> Option<Self> {
        match av {
            AttributeValue::S(s) => Some(Self::S(s.clone())),
            AttributeValue::N(n) => Some(Self::N(n.clone())),
            AttributeValue::B(b) => Some(Self::B(b.clone())),
            _ => None,
        }
    }

    pub fn from_attribute_with_type(attr: &AttributeValue, expected: KeyType) -> Option<Self> {
        match (attr, expected) {
            (AttributeValue::S(s), KeyType::S) => Some(Self::S(s.clone())),
            (AttributeValue::N(n), KeyType::N) => Some(Self::N(n.clone())),
            (AttributeValue::B(b), KeyType::B) => Some(Self::B(b.clone())),
            _ => None,
        }
    }
}

impl From<String> for KeyValue {
    fn from(s: String) -> Self {
        Self::S(s)
    }
}
impl From<&str> for KeyValue {
    fn from(s: &str) -> Self {
        Self::S(s.to_string())
    }
}
impl From<Vec<u8>> for KeyValue {
    fn from(b: Vec<u8>) -> Self {
        Self::B(b)
    }
}
impl From<&[u8]> for KeyValue {
    fn from(b: &[u8]) -> Self {
        Self::B(b.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKey {
    pub pk: KeyValue,
    pub sk: Option<KeyValue>,
}

impl PrimaryKey {
    pub fn simple(pk: impl Into<KeyValue>) -> Self {
        Self {
            pk: pk.into(),
            sk: None,
        }
    }

    pub fn composite(pk: impl Into<KeyValue>, sk: impl Into<KeyValue>) -> Self {
        Self {
            pk: pk.into(),
            sk: Some(sk.into()),
        }
    }

    pub fn has_sort_key(&self) -> bool {
        self.sk.is_some()
    }

    pub fn to_storage_key(&self) -> String {
        let pk_part = encode_key_component(&self.pk);
        match &self.sk {
            Some(sk) => {
                let sk_part = encode_key_component(sk);
                format!("{}#{}", pk_part, sk_part)
            }
            None => pk_part,
        }
    }
}

fn encode_key_component(key: &KeyValue) -> String {
    match key {
        KeyValue::S(s) => format!("S:{}", escape_key_chars(s)),
        KeyValue::N(n) => format!("N:{}", n),
        KeyValue::B(b) => format!("B:{}", base64_encode(b)),
    }
}

fn escape_key_chars(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '#' => result.push_str("\\#"),
            ':' => result.push_str("\\:"),
            '\\' => result.push_str("\\\\"),
            _ => result.push(c),
        }
    }
    result
}

#[derive(Debug, Clone, Copy)]
pub enum KeyType {
    S,
    N,
    B,
}

#[derive(Debug, Clone)]
pub struct KeyAttribute {
    pub name: String,
    pub key_type: KeyType,
}

#[derive(Debug, Clone)]
pub struct KeySchema {
    pub partition_key: KeyAttribute,
    pub sort_key: Option<KeyAttribute>,
}

impl KeyAttribute {
    pub fn new(name: impl Into<String>, key_type: KeyType) -> Self {
        Self {
            name: name.into(),
            key_type,
        }
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, KeyType::S)
    }
    pub fn number(name: impl Into<String>) -> Self {
        Self::new(name, KeyType::N)
    }
    pub fn binary(name: impl Into<String>) -> Self {
        Self::new(name, KeyType::B)
    }
}

impl KeyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::S => "S",
            KeyType::N => "N",
            KeyType::B => "B",
        }
    }

    pub fn matches(&self, value: &KeyValue) -> bool {
        matches!(
            (self, value),
            (KeyType::S, KeyValue::S(_))
                | (KeyType::N, KeyValue::N(_))
                | (KeyType::B, KeyValue::B(_))
        )
    }

    pub fn matches_attribute(&self, value: &AttributeValue) -> bool {
        matches!(
            (self, value),
            (KeyType::S, AttributeValue::S(_))
                | (KeyType::N, AttributeValue::N(_))
                | (KeyType::B, AttributeValue::B(_))
        )
    }
}

impl KeySchema {
    pub fn simple(pk_name: impl Into<String>, pk_type: KeyType) -> Self {
        Self {
            partition_key: KeyAttribute::new(pk_name, pk_type),
            sort_key: None,
        }
    }
    pub fn composite(
        pk_name: impl Into<String>,
        pk_type: KeyType,
        sk_name: impl Into<String>,
        sk_type: KeyType,
    ) -> Self {
        Self {
            partition_key: KeyAttribute::new(pk_name, pk_type),
            sort_key: Some(KeyAttribute::new(sk_name, sk_type)),
        }
    }

    pub fn has_sort_key(&self) -> bool {
        self.sort_key.is_some()
    }
    pub fn pk_name(&self) -> &str {
        &self.partition_key.name
    }
    pub fn sk_name(&self) -> Option<&str> {
        self.sort_key.as_ref().map(|sk| sk.name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_type_maching() {
        assert!(KeyType::S.matches(&KeyValue::S("hello".into())));
        assert!(!KeyType::S.matches(&KeyValue::N("123".into())));
        assert!(KeyType::N.matches(&KeyValue::N("123".into())));
        assert!(KeyType::B.matches(&KeyValue::B(vec![1, 2, 3].into())));
    }

    #[test]
    fn key_value_from_attribute() {
        // valid conversions
        assert_eq!(
            KeyValue::from_attribute_value(&AttributeValue::S("foo".into())),
            Some(KeyValue::S("foo".into()))
        );
        assert_eq!(
            KeyValue::from_attribute_value(&AttributeValue::N("123".into())),
            Some(KeyValue::N("123".into()))
        );

        // invalid conversions
        assert_eq!(KeyValue::from_attribute_value(&AttributeValue::Null), None);
        assert_eq!(
            KeyValue::from_attribute_value(&AttributeValue::Bool(true)),
            None
        );
        assert_eq!(
            KeyValue::from_attribute_value(&AttributeValue::L(vec![])),
            None
        );
    }

    mod storage_key {
        use super::*;

        #[test]
        fn simple() {
            let pk = PrimaryKey::simple("user123");
            assert_eq!(pk.to_storage_key(), "S:user123");

            let pk = PrimaryKey::simple(KeyValue::N("123".into()));
            assert_eq!(pk.to_storage_key(), "N:123");
        }

        #[test]
        fn composite() {
            let pk = PrimaryKey::composite("user123", "order456");
            assert_eq!(pk.to_storage_key(), "S:user123#S:order456");

            let pk = PrimaryKey::composite("user123", KeyValue::N("456".into()));
            assert_eq!(pk.to_storage_key(), "S:user123#N:456");
        }

        #[test]
        fn binary() {
            let pk = PrimaryKey::simple(KeyValue::B(vec![0x00, 0x01, 0x02]));
            // base64 of [0, 1, 2] == "AAEC"
            assert_eq!(pk.to_storage_key(), "B:AAEC");
        }

        #[test]
        fn special_chars() {
            let pk = PrimaryKey::simple("user#123:woot");
            assert_eq!(pk.to_storage_key(), "S:user\\#123\\:woot");
        }
    }
}
