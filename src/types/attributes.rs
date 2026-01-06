use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, PartialEq)]
pub enum AttributeValue {
    // scalar types
    S(String),
    N(String),
    B(Vec<u8>),
    Bool(bool),
    Null,

    // document types
    M(BTreeMap<String, AttributeValue>),
    L(Vec<AttributeValue>),

    // set types
    Ss(BTreeSet<String>),
    Ns(BTreeSet<String>),
    Bs(BTreeSet<Vec<u8>>),
}

impl AttributeValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::S(_) => "S",
            Self::N(_) => "N",
            Self::B(_) => "B",
            Self::Bool(_) => "BOOL",
            Self::Null => "NULL",
            Self::M(_) => "M",
            Self::L(_) => "L",
            Self::Ss(_) => "SS",
            Self::Ns(_) => "NS",
            Self::Bs(_) => "BS",
        }
    }

    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::S(_) | Self::N(_) | Self::B(_) | Self::Bool(_) | Self::Null
        )
    }
    pub fn is_document(&self) -> bool {
        matches!(self, Self::M(_) | Self::L(_))
    }
    pub fn is_set(&self) -> bool {
        matches!(self, Self::Ss(_) | Self::Ns(_) | Self::Bs(_))
    }

    // scalar types
    pub fn as_s(&self) -> Option<&str> {
        match self {
            Self::S(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_n(&self) -> Option<&str> {
        match self {
            Self::N(n) => Some(n),
            _ => None,
        }
    }
    pub fn as_b(&self) -> Option<&[u8]> {
        match self {
            Self::B(b) => Some(b),
            _ => None,
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches![self, Self::Null]
    }

    // document types
    pub fn as_m(&self) -> Option<&BTreeMap<String, AttributeValue>> {
        match self {
            Self::M(m) => Some(m),
            _ => None,
        }
    }
    pub fn as_l(&self) -> Option<&Vec<AttributeValue>> {
        match self {
            Self::L(l) => Some(l),
            _ => None,
        }
    }

    // set types
    pub fn as_ss(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Ss(ss) => Some(ss),
            _ => None,
        }
    }
    pub fn as_ns(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Ns(ns) => Some(ns),
            _ => None,
        }
    }
    pub fn as_bs(&self) -> Option<&BTreeSet<Vec<u8>>> {
        match self {
            Self::Bs(bs) => Some(bs),
            _ => None,
        }
    }
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        Self::S(s)
    }
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        Self::S(s.to_string())
    }
}

impl From<bool> for AttributeValue {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<Vec<u8>> for AttributeValue {
    fn from(b: Vec<u8>) -> Self {
        Self::B(b.to_vec())
    }
}

impl From<&[u8]> for AttributeValue {
    fn from(b: &[u8]) -> Self {
        Self::B(b.to_vec())
    }
}

// number conversions use string representation
macro_rules! impl_from_number {
    ($($t:ty),*) => {
        $(
            impl From<$t> for AttributeValue {
                fn from(n: $t) -> Self {
                    Self::N(n.to_string())
                }
            }
        )*
    };
}
impl_from_number!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64
);

impl From<Vec<AttributeValue>> for AttributeValue {
    fn from(l: Vec<AttributeValue>) -> Self {
        Self::L(l)
    }
}

impl From<BTreeMap<String, AttributeValue>> for AttributeValue {
    fn from(m: BTreeMap<String, AttributeValue>) -> Self {
        Self::M(m)
    }
}

impl<const N: usize> From<[(&str, AttributeValue); N]> for AttributeValue {
    fn from(arr: [(&str, AttributeValue); N]) -> Self {
        Self::M(arr.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }
}
