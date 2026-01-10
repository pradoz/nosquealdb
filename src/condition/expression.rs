use super::path::AttributePath;
use crate::types::AttributeValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrType {
    String,
    Number,
    Binary,
    Boolean,
    Null,
    Map,
    List,
    StringSet,
    NumberSet,
    BinarySet,
}

impl AttrType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttrType::String => "S",
            AttrType::Number => "N",
            AttrType::Binary => "B",
            AttrType::Boolean => "BOOL",
            AttrType::Null => "NULL",
            AttrType::Map => "M",
            AttrType::List => "L",
            AttrType::StringSet => "SS",
            AttrType::NumberSet => "NS",
            AttrType::BinarySet => "BS",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Condition {
    Compare {
        path: AttributePath,
        op: CompareOp,
        value: AttributeValue,
    },
    Between {
        path: AttributePath,
        low: AttributeValue,
        high: AttributeValue,
    },
    AttributeExists(AttributePath),
    AttributeNotExists(AttributePath),
    BeginsWith {
        path: AttributePath,
        prefix: AttributeValue,
    },
    Contains {
        path: AttributePath,
        operand: AttributeValue,
    },
    AttributeType {
        path: AttributePath,
        attribute_type: AttrType,
    },
    Size {
        path: AttributePath,
        op: CompareOp,
        value: usize,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}

impl Condition {
    pub fn eq(path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        Self::Compare {
            path: path.into(),
            op: CompareOp::Eq,
            value: value.into(),
        }
    }
}
