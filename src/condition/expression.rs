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

    pub fn ne(path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        Self::Compare {
            path: path.into(),
            op: CompareOp::Ne,
            value: value.into(),
        }
    }
    pub fn lt(path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        Self::Compare {
            path: path.into(),
            op: CompareOp::Lt,
            value: value.into(),
        }
    }
    pub fn le(path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        Self::Compare {
            path: path.into(),
            op: CompareOp::Le,
            value: value.into(),
        }
    }
    pub fn gt(path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        Self::Compare {
            path: path.into(),
            op: CompareOp::Gt,
            value: value.into(),
        }
    }
    pub fn ge(path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        Self::Compare {
            path: path.into(),
            op: CompareOp::Ge,
            value: value.into(),
        }
    }
    pub fn between(
        path: impl Into<AttributePath>,
        low: impl Into<AttributeValue>,
        high: impl Into<AttributeValue>,
    ) -> Self {
        Self::Between {
            path: path.into(),
            low: low.into(),
            high: high.into(),
        }
    }

    pub fn attr_exists(path: impl Into<AttributePath>) -> Self {
        Self::AttributeExists(path.into())
    }

    pub fn attr_not_exists(path: impl Into<AttributePath>) -> Self {
        Self::AttributeNotExists(path.into())
    }

    pub fn attr_type(path: impl Into<AttributePath>, attr_type: AttrType) -> Self {
        Self::AttributeType {
            path: path.into(),
            attribute_type: attr_type,
        }
    }

    pub fn begins_with(path: impl Into<AttributePath>, prefix: impl Into<AttributeValue>) -> Self {
        Self::BeginsWith {
            path: path.into(),
            prefix: prefix.into(),
        }
    }

    pub fn contains(path: impl Into<AttributePath>, op: impl Into<AttributeValue>) -> Self {
        Self::Contains {
            path: path.into(),
            operand: op.into(),
        }
    }

    pub fn size_eq(path: impl Into<AttributePath>, size: usize) -> Self {
        Self::Size {
            path: path.into(),
            op: CompareOp::Eq,
            value: size,
        }
    }

    pub fn size_gt(path: impl Into<AttributePath>, size: usize) -> Self {
        Self::Size {
            path: path.into(),
            op: CompareOp::Gt,
            value: size,
        }
    }

    pub fn size_lt(path: impl Into<AttributePath>, size: usize) -> Self {
        Self::Size {
            path: path.into(),
            op: CompareOp::Lt,
            value: size,
        }
    }

    pub fn not(self) -> Self {
        Self::Not(Box::new(self))
    }

    pub fn and(self, other: Condition) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Condition) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }
}

pub struct ConditionBuilder {
    path: AttributePath,
}

impl ConditionBuilder {
    pub fn new(path: impl Into<AttributePath>) -> Self {
        Self { path: path.into() }
    }

    pub fn exists(self) -> Condition {
        Condition::attr_exists(self.path)
    }

    pub fn not_exists(self) -> Condition {
        Condition::attr_not_exists(self.path)
    }

    pub fn eq(self, value: impl Into<AttributeValue>) -> Condition {
        Condition::eq(self.path, value)
    }

    pub fn ne(self, value: impl Into<AttributeValue>) -> Condition {
        Condition::ne(self.path, value)
    }

    pub fn lt(self, value: impl Into<AttributeValue>) -> Condition {
        Condition::lt(self.path, value)
    }

    pub fn le(self, value: impl Into<AttributeValue>) -> Condition {
        Condition::le(self.path, value)
    }

    pub fn gt(self, value: impl Into<AttributeValue>) -> Condition {
        Condition::gt(self.path, value)
    }

    pub fn ge(self, value: impl Into<AttributeValue>) -> Condition {
        Condition::ge(self.path, value)
    }

    pub fn between(
        self,
        low: impl Into<AttributeValue>,
        high: impl Into<AttributeValue>,
    ) -> Condition {
        Condition::between(self.path, low, high)
    }

    pub fn begins_with(self, prefix: impl Into<AttributeValue>) -> Condition {
        Condition::begins_with(self.path, prefix)
    }

    pub fn contains(self, operand: impl Into<AttributeValue>) -> Condition {
        Condition::contains(self.path, operand)
    }

    pub fn is_type(self, attr_type: AttrType) -> Condition {
        Condition::attr_type(self.path, attr_type)
    }

    pub fn size_eq(self, size: usize) -> Condition {
        Condition::size_eq(self.path, size)
    }

    pub fn size_gt(self, size: usize) -> Condition {
        Condition::size_gt(self.path, size)
    }

    pub fn size_lt(self, size: usize) -> Condition {
        Condition::size_lt(self.path, size)
    }
}

pub fn attr(path: impl Into<AttributePath>) -> ConditionBuilder {
    ConditionBuilder::new(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_operators() {
        let cond = attr("status")
            .eq("caffeinated")
            .and(attr("caffeine_content").ge(300i32));
        assert!(matches!(cond, Condition::And(_, _)));

        let cond = attr("role").eq("admin").or(attr("role").eq("manager"));
        assert!(matches!(cond, Condition::Or(_, _)));

        let cond = attr("deleted").exists().not();
        assert!(matches!(cond, Condition::Not(_)));
    }

    #[test]
    fn attr_type_condition() {
        let cond = attr("data").is_type(AttrType::Map);
        assert!(matches!(
            cond,
            Condition::AttributeType {
                attribute_type: AttrType::Map,
                ..
            }
        ));
    }

    #[test]
    fn size_condition() {
        let cond = attr("tags").size_gt(5);
        assert!(matches!(
            cond,
            Condition::Size {
                op: CompareOp::Gt,
                value: 5,
                ..
            }
        ));
    }

    mod builder {
        use super::*;

        #[test]
        fn creates_compare_conditions() {
            let cond = attr("status").eq("active");
            assert!(matches!(
                cond,
                Condition::Compare {
                    op: CompareOp::Eq,
                    ..
                }
            ));

            let cond = attr("tenure").gt(4i32);
            assert!(matches!(
                cond,
                Condition::Compare {
                    op: CompareOp::Gt,
                    ..
                }
            ));
        }

        #[test]
        fn creates_function_conditions() {
            let cond = attr("email").exists();
            assert!(matches!(cond, Condition::AttributeExists(_)));
            let cond = attr("email").not_exists();
            assert!(matches!(cond, Condition::AttributeNotExists(_)));
            let cond = attr("username").begins_with("AAA");
            assert!(matches!(cond, Condition::BeginsWith { .. }));
        }

        #[test]
        fn nested_path() {
            let path = AttributePath::new("address").key("city");
            let cond = ConditionBuilder::new(path).eq("Los Angeles");
            let Condition::Compare { path, .. } = cond else {
                panic!("expected compare condition");
            };

            assert_eq!(path.root(), Some("address"));
            assert_eq!(path.depth(), 2);
        }
    }
}
