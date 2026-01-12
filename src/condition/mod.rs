mod eval;
mod expression;
mod path;

pub use eval::evaluate;
pub use expression::{AttrType, CompareOp, Condition, ConditionBuilder, attr};
pub use path::{AttributePath, PathSegment};
