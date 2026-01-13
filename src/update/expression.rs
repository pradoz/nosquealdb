use crate::condition::AttributePath;
use crate::types::AttributeValue;

#[derive(Debug, Clone)]
pub enum UpdateAction {
    Set {
        path: AttributePath,
        value: AttributeValue,
    },
    SetIfNotExists {
        path: AttributePath,
        value: AttributeValue,
    },
    Remove {
        path: AttributePath,
    },
    Add {
        path: AttributePath,
        value: AttributeValue,
    },
    Delete {
        path: AttributePath,
        value: AttributeValue,
    },
}

#[derive(Debug, Default, Clone)]
pub struct UpdateExpression {
    actions: Vec<UpdateAction>,
}

impl UpdateExpression {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    pub fn set(mut self, path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        self.actions.push(UpdateAction::Set {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn set_if_not_exists(
        mut self,
        path: impl Into<AttributePath>,
        value: impl Into<AttributeValue>,
    ) -> Self {
        self.actions.push(UpdateAction::SetIfNotExists {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn remove(mut self, path: impl Into<AttributePath>) -> Self {
        self.actions
            .push(UpdateAction::Remove { path: path.into() });
        self
    }

    pub fn add(mut self, path: impl Into<AttributePath>, value: impl Into<AttributeValue>) -> Self {
        self.actions.push(UpdateAction::Add {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn delete(
        mut self,
        path: impl Into<AttributePath>,
        value: impl Into<AttributeValue>,
    ) -> Self {
        self.actions.push(UpdateAction::Delete {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn with_action(mut self, action: UpdateAction) -> Self {
        self.actions.push(action);
        self
    }

    pub fn actions(&self) -> &[UpdateAction] {
        &self.actions
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }
}

pub fn update() -> UpdateExpression {
    UpdateExpression::new()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty() {
        let expr = update();
        assert!(expr.is_empty());
        assert_eq!(expr.len(), 0);
    }

    #[test]
    fn builder() {
        let expr = update()
            .set("name", "Alice")
            .set("id", 42i32)
            .remove("foo")
            .add("count", 100i32);
        assert!(!expr.is_empty());
        assert_eq!(expr.len(), 4);
    }
}
