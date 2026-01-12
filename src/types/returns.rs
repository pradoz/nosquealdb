use super::Item;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReturnValue {
    #[default] // return nothing by default
    None,
    AllOld,
    AllNew,
}

#[derive(Debug, Clone)]
pub struct WriteResult {
    pub attributes: Option<Item>,
    pub was_update: bool,
}

impl WriteResult {
    pub fn none() -> Self {
        Self {
            attributes: None,
            was_update: false,
        }
    }
    pub fn created() -> Self {
        Self {
            attributes: None,
            was_update: false,
        }
    }
    pub fn updated() -> Self {
        Self {
            attributes: None,
            was_update: true,
        }
    }
    pub fn with_attributes(mut self, item: Option<Item>) -> Self {
        self.attributes = item;
        self
    }
}
