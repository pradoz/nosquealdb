use std::collections::HashSet;

use super::types::{TransactGetItem, TransactGetResult, TransactWriteItem};
use crate::condition::evaluate;
use crate::error::TableResult;
use crate::types::{Item, KeySchema, PrimaryKey};
use crate::update::UpdateExecutor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionFailureReason {
    ConditionCheckFailed { index: usize },
    ItemNotFound { index: usize },
    KeyModification { index: usize },
    DuplicateItem { index: usize },
    InvalidKey { index: usize, message: String },
}

impl TransactionFailureReason {
    pub fn index(&self) -> usize {
        match self {
            Self::ConditionCheckFailed { index } => *index,
            Self::ItemNotFound { index } => *index,
            Self::KeyModification { index } => *index,
            Self::DuplicateItem { index } => *index,
            Self::InvalidKey { index, .. } => *index,
        }
    }
}

impl std::fmt::Display for TransactionFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConditionCheckFailed { index } => {
                write!(f, "condition check failed at index {}", index)
            }
            Self::ItemNotFound { index } => {
                write!(f, "item not found at index {}", index)
            }
            Self::KeyModification { index } => {
                write!(f, "cannot modify key attributes at index {}", index)
            }
            Self::DuplicateItem { index } => {
                write!(f, "duplicate item at index {}", index)
            }
            Self::InvalidKey { index, message } => {
                write!(f, "invalid key at index {}: {}", index, message)
            }
        }
    }
}

pub struct TransactionExecutor;

impl TransactionExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn validate_write(
        &self,
        items: &[TransactWriteItem],
        schema: &KeySchema,
        get_item: impl Fn(&PrimaryKey) -> TableResult<Option<Item>>,
    ) -> Result<(), TransactionFailureReason> {
        let mut seen = HashSet::new();

        for (index, item) in items.iter().enumerate() {
            let key = self.extract_key(item, schema, index)?;
            let key_str = key.to_storage_key();

            if seen.contains(&key_str) {
                return Err(TransactionFailureReason::DuplicateItem { index });
            }
            seen.insert(key_str);
            self.validate_write_item(item, &key, schema, index, &get_item)?;
        }

        Ok(())
    }

    fn extract_key(
        &self,
        item: &TransactWriteItem,
        schema: &KeySchema,
        index: usize,
    ) -> Result<PrimaryKey, TransactionFailureReason> {
        match item {
            TransactWriteItem::Put { item, .. } => {
                item.extract_key(schema)
                    .ok_or(TransactionFailureReason::InvalidKey {
                        index,
                        message: "missing key attributes".to_string(),
                    })
            }
            TransactWriteItem::Update { key, .. } => Ok(key.clone()),
            TransactWriteItem::Delete { key, .. } => Ok(key.clone()),
            TransactWriteItem::ConditionCheck { key, .. } => Ok(key.clone()),
        }
    }

    fn validate_write_item(
        &self,
        item: &TransactWriteItem,
        key: &PrimaryKey,
        schema: &KeySchema,
        index: usize,
        get_item: impl Fn(&PrimaryKey) -> TableResult<Option<Item>>,
    ) -> Result<(), TransactionFailureReason> {
        let current = get_item(key).map_err(|_| TransactionFailureReason::InvalidKey {
            index,
            message: "failed to read item".to_string(),
        })?;

        match item {
            TransactWriteItem::Put { item, condition } => {
                item.validate_key(schema)
                    .map_err(|e| TransactionFailureReason::InvalidKey {
                        index,
                        message: e.to_string(),
                    })?;

                if let Some(cond) = condition {
                    let check = current.unwrap_or_default();
                    if !evaluate(cond, &check).unwrap_or(false) {
                        return Err(TransactionFailureReason::ConditionCheckFailed { index });
                    }
                }
            }
            TransactWriteItem::Update {
                expression,
                condition,
                ..
            } => {
                let existing = current.ok_or(TransactionFailureReason::ItemNotFound { index })?;

                if let Some(cond) = condition {
                    if !evaluate(cond, &existing).unwrap_or(false) {
                        return Err(TransactionFailureReason::ConditionCheckFailed { index });
                    }
                }

                let executor = UpdateExecutor::new();
                let updated = executor.execute(existing, expression).map_err(|_| {
                    TransactionFailureReason::InvalidKey {
                        index,
                        message: "update execution failed".to_string(),
                    }
                })?;

                let new_key = updated
                    .extract_key(schema)
                    .ok_or(TransactionFailureReason::KeyModification { index })?;
                if &new_key != key {
                    return Err(TransactionFailureReason::KeyModification { index });
                }
            }
            TransactWriteItem::Delete { condition, .. } => {
                if let Some(cond) = condition {
                    let check = current.unwrap_or_default();
                    if !evaluate(cond, &check).unwrap_or(false) {
                        return Err(TransactionFailureReason::ConditionCheckFailed { index });
                    }
                }
            }
            TransactWriteItem::ConditionCheck { condition, .. } => {
                let check = current.unwrap_or_default();
                if !evaluate(condition, &check).unwrap_or(false) {
                    return Err(TransactionFailureReason::ConditionCheckFailed { index });
                }
            }
        }

        Ok(())
    }

    pub fn execute_get(
        &self,
        items: &[TransactGetItem],
        get_item: impl Fn(&PrimaryKey) -> TableResult<Option<Item>>,
    ) -> TableResult<TransactGetResult> {
        let mut results = Vec::with_capacity(items.len());

        for item in items {
            match item {
                TransactGetItem::Get { key } => {
                    let item = get_item(key)?;
                    results.push(item);
                }
            }
        }

        Ok(TransactGetResult::new(results))
    }
}
