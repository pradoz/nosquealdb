use crate::types::{Item, KeySchema};
use std::collections::HashSet;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum Projection {
    #[default]
    All,
    KeysOnly,
    Include(HashSet<String>),
}

impl Projection {
    // project that includes specific attributes
    pub fn include<I, S>(attrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Include(attrs.into_iter().map(Into::into).collect())
    }

    pub fn project_item(
        &self,
        item: &Item,
        table_schema: &KeySchema,
        index_schema: &KeySchema,
    ) -> Item {
        match self {
            Projection::All => item.clone(),
            Projection::KeysOnly => {
                let key_names = self.collect_key_names(table_schema, index_schema);
                self.filter_item(item, &key_names)
            }
            Projection::Include(attrs) => {
                let mut key_names = self.collect_key_names(table_schema, index_schema);
                key_names.extend(attrs.iter().cloned());
                self.filter_item(item, &key_names)
            }
        }
    }

    fn collect_key_names(
        &self,
        table_schema: &KeySchema,
        index_schema: &KeySchema,
    ) -> HashSet<String> {
        let mut key_names = HashSet::new();

        key_names.insert(table_schema.partition_key.name.clone());
        if let Some(sk) = &table_schema.sort_key {
            key_names.insert(sk.name.clone());
        }

        key_names.insert(index_schema.partition_key.name.clone());
        if let Some(sk) = &index_schema.sort_key {
            key_names.insert(sk.name.clone());
        }

        key_names
    }

    fn filter_item(&self, item: &Item, include: &HashSet<String>) -> Item {
        item.iter()
            .filter(|(name, _)| include.contains(*name))
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;

    fn table_schema() -> KeySchema {
        KeySchema::composite("pk", KeyType::S, "sk", KeyType::S)
    }

    fn index_schema() -> KeySchema {
        KeySchema::composite("gsi_pk", KeyType::S, "gsi_sk", KeyType::S)
    }

    fn test_item() -> Item {
        Item::new()
            .with_s("pk", "user1")
            .with_s("sk", "order#001")
            .with_s("gsi_pk", "2026-01")
            .with_s("gsi_sk", "user1")
            .with_s("name", "Test Order")
            .with_n("amount", 100)
            .with_s("status", "pending")
    }

    #[test]
    fn project_all() {
        let item = test_item();
        let projected = Projection::All.project_item(&item, &table_schema(), &index_schema());
        assert_eq!(item.len(), projected.len());
    }

    #[test]
    fn project_keys_only() {
        let item = test_item();
        let projected = Projection::KeysOnly.project_item(&item, &table_schema(), &index_schema());

        // should have: pk, sk, gsi_pk, gsi_sk
        assert_eq!(projected.len(), 4);
        assert!(projected.contains("pk"));
        assert!(projected.contains("sk"));
        assert!(projected.contains("gsi_pk"));
        assert!(projected.contains("gsi_sk"));
        assert!(!projected.contains("name"));
        assert!(!projected.contains("amount"));
    }

    #[test]
    fn projection_include_specific() {
        let item = test_item();
        let projected = Projection::include(["name", "amount"]).project_item(
            &item,
            &table_schema(),
            &index_schema(),
        );

        // should have: pk, sk, gsi_pk, gsi_sk, name, amount
        assert_eq!(projected.len(), 6);
        assert!(projected.contains("pk"));
        assert!(projected.contains("sk"));
        assert!(projected.contains("gsi_pk"));
        assert!(projected.contains("gsi_sk"));
        assert!(projected.contains("name"));
        assert!(projected.contains("amount"));
        assert!(!projected.contains("status"));
    }
}
