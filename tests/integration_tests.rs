use nosquealdb::{
    AttributeValue, DeleteRequest, GsiBuilder, Item, KeyCondition, KeySchema, KeyType, LsiBuilder,
    PrimaryKey, PutRequest, QueryRequest, Table, TableBuilder, UpdateRequest,
};
use std::collections::BTreeMap;

#[test]
fn nested_document_survives_roundtrip() {
    let mut table = Table::new("docs", KeySchema::simple("id", KeyType::S));

    let mut address = BTreeMap::new();
    address.insert("city".to_string(), AttributeValue::S("Candyland".into()));
    address.insert("zip".to_string(), AttributeValue::N("12345".into()));

    let item = Item::new()
        .with_s("id", "doc-1")
        .with("address", AttributeValue::M(address))
        .with(
            "scores",
            AttributeValue::L(vec![
                AttributeValue::N("95".into()),
                AttributeValue::N("87".into()),
            ]),
        );

    table.put_item(item).unwrap();

    let retrieved = table
        .get_item(&PrimaryKey::simple("doc-1"))
        .unwrap()
        .unwrap();
    let addr = retrieved.get("address").unwrap().as_m().unwrap();
    assert_eq!(
        addr.get("city"),
        Some(&AttributeValue::S("Candyland".into()))
    );
}

#[test]
fn special_characters_in_keys() {
    let mut table = Table::new("test", KeySchema::simple("id", KeyType::S));

    let keys = ["key#hash", "key:colon", "key\\slash", "a#:\\b"];

    for key in &keys {
        table
            .put_item(Item::new().with_s("id", *key).with_s("k", *key))
            .unwrap();
    }

    for key in &keys {
        let item = table.get_item(&PrimaryKey::simple(*key)).unwrap().unwrap();
        assert_eq!(item.get("k"), Some(&AttributeValue::S((*key).into())));
    }
}

#[test]
fn composite_keys_are_isolated() {
    let mut table = Table::new(
        "orders",
        KeySchema::composite("user", KeyType::S, "order", KeyType::S),
    );

    table
        .put_item(
            Item::new()
                .with_s("user", "a")
                .with_s("order", "1")
                .with_n("v", 1),
        )
        .unwrap();
    table
        .put_item(
            Item::new()
                .with_s("user", "a")
                .with_s("order", "2")
                .with_n("v", 2),
        )
        .unwrap();
    table
        .put_item(
            Item::new()
                .with_s("user", "b")
                .with_s("order", "1")
                .with_n("v", 3),
        )
        .unwrap();

    assert_eq!(table.len(), 3);

    let get_v = |u, o| -> i32 {
        let item = table
            .get_item(&PrimaryKey::composite(u, o))
            .unwrap()
            .unwrap();
        item.get("v").unwrap().as_n().unwrap().parse().unwrap()
    };

    assert_eq!(get_v("a", "1"), 1);
    assert_eq!(get_v("a", "2"), 2);
    assert_eq!(get_v("b", "1"), 3);
}

mod query {
    use super::*;

    #[test]
    fn empty_partition_returns_empty_result() {
        let mut table = Table::new(
            "orders",
            KeySchema::composite("user", KeyType::S, "order", KeyType::S),
        );

        table
            .put_item(
                Item::new()
                    .with_s("user", "user1")
                    .with_s("order", "order1")
                    .with_n("amount", 100),
            )
            .unwrap();

        let result = table.query(KeyCondition::pk("nonexistent")).unwrap();

        assert_eq!(result.count, 0);
        assert!(result.items.is_empty());
        assert_eq!(result.scanned_count, 1);
    }

    #[test]
    fn limit_respects_sort_order() {
        let mut table = Table::new(
            "test",
            KeySchema::composite("pk", KeyType::S, "sk", KeyType::S),
        );

        // random order
        for sk in ["c", "a", "e", "b", "d"] {
            table
                .put_item(Item::new().with_s("pk", "user1").with_s("sk", sk))
                .unwrap();
        }

        // forward with limit
        let result = table
            .query(QueryRequest::new(KeyCondition::pk("user1")).limit(3))
            .unwrap();

        let found_sks: Vec<&str> = result
            .items
            .iter()
            .map(|item| item.get("sk").unwrap().as_s().unwrap())
            .collect();
        assert_eq!(found_sks, vec!["a", "b", "c"]);

        // reverse with limit
        let result = table
            .query(
                QueryRequest::new(KeyCondition::pk("user1"))
                    .limit(3)
                    .reverse(),
            )
            .unwrap();

        let found_sks: Vec<&str> = result
            .items
            .iter()
            .map(|item| item.get("sk").unwrap().as_s().unwrap())
            .collect();
        assert_eq!(found_sks, vec!["e", "d", "c"]);
    }

    #[test]
    fn numeric_sort_key_ordering() {
        let mut table = Table::new(
            "test",
            KeySchema::composite("pk", KeyType::S, "sk", KeyType::N),
        );

        // random order
        for i in [100, -1, 20, 0, -42, 37, 8] {
            table
                .put_item(
                    Item::new()
                        .with_s("pk", "user1")
                        .with_n("sk", i)
                        .with_n("value", i),
                )
                .unwrap();
        }

        // forward order
        let result = table
            .query(QueryRequest::new(KeyCondition::pk("user1")))
            .unwrap();

        let found: Vec<i32> = result
            .items
            .iter()
            .map(|item| item.get("value").unwrap().as_n().unwrap().parse().unwrap())
            .collect();
        assert_eq!(found, vec![-42, -1, 0, 8, 20, 37, 100]);
    }
}

mod update {
    use super::*;
    use nosquealdb::UpdateExpression;
    use nosquealdb::condition::attr;

    fn update_expr() -> UpdateExpression {
        UpdateExpression::new()
    }

    #[test]
    fn indexes() {
        let mut table = TableBuilder::new(
            "test",
            KeySchema::composite("pk", KeyType::S, "sk", KeyType::S),
        )
        .with_gsi(GsiBuilder::new(
            "by-status",
            KeySchema::simple("status", KeyType::S),
        ))
        .build();

        // set initial state
        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("status", "pending"),
            )
            .unwrap();

        // verify initial state
        let result = table
            .query_gsi("by-status", KeyCondition::pk("pending"))
            .unwrap();
        assert_eq!(result.count, 1);

        // update state
        let key = PrimaryKey::composite("user1", "order1");
        table
            .update_item(&key, update_expr().set("status", "shipped"))
            .unwrap();

        // verify updated state
        let result = table
            .query_gsi("by-status", KeyCondition::pk("pending"))
            .unwrap();
        assert_eq!(result.count, 0);
        let result = table
            .query_gsi("by-status", KeyCondition::pk("shipped"))
            .unwrap();
        assert_eq!(result.count, 1);
    }

    #[test]
    fn atomic_counter() {
        let mut table = Table::new("test", KeySchema::simple("pk", KeyType::S));

        table
            .put_item(Item::new().with_s("pk", "view_count").with_n("value", 0))
            .unwrap();

        let key = PrimaryKey::simple("view_count");
        for _ in 0..10 {
            table
                .update_item(&key, update_expr().add("value", 1i32))
                .unwrap();
        }

        let item = table.get_item(&key).unwrap().unwrap();
        assert_eq!(item.get("value"), Some(&AttributeValue::N("10".into())));
    }

    #[test]
    fn optimistic_locking() {
        let mut table = Table::new("test", KeySchema::simple("id", KeyType::S));

        table
            .put_item(
                Item::new()
                    .with_s("id", "doc1")
                    .with_s("content", "lorem ipsum")
                    .with_n("version", 1),
            )
            .unwrap();

        let key = PrimaryKey::simple("doc1");

        // update with correct version, should succeed
        let result = table.update(
            UpdateRequest::new(
                key.clone(),
                UpdateExpression::new()
                    .set("content", "dolor something")
                    .set("version", 2i32),
            )
            .condition(attr("version").eq(1i32)),
        );
        assert!(result.is_ok());

        // update with stale version, should fail
        let result = table.update(
            UpdateRequest::new(
                key.clone(),
                UpdateExpression::new()
                    .set("content", "stale text")
                    .set("version", 2i32),
            )
            .condition(attr("version").eq(1i32)),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().is_condition_failed());

        // content reflects first (successful) update
        let item = table.get_item(&key).unwrap().unwrap();
        assert_eq!(
            item.get("content"),
            Some(&AttributeValue::S("dolor something".into()))
        );
        assert_eq!(item.get("version"), Some(&AttributeValue::N("2".into())));
    }
}

mod transactions {
    use super::*;
    use nosquealdb::{
        TransactGetItem, TransactGetRequest, TransactWriteItem, TransactWriteRequest,
        UpdateExpression, condition::attr,
    };

    #[test]
    fn atomic_transfer() {
        let mut table = TableBuilder::new("account", KeySchema::simple("id", KeyType::S)).build();
        let items = vec![
            TransactWriteItem::put(Item::new().with_s("id", "a").with_n("balance", 100)),
            TransactWriteItem::put(Item::new().with_s("id", "b").with_n("balance", 200)),
        ];
        let result = table.transact_write(items);
        assert!(result.is_ok());

        // transfer 30 from A to B atomically
        let result = table.transact_write(
            TransactWriteRequest::new()
                .update_with_condition(
                    PrimaryKey::simple("a"),
                    UpdateExpression::new().add("balance", -50i32),
                    attr("balance").ge(50i32),
                )
                .update(
                    PrimaryKey::simple("b"),
                    UpdateExpression::new().add("balance", 50i32),
                ),
        );
        assert!(result.is_ok());

        let a = table.get_item(&PrimaryKey::simple("a")).unwrap().unwrap();
        let b = table.get_item(&PrimaryKey::simple("b")).unwrap().unwrap();
        assert_eq!(a.get("balance"), Some(&AttributeValue::N("50".into())));
        assert_eq!(b.get("balance"), Some(&AttributeValue::N("250".into())));

        // insufficient funds, should fail
        let result = table.transact_write(
            TransactWriteRequest::new()
                .update_with_condition(
                    PrimaryKey::simple("a"),
                    UpdateExpression::new().add("balance", -51i32),
                    attr("balance").ge(51i32),
                )
                .update(
                    PrimaryKey::simple("b"),
                    UpdateExpression::new().add("balance", 51i32),
                ),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transaction_canceled());

        // balances should remain the same
        let a = table.get_item(&PrimaryKey::simple("a")).unwrap().unwrap();
        let b = table.get_item(&PrimaryKey::simple("b")).unwrap().unwrap();
        assert_eq!(a.get("balance"), Some(&AttributeValue::N("50".into())));
        assert_eq!(b.get("balance"), Some(&AttributeValue::N("250".into())));
    }
}

mod gsi {
    use super::*;

    #[test]
    fn nonexistent_index_returns_error() {
        let table = Table::new(
            "orders",
            KeySchema::composite("user", KeyType::S, "order", KeyType::S),
        );

        let result = table.query_gsi("nonexistent-index", KeyCondition::pk("user1"));

        assert!(result.is_err());
        assert!(result.unwrap_err().is_index_not_found());
    }

    #[test]
    fn mutated_when_table_item_is_updated() {
        let mut table = TableBuilder::new(
            "test",
            KeySchema::composite("pk", KeyType::S, "sk", KeyType::S),
        )
        .with_gsi(GsiBuilder::new(
            "by-status",
            KeySchema::simple("status", KeyType::S),
        ))
        .build();

        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("status", "pending"),
            )
            .unwrap();

        let result = table
            .query_gsi("by-status", KeyCondition::pk("pending"))
            .unwrap();
        assert_eq!(result.count, 1);

        // change status
        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("status", "shipped"),
            )
            .unwrap();

        // no longer present in GSI
        let result = table
            .query_gsi("by-status", KeyCondition::pk("pending"))
            .unwrap();
        assert_eq!(result.count, 0);

        // query changed status
        let result = table
            .query_gsi("by-status", KeyCondition::pk("shipped"))
            .unwrap();
        assert_eq!(result.count, 1);
    }

    #[test]
    fn sparse_index_behavior() {
        let schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::S);
        let mut table = TableBuilder::new("test", schema)
            .with_gsi(GsiBuilder::new(
                "by-status",
                KeySchema::simple("status", KeyType::S),
            ))
            .build();

        // insert item with status. should appear in GSI
        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("status", "pending"),
            )
            .unwrap();
        assert_eq!(table.gsi("by-status").unwrap().len(), 1);

        // update item _without_ status. should not appear in GSI
        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_n("amount", 100),
            )
            .unwrap();
        assert_eq!(table.gsi("by-status").unwrap().len(), 0);

        // still exists in table
        let item = table
            .get_item(&PrimaryKey::composite("user1", "order1"))
            .unwrap();
        assert!(item.is_some());
    }

    #[test]
    fn updated_on_delete() {
        let schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::S);
        let mut table = TableBuilder::new("test", schema)
            .with_gsi(GsiBuilder::new(
                "by-date",
                KeySchema::simple("date", KeyType::S),
            ))
            .build();

        table
            .put(PutRequest::new(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("date", "2026-01-08"),
            ))
            .unwrap();
        assert_eq!(table.gsi("by-date").unwrap().len(), 1);

        table
            .delete(DeleteRequest::new(PrimaryKey::composite("user1", "order1")))
            .unwrap();
        assert_eq!(table.gsi("by-date").unwrap().len(), 0);
    }
}

mod lsi {
    use super::*;

    #[test]
    fn nonexistent_index_returns_error() {
        let table = Table::new(
            "orders",
            KeySchema::composite("user", KeyType::S, "order", KeyType::S),
        );

        let result = table.query_lsi("nonexistent-index", KeyCondition::pk("user1"));

        assert!(result.is_err());
        assert!(result.unwrap_err().is_index_not_found());
    }

    #[test]
    fn maintains_consistency_with_table() {
        let schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::S);
        let mut table = TableBuilder::new("test", schema)
            .with_lsi(LsiBuilder::new("by-date", "date", KeyType::S))
            .build();

        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("date", "2026-01-08"),
            )
            .unwrap();

        let result = table
            .query_lsi("by-date", KeyCondition::pk("user1").sk_eq("2026-01-08"))
            .unwrap();
        assert_eq!(result.count, 1);

        // update the date
        table
            .put_item(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("date", "2027-01-08"),
            )
            .unwrap();

        // update reflected in GSI
        let result = table
            .query_lsi("by-date", KeyCondition::pk("user1").sk_eq("2026-01-08"))
            .unwrap();
        assert_eq!(result.count, 0);

        let result = table
            .query_lsi("by-date", KeyCondition::pk("user1").sk_eq("2027-01-08"))
            .unwrap();
        assert_eq!(result.count, 1);

        // delete item
        table
            .delete_item(&PrimaryKey::composite("user1", "order1"))
            .unwrap();
        let result = table
            .query_lsi("by-date", KeyCondition::pk("user1"))
            .unwrap();
        assert_eq!(result.count, 0);
    }
}

mod conditional_write {
    use super::*;

    #[test]
    fn preserves_indexes() {
        let mut table = TableBuilder::new(
            "test",
            KeySchema::composite("pk", KeyType::S, "sk", KeyType::S),
        )
        .with_gsi(GsiBuilder::new(
            "by-status",
            KeySchema::simple("status", KeyType::S),
        ))
        .build();

        table
            .put(
                PutRequest::new(
                    Item::new()
                        .with_s("pk", "user1")
                        .with_s("sk", "order1")
                        .with_s("status", "active"),
                )
                .if_not_exists(),
            )
            .unwrap();

        // already exists, should fail
        let result = table.put(
            PutRequest::new(
                Item::new()
                    .with_s("pk", "user1")
                    .with_s("sk", "order1")
                    .with_s("status", "active"),
            )
            .if_not_exists(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().item_already_exists());

        // original item still exists
        let result = table
            .query_gsi("by-status", KeyCondition::pk("active"))
            .unwrap();
        assert_eq!(result.count, 1);

        // wrong status, should not exist
        let result = table
            .query_gsi("by-status", KeyCondition::pk("inactive"))
            .unwrap();
        assert_eq!(result.count, 0);
    }
}

mod projection {
    use super::*;

    #[test]
    fn gsi_keys_only() {
        let table_schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::S);
        let gsi_schema = KeySchema::composite("gsi_pk", KeyType::S, "gsi_sk", KeyType::S);

        let mut table = TableBuilder::new("test", table_schema)
            .with_gsi(GsiBuilder::new("by-gsi", gsi_schema).keys_only())
            .build();

        table
            .put_item(
                Item::new()
                    .with_s("pk", "pk1")
                    .with_s("sk", "sk1")
                    .with_s("gsi_pk", "gsi_pk1")
                    .with_s("gsi_sk", "gsi_sk1")
                    .with_s("data", "should not show")
                    .with_n("amount", 100),
            )
            .unwrap();

        let result = table
            .query_gsi("by-gsi", KeyCondition::pk("gsi_pk1"))
            .unwrap();
        assert_eq!(result.count, 1);
        let item = &result.items[0];

        // should have key attributes
        assert!(item.contains("pk"));
        assert!(item.contains("sk"));
        assert!(item.contains("gsi_pk"));
        assert!(item.contains("gsi_sk"));

        // should not have non-key attributes
        assert!(!item.contains("data"));
        assert!(!item.contains("amount"));
    }

    #[test]
    fn gsi_include_keys() {
        let table_schema = KeySchema::composite("pk", KeyType::S, "sk", KeyType::S);
        let gsi_schema = KeySchema::simple("category", KeyType::S);

        let mut table = TableBuilder::new("test", table_schema)
            .with_gsi(GsiBuilder::new("by-category", gsi_schema).include(["name", "price"]))
            .build();

        table
            .put_item(
                Item::new()
                    .with_s("pk", "pk1")
                    .with_s("sk", "sk1")
                    .with_s("category", "computers")
                    .with_s("name", "laptop")
                    .with_s("description", "super awesome laptop")
                    .with_n("price", 99.99)
                    .with_n("stock", 2),
            )
            .unwrap();

        let result = table
            .query_gsi("by-category", KeyCondition::pk("computers"))
            .unwrap();
        assert_eq!(result.count, 1);
        let item = &result.items[0];

        // should have key attributes
        assert!(item.contains("pk"));
        assert!(item.contains("sk"));
        assert!(item.contains("category"));
        assert!(item.contains("name"));
        assert!(item.contains("price"));

        // should not have non-key attributes
        assert!(!item.contains("description"));
        assert!(!item.contains("stock"));
    }
}

mod binary {
    use super::*;

    #[test]
    fn it_works() {
        let mut table = Table::new(
            "test",
            KeySchema::composite("pk", KeyType::B, "sk", KeyType::B),
        );

        let pk = vec![0x00, 0x01, 0x02];
        let sk = vec![0xAB, 0xCD, 0xEF];

        table
            .put_item(
                Item::new()
                    .with_b("pk", pk.clone())
                    .with_b("sk", sk.clone())
                    .with_n("data", 42),
            )
            .unwrap();

        let key = PrimaryKey::composite(pk.clone(), sk.clone());
        let item = table.get_item(&key).unwrap().unwrap();

        assert_eq!(item.get("pk").unwrap().as_b(), Some(pk.as_slice()));
        assert_eq!(item.get("sk").unwrap().as_b(), Some(sk.as_slice()));
        assert_eq!(item.get("data").unwrap().as_n(), Some("42".into()));
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn empty_string() {
        let mut table = Table::new("test", KeySchema::simple("pk", KeyType::S));

        table
            .put_item(Item::new().with_s("pk", "").with_n("data", 42))
            .unwrap();
        assert_eq!(table.len(), 1);

        let item = table.get_item(&PrimaryKey::simple("")).unwrap().unwrap();
        assert_eq!(item.get("pk").unwrap().as_s(), Some(""));
        assert_eq!(item.get("data").unwrap().as_n(), Some("42".into()));
    }

    #[test]
    fn large_item_roundtrip() {
        let mut table = Table::new("test", KeySchema::simple("pk", KeyType::S));

        let mut item = Item::new().with_s("pk", "large-item");

        for i in 0..100 {
            item = item
                .with_s(format!("str_{}", i), format!("value_{}", i))
                .with_n(format!("num_{}", i), i);
        }

        let mut nested = BTreeMap::new();
        for i in 0..50 {
            nested.insert(format!("key_{}", i), AttributeValue::N(i.to_string()));
        }
        item = item.with_m("nested", nested);

        table.put_item(item).unwrap();

        let retrieved = table
            .get_item(&PrimaryKey::simple("large-item"))
            .unwrap()
            .unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(retrieved.get("str_50").unwrap().as_s(), Some("value_50"));
        assert_eq!(retrieved.get("num_99").unwrap().as_n(), Some("99"));

        let nested = retrieved.get("nested").unwrap().as_m().unwrap();
        assert_eq!(nested.get("key_25"), Some(&AttributeValue::N("25".into())));
    }
}
