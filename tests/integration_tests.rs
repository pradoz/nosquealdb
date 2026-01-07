use nosquealdb::{AttributeValue, Item, KeySchema, KeyType, PrimaryKey, Table};
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
