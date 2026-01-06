use nosquealdb::{MemoryStorage, Storage, StorageError, StorageExt};

fn to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn from_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

mod usage_patterns {
    use super::*;

    #[test]
    fn entity_crud_with_composite_keys() {
        let mut db = MemoryStorage::new();

        // create
        db.put("user:123:name", to_bytes("Alice")).unwrap();
        db.put("user:123:email", to_bytes("alice@example.com"))
            .unwrap();

        // read
        let name = db.get("user:123:name").unwrap().map(|b| from_bytes(&b));
        assert_eq!(name, Some("Alice".to_string()));

        // update
        db.put("user:123:name", to_bytes("Alice Smith")).unwrap();

        // delete
        db.delete("user:123:name").unwrap();
        db.delete("user:123:email").unwrap();
        assert!(db.is_empty());
    }

    #[test]
    fn counter_increment_pattern() {
        let mut db = MemoryStorage::new();

        let get_count = |db: &MemoryStorage, key: &str| -> u64 {
            db.get(key)
                .unwrap()
                .map(|b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
                .unwrap_or(0)
        };

        let increment = |db: &mut MemoryStorage, key: &str| {
            let current = get_count(db, key);
            db.put(key, (current + 1).to_le_bytes().to_vec()).unwrap();
        };

        increment(&mut db, "visits");
        increment(&mut db, "visits");
        increment(&mut db, "visits");

        assert_eq!(get_count(&db, "visits"), 3);
    }

    #[test]
    fn prefix_scanning() {
        let mut db = MemoryStorage::new();

        db.put("user:1:name", to_bytes("Alice")).unwrap();
        db.put("user:2:name", to_bytes("Bob")).unwrap();
        db.put("order:1", to_bytes("order data")).unwrap();

        assert_eq!(db.count_with_prefix("user:"), 2);
        assert_eq!(db.count_with_prefix("order:"), 1);
        assert_eq!(db.count_with_prefix("nonexistent:"), 0);
    }
}

mod conditional_operations {
    use super::*;

    #[test]
    fn put_if_not_exists_prevents_overwrite() {
        let mut db = MemoryStorage::new();

        db.put_if_not_exists("key", to_bytes("first")).unwrap();
        let result = db.put_if_not_exists("key", to_bytes("second"));

        assert!(result.unwrap_err().key_already_exists());
        assert_eq!(db.get("key").unwrap(), Some(to_bytes("first")));
    }

    #[test]
    fn update_requires_existing_key() {
        let mut db = MemoryStorage::new();

        let result = db.update("missing", to_bytes("value"));
        assert!(result.unwrap_err().is_not_found());

        db.put("exists", to_bytes("original")).unwrap();
        db.update("exists", to_bytes("updated")).unwrap();
        assert_eq!(db.get("exists").unwrap(), Some(to_bytes("updated")));
    }

    #[test]
    fn get_or_error_distinguishes_missing_from_empty() {
        let mut db = MemoryStorage::new();

        // missing key should error
        assert!(db.get_or_error("missing").unwrap_err().is_not_found());

        // empty value is valid
        db.put("empty", vec![]).unwrap();
        assert_eq!(db.get_or_error("empty").unwrap(), vec![]);
    }
}

mod error_handling {
    use super::*;

    #[test]
    fn errors_expose_key_for_debugging() {
        let err = StorageError::not_found("user:123");

        assert_eq!(err.key(), Some("user:123"));
        assert!(err.to_string().contains("user:123"));
    }

    #[test]
    fn errors_can_be_pattern_matched() {
        let mut db = MemoryStorage::new();
        db.put("existing", vec![]).unwrap();

        match db.put_if_not_exists("existing", vec![]) {
            Err(StorageError::KeyAlreadyExists { key }) => {
                assert_eq!(key, "existing");
            }
            _ => panic!("Expected KeyAlreadyExists"),
        }
    }
}
#[test]
fn cloned_storage_is_independent() {
    let mut original = MemoryStorage::new();
    original.put("key", to_bytes("original")).unwrap();

    let mut clone = original.clone();
    clone.put("key", to_bytes("modified")).unwrap();

    // mutations should not cross
    assert_eq!(original.get("key").unwrap(), Some(to_bytes("original")));
    assert_eq!(clone.get("key").unwrap(), Some(to_bytes("modified")));
}

#[test]
fn handles_many_items() {
    let mut db = MemoryStorage::with_capacity(10_000);

    for i in 0..10_000 {
        db.put(&format!("key:{i:05}"), format!("value:{i}").into_bytes())
            .unwrap();
    }

    assert_eq!(db.len(), 10_000);
    assert!(db.exists("key:05000").unwrap());

    // retrieval works
    assert_eq!(db.get("key:09999").unwrap(), Some(b"value:9999".to_vec()));
}
