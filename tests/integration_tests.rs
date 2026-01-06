use nosquealdb::{MemoryStorage, Storage, StorageError, StorageExt, StorageResult};

fn create_test_db() -> MemoryStorage {
    MemoryStorage::new()
}

fn to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn from_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

mod user_records {
    use super::*;

    struct User {
        id: String,
        name: String,
        email: String,
    }

    impl User {
        fn save(&self, db: &mut impl Storage) -> StorageResult<()> {
            db.put(&format!("user:{}:name", self.id), to_bytes(&self.name))?;
            db.put(&format!("user:{}:email", self.id), to_bytes(&self.email))?;
            Ok(())
        }

        fn load(db: &impl Storage, id: &str) -> StorageResult<Option<Self>> {
            let name = match db.get(&format!("user:{}:name", id))? {
                Some(bytes) => from_bytes(&bytes),
                None => return Ok(None),
            };

            let email = match db.get(&format!("user:{}:email", id))? {
                Some(bytes) => from_bytes(&bytes),
                None => return Ok(None),
            };

            Ok(Some(User {
                id: id.to_string(),
                name,
                email,
            }))
        }

        fn delete(db: &mut impl Storage, id: &str) -> StorageResult<()> {
            db.delete(&format!("user:{}:name", id))?;
            db.delete(&format!("user:{}:email", id))?;
            Ok(())
        }
    }

    #[test]
    fn crud_operations() {
        let mut db = create_test_db();

        // create
        let user = User {
            id: "123".to_string(),
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        };
        user.save(&mut db).unwrap();

        // read
        let loaded = User::load(&db, "123").unwrap().unwrap();
        assert_eq!(loaded.name, "Alice");
        assert_eq!(loaded.email, "alice@example.com");

        // update
        let updated = User {
            id: "123".to_string(),
            name: "Alice Smith".to_string(),
            email: "alice.smith@example.com".to_string(),
        };
        updated.save(&mut db).unwrap();

        let loaded = User::load(&db, "123").unwrap().unwrap();
        assert_eq!(loaded.name, "Alice Smith");

        // delete
        User::delete(&mut db, "123").unwrap();
        assert!(User::load(&db, "123").unwrap().is_none());
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let db = create_test_db();
        assert!(User::load(&db, "nonexistent").unwrap().is_none());
    }
}

mod counter_pattern {
    use super::*;

    fn increment(db: &mut impl Storage, key: &str) -> StorageResult<u64> {
        let current = match db.get(key)? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(arr)
            }
            None => 0,
        };

        let new_value = current + 1;
        db.put(key, new_value.to_le_bytes().to_vec())?;
        Ok(new_value)
    }

    fn get_count(db: &impl Storage, key: &str) -> StorageResult<u64> {
        match db.get(key)? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.try_into().unwrap_or([0; 8]);
                Ok(u64::from_le_bytes(arr))
            }
            None => Ok(0),
        }
    }

    #[test]
    fn increment_counter() {
        let mut db = create_test_db();

        assert_eq!(increment(&mut db, "visits").unwrap(), 1);
        assert_eq!(increment(&mut db, "visits").unwrap(), 2);
        assert_eq!(increment(&mut db, "visits").unwrap(), 3);

        assert_eq!(get_count(&db, "visits").unwrap(), 3);
    }

    #[test]
    fn multiple_independent_counters() {
        let mut db = create_test_db();

        increment(&mut db, "page:home").unwrap();
        increment(&mut db, "page:home").unwrap();
        increment(&mut db, "page:about").unwrap();

        assert_eq!(get_count(&db, "page:home").unwrap(), 2);
        assert_eq!(get_count(&db, "page:about").unwrap(), 1);
        assert_eq!(get_count(&db, "page:contact").unwrap(), 0);
    }
}

mod prefix_scanning {
    use super::*;

    #[test]
    fn find_all_user_keys() {
        let mut db = create_test_db();

        db.put("user:1:name", to_bytes("Alice")).unwrap();
        db.put("user:1:email", to_bytes("alice@example.com"))
            .unwrap();
        db.put("user:2:name", to_bytes("Bob")).unwrap();
        db.put("user:2:email", to_bytes("bob@example.com")).unwrap();
        db.put("order:1", to_bytes("order data")).unwrap();

        let user_keys: Vec<_> = db.keys_with_prefix("user:").collect();
        assert_eq!(user_keys.len(), 4);

        let user1_keys: Vec<_> = db.keys_with_prefix("user:1:").collect();
        assert_eq!(user1_keys.len(), 2);

        assert_eq!(db.count_with_prefix("user:"), 4);
        assert_eq!(db.count_with_prefix("order:"), 1);
    }

    #[test]
    fn collect_values_by_prefix() {
        let mut db = create_test_db();

        db.put("config:db_host", to_bytes("localhost")).unwrap();
        db.put("config:db_port", to_bytes("5432")).unwrap();
        db.put("config:api_key", to_bytes("secret")).unwrap();
        db.put("data:something", to_bytes("not config")).unwrap();

        let config_keys: Vec<_> = db.keys_with_prefix("config:").collect();
        let config_values: Vec<_> = config_keys
            .iter()
            .filter_map(|k| db.get(k).ok().flatten())
            .map(|v| from_bytes(&v))
            .collect();

        assert_eq!(config_values.len(), 3);
        assert!(config_values.contains(&"localhost".to_string()));
        assert!(config_values.contains(&"5432".to_string()));
        assert!(config_values.contains(&"secret".to_string()));
    }
}

mod conditional_operations {
    use super::*;

    #[test]
    fn create_if_not_exists() {
        let mut db = create_test_db();

        // First creation succeeds
        assert!(db.put_if_not_exists("unique", to_bytes("first")).is_ok());

        // Second creation fails
        let result = db.put_if_not_exists("unique", to_bytes("second"));
        assert!(result.is_err());
        assert!(result.unwrap_err().key_already_exists());

        // Original value preserved
        assert_eq!(db.get("unique").unwrap(), Some(to_bytes("first")));
    }

    #[test]
    fn update_only_existing() {
        let mut db = create_test_db();

        // Cannot update non-existent key
        let result = db.update("missing", to_bytes("value"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());

        // Can update existing key
        db.put("exists", to_bytes("original")).unwrap();
        db.update("exists", to_bytes("updated")).unwrap();
        assert_eq!(db.get("exists").unwrap(), Some(to_bytes("updated")));
    }

    #[test]
    fn get_or_error_semantics() {
        let mut db = create_test_db();
        db.put("exists", to_bytes("value")).unwrap();

        // Returns value when exists
        assert_eq!(db.get_or_error("exists").unwrap(), to_bytes("value"));

        // Returns error when missing
        let result = db.get_or_error("missing");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }
}

mod batch_operations {
    use super::*;

    #[test]
    fn get_many_items() {
        let mut db = create_test_db();

        db.put("a", vec![1]).unwrap();
        db.put("b", vec![2]).unwrap();
        db.put("c", vec![3]).unwrap();

        let results = db.get_many(&["a", "missing", "c", "b"]).unwrap();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0], Some(vec![1]));
        assert_eq!(results[1], None);
        assert_eq!(results[2], Some(vec![3]));
        assert_eq!(results[3], Some(vec![2]));
    }

    #[test]
    fn delete_and_get_returns_old_value() {
        let mut db = create_test_db();
        db.put("key", to_bytes("value")).unwrap();

        let old = db.delete_and_get_old("key").unwrap();
        assert_eq!(old, Some(to_bytes("value")));
        assert!(!db.exists("key").unwrap());
    }
}

mod stress_tests {
    use super::*;

    #[test]
    fn handles_10000_items() {
        let mut db = MemoryStorage::with_capacity(10_000);

        // Insert
        for i in 0..10_000 {
            let key = format!("key:{:05}", i);
            let value = format!("value:{}", i).into_bytes();
            db.put(&key, value).unwrap();
        }

        assert_eq!(db.len(), 10_000);

        // Random access
        assert_eq!(db.get("key:00000").unwrap(), Some(b"value:0".to_vec()));
        assert_eq!(db.get("key:05000").unwrap(), Some(b"value:5000".to_vec()));
        assert_eq!(db.get("key:09999").unwrap(), Some(b"value:9999".to_vec()));

        // Delete half
        for i in 0..5_000 {
            let key = format!("key:{:05}", i);
            db.delete(&key).unwrap();
        }

        assert_eq!(db.len(), 5_000);
        assert!(!db.exists("key:00000").unwrap());
        assert!(db.exists("key:05000").unwrap());
    }

    #[test]
    fn handles_large_values() {
        let mut db = create_test_db();

        // 1MB value
        let large_value = vec![42u8; 1_000_000];
        db.put("large", large_value.clone()).unwrap();

        assert_eq!(db.get("large").unwrap(), Some(large_value));
        assert_eq!(db.total_value_bytes(), 1_000_000);
    }
}

mod isolation {
    use super::*;

    #[test]
    fn cloned_db_is_independent() {
        let mut original = create_test_db();
        original.put("key", to_bytes("original")).unwrap();

        let mut clone = original.clone();

        // Modify clone
        clone.put("key", to_bytes("modified")).unwrap();
        clone.put("new_key", to_bytes("new")).unwrap();

        // Original is unchanged
        assert_eq!(original.get("key").unwrap(), Some(to_bytes("original")));
        assert_eq!(original.get("new_key").unwrap(), None);

        // Clone has changes
        assert_eq!(clone.get("key").unwrap(), Some(to_bytes("modified")));
        assert_eq!(clone.get("new_key").unwrap(), Some(to_bytes("new")));
    }
}

mod error_handling {
    use super::*;

    #[test]
    fn error_information_is_accessible() {
        let err = StorageError::not_found("my_key");

        assert_eq!(err.key(), Some("my_key"));
        assert!(err.is_not_found());
        assert!(!err.key_already_exists());
        assert_eq!(err.to_string(), "key not found: my_key");
    }

    #[test]
    fn errors_can_be_matched() {
        let mut db = create_test_db();
        db.put("existing", vec![]).unwrap();

        let result = db.put_if_not_exists("existing", vec![]);

        match result {
            Err(StorageError::KeyAlreadyExists { key }) => {
                assert_eq!(key, "existing");
            }
            _ => panic!("Expected KeyAlreadyExists error"),
        }
    }
}

mod iterators {
    use super::*;

    #[test]
    fn into_iter_consumes_storage() {
        let mut db = create_test_db();
        db.put("a", vec![1]).unwrap();
        db.put("b", vec![2]).unwrap();

        let mut pairs: Vec<_> = db.into_iter().collect();
        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

        assert_eq!(
            pairs,
            vec![("a".to_string(), vec![1]), ("b".to_string(), vec![2]),]
        );
    }

    #[test]
    fn iter_borrows_storage() {
        let mut db = create_test_db();
        db.put("x", vec![10]).unwrap();
        db.put("y", vec![20]).unwrap();

        // Can iterate without consuming
        let count = db.iter().count();
        assert_eq!(count, 2);

        // Storage still usable
        assert_eq!(db.len(), 2);
        db.put("z", vec![30]).unwrap();
        assert_eq!(db.len(), 3);
    }
}
