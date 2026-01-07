pub mod error;
pub mod storage;
pub mod types;

pub use error::{StorageError, StorageResult};
pub use storage::{MemoryStorage, Storage, StorageExt};
pub use types::{
    AttributeValue, DecodeError, Item, KeyAttribute, KeySchema, KeyType, KeyValue, PrimaryKey,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
