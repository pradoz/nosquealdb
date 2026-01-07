pub mod error;
pub mod storage;
pub mod table;
pub mod types;

pub use error::{StorageError, StorageResult, TableError, TableResult};
pub use storage::{MemoryStorage, Storage, StorageExt};
pub use table::Table;
pub use types::{
    AttributeValue, DecodeError, Item, KeyAttribute, KeySchema, KeyType, KeyValue, PrimaryKey,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
