pub mod condition;
pub mod error;
pub mod index;
pub mod query;
pub mod storage;
pub mod table;
pub mod types;
pub mod update;
pub mod utils;

pub use error::{StorageError, StorageResult, TableError, TableResult};
pub use index::{GlobalSecondaryIndex, GsiBuilder, LocalSecondaryIndex, LsiBuilder, Projection};
pub use query::{KeyCondition, QueryOptions, QueryResult, SortKeyOp};
pub use storage::{MemoryStorage, Storage, StorageExt};
pub use table::{
    DeleteRequest, GetRequest, PutRequest, QueryRequest, ScanRequest, Table, TableBuilder,
    UpdateRequest,
};
pub use types::{
    AttributeValue, DecodeError, Item, KeyAttribute, KeySchema, KeyType, KeyValidationError,
    KeyValue, PrimaryKey, ReturnValue, WriteResult,
};
pub use update::{UpdateAction, UpdateExecutor, UpdateExpression};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
