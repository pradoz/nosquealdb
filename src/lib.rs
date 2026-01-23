pub mod batch;
pub mod condition;
pub mod error;
pub mod index;
pub mod query;
pub mod storage;
pub mod table;
pub mod transaction;
pub mod types;
pub mod update;
pub mod utils;

pub use batch::{
    BatchExecutor, BatchGetRequest, BatchGetResult, BatchWriteItem, BatchWriteRequest,
    BatchWriteResult,
};
pub use error::{StorageError, StorageResult, TableError, TableResult, TransactionCancelReason};
pub use index::{GlobalSecondaryIndex, GsiBuilder, LocalSecondaryIndex, LsiBuilder, Projection};
pub use query::{KeyCondition, QueryOptions, QueryResult, SortKeyOp};
pub use storage::{MemoryStorage, Storage, StorageExt};
pub use table::{
    DeleteRequest, GetRequest, PutRequest, QueryRequest, ScanRequest, Table, TableBuilder,
    UpdateRequest,
};
pub use transaction::{
    TransactGetItem, TransactGetRequest, TransactGetResult, TransactWriteItem,
    TransactWriteRequest, TransactionExecutor, TransactionFailureReason,
};
pub use types::{
    AttributeValue, DecodeError, Item, KeyAttribute, KeySchema, KeyType, KeyValidationError,
    KeyValue, PrimaryKey, ReturnValue, WriteResult, encode_key_component, escape_key_chars,
};
pub use update::{UpdateAction, UpdateExecutor, UpdateExpression};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
