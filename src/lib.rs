pub mod error;
pub mod storage;

pub use error::{StorageError, StorageResult};
pub use storage::{MemoryStorage, Storage, StorageExt};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
