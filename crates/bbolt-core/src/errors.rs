//! Error types for bbolt

use thiserror::Error;

/// Result type alias for bbolt operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during bbolt operations
#[derive(Error, Debug)]
pub enum Error {
    /// Database is not open
    #[error("database is not open")]
    DatabaseNotOpen,

    /// Database is read-only
    #[error("database was opened in read-only mode")]
    DatabaseReadOnly,

    /// Invalid database mapping
    #[error("database file is not correctly mapped")]
    InvalidMapping,

    /// Invalid database file
    #[error("invalid database file")]
    Invalid,

    /// Version mismatch
    #[error("version mismatch")]
    VersionMismatch,

    /// Checksum mismatch
    #[error("checksum mismatch")]
    Checksum,

    /// Operation timed out
    #[error("operation timed out")]
    Timeout,

    /// Transaction is closed
    #[error("transaction is not open")]
    TxClosed,

    /// Transaction is not writable
    #[error("transaction is not writable")]
    TxNotWritable,

    /// Bucket not found
    #[error("bucket not found")]
    BucketNotFound,

    /// Bucket already exists
    #[error("bucket already exists")]
    BucketExists,

    /// Bucket name is required
    #[error("bucket name is required")]
    BucketNameRequired,

    /// Key is required
    #[error("key is required")]
    KeyRequired,

    /// Key is too large
    #[error("key too large")]
    KeyTooLarge,

    /// Value is too large
    #[error("value too large")]
    ValueTooLarge,

    /// Incompatible value (trying to use bucket key as value or vice versa)
    #[error("incompatible value")]
    IncompatibleValue,

    /// Free pages not loaded
    #[error("free pages are not loaded")]
    FreePagesNotLoaded,

    /// Different database files
    #[error("source and target buckets are not in the same db file")]
    DifferentDB,

    /// Same buckets (cannot move bucket to itself)
    #[error("source and target buckets are the same")]
    SameBuckets,

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Database is full (for batch operations)
    #[error("database is full")]
    DatabaseFull,

    /// Page not found
    #[error("page {0} not found")]
    PageNotFound(u64),

    /// File lock error
    #[error("failed to acquire file lock")]
    FileLock,

    /// Other errors
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Returns true if this is a recoverable error
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Error::Io(_)
        )
    }
}