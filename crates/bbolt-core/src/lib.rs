//! bbolt-core - Core implementation of bbolt database
//!
//! This crate provides the core implementation of a bbolt-compatible
//! embedded key-value database in pure Rust.
//!
//! # Features
//!
//! - Pure Rust implementation
//! - Binary compatible with Go bbolt file format
//! - MVCC with multiple readers and single writer
//! - B+tree storage
//! - Copy-on-write semantics
//! - Memory-mapped file I/O
//!
//! # Example
//!
//! ```rust,no_run
//! use bbolt_core::{Db, Options};
//!
//! let opts = Options::default();
//! let db = Db::open(std::path::Path::new("test.db"), opts).unwrap();
//!
//! // Use the database...
//!
//! db.close().unwrap();
//! ```

pub mod bucket;
pub mod constants;
pub mod cursor;
pub mod db;
pub mod errors;
pub mod freelist;
pub mod logger;
pub mod node;
pub mod page;
pub mod platform;
pub mod tx;

// Re-exports
pub use bucket::{Bucket, BucketStats};
pub use constants::{LeafFlags, PageFlags};
pub use cursor::Cursor;
pub use db::{Db, DbInfo, Options, Stats};
pub use errors::{Error, Result};
pub use freelist::Freelist;
pub use logger::{DefaultLogger, DiscardLogger, Logger, SimpleLogger};
pub use node::Node;
pub use page::{
    BranchPageElement, InBucket, Inode, LeafPageElement, Meta, Page, PageInfo, Pgid, Txid,
};
pub use tx::{Tx, TxStats, WriteTo};

#[cfg(test)]
mod concurrent_test;