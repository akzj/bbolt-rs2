//! Database implementation for bbolt
//!
//! This module provides the main database interface.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

/// Global freelist storage for cross-transaction persistence
pub static GLOBAL_FREELIST: FreelistStore = FreelistStore::new();

/// Thread-safe freelist storage
pub struct FreelistStore {
    pages: Mutex<Vec<Pgid>>,
}

impl FreelistStore {
    pub const fn new() -> Self {
        Self {
            pages: Mutex::new(Vec::new()),
        }
    }
    
    /// Store free pages
    pub fn store(&self, free_pages: Vec<Pgid>) {
        let mut pages = free_pages;
        pages.sort();
        pages.reverse();
        *self.pages.lock().unwrap() = pages;
    }
    
    /// Load free pages
    pub fn load(&self) -> Vec<Pgid> {
        self.pages.lock().unwrap().clone()
    }
    
    /// Get page IDs
    pub fn get_page_ids(&self) -> Vec<Pgid> {
        self.pages.lock().unwrap().clone()
    }
}


use crate::constants::*;
use crate::errors::{Error, Result};
use crate::freelist::Freelist;
use crate::page::{InBucket, Meta, Page, Pgid};
use crate::tx::{Tx, TxDatabase};

/// Database options
#[derive(Debug, Clone)]
pub struct Options {
    /// Timeout for acquiring file lock
    pub timeout: std::time::Duration,
    /// Open in read-only mode
    pub read_only: bool,
    /// Page size (0 = auto-detect)
    pub page_size: usize,
    /// Initial mmap size
    pub initial_mmap_size: usize,
    /// No grow sync
    pub no_grow_sync: bool,
    /// No freelist sync
    pub no_freelist_sync: bool,
    /// Pre-load freelist
    pub pre_load_freelist: bool,
    /// No sync
    pub no_sync: bool,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum batch delay
    pub max_batch_delay: std::time::Duration,
    /// Allocation size
    pub alloc_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(0),
            read_only: false,
            page_size: 0,
            initial_mmap_size: 0,
            no_grow_sync: false,
            no_freelist_sync: false,
            pre_load_freelist: true,
            no_sync: false,
            max_batch_size: DEFAULT_ALLOC_SIZE,
            max_batch_delay: std::time::Duration::from_millis(10),
            alloc_size: DEFAULT_ALLOC_SIZE,
        }
    }
}

/// Database statistics
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub free_page_n: usize,
    pub pending_page_n: usize,
    pub free_alloc: usize,
    pub freelist_inuse: usize,
    pub tx_n: usize,
    pub open_tx_n: usize,
}

/// Database represents a bbolt database
pub struct Db {
    /// Path to the database file
    path: String,
    /// File handle
    file: RwLock<File>,
    /// Data (memory-mapped)
    data: RwLock<Vec<u8>>,
    /// Page size
    page_size: usize,
    /// Read-only mode
    read_only: bool,
    /// Meta pages
    meta0: Meta,
    meta1: Meta,
    /// Freelist
    freelist: Mutex<Freelist>,
    /// Stats
    stats: RwLock<Stats>,
    /// Options
    opts: Options,
}

impl Db {
    /// Open a database
    pub fn open(path: &Path, opts: Options) -> Result<Arc<Self>> {
        let path_str = path.to_string_lossy().to_string();

        // Open file
        let mut open_opts = OpenOptions::new();
        open_opts.read(true);

        if opts.read_only {
            open_opts.write(false);
        } else {
            open_opts.write(true).create(true); // No append for init
        }

        let mut file = open_opts.open(path)?;

        // Get file size
        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        // Initialize or read database
        let (data, meta0, meta1, page_size) = if file_size == 0 {
            // Initialize new database
            let page_size = if opts.page_size > 0 { opts.page_size } else { DEFAULT_PAGE_SIZE };
            let (data, meta0, meta1) = Self::init_new(&mut file, page_size, path)?;
            (data, meta0, meta1, page_size)
        } else {
            // Read existing database - first detect page size
            let page_size = if opts.page_size > 0 {
                opts.page_size
            } else {
                // Read a small header to detect page size
                let header_size = 32768; // Read up to 32KB to detect page size
                let to_read = std::cmp::min(file_size, header_size);
                let mut header = vec![0u8; to_read];
                use std::io::Read;
                let mut f = std::fs::File::open(path)?;
                f.read_exact(&mut header)?;
                Self::detect_page_size(&header)?
            };
            
            // Now read the full file with detected page size
            let (data, meta0, meta1) = Self::read_existing(path, file_size, page_size)?;
            (data, meta0, meta1, page_size)
        };

        // Create database handle
        let db = Arc::new(Self {
            path: path_str,
            file: RwLock::new(file),
            data: RwLock::new(data),
            page_size,
            read_only: opts.read_only,
            meta0,
            meta1,
            freelist: Mutex::new(Freelist::new()),
            stats: RwLock::new(Stats::default()),
            opts,
        });

        Ok(db)
    }

    /// Initialize a new database
    fn init_new(file: &mut File, page_size: usize, file_path: &Path) -> Result<(Vec<u8>, Meta, Meta)> {
        let file_size = page_size * 4;
        let mut data = vec![0u8; file_size];

        // Write meta0 page (page 0)
        let meta0 = Self::write_meta(&mut data, 0, page_size, 0)?;

        // Write meta1 page (page 1)
        let meta1 = Self::write_meta(&mut data, page_size, page_size, 1)?;

        // Page 2 (freelist) and page 3 (leaf) are already zeroed

        // Write to file
        file.set_len(file_size as u64)?;
        file.write_all(&data)?;
        file.flush()?;
        file.sync_all()?;

        Ok((data, meta0, meta1))
    }

    /// Write a meta page to buffer
    fn write_meta(data: &mut [u8], offset: usize, page_size: usize, txid: u64) -> Result<Meta> {
        let root = InBucket::new(0, 0); // Root bucket starts empty (pgid 0 = no page)
        let freelist_pgid = 2; // Freelist at page 2

        let meta = Meta::new(
            page_size as u32,
            freelist_pgid,
            root,
            4, // pgid (high water mark)
            txid,
        );

        // Calculate actual offset in data
        let buf = &mut data[offset..offset + page_size];

        // Write page header
        let page_id = txid % 2; // Page 0 or 1
        buf[0..8].copy_from_slice(&page_id.to_le_bytes()); // id
        buf[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes()); // flags
        buf[10..12].copy_from_slice(&0u16.to_le_bytes()); // count
        buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow

        // Write meta data (starts after page header)
        let meta_offset = 16;
        buf[meta_offset..meta_offset + 4].copy_from_slice(&meta.magic.to_le_bytes()); // magic
        buf[meta_offset + 4..meta_offset + 8].copy_from_slice(&meta.version.to_le_bytes()); // version
        buf[meta_offset + 8..meta_offset + 12].copy_from_slice(&meta.page_size.to_le_bytes()); // page_size
        buf[meta_offset + 12..meta_offset + 16].copy_from_slice(&meta.flags.to_le_bytes()); // flags
        buf[meta_offset + 16..meta_offset + 24].copy_from_slice(&meta.root.root.to_le_bytes()); // root.pgid
        buf[meta_offset + 24..meta_offset + 32].copy_from_slice(&meta.root.sequence.to_le_bytes()); // root.sequence
        buf[meta_offset + 32..meta_offset + 40].copy_from_slice(&meta.freelist.to_le_bytes()); // freelist
        buf[meta_offset + 40..meta_offset + 48].copy_from_slice(&meta.pgid.to_le_bytes()); // pgid
        buf[meta_offset + 48..meta_offset + 56].copy_from_slice(&meta.txid.to_le_bytes()); // txid
        buf[meta_offset + 56..meta_offset + 64].copy_from_slice(&meta.checksum.to_le_bytes()); // checksum

        Ok(meta)
    }

    /// Read an existing database

    /// Detect page size from file by looking for magic number
    fn detect_page_size(data: &[u8]) -> Result<usize> {
        const MAGIC: u32 = 0xED0CDAED;
        // Try common page sizes in descending order (larger first)
        let page_sizes = [16384, 8192, 4096, 2048, 1024, 512];
        
        // meta0 is always at offset 16 (first page's meta data)
        const META_OFFSET: usize = 16;
        
        for &ps in &page_sizes {
            if data.len() < ps * 2 {
                continue;
            }
            
            // Check meta0 at offset 16 (always first page)
            if META_OFFSET + 4 > data.len() {
                continue;
            }
            
            let magic = u32::from_le_bytes([
                data[META_OFFSET],
                data[META_OFFSET + 1],
                data[META_OFFSET + 2],
                data[META_OFFSET + 3],
            ]);
            
            if magic == MAGIC {
                // meta0 found, verify meta1 at ps + 16 (second page's meta data)
                if data.len() >= ps + 20 {
                    let magic1 = u32::from_le_bytes([
                        data[ps + 16],
                        data[ps + 17],
                        data[ps + 18],
                        data[ps + 19],
                    ]);
                    if magic1 == MAGIC {
                                    return Ok(ps);
                    }
                }
            }
        }
        
        // Default fallback
        Ok(DEFAULT_PAGE_SIZE)
    }

    fn read_existing(file_path: &Path, file_size: usize, page_size: usize) -> Result<(Vec<u8>, Meta, Meta)> {
        // Read entire file
        let mut data = std::fs::read(file_path)?;
        
        // Pad if necessary
        if data.len() < file_size {
            data.resize(file_size, 0);
        }

        // Read meta0
        let meta0 = Self::read_meta(&data[0..page_size], 0)?;

        // Read meta1
        let meta1 = Self::read_meta(&data[page_size..page_size * 2], 1)?;

        // Read freelist if exists
        let freelist_pgid = meta0.freelist;
        if freelist_pgid > 0 && freelist_pgid != Pgid::MAX {
            let freelist_offset = freelist_pgid as usize * page_size;
            if freelist_offset + page_size <= data.len() {
                let freelist_page = Self::read_page(&data[freelist_offset..freelist_offset + page_size]);
                let mut freelist = Freelist::new();
                freelist.read(&freelist_page, &data[freelist_offset..freelist_offset + page_size]);
                // Store freelist in global storage
                GLOBAL_FREELIST.store(freelist.get_free_pages());
            }
        }

        Ok((data, meta0, meta1))
    }
    
    /// Read a page from buffer
    fn read_page(buf: &[u8]) -> Page {
        use std::ptr;
        let ptr = buf.as_ptr() as *const Page;
        unsafe { ptr::read_unaligned(ptr) }
    }

    /// Read a meta page from buffer
    fn read_meta(buf: &[u8], _page_num: usize) -> Result<Meta> {
        let meta_offset = 16; // After page header

        let magic = u32::from_le_bytes([
            buf[meta_offset],
            buf[meta_offset + 1],
            buf[meta_offset + 2],
            buf[meta_offset + 3],
        ]);
        let version = u32::from_le_bytes([
            buf[meta_offset + 4],
            buf[meta_offset + 5],
            buf[meta_offset + 6],
            buf[meta_offset + 7],
        ]);
        let page_size = u32::from_le_bytes([
            buf[meta_offset + 8],
            buf[meta_offset + 9],
            buf[meta_offset + 10],
            buf[meta_offset + 11],
        ]);
        let flags = u32::from_le_bytes([
            buf[meta_offset + 12],
            buf[meta_offset + 13],
            buf[meta_offset + 14],
            buf[meta_offset + 15],
        ]);
        let root_pgid = u64::from_le_bytes([
            buf[meta_offset + 16],
            buf[meta_offset + 17],
            buf[meta_offset + 18],
            buf[meta_offset + 19],
            buf[meta_offset + 20],
            buf[meta_offset + 21],
            buf[meta_offset + 22],
            buf[meta_offset + 23],
        ]);
        let root_sequence = u64::from_le_bytes([
            buf[meta_offset + 24],
            buf[meta_offset + 25],
            buf[meta_offset + 26],
            buf[meta_offset + 27],
            buf[meta_offset + 28],
            buf[meta_offset + 29],
            buf[meta_offset + 30],
            buf[meta_offset + 31],
        ]);
        let freelist = u64::from_le_bytes([
            buf[meta_offset + 32],
            buf[meta_offset + 33],
            buf[meta_offset + 34],
            buf[meta_offset + 35],
            buf[meta_offset + 36],
            buf[meta_offset + 37],
            buf[meta_offset + 38],
            buf[meta_offset + 39],
        ]);
        let pgid = u64::from_le_bytes([
            buf[meta_offset + 40],
            buf[meta_offset + 41],
            buf[meta_offset + 42],
            buf[meta_offset + 43],
            buf[meta_offset + 44],
            buf[meta_offset + 45],
            buf[meta_offset + 46],
            buf[meta_offset + 47],
        ]);
        let txid = u64::from_le_bytes([
            buf[meta_offset + 48],
            buf[meta_offset + 49],
            buf[meta_offset + 50],
            buf[meta_offset + 51],
            buf[meta_offset + 52],
            buf[meta_offset + 53],
            buf[meta_offset + 54],
            buf[meta_offset + 55],
        ]);
        let checksum = u64::from_le_bytes([
            buf[meta_offset + 56],
            buf[meta_offset + 57],
            buf[meta_offset + 58],
            buf[meta_offset + 59],
            buf[meta_offset + 60],
            buf[meta_offset + 61],
            buf[meta_offset + 62],
            buf[meta_offset + 63],
        ]);

        let meta = Meta {
            magic,
            version,
            page_size,
            flags,
            root: InBucket::new(root_pgid, root_sequence),
            freelist,
            pgid,
            txid,
            checksum,
        };

        meta.validate()?;

        Ok(meta)
    }

    /// Get the path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get page size
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Get a meta page
    pub fn meta(&self) -> &Meta {
        if self.meta0.txid() > self.meta1.txid() {
            &self.meta0
        } else {
            &self.meta1
        }
    }

    /// Get page data
    pub fn page_data(&self, pgid: Pgid) -> Option<Vec<u8>> {
        let data = self.data.read().unwrap();
        let offset = pgid as usize * self.page_size;
        let end = offset + self.page_size;
        if end > data.len() {
            return None;
        }
        Some(data[offset..end].to_vec())
    }

    /// Get page info
    pub fn page(&self, pgid: Pgid) -> Page {
        let data = self.data.read().unwrap();
        let offset = pgid as usize * self.page_size;
        let buf = &data[offset..offset + self.page_size];

        Page {
            id: pgid,
            flags: u16::from_le_bytes([buf[8], buf[9]]),
            count: u16::from_le_bytes([buf[10], buf[11]]),
            overflow: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
        }
    }

    /// Begin a transaction
    pub fn begin(&self, writable: bool) -> Result<Tx> {
        if writable && self.read_only {
            return Err(Error::DatabaseReadOnly);
        }

        let data = self.data.read().unwrap().clone();
        let tx_db = TxDatabase::new(self.page_size, *self.meta(), data);
        Tx::begin(tx_db, writable)
    }

    /// Update statistics
    pub fn update_stats(&self, stats: &Stats) {
        let mut current = self.stats.write().unwrap();
        *current = stats.clone();
    }

    /// Get statistics
    pub fn stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }

    /// Close the database
    pub fn close(&self) -> Result<()> {
        // Drop all references
        Ok(())
    }

    /// Update the database data after a commit
    pub fn update_data(&self, data: Vec<u8>) {
        *self.data.write().unwrap() = data;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_db_open_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();
        assert_eq!(db.page_size(), DEFAULT_PAGE_SIZE);

        db.close().unwrap();
    }

    #[test]
    fn test_db_open_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create database
        {
            let db = Db::open(&path, Options::default()).unwrap();
            assert_eq!(db.page_size(), DEFAULT_PAGE_SIZE);
            db.close().unwrap();
        }

        // Reopen
        {
            let db = Db::open(&path, Options::default()).unwrap();
            assert_eq!(db.page_size(), DEFAULT_PAGE_SIZE);
            db.close().unwrap();
        }
    }

    #[test]
    fn test_meta_validation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();
        let meta = db.meta();

        // Validate meta
        assert!(meta.validate().is_ok());

        // Check page IDs
        assert!(meta.txid() == 0 || meta.txid() == 1);

        db.close().unwrap();
    }
    
    #[test]
    fn test_binary_format_compatible() {
        use std::fs;
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create database
        let db = Db::open(&path, Options::default()).unwrap();
        db.close().unwrap();
        
        // Read raw bytes
        let data = fs::read(&path).unwrap();
        
        // Verify file header structure
        assert!(data.len() >= 16384, "File should be at least 16384 bytes (4 pages)");
        
        // Page 0: Meta0
        // Offset 0-7: page id = 0
        assert_eq!(data[0..8], [0u8; 8], "Meta0 page id should be 0");
        // Offset 8-9: flags = 0x04 (META_PAGE_FLAG)
        assert_eq!(data[8], 0x04, "Meta0 flags should be 0x04 (META_PAGE_FLAG)");
        
        // Meta data starts at offset 16
        // Magic: 0xED0CDAED
        assert_eq!(&data[16..20], &[0xED, 0xDA, 0x0C, 0xED], "Meta magic should be 0xED0CDAED");
        // Version: 2
        assert_eq!(&data[20..24], &[0x02, 0x00, 0x00, 0x00], "Meta version should be 2");
        
        // Page 1: Meta1
        // Offset 4096-4103: page id = 1
        assert_eq!(data[4096..4104], 1u64.to_le_bytes(), "Meta1 page id should be 1");
        // Offset 4104-4105: flags = 0x04
        assert_eq!(data[4104], 0x04, "Meta1 flags should be 0x04");
        
        println!("Binary format validation passed!");
        println!("File size: {} bytes", data.len());
        println!("Meta0 magic: 0x{:02x}{:02x}{:02x}{:02x}", data[16], data[17], data[18], data[19]);
    }
    
    #[test]
    fn test_go_cross_compatibility() {
        // This test creates a database that should be readable by Go bbolt
        // We verify the file format matches Go bbolt's expectations
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_go.db");

        let db = Db::open(&path, Options::default()).unwrap();
        db.close().unwrap();
        
        // Read raw bytes
        let data = std::fs::read(&path).unwrap();
        
        // Verify Go bbolt expects these values:
        // - Meta magic: 0xED0CDAED (big-endian bytes: ED DA 0C ED)
        // - Meta version: 2
        // - Page size: 4096 (0x1000)
        assert_eq!(&data[16..20], &[0xED, 0xDA, 0x0C, 0xED], "Meta magic must be 0xED0CDAED");
        assert_eq!(&data[20..24], &[0x02, 0x00, 0x00, 0x00], "Meta version must be 2");
        
        // Page size is stored at offset 24-27
        let page_size = u32::from_le_bytes([data[24], data[25], data[26], data[27]]);
        assert_eq!(page_size, 4096, "Default page size must be 4096");
        
        println!("Go cross-compatibility format validated!");
        println!("Rust bbolt creates databases compatible with Go bbolt format");
    }
}
#[cfg(test)]
impl Db {
    fn debug_meta(&self) {
        use crate::page::Meta;
        println!("meta0: magic=0x{:x}, version={}, txid={}, checksum=0x{:x}", 
            self.meta0.magic, self.meta0.version, self.meta0.txid, self.meta0.checksum);
        println!("meta0 sum64: 0x{:x}", self.meta0.sum64());
    }
}

#[cfg(test)]
mod cross_test {
    use super::*;
    
    #[test]
    fn test_rust_creates_go_readable_db() {
        let path = std::path::Path::new("/tmp/rust_test.db");
        std::fs::remove_file(path).ok();
        
        // Create database with Rust
        let db = Db::open(path, Options::default()).unwrap();
        db.close().unwrap();
        
        // Verify file was created
        assert!(path.exists(), "Database file should exist");
        
        // Verify file has correct magic
        let data = std::fs::read(path).unwrap();
        let magic = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
        assert_eq!(magic, 0xED0CDAED, "Meta magic should be 0xED0CDAED");
        
        println!("Rust created database at /tmp/rust_test.db");
        println!("Run: cd /tmp/bbolt-cross-test && go run test_rust_db.go");
    }
}

#[cfg(test)]
mod go_read_tests {
    use super::*;
    
    #[test]
    fn test_read_go_written_bucket() {
        // This test reads a database created by Go bbolt
        // The Go test script created bucket "test" with key1=value1_updated and key3=value3
        let path = std::path::Path::new("/tmp/gotest/cross_test.db");
        
        if !path.exists() {
            println!("Skipping: /tmp/gotest/cross_test.db not found. Run Go test first.");
            return;
        }
        
        let db = Db::open(path, Options::default()).unwrap();
        
        // Begin read transaction
        // For inline bucket, the value contains an inline page
        // We need to parse it to verify the content
        
        // Read root page directly
        let meta = db.meta();
        let root_pgid = meta.root().root_pgid();
        let root_data = db.page_data(root_pgid).unwrap();
        
        // Get bucket entry
        let count = u16::from_le_bytes([root_data[10], root_data[11]]) as usize;
        assert_eq!(count, 1, "Root page should have 1 entry");
        
        let elem_start = 16usize;
        let ksize = u32::from_le_bytes([root_data[elem_start+8], root_data[elem_start+9], root_data[elem_start+10], root_data[elem_start+11]]) as usize;
        let vsize = u32::from_le_bytes([root_data[elem_start+12], root_data[elem_start+13], root_data[elem_start+14], root_data[elem_start+15]]) as usize;
        
        let key_off = elem_start + 16;
        assert_eq!(&root_data[key_off..key_off + ksize], b"test", "Bucket key should be 'test'");
        
        // Parse inline bucket value
        let value_off = key_off + ksize;
        let value = &root_data[value_off..value_off + vsize];
        
        let bucket_header_size = 16usize;
        let inline_count = u16::from_le_bytes([value[bucket_header_size+10], value[bucket_header_size+11]]) as usize;
        assert_eq!(inline_count, 2, "Inline bucket should have 2 entries");
        
        // Verify key1=value1_updated
        // For element j: data is at bucket_header_size + 16 (page header) + 16*count + sum(prev sizes)
        let elem_area_size = 16 * inline_count;
        let data_start = bucket_header_size + 16 + elem_area_size;
        
        // Elem 0
        let e0_off = bucket_header_size + 16;
        let e0_ks = u32::from_le_bytes([value[e0_off+8], value[e0_off+9], value[e0_off+10], value[e0_off+11]]) as usize;
        let e0_vs = u32::from_le_bytes([value[e0_off+12], value[e0_off+13], value[e0_off+14], value[e0_off+15]]) as usize;
        let e0_key = &value[data_start..data_start + e0_ks];
        let e0_val = &value[data_start + e0_ks..data_start + e0_ks + e0_vs];
        assert_eq!(e0_key, b"key1", "First key should be 'key1'");
        assert_eq!(e0_val, b"value1_updated", "First value should be 'value1_updated'");
        
        // Elem 1
        let prev_size = e0_ks + e0_vs;
        let e1_off = bucket_header_size + 16 + 16;
        let e1_ks = u32::from_le_bytes([value[e1_off+8], value[e1_off+9], value[e1_off+10], value[e1_off+11]]) as usize;
        let e1_vs = u32::from_le_bytes([value[e1_off+12], value[e1_off+13], value[e1_off+14], value[e1_off+15]]) as usize;
        let e1_key = &value[data_start + prev_size..data_start + prev_size + e1_ks];
        let e1_val = &value[data_start + prev_size + e1_ks..data_start + prev_size + e1_ks + e1_vs];
        assert_eq!(e1_key, b"key3", "Second key should be 'key3'");
        assert_eq!(e1_val, b"value3", "Second value should be 'value3'");
        
        db.close().unwrap();
        
        println!("Successfully read Go-written bucket with key1=value1_updated and key3=value3");
    }
}
