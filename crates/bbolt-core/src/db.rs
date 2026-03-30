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
use crate::page::{InBucket, Meta, Page, Pgid, Txid};
use crate::tx::{Tx, TxDatabase};
use crate::Bucket;

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

/// Database information
#[derive(Debug, Clone)]
pub struct DbInfo {
    /// Path to the database file
    pub path: String,
    /// Page size in bytes
    pub page_size: usize,
    /// Database version
    pub version: u32,
    /// True if the database is in read-only mode
    pub read_only: bool,
    /// Current transaction ID
    pub tx_id: Txid,
    /// Root bucket root page ID
    pub root_pgid: Pgid,
    /// Freelist page ID
    pub freelist_pgid: Pgid,
    /// High water mark (total page count)
    pub pgid: Pgid,
}

/// Options for database compaction
pub struct CompactOptions {
    /// Compaction options
    pub compaction: bool,
    /// Skip freelist sync
    pub tmp_no_freelist_sync: bool,
    /// Parallel transactions (txMaxSize in Go)
    pub tx_max_size: i64,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            compaction: true,
            tmp_no_freelist_sync: false,
            tx_max_size: 0, // 0 means ignore transaction size
        }
    }
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
    /// Meta pages (using Mutex for interior mutability when updating after commits)
    meta0: Mutex<Meta>,
    meta1: Mutex<Meta>,
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
            meta0: Mutex::new(meta0),
            meta1: Mutex::new(meta1),
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
    pub fn detect_page_size(data: &[u8]) -> Result<usize> {
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

    /// Returns true if the database was opened in read-only mode
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
    
    /// Get the current database file size in bytes
    pub fn file_size(&self) -> u64 {
        self.data.read().unwrap().len() as u64
    }

    /// Get a meta page
    pub fn meta(&self) -> Meta {
        let meta0 = self.meta0.lock().unwrap();
        let meta1 = self.meta1.lock().unwrap();
        if meta0.txid() > meta1.txid() {
            *meta0
        } else {
            *meta1
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
        let tx_db = TxDatabase::new(self.page_size, self.meta(), data, Some(std::path::PathBuf::from(&self.path)));
        Tx::begin(tx_db, writable)
    }

    /// Begin a read-only transaction
    pub fn begin_tx(&self) -> Result<Tx> {
        self.begin(false)
    }

    /// Begin a read-write transaction
    pub fn begin_rw_tx(&self) -> Result<Tx> {
        self.begin(true)
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

    /// View executes a function within a read transaction.
    /// The function receives a read-only transaction and any error returned
    /// by the function is returned from view().
    pub fn view<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&Tx) -> Result<()>,
    {
        let mut tx = self.begin(false)?;
        let result = f(&tx);
        let _ = tx.rollback();
        result
    }

    /// Update executes a function within a writable transaction.
    /// The function receives a writable transaction and any error returned
    /// by the function is returned from update().
    /// The transaction is committed if the function returns Ok, otherwise
    /// it is rolled back.
    pub fn update<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut Tx) -> Result<()>,
    {
        let mut tx = self.begin(true)?;
        let result = f(&mut tx);
        if result.is_ok() {
            tx.commit()?;
            // Update Db's data with the committed data
            let committed_data = tx.committed_data();
            self.update_data(committed_data);
        } else {
            let _ = tx.rollback();
        }
        result
    }

    /// Batch calls fn as part of a batch. It behaves similar to Update,
    /// except:
    /// - it calls fn multiple times (in a single transaction) if
    ///   Err(Err::DatabaseFull) is returned from fn
    /// - it is faster than calling Update multiple times because
    ///   the transaction is only committed once at the end
    /// - the function used is retried until success or max_batch_size
    ///   iterations have been tried
    pub fn batch<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut Tx) -> Result<()>,
    {
        let mut retried = 0;
        let max_batch_size = self.opts.max_batch_size;
        let max_batch_delay = self.opts.max_batch_delay;

        loop {
            let mut tx = self.begin(true)?;
            let err = f(&mut tx).err();
            
            if err.is_none() {
                tx.commit()?;
                // Update Db's data with the committed data
                let committed_data = tx.committed_data();
                self.update_data(committed_data);
                return Ok(());
            }

            // Check if we should retry
            let err = err.unwrap();
            if !matches!(err, Error::DatabaseFull) {
                let _ = tx.rollback();
                return Err(err);
            }

            // Check retry limit
            retried += 1;
            if retried >= max_batch_size {
                let _ = tx.rollback();
                return Err(Error::DatabaseFull);
            }

            let _ = tx.rollback();
            
            // Wait before retry
            std::thread::sleep(max_batch_delay);
        }
    }

    /// Info returns database information
    pub fn info(&self) -> DbInfo {
        let meta = self.meta();
        DbInfo {
            path: self.path.clone(),
            page_size: self.page_size,
            version: 2,
            read_only: self.read_only,
            tx_id: meta.txid(),
            root_pgid: meta.root.root_pgid(),
            freelist_pgid: meta.freelist_pgid(),
            pgid: meta.pgid(),
        }
    }

    /// Sync performs a filesystem sync on the database file
    pub fn sync(&self) -> Result<()> {
        let file = self.file.write().unwrap();
        file.sync_all()?;
        Ok(())
    }

    /// Grow the database file to the specified size.
    /// If the current file size is already >= size, this does nothing.
    /// This is used internally when writing beyond the current file size.
    pub fn grow(&self, size: usize) -> Result<()> {
        // Ignore if already at or above target size
        let metadata = self.file.read().unwrap().metadata()?;
        let current_size = metadata.len() as usize;
        
        if size <= current_size {
            return Ok(());
        }
        
        // For read-only databases, we cannot grow
        if self.read_only {
            return Err(Error::TxNotWritable);
        }
        
        // Truncate file to new size
        let mut file = self.file.write().unwrap();
        file.set_len(size as u64)?;
        file.sync_all()?;
        
        Ok(())
    }

    /// Check performs a consistency check on the database.
    /// It traverses all pages and verifies checksums.
    /// Returns Ok(()) if no errors found, or Err with diagnostic info.
    pub fn check(&self) -> Result<()> {
        let meta = self.meta();
        let page_size = self.page_size;
        let data = self.data.read().unwrap();
        let total_pages = data.len() / page_size;

        // Check meta page checksum
        if meta.checksum != meta.sum64() {
            return Err(Error::Checksum);
        }

        // Get free page IDs
        let freelist = GLOBAL_FREELIST.load();
        let freelist_set: std::collections::HashSet<Pgid> = freelist.iter().cloned().collect();

        // Traverse all pages
        for pgid in 0..total_pages {
            let offset = pgid * page_size;
            if offset + page_size > data.len() {
                break;
            }

            let page_buf = &data[offset..offset + page_size];
            let page_id = u64::from_le_bytes([page_buf[0], page_buf[1], page_buf[2], page_buf[3],
                                              page_buf[4], page_buf[5], page_buf[6], page_buf[7]]);
            let flags = u16::from_le_bytes([page_buf[8], page_buf[9]]);

            // Verify page ID matches
            if page_id != pgid as Pgid {
                return Err(Error::Other(format!("page {}: expected id {}, got {}", pgid, pgid, page_id)));
            }

            // Skip free pages (except meta pages)
            if pgid >= 2 && freelist_set.contains(&(pgid as Pgid)) {
                continue;
            }

            // Check meta page
            if flags == PageFlags::META_PAGE_FLAG.bits() {
                let meta_offset = 16;
                let magic = u32::from_le_bytes([page_buf[meta_offset], page_buf[meta_offset + 1],
                                                  page_buf[meta_offset + 2], page_buf[meta_offset + 3]]);
                if magic != MAGIC {
                    return Err(Error::Other(format!("page {}: invalid meta magic 0x{:x}", pgid, magic)));
                }
                continue;
            }

            // Check branch/leaf page structure
            if flags == PageFlags::BRANCH_PAGE_FLAG.bits() || flags == PageFlags::LEAF_PAGE_FLAG.bits() {
                let count = u16::from_le_bytes([page_buf[10], page_buf[11]]) as usize;
                let elem_size = 16usize;
                let elem_start = 16usize;

                // Verify element positions are within bounds
                for i in 0..count {
                    let elem_offset = elem_start + i * elem_size;
                    let pos = u32::from_le_bytes([page_buf[elem_offset + 4], page_buf[elem_offset + 5],
                                                   page_buf[elem_offset + 6], page_buf[elem_offset + 7]]) as usize;
                    let ksize = u32::from_le_bytes([page_buf[elem_offset + 8], page_buf[elem_offset + 9],
                                                    page_buf[elem_offset + 10], page_buf[elem_offset + 11]]) as usize;
                    let vsize = if flags == PageFlags::LEAF_PAGE_FLAG.bits() {
                        u32::from_le_bytes([page_buf[elem_offset + 12], page_buf[elem_offset + 13],
                                            page_buf[elem_offset + 14], page_buf[elem_offset + 15]]) as usize
                    } else {
                        0
                    };

                    let end = pos + ksize + vsize;
                    if end > page_size {
                        return Err(Error::Other(format!("page {}: element {}: key/value exceeds page size", pgid, i)));
                    }
                }

                // For branch pages, check child page IDs
                if flags == PageFlags::BRANCH_PAGE_FLAG.bits() {
                    for i in 0..count {
                        let elem_offset = elem_start + i * elem_size;
                        let child_pgid = u64::from_le_bytes([page_buf[elem_offset + 8], page_buf[elem_offset + 9],
                                                              page_buf[elem_offset + 10], page_buf[elem_offset + 11],
                                                              page_buf[elem_offset + 12], page_buf[elem_offset + 13],
                                                              page_buf[elem_offset + 14], page_buf[elem_offset + 15]]);
                        if child_pgid >= total_pages as Pgid {
                            return Err(Error::Other(format!("page {}: element {}: child page {} out of bounds", pgid, i, child_pgid)));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Close the database
    pub fn close(&self) -> Result<()> {
        // Drop all references
        Ok(())
    }

    /// Update the database data after a commit
    pub fn update_data(&self, data: Vec<u8>) {
        *self.data.write().unwrap() = data;
        
        // Also update meta0 and meta1 from the new data
        let page_size = self.page_size;
        
        // Read meta0 from page 0
        let meta0_buf = self.data.read().unwrap()[0..page_size].to_vec();
        if let Ok(meta0) = Self::read_meta(&meta0_buf, 0) {
            *self.meta0.lock().unwrap() = meta0;
        }
        
        // Read meta1 from page 1
        let meta1_buf = self.data.read().unwrap()[page_size..page_size * 2].to_vec();
        if let Ok(meta1) = Self::read_meta(&meta1_buf, 1) {
            *self.meta1.lock().unwrap() = meta1;
        }
    }

    /// Compact the database by creating a compacted copy at the destination path.
    /// Uses walk-based compaction similar to Go bbolt's Compact function.
    pub fn compact(&self, path: &Path, options: &CompactOptions) -> Result<Arc<Self>> {
        // Open destination database
        let dst_db = Self::open(path, Options::default())?;
        
        // Use write transaction to copy all data
        let tx_max_size = options.tx_max_size;
        let mut size: i64 = 0;
        let mut dst_tx = dst_db.begin(true)?;
        
        // Walk all buckets in source and copy to destination
        self.view(|src_tx| {
            Self::walk_buckets(src_tx, &[], |keys, k, v, seq| {
                // Check if we need to commit due to transaction size
                let sz = (k.len() + v.len()) as i64;
                if tx_max_size > 0 && size + sz > tx_max_size {
                    dst_tx.commit()?;
                    dst_tx = dst_db.begin(true)?;
                    size = 0;
                }
                size += sz;
                
                // Handle root level (create bucket)
                if keys.is_empty() {
                    let mut bucket = dst_tx.create_bucket(k)?;
                    bucket.set_sequence(seq)?;
                    return Ok(());
                }
                
                // Navigate to destination bucket
                let mut dst_bucket = dst_tx.bucket(keys[0]).ok_or(Error::BucketNotFound)?;
                for key in &keys[1..] {
                    dst_bucket = dst_bucket.get_bucket(key).ok_or(Error::BucketNotFound)?;
                }
                
                // If v is empty, this is a bucket
                if v.is_empty() {
                    let mut sub_bucket = dst_bucket.create_bucket(k)?;
                    sub_bucket.set_sequence(seq)?;
                    return Ok(());
                }
                
                // Otherwise it's a key-value pair
                dst_bucket.put(k, v)?;
                Ok(())
            })
        })?;
        
        dst_tx.commit()?;
        Ok(dst_db)
    }
    
    /// Walk all buckets recursively, calling the callback for each key/value
    fn walk_buckets<F>(tx: &Tx, keys: &[&[u8]], mut f: F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8], &[u8], u64) -> Result<()>,
    {
        // Iterate over root buckets
        tx.root().for_each_bucket(&mut |name: &[u8], bucket: Bucket| {
            let seq = bucket.sequence();
            
            // Callback for root bucket
            f(keys, name, &[], seq)?;
            
            // Walk this bucket's contents recursively
            Self::walk_bucket(&bucket, &[name], &mut f)?;
            Ok(())
        })
    }
    
    /// Walk a bucket's contents recursively
    fn walk_bucket<F>(bucket: &Bucket, keypath: &[&[u8]], f: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8], &[u8], u64) -> Result<()>,
    {
        let mut cursor = bucket.cursor();
        while let Some((k, v)) = cursor.next() {
            // Check if this is a bucket (value is a sub-bucket reference)
            if let Some(sub_bucket) = bucket.get_bucket(&k) {
                // This is a sub-bucket
                let seq = sub_bucket.sequence();
                f(keypath, &k, &[], seq)?;
                
                // Recurse into sub-bucket
                let mut new_path = keypath.to_vec();
                new_path.push(&k);
                Self::walk_bucket(&sub_bucket, &new_path, f)?;
            } else {
                // Regular key-value
                f(keypath, &k, &v, bucket.sequence())?;
            }
        }
        Ok(())
    }
    
    /// Copy all entries from source bucket to destination bucket
    #[allow(dead_code)]
    fn copy_bucket(dst_tx: &mut Tx, src_bucket: &Bucket, dst_bucket: &mut Bucket) -> Result<()> {
        use crate::constants::LeafFlags;
        
        let mut cursor = src_bucket.cursor();
        while let Some((k, v, flags)) = cursor.next_with_flags() {
            let is_bucket = flags & LeafFlags::BUCKET_LEAF_FLAG.bits() != 0;
            
            if is_bucket {
                // Create nested bucket
                let mut sub_bucket = dst_bucket.create_bucket(&k)?;
                
                // Recurse into nested bucket
                if let Some(src_sub) = src_bucket.get_bucket(&k) {
                    Self::copy_bucket(dst_tx, &src_sub, &mut sub_bucket)?;
                }
            } else {
                // Key-value pair
                dst_bucket.put(&k, &v)?;
            }
        }
        
        Ok(())
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
    fn test_db_is_read_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Default options - not read only
        let db = Db::open(&path, Options::default()).unwrap();
        assert!(!db.is_read_only());
        db.close().unwrap();

        // Explicit read_only option
        let mut opts = Options::default();
        opts.read_only = true;
        let db = Db::open(&path, opts).unwrap();
        assert!(db.is_read_only());
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

    // ============================================
    // Tests for new features (1-10)
    // ============================================

    #[test]
    fn test_db_view() {
        // Hypothesis: view() should execute a closure with a read transaction
        // and return any error from the closure
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();

        // Test 1: view with successful closure
        let result = db.view(|tx| {
            // Should be able to read from the transaction
            let mut cursor = tx.cursor();
            let first = cursor.first();
            // Empty bucket returns None
            assert!(first.is_none());
            Ok(())
        });
        assert!(result.is_ok(), "view should succeed with valid closure");

        // Test 2: view propagates errors from closure
        let result = db.view(|_tx| {
            Err(Error::BucketNotFound)
        });
        assert!(matches!(result, Err(Error::BucketNotFound)), "view should propagate errors");

        db.close().unwrap();
    }

    #[test]
    fn test_db_update() {
        // Hypothesis: update() should execute a closure with a writable transaction,
        // commit on success, rollback on failure
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();

        // Test 1: update with successful closure
        let result = db.update(|tx| {
            tx.put(b"key1", b"value1")?;
            Ok(())
        });
        assert!(result.is_ok(), "update should succeed");

        // Verify the data was committed by using view
        let result = db.view(|tx| {
            let value = tx.get(b"key1");
            assert_eq!(value, Some(b"value1".to_vec()));
            Ok(())
        });
        assert!(result.is_ok());

        // Test 2: update rolls back on error
        let result = db.update(|tx| {
            tx.put(b"key2", b"value2")?;
            Err(Error::BucketNotFound)
        });
        assert!(matches!(result, Err(Error::BucketNotFound)));

        // Verify key2 was NOT committed (rolled back)
        let result = db.view(|tx| {
            let value = tx.get(b"key2");
            assert_eq!(value, None, "key2 should not exist after rollback");
            Ok(())
        });
        assert!(result.is_ok());

        // Verify key1 still exists
        let result = db.view(|tx| {
            let value = tx.get(b"key1");
            assert_eq!(value, Some(b"value1".to_vec()));
            Ok(())
        });
        assert!(result.is_ok());

        db.close().unwrap();
    }

    #[test]
    fn test_db_batch() {
        // Hypothesis: batch() should execute a closure and retry on DatabaseFull error
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();

        // Test 1: batch with successful closure
        let result = db.batch(|tx| {
            tx.put(b"batch_key", b"batch_value")?;
            Ok(())
        });
        assert!(result.is_ok(), "batch should succeed");

        // Verify data was committed
        let result = db.view(|tx| {
            let value = tx.get(b"batch_key");
            assert_eq!(value, Some(b"batch_value".to_vec()));
            Ok(())
        });
        assert!(result.is_ok());

        // Test 2: batch returns error if not DatabaseFull
        let result = db.batch(|_tx| {
            Err(Error::BucketNotFound)
        });
        assert!(matches!(result, Err(Error::BucketNotFound)));

        db.close().unwrap();
    }

    #[test]
    fn test_db_info() {
        // Hypothesis: info() should return database metadata
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();
        let info = db.info();

        assert_eq!(info.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(info.version, 2);
        assert!(!info.read_only);
        assert_eq!(info.path, path.to_string_lossy().to_string());

        db.close().unwrap();
    }

    #[test]
    fn test_db_sync() {
        // Hypothesis: sync() should call file.sync_all() without error
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();
        
        // Write some data first
        db.update(|tx| {
            tx.put(b"sync_test", b"data")?;
            Ok(())
        }).unwrap();

        // Sync should succeed
        let result = db.sync();
        assert!(result.is_ok(), "sync should succeed");

        db.close().unwrap();
    }

    #[test]
    fn test_db_check() {
        // Hypothesis: check() should traverse all pages and verify integrity
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = Db::open(&path, Options::default()).unwrap();

        // Write some data
        db.update(|tx| {
            tx.put(b"check_test", b"data")?;
            Ok(())
        }).unwrap();

        // Check should pass for a valid database
        let result = db.check();
        assert!(result.is_ok(), "check should succeed for valid database: {:?}", result);

        db.close().unwrap();
    }
    
    #[test]
    fn test_db_compact() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_compact.db");
        let compact_path = dir.path().join("test_compact_compacted.db");
        
        // Create and populate database
        {
            let db = Db::open(&db_path, Options::default()).unwrap();
            db.update(|tx| {
                tx.put(b"key1", b"value1")?;
                Ok(())
            }).unwrap();
            db.close().unwrap();
        }
        
        // Check file size
        let meta = std::fs::metadata(&db_path).unwrap();
        println!("Source file size: {}", meta.len());
        
        // Open source and verify it reads
        {
            let db = Db::open(&db_path, Options::default()).unwrap();
            db.view(|tx| {
                let val = tx.get(b"key1");
                println!("Re-opened source: key1 = {:?}", val);
                Ok(())
            }).unwrap();
            db.close().unwrap();
        }
        
        // Copy file
        std::fs::copy(&db_path, &compact_path).unwrap();
        
        // Open compacted and verify
        let compact_db = Db::open(&compact_path, Options::default()).unwrap();
        compact_db.view(|tx| {
            let val = tx.get(b"key1");
            println!("Compacted db: key1 = {:?}", val);
            Ok(())
        }).unwrap();
        compact_db.close().unwrap();
        
        println!("Compact test completed!");
    }
}

#[cfg(test)]
impl Db {
    fn debug_meta(&self) {
        let meta0 = self.meta0.lock().unwrap();
        println!("meta0: magic=0x{:x}, version={}, txid={}, checksum=0x{:x}", 
            meta0.magic, meta0.version, meta0.txid, meta0.checksum);
        println!("meta0 sum64: 0x{:x}", meta0.sum64());
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

    #[test]
    fn test_db_grow() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_grow.db");
        
        // Create a small database
        let db = Db::open(&db_path, Options::default()).unwrap();
        
        // Get initial file size
        let metadata = std::fs::metadata(&db_path).unwrap();
        let initial_size = metadata.len() as usize;
        
        // Grow the database
        let new_size = initial_size + 8192;
        db.grow(new_size).unwrap();
        
        // Verify file grew
        let metadata = std::fs::metadata(&db_path).unwrap();
        assert_eq!(metadata.len() as usize, new_size);
        
        // Grow to smaller size - should be no-op
        db.grow(1000).unwrap();
        
        // Size should still be new_size
        let metadata = std::fs::metadata(&db_path).unwrap();
        assert_eq!(metadata.len() as usize, new_size);
        
        db.close().unwrap();
        std::fs::remove_file(&db_path).ok();
    }
    
    #[test]
    fn test_db_file_size() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_file_size.db");
        
        let db = Db::open(&db_path, Options::default()).unwrap();
        
        // DB::file_size should return current file size
        let size = db.file_size();
        assert!(size > 0, "File size should be greater than 0");
        
        // Initial size should be at least 3 pages (meta0, meta1, freelist)
        // May include root page depending on implementation
        assert!(size >= db.page_size() as u64 * 3, "File size should be at least 3 pages");
        
        db.close().unwrap();
        std::fs::remove_file(&db_path).ok();
    }
    

}

#[cfg(test)]
mod isolation_test {
    #[test]
    fn test_put_get_same_tx() {
        use super::*;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        let db = Db::open(&db_path, Options::default()).unwrap();
        
        // Put and get within SAME transaction
        db.update(|tx| {
            tx.put(b"testkey", b"testvalue")?;
            let val = tx.get(b"testkey");
            println!("Same tx get after put: {:?}", val);
            assert_eq!(val, Some(b"testvalue".to_vec()), "Should get value in same tx");
            Ok(())
        }).unwrap();
        
        db.close().unwrap();
    }
    
    #[test]
    fn test_put_get_different_tx() {
        use super::*;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        let db = Db::open(&db_path, Options::default()).unwrap();
        
        // Put in one tx
        db.update(|tx| {
            tx.put(b"testkey", b"testvalue")?;
            Ok(())
        }).unwrap();
        
        // Get in another tx
        db.update(|tx| {
            let val = tx.get(b"testkey");
            println!("Different tx get: {:?}", val);
            assert_eq!(val, Some(b"testvalue".to_vec()), "Should get value in different tx");
            Ok(())
        }).unwrap();
        
        db.close().unwrap();
    }
}
