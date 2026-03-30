//! Transaction implementation for bbolt
//!
//! Transactions provide read-only or read-write access to the database.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use crate::bucket::Bucket;
use crate::constants::*;
use crate::db::GLOBAL_FREELIST;
use crate::errors::{Error, Result};
use crate::page::{Meta, Page, PageInfo, Pgid, Txid};

/// WriteTo trait - similar to Go's io.WriterTo interface.
/// Types implementing this trait can write themselves to a writer.
pub trait WriteTo {
    /// Write self to the provided writer.
    /// Returns the number of bytes written.
    fn write_to(&mut self, w: &mut dyn Write) -> Result<usize>;
}

impl WriteTo for Tx {
    /// Write the entire database to a writer.
    /// 
    /// This writes the entire database content (both meta pages and data pages)
    /// to the provided writer.
    fn write_to(&mut self, w: &mut dyn Write) -> Result<usize> {
        Tx::write_to(self, w)
    }
}

/// Transaction statistics
#[derive(Debug, Clone, Default)]
pub struct TxStats {
    pub page_count: i64,
    pub page_alloc: i64,
    pub cursor_count: i64,
    pub node_count: i64,
    pub node_deref: i64,
    pub rebalance: i64,
    pub rebalance_time_ms: i64,
    pub split: i64,
    pub spill: i64,
    pub spill_time_ms: i64,
    pub write_time_ms: i64,
}

impl TxStats {
    /// Get page count
    pub fn get_page_count(&self) -> i64 {
        self.page_count
    }

    /// Get cursor count
    pub fn get_cursor_count(&self) -> i64 {
        self.cursor_count
    }

    /// Get node count
    pub fn get_node_count(&self) -> i64 {
        self.node_count
    }

    /// Get rebalance count
    pub fn get_rebalance_count(&self) -> i64 {
        self.rebalance
    }

    /// Get rebalance time in milliseconds
    pub fn get_rebalance_time(&self) -> i64 {
        self.rebalance_time_ms
    }

    /// Get split count
    pub fn get_split_count(&self) -> i64 {
        self.split
    }

    /// Get spill count
    pub fn get_spill_count(&self) -> i64 {
        self.spill
    }

    /// Get write count
    pub fn get_write_count(&self) -> i64 {
        self.write_time_ms
    }

    /// Add another TxStats to this one
    pub fn add(&mut self, other: &TxStats) {
        self.page_count += other.page_count;
        self.page_alloc += other.page_alloc;
        self.cursor_count += other.cursor_count;
        self.node_count += other.node_count;
        self.node_deref += other.node_deref;
        self.rebalance += other.rebalance;
        self.rebalance_time_ms += other.rebalance_time_ms;
        self.split += other.split;
        self.spill += other.spill;
        self.spill_time_ms += other.spill_time_ms;
        self.write_time_ms += other.write_time_ms;
    }

    /// Subtract another TxStats from this one
    pub fn sub(&self, other: &TxStats) -> TxStats {
        TxStats {
            page_count: self.page_count - other.page_count,
            page_alloc: self.page_alloc - other.page_alloc,
            cursor_count: self.cursor_count - other.cursor_count,
            node_count: self.node_count - other.node_count,
            node_deref: self.node_deref - other.node_deref,
            rebalance: self.rebalance - other.rebalance,
            rebalance_time_ms: self.rebalance_time_ms - other.rebalance_time_ms,
            split: self.split - other.split,
            spill: self.spill - other.spill,
            spill_time_ms: self.spill_time_ms - other.spill_time_ms,
            write_time_ms: self.write_time_ms - other.write_time_ms,
        }
    }
}

/// Transaction represents a read-only or read-write database transaction
pub struct Tx {
    /// Whether this is a writable transaction
    pub writable: bool,
    /// Whether this is a managed transaction (auto-commit/rollback)
    managed: bool,
    /// Database handle
    db: TxDatabase,
    /// Meta page
    meta: Meta,
    /// Root bucket
    root: Bucket,
    /// Stats
    pub stats: TxStats,
    /// Commit handlers (callbacks executed after successful commit)
    commit_handlers: Vec<Box<dyn FnOnce() + 'static>>,
}

impl Tx {
    /// Begin a new transaction
    pub fn begin(db: TxDatabase, writable: bool) -> Result<Self> {
        let meta = *db.meta()?;

        let root = Bucket::with_writable(*meta.root(), db.clone(), writable);

        let tx = Self {
            writable,
            managed: false,
            db,
            meta,
            root,
            stats: TxStats::default(),
            commit_handlers: Vec::new(),
        };

        Ok(tx)
    }
    
    /// Add a handler to be executed after the transaction successfully commits.
    pub fn on_commit<F>(&mut self, callback: F)
    where
        F: FnOnce() + 'static,
    {
        self.commit_handlers.push(Box::new(callback));
    }
    
    /// Get the database handle
    pub fn db(&self) -> TxDatabase {
        self.db.clone()
    }
    
    /// Get database size in bytes
    pub fn size(&self) -> u64 {
        self.db.data.lock().unwrap().len() as u64
    }

    /// Get the transaction ID
    pub fn id(&self) -> Txid {
        self.meta.txid()
    }

    /// Returns a debug string describing the transaction
    pub fn inspect(&self) -> String {
        format!(
            "Tx {{ id: {}, writable: {}, page_count: {} }}",
            self.id(),
            self.writable,
            self.stats.page_count
        )
    }

    /// Check if writable
    pub fn writable(&self) -> bool {
        self.writable
    }

    /// Create a cursor for the root bucket
    pub fn cursor(&self) -> crate::cursor::Cursor {
        self.root.cursor()
    }
    
    /// Get a reference to the root bucket
    pub fn root(&self) -> &Bucket {
        &self.root
    }

    /// Get a bucket by name (empty name returns root bucket)
    pub fn bucket(&self, name: &[u8]) -> Option<Bucket> {
        if name.is_empty() {
            Some(Bucket::with_writable(*self.meta.root(), self.db.clone(), self.writable))
        } else {
            self.root.get_bucket(name)
        }
    }
    
    /// Iterate over all root-level buckets (created via create_bucket)
    /// Returns iterator of (name, Bucket) pairs
    pub fn buckets(&self) -> Vec<(Vec<u8>, Bucket)> {
        let mut result = Vec::new();
        if let Err(e) = self.root.for_each_bucket(|name, bucket| {
            result.push((name.to_vec(), bucket));
            Ok(())
        }) {
            // Ignore errors, return what we have
        }
        result
    }

    /// Get a value from the root bucket
    /// Get the database handle (for cursor debugging)
    pub fn get_db(&self) -> TxDatabase {
        self.db.clone()
    }
    
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.root.get(key)
    }

    /// Put a value in the root bucket
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        self.root.put(key, value)
    }

    /// Create a new bucket at the root level
    pub fn create_bucket(&mut self, key: &[u8]) -> Result<Bucket> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        self.root.create_bucket(key)
    }

    /// Delete a value
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        self.root.delete(key)
    }

    /// Get page data
    pub fn page_data(&self, pgid: Pgid) -> Option<Vec<u8>> {
        // Check dirty pages first
        if let Some(data) = self.db.get_pages().lock().unwrap().get(&pgid) {
            return Some(data.clone());
        }
        // Get from database
        self.db.page_data(pgid)
    }

    /// Get page info (low-level page access)
    pub fn page(&self, pgid: Pgid) -> Page {
        self.db.page(pgid)
    }

    /// Get page information by page ID.
    ///
    /// Returns `Ok(Some(PageInfo))` if the page exists, `Ok(None)` if the page ID
    /// is at or beyond the high water mark, or an error if the transaction is
    /// closed or the freelist is not loaded.
    ///
    /// The page type will be "free" if the page has been freed.
    pub fn page_info(&self, id: u64) -> Result<Option<PageInfo>> {
        // Check if transaction is closed
        if self.db.meta().is_err() {
            return Err(Error::TxClosed);
        }

        // Return None if page ID is at or beyond high water mark
        if id >= self.meta.pgid() {
            return Ok(None);
        }

        // Build page info
        let pgid = id as Pgid;
        let p = self.db.page(pgid);

        // Check if page is freed (requires freelist to be loaded)
        let typ = if self.meta.freelist_pgid() != PGID_NO_FREELIST {
            let free_pages = GLOBAL_FREELIST.load();
            if free_pages.is_empty() {
                // Freelist pgid is set but no pages have been freed yet
                p.typ().to_string()
            } else if free_pages.contains(&pgid) {
                "free".to_string()
            } else {
                p.typ().to_string()
            }
        } else {
            p.typ().to_string()
        };

        let info = PageInfo {
            id: id as u64,
            typ,
            count: p.count,
            overflow: p.overflow,
        };

        Ok(Some(info))
    }

    /// for_each_page iterates over every page within a given page tree
    /// and executes a function for each page.
    ///
    /// The function receives the page ID, page reference, and depth.
    /// For branch pages, it recursively visits child pages.
    pub fn for_each_page<F>(&self, mut f: F)
    where
        F: FnMut(Pgid, &Page, usize),
    {
        let mut stack = vec![0u64];
        self.for_each_page_internal(&mut stack, &mut f, 0);
    }

    /// Internal helper for for_each_page recursion
    fn for_each_page_internal<F>(&self, stack: &mut Vec<Pgid>, f: &mut F, depth: usize)
    where
        F: FnMut(Pgid, &Page, usize),
    {
        let pgid = stack[stack.len() - 1];
        let p = self.page(pgid);

        // Execute function for this page
        f(pgid, &p, depth);

        // Recursively visit children for branch pages
        if p.is_branch() {
            let count = p.count as usize;
            let elem_start = 16usize; // PAGE_HEADER_SIZE
            for i in 0..count {
                let elem_offset = elem_start + i * 16;
                let pgid_bytes = [
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 8],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 9],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 10],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 11],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 12],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 13],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 14],
                    self.db.page_data(pgid).as_ref().unwrap()[elem_offset + 15],
                ];
                let child_pgid = Pgid::from_le_bytes(pgid_bytes);
                stack.push(child_pgid);
                self.for_each_page_internal(stack, f, depth + 1);
                stack.pop();
            }
        }
    }

    /// Copy the database to a writer.
    ///
    /// This writes the entire database content (both meta pages and data pages)
    /// to the provided writer.
    pub fn copy(&mut self, w: &mut dyn Write) -> Result<()> {
        self.write_to(w)?;
        Ok(())
    }

    /// Write the entire database to a writer.
    ///
    /// Returns the number of bytes written.
    ///
    /// This is the main implementation used by both `copy` and `copy_file`.
    pub fn write_to(&self, w: &mut dyn Write) -> Result<usize> {
        let page_size = self.db.page_size();
        let mut n = 0;

        // Generate a meta page buffer
        let mut buf = vec![0u8; page_size];

        // Write meta 0 with current txid
        buf[0..8].copy_from_slice(&0u64.to_le_bytes()); // page id = 0
        buf[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes());
        buf[10..12].copy_from_slice(&0u16.to_le_bytes()); // count = 0
        buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow = 0

        let meta_offset = 16;
        buf[meta_offset..meta_offset + 4].copy_from_slice(&self.meta.magic.to_le_bytes());
        buf[meta_offset + 4..meta_offset + 8].copy_from_slice(&self.meta.version.to_le_bytes());
        buf[meta_offset + 8..meta_offset + 12].copy_from_slice(&self.meta.page_size.to_le_bytes());
        buf[meta_offset + 12..meta_offset + 16].copy_from_slice(&self.meta.flags.to_le_bytes());
        buf[meta_offset + 16..meta_offset + 24].copy_from_slice(&self.meta.root.root.to_le_bytes());
        buf[meta_offset + 24..meta_offset + 32].copy_from_slice(&self.meta.root.sequence.to_le_bytes());
        buf[meta_offset + 32..meta_offset + 40].copy_from_slice(&self.meta.freelist.to_le_bytes());
        buf[meta_offset + 40..meta_offset + 48].copy_from_slice(&self.meta.pgid.to_le_bytes());
        buf[meta_offset + 48..meta_offset + 56].copy_from_slice(&self.meta.txid.to_le_bytes());
        // Calculate checksum for meta 0
        let checksum = self.calculate_meta_checksum(&buf, meta_offset);
        buf[meta_offset + 56..meta_offset + 64].copy_from_slice(&checksum.to_le_bytes());

        w.write_all(&buf)?;
        n += page_size;

        // Write meta 1 with lower transaction id (txid - 1)
        buf[0..8].copy_from_slice(&1u64.to_le_bytes()); // page id = 1
        buf[meta_offset + 48..meta_offset + 56].copy_from_slice(&self.meta.txid.wrapping_sub(1).to_le_bytes());
        // Calculate checksum for meta 1
        let checksum = self.calculate_meta_checksum(&buf, meta_offset);
        buf[meta_offset + 56..meta_offset + 64].copy_from_slice(&checksum.to_le_bytes());

        w.write_all(&buf)?;
        n += page_size;

        // Copy data pages (from page 2 onwards)
        let data = self.db.get_data();
        let data_offset = 2 * page_size;
        let data_size = data.len() - data_offset;

        if data_size > 0 {
            w.write_all(&data[data_offset..])?;
            n += data_size;
        }

        Ok(n)
    }

    /// Calculate checksum for a meta page buffer
    fn calculate_meta_checksum(&self, buf: &[u8], meta_offset: usize) -> u64 {
        // Create a Meta struct from the buffer for checksum calculation
        // buf[meta_offset..] contains the 64-byte Meta structure
        let ptr = buf[meta_offset..meta_offset + 64].as_ptr();
        let meta = unsafe { &*(ptr as *const Meta) };
        meta.sum64()
    }

    /// Copy the database to a file at the given path.
    ///
    /// The file will be created with the specified mode if it doesn't exist,
    /// or truncated if it does exist.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the output file
    /// * `mode` - The file permissions (e.g., 0o644 on Unix, ignored on other platforms)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tx = db.begin_read()?;
    /// tx.copy_file("backup.db", 0o644)?;
    /// ```
    #[cfg(unix)]
    pub fn copy_file<P: AsRef<Path>>(&self, path: P, mode: u32) -> Result<()> {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        let path = path.as_ref();

        // Open file with specified mode
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(path)?;

        let mut file = file;
        self.write_to(&mut file)?;

        // Close the file
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    /// Copy the database to a file at the given path (non-Unix implementation).
    ///
    /// On non-Unix platforms, the mode parameter is ignored and a default mode is used.
    #[cfg(not(unix))]
    pub fn copy_file<P: AsRef<Path>>(&self, path: P, _mode: u32) -> Result<()> {
        use std::fs::File;

        let path = path.as_ref();

        // Create file with default permissions
        let mut file = File::create(path)?;

        self.write_to(&mut file)?;

        // Close the file
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    /// Allocate a new page
    pub fn allocate(&mut self, count: usize) -> Result<Page> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        let page = Page::new(self.meta.pgid(), 0, 0, 0);
        self.stats.page_count += count as i64;
        self.stats.page_alloc += (count * self.db.page_size()) as i64;
        Ok(page)
    }

    /// Commit the transaction
    pub fn commit(&mut self) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        // Write dirty pages
        self.write_pages()?;
        
        // Execute commit handlers
        let handlers: Vec<Box<dyn FnOnce() + 'static>> = self.commit_handlers.drain(..).collect();
        for handler in handlers {
            handler();
        }
        
        Ok(())
    }

    /// Rollback the transaction
    pub fn rollback(&mut self) -> Result<()> {
        self.db.get_pages().lock().unwrap().clear();
        Ok(())
    }

    /// Get the committed data (call after commit)
    pub fn committed_data(&self) -> Vec<u8> {
        let data = self.db.get_data();
        // Debug: check page 3
        let page_size = self.db.page_size();
        let offset = 3 * page_size;
        if data.len() >= offset + 16 {
            for i in 0..3 {
                let elem_offset = offset + 16 + i * 16;
                let pos = u32::from_le_bytes([data[elem_offset+4], data[elem_offset+5], data[elem_offset+6], data[elem_offset+7]]);
                let ksize = u32::from_le_bytes([data[elem_offset+8], data[elem_offset+9], data[elem_offset+10], data[elem_offset+11]]);
                let vsize = u32::from_le_bytes([data[elem_offset+12], data[elem_offset+13], data[elem_offset+14], data[elem_offset+15]]);
                if pos > 0 && pos < 4096 && ksize > 0 {
                    let key_end = (pos as usize + ksize as usize).min(4096);
                }
            }
        }
        data
    }

    /// Write dirty pages to the database
    fn write_pages(&mut self) -> Result<()> {
        // Sync the root bucket's state to meta before writing
        // This ensures any changes to root_pgid are persisted
        self.meta.root = self.root.inbucket();
        
        // Get all dirty page IDs
        let pages = self.db.get_pages().lock().unwrap();
        let pgids: Vec<Pgid> = pages.keys().cloned().collect();
        drop(pages);
        
        // Write all dirty pages
        for pgid in pgids {
            let pages = self.db.get_pages().lock().unwrap();
            if let Some(data) = pages.get(&pgid) {
                self.db.write_page(pgid, data)?;
            }
        }
        
        // Write updated meta pages (0 and 1)
        self.write_meta_pages()?;
        
        // Write freelist to its dedicated page
        self.write_freelist()?;
        
        // Sync all changes to disk
        self.db.sync_to_disk()?;
        
        Ok(())
    }
    
    /// Write meta pages with updated root bucket info
    fn write_meta_pages(&self) -> Result<()> {
        let page_size = self.db.page_size();
        
        // Create meta0 with txid = current txid
        let mut meta0 = Meta::new(
            page_size as u32,
            self.meta.freelist_pgid(),
            self.meta.root,
            self.db.next_pgid(),
            self.meta.txid().wrapping_add(1),
        );
        
        // Create meta1 with txid = current txid - 1 (or keep current)
        let mut meta1 = Meta::new(
            page_size as u32,
            self.meta.freelist_pgid(),
            self.meta.root,
            self.db.next_pgid(),
            self.meta.txid(),
        );
        
        // Serialize meta0 to page 0
        let mut meta0_data = vec![0u8; page_size];
        meta0_data[0..8].copy_from_slice(&0u64.to_le_bytes()); // page id
        meta0_data[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes());
        meta0_data[10..12].copy_from_slice(&0u16.to_le_bytes());
        meta0_data[12..16].copy_from_slice(&0u32.to_le_bytes());
        
        let meta_offset = 16;
        meta0_data[meta_offset..meta_offset + 4].copy_from_slice(&meta0.magic.to_le_bytes());
        meta0_data[meta_offset + 4..meta_offset + 8].copy_from_slice(&meta0.version.to_le_bytes());
        meta0_data[meta_offset + 8..meta_offset + 12].copy_from_slice(&meta0.page_size.to_le_bytes());
        meta0_data[meta_offset + 12..meta_offset + 16].copy_from_slice(&meta0.flags.to_le_bytes());
        meta0_data[meta_offset + 16..meta_offset + 24].copy_from_slice(&meta0.root.root.to_le_bytes());
        meta0_data[meta_offset + 24..meta_offset + 32].copy_from_slice(&meta0.root.sequence.to_le_bytes());
        meta0_data[meta_offset + 32..meta_offset + 40].copy_from_slice(&meta0.freelist.to_le_bytes());
        meta0_data[meta_offset + 40..meta_offset + 48].copy_from_slice(&meta0.pgid.to_le_bytes());
        meta0_data[meta_offset + 48..meta_offset + 56].copy_from_slice(&meta0.txid.to_le_bytes());
        meta0_data[meta_offset + 56..meta_offset + 64].copy_from_slice(&meta0.checksum.to_le_bytes());
        
        self.db.write_page(0, &meta0_data)?;
        
        // Serialize meta1 to page 1
        let mut meta1_data = vec![0u8; page_size];
        meta1_data[0..8].copy_from_slice(&1u64.to_le_bytes()); // page id
        meta1_data[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes());
        meta1_data[10..12].copy_from_slice(&0u16.to_le_bytes());
        meta1_data[12..16].copy_from_slice(&0u32.to_le_bytes());
        
        meta1_data[meta_offset..meta_offset + 4].copy_from_slice(&meta1.magic.to_le_bytes());
        meta1_data[meta_offset + 4..meta_offset + 8].copy_from_slice(&meta1.version.to_le_bytes());
        meta1_data[meta_offset + 8..meta_offset + 12].copy_from_slice(&meta1.page_size.to_le_bytes());
        meta1_data[meta_offset + 12..meta_offset + 16].copy_from_slice(&meta1.flags.to_le_bytes());
        meta1_data[meta_offset + 16..meta_offset + 24].copy_from_slice(&meta1.root.root.to_le_bytes());
        meta1_data[meta_offset + 24..meta_offset + 32].copy_from_slice(&meta1.root.sequence.to_le_bytes());
        meta1_data[meta_offset + 32..meta_offset + 40].copy_from_slice(&meta1.freelist.to_le_bytes());
        meta1_data[meta_offset + 40..meta_offset + 48].copy_from_slice(&meta1.pgid.to_le_bytes());
        meta1_data[meta_offset + 48..meta_offset + 56].copy_from_slice(&meta1.txid.to_le_bytes());
        meta1_data[meta_offset + 56..meta_offset + 64].copy_from_slice(&meta1.checksum.to_le_bytes());
        
        self.db.write_page(1, &meta1_data)?;
        
        Ok(())
    }

    /// Write freelist to its dedicated page
    fn write_freelist(&self) -> Result<()> {
        let freelist = crate::db::GLOBAL_FREELIST.load();
        let freelist_ids: Vec<Pgid> = freelist.iter().cloned().collect();
        
        // Allocate a page for freelist if needed (use page 2, freelist_pgid)
        let freelist_pgid = self.meta.freelist_pgid();
        if freelist_pgid == Pgid::MAX {
            return Ok(()); // No freelist page yet
        }
        
        // Create freelist page data
        let page_size = self.db.page_size();
        let mut page_data = vec![0u8; page_size];
        
        // Write page header
        let page_ptr = page_data.as_mut_ptr() as *mut Page;
        unsafe {
            (*page_ptr).id = freelist_pgid;
            (*page_ptr).flags = FREELIST_PAGE_FLAG;
            (*page_ptr).count = freelist_ids.len() as u16;
            (*page_ptr).overflow = 0;
        }
        
        // Write freelist IDs after header
        let header_size = PAGE_HEADER_SIZE as usize;
        let ids_ptr = page_data[header_size..].as_mut_ptr() as *mut Pgid;
        for (i, &id) in freelist_ids.iter().enumerate() {
            unsafe { ids_ptr.add(i).write(id) };
        }
        
        self.db.write_page(freelist_pgid, &page_data)?;
        Ok(())
    }

    /// Get transaction stats
    pub fn stats(&self) -> &TxStats {
        &self.stats
    }
    
    /// Get the meta page
    pub fn meta(&self) -> &Meta {
        &self.meta
    }
}

/// Clone-able database handle for transactions
#[derive(Debug)]
pub struct TxDatabase {
    page_size: usize,
    meta: Meta,
    data: std::sync::Mutex<Vec<u8>>,
    /// Dirty pages managed by this transaction
    pages: std::sync::Arc<std::sync::Mutex<HashMap<Pgid, Vec<u8>>>>,
    /// Next available page ID
    next_pgid: Pgid,
    /// File path for syncing
    path: std::sync::RwLock<Option<std::path::PathBuf>>,
}

impl Clone for TxDatabase {
    fn clone(&self) -> Self {
        Self {
            page_size: self.page_size,
            meta: self.meta,
            data: std::sync::Mutex::new(self.data.lock().unwrap().clone()),
            pages: self.pages.clone(),
            next_pgid: self.next_pgid,
            path: std::sync::RwLock::new(self.path.read().unwrap().clone()),
        }
    }
}

impl TxDatabase {
    /// Create from existing database
    pub fn new(page_size: usize, meta: Meta, data: Vec<u8>, path: Option<std::path::PathBuf>) -> Self {
        let pages = std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));
        let next_pgid = meta.pgid.max(4); // Start from page 4 (0=meta0, 1=meta1, 2=freelist, 3=root)
        Self { page_size, meta, data: std::sync::Mutex::new(data), pages, next_pgid, path: std::sync::RwLock::new(path) }
    }
    
    /// Set the file path
    pub fn set_path(&self, path: std::path::PathBuf) {
        *self.path.write().unwrap() = Some(path);
    }
    
    /// Get the file path
    pub fn path(&self) -> Option<std::path::PathBuf> {
        self.path.read().unwrap().clone()
    }
    

    /// Get the next available page ID
    pub fn next_pgid(&self) -> Pgid {
        self.next_pgid
    }
    
    /// Set the next available page ID
    pub fn set_next_pgid(&mut self, pgid: Pgid) {
        self.next_pgid = pgid;
    }
    
    /// Get the pages Arc
    pub fn get_pages(&self) -> &std::sync::Arc<std::sync::Mutex<HashMap<Pgid, Vec<u8>>>> {
        &self.pages
    }
    
    /// Allocate a new page and store it in dirty pages
    pub fn allocate_page(&self, pgid: Pgid) -> Vec<u8> {
        let mut data = vec![0u8; self.page_size];
        // Write page header: id (8 bytes) + flags (2 bytes) + count (2 bytes) + overflow (4 bytes)
        data[0..8].copy_from_slice(&pgid.to_le_bytes());
        data[8..10].copy_from_slice(&0u16.to_le_bytes()); // flags
        data[10..12].copy_from_slice(&0u16.to_le_bytes()); // count
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow
        
        let mut pages = self.pages.lock().unwrap();
        pages.insert(pgid, data.clone());
        data
    }

    /// Get page size
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Get meta page
    pub fn meta(&self) -> Result<&Meta> {
        Ok(&self.meta)
    }

    /// Get page data
    pub fn page_data(&self, pgid: Pgid) -> Option<Vec<u8>> {
        // Check dirty pages first
        if let Some(data) = self.pages.lock().unwrap().get(&pgid) {
            return Some(data.clone());
        }
        let offset = pgid as usize * self.page_size;
        let data = self.data.lock().unwrap();
        let end = offset + self.page_size;
        if end > data.len() {
            return None;
        }
        Some(data[offset..end].to_vec())
    }

    /// Get page info
    pub fn page(&self, pgid: Pgid) -> Page {
        let offset = pgid as usize * self.page_size;
        let data = self.data.lock().unwrap();
        let buf = &data[offset..offset + self.page_size];
        Page {
            id: pgid,
            flags: u16::from_le_bytes([buf[8], buf[9]]),
            count: u16::from_le_bytes([buf[10], buf[11]]),
            overflow: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
        }
    }

    /// Write a page to the data buffer
    pub fn write_page(&self, pgid: Pgid, data: &[u8]) -> Result<()> {
        let offset = pgid as usize * self.page_size;
        let mut db_data = self.data.lock().unwrap();
        if offset >= db_data.len() {
            // This page is beyond current data - still write it
            db_data.resize(offset + self.page_size, 0);
        }
        db_data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Get the current data (including any written pages)
    pub fn get_data(&self) -> Vec<u8> {
        self.data.lock().unwrap().clone()
    }
    
    /// Sync data to a file
    pub fn sync_to_file<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        use std::io::Write;
        let data = self.data.lock().unwrap();
        let mut file = std::fs::File::create(path)?;
        file.write_all(&data)?;
        file.sync_all()?;
        Ok(())
    }
    
    /// Sync all dirty pages to disk using the stored path
    pub fn sync_to_disk(&self) -> std::io::Result<()> {
        if let Some(ref path) = *self.path.read().unwrap() {
            self.sync_to_file(path)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::InBucket;
    use tempfile::TempDir;

    #[test]
    fn test_write_to() {
        // Create a minimal database for testing
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2, // freelist_pgid
            InBucket::new(3, 0), // root bucket at page 3
            4, // pgid = 4 (2 meta pages + 1 freelist + 1 root)
            1, // txid = 1
        );

        // Create minimal data (2 meta pages + some data)
        let mut data = vec![0u8; page_size * 5];
        // Page 0 and 1 are meta pages, will be overwritten
        // Pages 2-4 are data

        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        // Test write_to
        let mut buf = Vec::new();
        let n = tx.write_to(&mut buf).unwrap();

        // Should have written: 2 meta pages + 3 data pages
        assert_eq!(n, page_size * 5);

        // Verify meta 0 is at the start
        assert_eq!(buf[0..8], 0u64.to_le_bytes()); // page id = 0

        // Verify meta 1 is at page_size
        assert_eq!(buf[page_size..page_size + 8], 1u64.to_le_bytes()); // page id = 1
    }

    #[test]
    fn test_copy() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let mut tx = Tx::begin(db, false).unwrap();

        // Test copy
        let mut buf = Vec::new();
        tx.copy(&mut buf).unwrap();

        assert_eq!(buf.len(), page_size * 5);
    }

    #[test]
    fn test_copy_file() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        // Create temp file
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_copy.db");

        // Copy to file
        #[cfg(unix)]
        tx.copy_file(&path, 0o644).unwrap();
        #[cfg(not(unix))]
        tx.copy_file(&path, 0).unwrap();

        // Verify file exists and has correct size
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len() as usize, page_size * 5);
    }

    #[test]
    fn test_page_info() {
        // Clear any global freelist state from other tests
        GLOBAL_FREELIST.store(vec![]);

        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4, // pgid = 4
            1,
        );

        let mut data = vec![0u8; page_size * 5];
        // Set page 0 and 1 to be meta pages
        data[0 * page_size + 8..0 * page_size + 10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes());
        data[1 * page_size + 8..1 * page_size + 10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes());
        // Set page 3 to be a leaf page
        data[3 * page_size + 8..3 * page_size + 10].copy_from_slice(&PageFlags::LEAF_PAGE_FLAG.bits().to_le_bytes());
        data[3 * page_size + 10..3 * page_size + 12].copy_from_slice(&2u16.to_le_bytes()); // count = 2

        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        // Page 0 (meta) should return info
        let info = tx.page_info(0).unwrap();
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.id, 0);
        assert_eq!(info.typ, "meta");

        // Page 3 (leaf) should return info
        let info = tx.page_info(3).unwrap();
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.id, 3);
        assert_eq!(info.typ, "leaf");

        // Page beyond pgid should return None
        let info = tx.page_info(5).unwrap();
        assert!(info.is_none());

        // Page at pgid should return None
        let info = tx.page_info(4).unwrap();
        assert!(info.is_none());
    }

    #[test]
    fn test_page_info_free_page() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            5, // pgid = 5
            1,
        );

        let data = vec![0u8; page_size * 6];

        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        // Add page 3 to free list
        GLOBAL_FREELIST.store(vec![3]);

        // Page 3 should be marked as free
        let info = tx.page_info(3).unwrap();
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.typ, "free");

        // Clean up global state
        GLOBAL_FREELIST.store(vec![]);
    }

    #[test]
    fn test_meta_checksum_in_write_to() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            5, // txid = 5
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        let mut buf = Vec::new();
        tx.write_to(&mut buf).unwrap();

        // Verify meta 0 has correct checksum
        let meta0_offset = 16; // Meta data starts at offset 16
        let ptr = unsafe { buf[meta0_offset..meta0_offset + 64].as_ptr() };
        let written_meta = unsafe { &*(ptr as *const Meta) };
        let expected_checksum = written_meta.sum64();

        // The checksum written should match
        let checksum_offset = meta0_offset + 56;
        let written_checksum = u64::from_le_bytes([
            buf[checksum_offset],
            buf[checksum_offset + 1],
            buf[checksum_offset + 2],
            buf[checksum_offset + 3],
            buf[checksum_offset + 4],
            buf[checksum_offset + 5],
            buf[checksum_offset + 6],
            buf[checksum_offset + 7],
        ]);
        assert_eq!(written_checksum, expected_checksum);

        // Meta 1 should have txid - 1
        let meta1_txid_offset = page_size + meta0_offset + 48;
        let meta1_txid = u64::from_le_bytes([
            buf[meta1_txid_offset],
            buf[meta1_txid_offset + 1],
            buf[meta1_txid_offset + 2],
            buf[meta1_txid_offset + 3],
            buf[meta1_txid_offset + 4],
            buf[meta1_txid_offset + 5],
            buf[meta1_txid_offset + 6],
            buf[meta1_txid_offset + 7],
        ]);
        assert_eq!(meta1_txid, 4); // 5 - 1
    }

    #[test]
    fn test_tx_write_to_trait() {
        // Test that the WriteTo trait is properly implemented
        use crate::tx::WriteTo;
        
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let mut tx = Tx::begin(db, false).unwrap();

        // Test WriteTo trait implementation
        let mut buf = Vec::new();
        let n = WriteTo::write_to(&mut tx, &mut buf).unwrap();

        // Should have written 5 pages (2 meta + 3 data)
        assert_eq!(n, page_size * 5);
        
        // Verify the data was written correctly
        assert_eq!(buf.len(), page_size * 5);
        
        // Verify meta page headers
        assert_eq!(buf[0..8], 0u64.to_le_bytes()); // meta 0 page id
        assert_eq!(buf[page_size..page_size + 8], 1u64.to_le_bytes()); // meta 1 page id
    }

    #[test]
    fn test_tx_inspect() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        let inspect = tx.inspect();
        assert!(inspect.contains("id: 1"));
        assert!(inspect.contains("writable: false"));
    }

    #[test]
    fn test_tx_inspect_writable() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, true).unwrap();

        let inspect = tx.inspect();
        assert!(inspect.contains("writable: true"));
    }

    #[test]
    fn test_tx_for_each_page() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        let mut pages_visited = Vec::new();
        tx.for_each_page(|pgid, _page, depth| {
            pages_visited.push((pgid, depth));
        });

        // Should visit at least the root page (page 3)
        assert!(!pages_visited.is_empty(), "Should visit at least root page");
    }

    #[test]
    fn test_tx_for_each_page_counts() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(0, 0), // empty bucket
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(db, false).unwrap();

        let mut count = 0;
        tx.for_each_page(|_pgid, _page, _depth| {
            count += 1;
        });

        // For empty bucket, should visit at least root (page 0)
        assert!(count >= 1, "Should visit at least root page");
    }
    
    #[test]
    fn test_tx_db() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let tx_db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(tx_db, false).unwrap();

        // Tx::db() should return the TxDatabase
        let db = tx.db();
        assert_eq!(db.page_size(), page_size);
    }
    
    #[test]
    fn test_tx_size() {
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(3, 0),
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let tx_db = TxDatabase::new(page_size, meta, data, None);
        let tx = Tx::begin(tx_db, false).unwrap();

        // Tx::size() should return the data size
        assert_eq!(tx.size(), (page_size * 5) as u64);
    }
    
    #[test]
    fn test_tx_on_commit() {
        use std::sync::{Arc, Mutex};
        
        let page_size = 4096;
        let meta = Meta::new(
            page_size as u32,
            2,
            InBucket::new(0, 0), // empty bucket
            4,
            1,
        );

        let data = vec![0u8; page_size * 5];
        let tx_db = TxDatabase::new(page_size, meta, data, None);
        let mut tx = Tx::begin(tx_db, true).unwrap();

        // Track if callback was called using Arc<Mutex>
        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();
        
        // Add commit handler
        tx.on_commit(move || {
            *called_clone.lock().unwrap() = true;
        });
        
        // Commit should execute the handler
        tx.commit().unwrap();
        
        assert!(*called.lock().unwrap(), "on_commit callback should have been called");
    }
}