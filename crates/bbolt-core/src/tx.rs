//! Transaction implementation for bbolt
//!
//! Transactions provide read-only or read-write access to the database.

use std::collections::HashMap;
use std::sync::Arc;

use crate::bucket::Bucket;
use crate::constants::*;
use crate::errors::{Error, Result};
use crate::page::{Meta, Page, Pgid, Txid};

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
        };

        Ok(tx)
    }

    /// Get the transaction ID
    pub fn id(&self) -> Txid {
        self.meta.txid()
    }

    /// Check if writable
    pub fn writable(&self) -> bool {
        self.writable
    }

    /// Create a cursor for the root bucket
    pub fn cursor(&self) -> crate::cursor::Cursor {
        self.root.cursor()
    }

    /// Get a value from the root bucket
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

    /// Get page info
    pub fn page(&self, pgid: Pgid) -> Page {
        self.db.page(pgid)
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
            eprintln!("DEBUG committed_data: count at {} = {}", offset+10, u16::from_le_bytes([data[offset+10], data[offset+11]]));
            for i in 0..3 {
                let elem_offset = offset + 16 + i * 16;
                let pos = u32::from_le_bytes([data[elem_offset+4], data[elem_offset+5], data[elem_offset+6], data[elem_offset+7]]);
                let ksize = u32::from_le_bytes([data[elem_offset+8], data[elem_offset+9], data[elem_offset+10], data[elem_offset+11]]);
                let vsize = u32::from_le_bytes([data[elem_offset+12], data[elem_offset+13], data[elem_offset+14], data[elem_offset+15]]);
                eprintln!("DEBUG: elem[{}] at {}: pos={}, ksize={}, vsize={}", i, elem_offset, pos, ksize, vsize);
                if pos > 0 && pos < 4096 && ksize > 0 {
                    let key_end = (pos as usize + ksize as usize).min(4096);
                    eprintln!("DEBUG: key at {} = {:?}", pos, std::str::from_utf8(&data[offset + pos as usize..offset + key_end]).unwrap_or("?"));
                }
            }
        }
        data
    }

    /// Write dirty pages to the database
    fn write_pages(&mut self) -> Result<()> {
        let pages = self.db.get_pages().lock().unwrap();
        let pgids: Vec<Pgid> = pages.keys().cloned().collect();
        eprintln!("DEBUG write_pages: {} dirty pages to write", pgids.len());
        drop(pages);
        for pgid in pgids {
            let pages = self.db.get_pages().lock().unwrap();
            if let Some(data) = pages.get(&pgid) {
                self.db.write_page(pgid, data)?;
            }
        }
        // Write freelist to its dedicated page
        self.write_freelist()?;
        Ok(())
    }

    /// Write freelist to its dedicated page
    fn write_freelist(&self) -> Result<()> {
        let freelist = crate::db::GLOBAL_FREELIST.load();
        let freelist_ids: Vec<Pgid> = freelist.iter().cloned().collect();
        eprintln!("DEBUG write_freelist: {} free pages to persist", freelist_ids.len());
        
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
        eprintln!("DEBUG write_freelist: wrote {} IDs to page {}", freelist_ids.len(), freelist_pgid);
        Ok(())
    }

    /// Get transaction stats
    pub fn stats(&self) -> &TxStats {
        &self.stats
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
}

impl Clone for TxDatabase {
    fn clone(&self) -> Self {
        Self {
            page_size: self.page_size,
            meta: self.meta,
            data: std::sync::Mutex::new(self.data.lock().unwrap().clone()),
            pages: self.pages.clone(),
        }
    }
}

impl TxDatabase {
    /// Create from existing database
    pub fn new(page_size: usize, meta: Meta, data: Vec<u8>) -> Self {
        let pages = std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));
        Self { page_size, meta, data: std::sync::Mutex::new(data), pages }
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
        eprintln!("DEBUG write_page: pgid={}, offset={}, db_data.len={}, data.len={}", pgid, offset, db_data.len(), data.len());
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
}

#[cfg(test)]
mod tests {
    // TODO: Add transaction tests
}