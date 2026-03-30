//! Freelist management for bbolt
//!
//! The freelist tracks pages that are no longer in use and can be
//! allocated for new data.

use std::collections::HashMap;

use crate::constants::*;
use crate::page::{Page, Pgid, Txid};

/// FreelistType determines the backend freelist implementation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreelistType {
    /// ArrayFreelist - simple array-based freelist
    Array = 0,
    /// HashMapFreelist - hashmap-based freelist for better concurrent access
    HashMap = 1,
}

impl Default for FreelistType {
    fn default() -> Self {
        FreelistType::Array
    }
}

impl std::fmt::Display for FreelistType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FreelistType::Array => write!(f, "array"),
            FreelistType::HashMap => write!(f, "hashmap"),
        }
    }
}

/// Freelist tracks free and pending pages
pub struct Freelist {
    /// Free page IDs
    free: Vec<Pgid>,
    /// Pending page IDs by transaction ID
    pending: HashMap<Txid, Vec<Pgid>>,
    /// Read-only transaction IDs
    readonly_txs: Vec<Txid>,
    /// Freelist type
    freelist_type: FreelistType,
}

impl Freelist {
    /// Create a new empty freelist with default type (Array)
    pub fn new() -> Self {
        Self::with_type(FreelistType::default())
    }

    /// Create a new empty freelist with specified type
    pub fn with_type(freelist_type: FreelistType) -> Self {
        Self {
            free: Vec::new(),
            pending: HashMap::new(),
            readonly_txs: Vec::new(),
            freelist_type,
        }
    }

    /// Get the freelist type
    pub fn freelist_type(&self) -> FreelistType {
        self.freelist_type
    }

    /// Read freelist from a page and data buffer
    pub fn read(&mut self, page: &Page, data: &[u8]) {
        self.free.clear();
        self.pending.clear();

        // Read IDs from the data buffer at offset PAGE_HEADER_SIZE
        let ids_ptr = data[PAGE_HEADER_SIZE as usize..].as_ptr() as *const Pgid;
        let ids = unsafe { std::slice::from_raw_parts(ids_ptr, page.count as usize) };
        for &id in ids {
            self.free.push(id);
        }
    }

    /// Write freelist to a page
    pub fn write(&self, page: &mut Page, data: &mut [u8]) {
        // Write free page IDs
        let ids_ptr = data[PAGE_HEADER_SIZE as usize..].as_mut_ptr() as *mut Pgid;
        for (i, &id) in self.free.iter().enumerate() {
            unsafe { ids_ptr.add(i).write(id) };
        }
        page.count = self.free.len() as u16;
    }

    /// Get the estimated write page size
    pub fn estimated_size(&self) -> usize {
        self.free.len() * std::mem::size_of::<Pgid>()
    }

    /// Initialize from a list of page IDs
    pub fn init(&mut self, ids: Vec<Pgid>) {
        self.free = ids;
        self.free.sort();
        self.free.reverse();
    }

    /// Allocate pages from the freelist
    pub fn allocate(&mut self, txid: Txid, count: usize) -> Pgid {
        if self.free.len() >= count {
            let mut pages = self.free.split_off(self.free.len() - count);
            return pages[0];
        }
        0 // No pages available
    }

    /// Get the number of free pages
    pub fn free_count(&self) -> usize {
        self.free.len()
    }
    
    /// Get the free pages vector
    pub fn get_free_pages(&self) -> Vec<Pgid> {
        self.free.clone()
    }

    /// Get the number of pending pages
    pub fn pending_count(&self) -> usize {
        self.pending.values().map(|v| v.len()).sum()
    }

    /// Get the total count of free and pending pages
    pub fn count(&self) -> usize {
        self.free_count() + self.pending_count()
    }

    /// Add a read-only transaction ID for tracking
    pub fn add_readonly_txid(&mut self, txid: Txid) {
        self.readonly_txs.push(txid);
    }

    /// Remove a read-only transaction ID
    pub fn remove_readonly_txid(&mut self, txid: Txid) {
        self.readonly_txs.retain(|&t| t != txid);
    }

    /// Release pending pages from closed read-only transactions
    pub fn release_pending_pages(&mut self) {
        let mut released = Vec::new();
        for txid in &self.readonly_txs {
            if let Some(pages) = self.pending.remove(txid) {
                released.extend(pages);
            }
        }
        self.readonly_txs.clear();
        self.free.extend(released);
        self.free.sort();
        self.free.reverse();
    }

    /// Free a page for a transaction
    pub fn free(&mut self, txid: Txid, page_id: Pgid) {
        self.pending
            .entry(txid)
            .or_insert_with(Vec::new)
            .push(page_id);
    }

    /// Check if a page is freed
    pub fn freed(&self, page_id: Pgid) -> bool {
        self.free.contains(&page_id)
    }

    /// Rollback pending pages for a transaction
    pub fn rollback(&mut self, txid: Txid) {
        self.pending.remove(&txid);
    }

    /// Copy all free and pending IDs to a slice
    pub fn copyall(&self, dst: &mut [Pgid]) -> usize {
        let mut count = 0;
        for &id in &self.free {
            dst[count] = id;
            count += 1;
        }
        for pages in self.pending.values() {
            for &id in pages {
                dst[count] = id;
                count += 1;
            }
        }
        count
    }

    /// Reload freelist from a page and data buffer
    pub fn reload(&mut self, page: &Page, data: &[u8]) {
        // Filter out pending pages from the read freelist
        let pending_ids: Vec<Pgid> = self.pending.values().flatten().copied().collect();
        self.read(page, data);
        self.free.retain(|id| !pending_ids.contains(id));
    }

    /// No-sync reload from page IDs
    pub fn nosync_reload(&mut self, ids: Vec<Pgid>) {
        let pending_ids: Vec<Pgid> = self.pending.values().flatten().copied().collect();
        self.free = ids;
        self.free.retain(|id| !pending_ids.contains(id));
        self.free.sort();
        self.free.reverse();
    }

    /// Get all free page IDs
    pub fn free_page_ids(&self) -> &[Pgid] {
        &self.free
    }

    /// Get pending pages by transaction ID
    pub fn pending_page_ids(&self) -> &HashMap<Txid, Vec<Pgid>> {
        &self.pending
    }

    /// Release pages for a transaction or older
    pub fn release(&mut self, txid: Txid) {
        let mut to_release = Vec::new();
        let txids: Vec<Txid> = self.pending.keys().copied().filter(|&t| t <= txid).collect();
        for t in txids {
            if let Some(pages) = self.pending.remove(&t) {
                to_release.extend(pages);
            }
        }
        self.free.extend(to_release);
        self.free.sort();
        self.free.reverse();
    }

    /// Release pages in a range
    pub fn release_range(&mut self, begin: Txid, end: Txid) {
        let mut to_release = Vec::new();
        let txids: Vec<Txid> = self
            .pending
            .keys()
            .copied()
            .filter(|&t| t >= begin && t <= end)
            .collect();
        for t in txids {
            if let Some(pages) = self.pending.remove(&t) {
                to_release.extend(pages);
            }
        }
        self.free.extend(to_release);
        self.free.sort();
        self.free.reverse();
    }

    /// Merge spans into the freelist
    pub fn merge_spans(&mut self, ids: Vec<Pgid>) {
        self.free.extend(ids);
        self.free.sort();
        self.free.reverse();
    }
}

impl Default for Freelist {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freelist_allocate() {
        let mut fl = Freelist::new();
        // init sorts ascending then reverses to descending [5, 4, 3, 2, 1]
        fl.init(vec![5, 4, 3, 2, 1]);

        // Allocate 2 pages - returns STARTING (lowest) page ID of the block
        let start = fl.allocate(1, 2);
        // Takes last 2 elements [2, 1], remaining is [5, 4, 3]
        // Returns 2 (the starting page of allocated block [2, 1])
        assert_eq!(start, 2);
        assert_eq!(fl.free_count(), 3);

        // Allocate 1 page - returns next starting page
        // Takes last 1 element [3], remaining is [5, 4]
        // Returns 3
        let start = fl.allocate(1, 1);
        assert_eq!(start, 3);
        assert_eq!(fl.free_count(), 2);

        // Allocate more than available
        let start = fl.allocate(1, 10);
        assert_eq!(start, 0);
    }

    #[test]
    fn test_freelist_free_pending() {
        let mut fl = Freelist::new();
        fl.init(vec![1, 2, 3]);

        // Free a page (pending)
        fl.free(1, 10);
        assert_eq!(fl.free_count(), 3);
        assert_eq!(fl.pending_count(), 1);
        assert!(!fl.freed(10));

        // Rollback
        fl.rollback(1);
        assert_eq!(fl.pending_count(), 0);
    }

    #[test]
    fn test_freelist_persist_roundtrip() {
        use crate::page::Page;
        
        let page_size = 4096;
        
        // Create freelist with some free pages
        let mut fl = Freelist::new();
        fl.init(vec![1, 2, 3, 4, 5]);
        
        // Serialize to buffer
        let mut data = vec![0u8; page_size];
        let mut page = Page::new(2, 0x10, 0, 0); // flags = 0x10 for freelist
        
        // Use write method
        fl.write(&mut page, &mut data);
        
        // Create new freelist and read from page
        let mut fl2 = Freelist::new();
        fl2.read(&page, &data);
        
        // Verify the data matches
        assert_eq!(fl2.free_count(), 5);
        let original_ids: Vec<u64> = fl.free_page_ids().to_vec();
        let read_ids: Vec<u64> = fl2.free_page_ids().to_vec();
        assert_eq!(original_ids, read_ids);
    }
    
    #[test]
    fn test_freelist_write_and_read() {
        use crate::page::Page;
        use crate::constants::PageFlags;
        
        let page_size = 4096;
        
        // Create and populate freelist
        let mut fl = Freelist::new();
        fl.init(vec![10, 20, 30, 40, 50]);
        
        // Write to buffer
        let mut data = vec![0u8; page_size];
        
        // Write page header
        data[0..8].copy_from_slice(&2u64.to_le_bytes()); // pgid = 2
        data[8..10].copy_from_slice(&PageFlags::FREELIST_PAGE_FLAG.bits().to_le_bytes());
        data[10..12].copy_from_slice(&(5u16).to_le_bytes()); // count = 5
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow
        
        let mut page = Page::new(2, PageFlags::FREELIST_PAGE_FLAG.bits(), 5, 0);
        fl.write(&mut page, &mut data);
        
        // Debug: check all relevant bytes
        
        // Read back into new freelist
        let mut fl2 = Freelist::new();
        
        // Debug using addr_of!
        let ptr = data.as_ptr();
        let id = unsafe { *(ptr as *const u64) };
        let flags = unsafe { *(ptr.add(8) as *const u16) };
        let count = unsafe { *(ptr.add(10) as *const u16) };
        
        let read_page = unsafe { &*(data.as_ptr() as *const Page) };
        fl2.read(read_page, &data);
        
        // Verify pages are preserved
        let ids = fl2.free_page_ids();
        assert_eq!(ids.len(), 5);
        assert!(ids.contains(&10));
        assert!(ids.contains(&50));
    }
}