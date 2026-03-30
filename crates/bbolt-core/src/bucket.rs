//! Bucket implementation for bbolt
//!
//! Buckets are collections of key-value pairs within the database.

use std::cmp::Ordering;

use crate::constants::*;
use crate::cursor::Cursor;
use crate::errors::{Error, Result};
use crate::page::{Meta, Page, Pgid};
use crate::tx::TxDatabase;

/// Bucket represents a collection of key-value pairs
pub struct Bucket {
    /// In-bucket information
    inbucket: crate::page::InBucket,
    /// Database handle for reading pages
    db: TxDatabase,
    /// Root page ID
    root_pgid: Pgid,
    /// Fill percent
    fill_percent: f64,
    /// Writable flag (set when used with writable transaction)
    writable: bool,
    /// Inline page data (for inline buckets with root_pgid=0)
    inline_data: Option<Vec<u8>>,
}

/// BucketStats records statistics about resources used by a bucket.
#[derive(Debug, Default, Clone)]
pub struct BucketStats {
    /// Number of logical branch pages
    pub branch_page_count: usize,
    /// Number of physical branch overflow pages
    pub branch_overflow_count: usize,
    /// Number of logical leaf pages
    pub leaf_page_count: usize,
    /// Number of physical leaf overflow pages
    pub leaf_overflow_count: usize,
    /// Number of keys/value pairs
    pub key_count: usize,
    /// Number of levels in B+tree
    pub depth: usize,
    /// Bytes allocated for physical branch pages
    pub branch_alloc: usize,
    /// Bytes actually used for branch data
    pub branch_inuse: usize,
    /// Bytes allocated for physical leaf pages
    pub leaf_alloc: usize,
    /// Bytes actually used for leaf data
    pub leaf_inuse: usize,
    /// Total number of buckets including the top bucket
    pub bucket_count: usize,
    /// Total number of inline buckets
    pub inline_bucket_count: usize,
    /// Bytes used for inline buckets
    pub inline_bucket_inuse: usize,
}

impl Bucket {
    /// Create a new bucket
    pub fn new(inbucket: &crate::page::InBucket) -> Self {
        Self {
            inbucket: *inbucket,
            db: TxDatabase::new(4096, Meta::default(), vec![]),
            root_pgid: inbucket.root_pgid(),
            fill_percent: DEFAULT_FILL_PERCENT,
            writable: false,
            inline_data: None,
        }
    }

    /// Create a new bucket with database access
    pub fn with_db(inbucket: crate::page::InBucket, db: TxDatabase) -> Self {
        Self {
            inbucket,
            db,
            root_pgid: inbucket.root_pgid(),
            fill_percent: DEFAULT_FILL_PERCENT,
            writable: false,
            inline_data: None,
        }
    }
    
    /// Create a bucket with inline data (for inline buckets)
    pub fn with_inline_data(inbucket: crate::page::InBucket, db: TxDatabase, inline_data: Vec<u8>) -> Self {
        Self {
            inbucket,
            db,
            root_pgid: inbucket.root_pgid(), // root_pgid=0 for inline
            fill_percent: DEFAULT_FILL_PERCENT,
            writable: false,
            inline_data: Some(inline_data),
        }
    }

    /// Create a new writable bucket
    pub fn with_writable(inbucket: crate::page::InBucket, db: TxDatabase, writable: bool) -> Self {
        Self {
            inbucket,
            db,
            root_pgid: inbucket.root_pgid(),
            fill_percent: DEFAULT_FILL_PERCENT,
            writable,
            inline_data: None,
        }
    }

    /// Get the root page ID
    pub fn root_page(&self) -> Pgid {
        self.root_pgid
    }

    /// Set the root page ID
    pub fn set_root_page(&mut self, pgid: Pgid) {
        self.root_pgid = pgid;
        self.inbucket.set_root(pgid);
    }

    /// Check if this bucket is writable
    pub fn writable(&self) -> bool {
        self.writable
    }

    /// Set writable flag
    pub fn set_writable(&mut self, writable: bool) {
        self.writable = writable;
    }

    /// Create a cursor for this bucket
    pub fn cursor(&self) -> Cursor {
        let mut cursor = Cursor::new();
        if self.root_pgid > 0 {
            cursor.set_root(self.root_pgid, self.db.clone());
        } else if let Some(ref data) = self.inline_data {
            // Inline bucket - set cursor with inline data
            cursor.set_root(0, self.db.clone());
            cursor.set_inline_data(data.clone());
        }
        cursor
    }

    /// Get a nested bucket by name - returns the value data for bucket entry
    pub fn bucket(&self, name: &[u8]) -> Option<Vec<u8>> {
        if name.is_empty() {
            return None;
        }
        
        // Use inline_data if available, otherwise use page_data
        let page_data: Vec<u8> = if self.root_pgid == 0 {
            self.inline_data.clone()?
        } else {
            self.page_data(self.root_pgid)?
        };
        
        self.search_leaf(&page_data, name)
    }
    
    /// Get nested bucket instance
    pub fn get_bucket(&self, name: &[u8]) -> Option<Bucket> {
        if name.is_empty() {
            return None;
        }
        
        // Empty bucket
        if self.root_pgid == 0 {
            return None;
        }
        
        // Use cursor to find the bucket entry
        let mut cursor = self.cursor();
        let result = cursor.seek(name)?;
        
        let (k, v, flags) = result;
        
        // Check if key matches and is a bucket entry
        if k != name {
            return None;
        }
        if flags & LeafFlags::BUCKET_LEAF_FLAG.bits() == 0 {
            return None;
        }
        
        // Parse InBucket from value
        if v.len() < 16 {
            return None;
        }
        
        let root_pgid = Pgid::from_le_bytes([
            v[0], v[1], v[2], v[3],
            v[4], v[5], v[6], v[7]
        ]);
        let sequence = u64::from_le_bytes([
            v[8], v[9], v[10], v[11],
            v[12], v[13], v[14], v[15]
        ]);
        let inbucket = crate::page::InBucket::new(root_pgid, sequence);
        
        // Check if this is an inline bucket (root_pgid == 0 and has page data)
        if root_pgid == 0 && v.len() > 16 {
            // Extract inline page data from value[16:]
            let inline_data = v[16..].to_vec();
            return Some(Bucket::with_inline_data(inbucket, self.db.clone(), inline_data));
        }
        
        Some(Bucket::with_db(inbucket, self.db.clone()))
    }

    /// Create a new nested bucket
    pub fn create_bucket(&mut self, key: &[u8]) -> Result<Bucket> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        if key.is_empty() {
            return Err(Error::BucketNameRequired);
        }
        
        // Check if bucket already exists
        if self.get_bucket(key).is_some() {
            return Err(Error::BucketExists);
        }
        
        // Allocate a new page for the bucket's root
        let page_size = self.db.page_size();
        let next_pgid = self.db.next_pgid();
        
        // Create the bucket's root page (leaf page)
        let mut bucket_root = vec![0u8; page_size];
        // Page header: id(8) + flags(2) + count(2) + overflow(4) = 16 bytes
        bucket_root[0..8].copy_from_slice(&next_pgid.to_le_bytes());
        bucket_root[8..10].copy_from_slice(&PageFlags::LEAF_PAGE_FLAG.bits().to_le_bytes()); // flags = LEAF_PAGE_FLAG
        bucket_root[10..12].copy_from_slice(&0u16.to_le_bytes()); // count = 0
        bucket_root[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow = 0
        
        // Store the bucket root page
        self.db.get_pages().lock().unwrap().insert(next_pgid, bucket_root);
        self.db.set_next_pgid(next_pgid + 1);
        
        // Create InBucket data
        let inbucket = crate::page::InBucket::new(next_pgid, 0);
        let mut inbucket_data = vec![0u8; 16];
        inbucket_data[0..8].copy_from_slice(&inbucket.root_pgid().to_le_bytes());
        inbucket_data[8..16].copy_from_slice(&inbucket.sequence().to_le_bytes());
        
        // Insert bucket entry with BUCKET_LEAF_FLAG
        self.put_with_flags(key, &inbucket_data, LeafFlags::BUCKET_LEAF_FLAG.bits())?;
        
        // Return the created bucket
        Ok(Bucket::with_db(inbucket, self.db.clone()))
    }

    /// Create a new nested bucket if it doesn't exist
    pub fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> Result<Bucket> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        if key.is_empty() {
            return Err(Error::BucketNameRequired);
        }
        
        // Check if bucket already exists
        if let Some(bucket) = self.get_bucket(key) {
            return Ok(bucket);
        }
        
        self.create_bucket(key)
    }

    /// Delete a nested bucket
    pub fn delete_bucket(&mut self, key: &[u8]) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        if key.is_empty() {
            return Err(Error::BucketNameRequired);
        }
        
        // Find the bucket entry
        let mut cursor = self.cursor();
        let result = cursor.seek(key);
        
        let (k, _, flags) = match result {
            Some(r) => r,
            None => return Err(Error::BucketNotFound),
        };
        
        if k != key {
            return Err(Error::BucketNotFound);
        }
        if flags & LeafFlags::BUCKET_LEAF_FLAG.bits() == 0 {
            return Err(Error::IncompatibleValue);
        }
        
        // Delete the bucket entry
        self.delete(key)?;
        
        Ok(())
    }
    


    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if key.is_empty() {
            return None;
        }

        // Empty bucket
        if self.root_pgid == 0 {
            return None;
        }

        // Search the B+tree starting from root page
        self.search(key)
    }

    /// Search for a key in the B+tree
    fn search(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Start from root page
        let mut pgid = self.root_pgid;

        loop {
            let page_data = self.page_data(pgid)?;

            // Check page flags at bytes 8-9
            let flags = u16::from_le_bytes([page_data[8], page_data[9]]);

            if flags == PageFlags::LEAF_PAGE_FLAG.bits() {
                // Search leaf page for key
                return self.search_leaf(&page_data, key);
            } else if flags == PageFlags::BRANCH_PAGE_FLAG.bits() {
                // Search branch page for next page
                pgid = self.search_branch(&page_data, key)?;
            } else {
                // Unknown page type
                return None;
            }
        }
    }

    /// Search a branch page for the child page containing the key
    fn search_branch(&self, data: &[u8], key: &[u8]) -> Option<Pgid> {
        // Parse element count from page header (bytes 10-11)
        let count = u16::from_le_bytes([data[10], data[11]]) as usize;
        if count == 0 {
            return None;
        }
        
        // Branch elements start at offset 16 (PAGE_HEADER_SIZE)
        // Each element is 16 bytes: pos(4) + ksize(4) + pgid(8)
        let elem_size = 16usize;
        let elem_start = 16usize;
        
        // Binary search to find the correct child
        let mut low = 0;
        let mut high = count;

        while low < high {
            let mid = (low + high) / 2;
            let elem_offset = elem_start + mid * elem_size;
            
            // Read ksize at offset 4-7
            let ksize = u32::from_le_bytes([
                data[elem_offset + 4],
                data[elem_offset + 5],
                data[elem_offset + 6],
                data[elem_offset + 7],
            ]) as usize;
            
            // Read key at offset 0-3 (pos)
            let pos = u32::from_le_bytes([
                data[elem_offset],
                data[elem_offset + 1],
                data[elem_offset + 2],
                data[elem_offset + 3],
            ]) as usize;
            
            let elem_key = &data[pos..pos + ksize];

            match elem_key.cmp(key) {
                Ordering::Less => low = mid + 1,
                Ordering::Equal | Ordering::Greater => high = mid,
            }
        }

        // Return the pgid at the found index
        if low < count {
            let elem_offset = elem_start + low * elem_size;
            let pgid = u64::from_le_bytes([
                data[elem_offset + 8],
                data[elem_offset + 9],
                data[elem_offset + 10],
                data[elem_offset + 11],
                data[elem_offset + 12],
                data[elem_offset + 13],
                data[elem_offset + 14],
                data[elem_offset + 15],
            ]);
            Some(pgid)
        } else {
            None
        }
    }

    /// Search a leaf page for a key-value pair
    fn search_leaf(&self, data: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        // Parse element count from page header (bytes 10-11)
        let count = u16::from_le_bytes([data[10], data[11]]) as usize;
        if count == 0 {
            return None;
        }
        
        // Leaf elements start at offset 16 (PAGE_HEADER_SIZE)
        // Each element is 16 bytes: flags(4) + pos(4) + ksize(4) + vsize(4)
        let elem_size = 16usize;
        let elem_start = 16usize;
        
        // Binary search to find the key
        let mut low = 0;
        let mut high = count;

        while low < high {
            let mid = (low + high) / 2;
            let elem_offset = elem_start + mid * elem_size;
            
            // Read flags, pos, ksize, vsize
            let flags = u32::from_le_bytes([
                data[elem_offset],
                data[elem_offset + 1],
                data[elem_offset + 2],
                data[elem_offset + 3],
            ]);
            let pos = u32::from_le_bytes([
                data[elem_offset + 4],
                data[elem_offset + 5],
                data[elem_offset + 6],
                data[elem_offset + 7],
            ]) as usize;
            let ksize = u32::from_le_bytes([
                data[elem_offset + 8],
                data[elem_offset + 9],
                data[elem_offset + 10],
                data[elem_offset + 11],
            ]) as usize;
            let vsize = u32::from_le_bytes([
                data[elem_offset + 12],
                data[elem_offset + 13],
                data[elem_offset + 14],
                data[elem_offset + 15],
            ]) as usize;
            
            // Use pos offset (absolute position within page) to find key
            let key_start = pos;
            let elem_key = &data[key_start..key_start + ksize];

            match elem_key.cmp(key) {
                Ordering::Less => low = mid + 1,
                Ordering::Equal => {
                    // Found exact match
                    // Value follows key (sequential within the data region)
                    let value_start = key_start + ksize;
                    return Some(data[value_start..value_start + vsize].to_vec());
                }
                Ordering::Greater => high = mid,
            }
        }

        None
    }

    /// Put a key-value pair
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }

        if key.is_empty() {
            return Err(Error::KeyRequired);
        }

        if key.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge);
        }

        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ValueTooLarge);
        }

        // If this is the first insert and bucket is empty, create root leaf page
        if self.root_pgid == 0 {
            // Allocate a new leaf page for the bucket root
            let pgid: u64 = 3; // First user page after meta0, meta1, freelist
            
            // Create page data with header
            let mut data = vec![0u8; self.db.page_size()];
            data[0..8].copy_from_slice(&pgid.to_le_bytes());
            data[8..10].copy_from_slice(&PageFlags::LEAF_PAGE_FLAG.bits().to_le_bytes());
            data[10..12].copy_from_slice(&0u16.to_le_bytes());
            data[12..16].copy_from_slice(&0u32.to_le_bytes());
            
            // Store in dirty pages
            let mut pages = self.db.get_pages().lock().unwrap();
            pages.insert(pgid, data);
            drop(pages);
            
            // Update root_pgid
            self.set_root_page(pgid);
        }

        // Search and update if key exists, or insert if not
        self.put_with_alloc(key, value)
    }

    fn put_with_flags(&mut self, key: &[u8], value: &[u8], flags: u32) -> Result<()> {
        let (leaf_pgid, leaf_data, insert_index) = self.find_leaf_with_index(key)?;
        
        let page_size = self.db.page_size();
        let elem_size = 16usize;
        let elem_start = 16usize;
        
        let count = u16::from_le_bytes([leaf_data[10], leaf_data[11]]) as usize;
        
        let mut key_exists = false;
        let mut existing_index = insert_index;
        let mut elems: Vec<(u32, u32, u32)> = Vec::new();
        
        for i in 0..count {
            let elem_offset = elem_start + i * elem_size;
            let pos = u32::from_le_bytes([leaf_data[elem_offset + 4], leaf_data[elem_offset + 5], leaf_data[elem_offset + 6], leaf_data[elem_offset + 7]]);
            let ksize = u32::from_le_bytes([leaf_data[elem_offset + 8], leaf_data[elem_offset + 9], leaf_data[elem_offset + 10], leaf_data[elem_offset + 11]]);
            let vsize = u32::from_le_bytes([leaf_data[elem_offset + 12], leaf_data[elem_offset + 13], leaf_data[elem_offset + 14], leaf_data[elem_offset + 15]]);
            elems.push((pos, ksize, vsize));
            if pos as usize + ksize as usize <= leaf_data.len() && &leaf_data[pos as usize..pos as usize + ksize as usize] == key {
                key_exists = true;
                existing_index = i;
            }
        }
        
        let new_count = if key_exists { count } else { count + 1 };
        // Key-value data starts AFTER all element headers (not after existing data)
        let new_kv_pos = elem_start + new_count * elem_size;
        let new_data_end = new_kv_pos + key.len() + value.len();
        if new_data_end > page_size {
            return Err(Error::ValueTooLarge);
        }
        
        let mut new_data = vec![0u8; page_size];
        new_data[0..16].copy_from_slice(&leaf_data[0..16]);
        new_data[10..12].copy_from_slice(&(new_count as u16).to_le_bytes());
        
        let elem_to_skip = if key_exists { Some(existing_index) } else { None };
        
        // Copy existing key-value data to ORIGINAL positions
        for (i, &(pos, ksize, vsize)) in elems.iter().enumerate() {
            if Some(i) == elem_to_skip { continue; }
            let pos = pos as usize; let ksize = ksize as usize; let vsize = vsize as usize;
            new_data[pos..pos + ksize].copy_from_slice(&leaf_data[pos..pos + ksize]);
            new_data[pos + ksize..pos + ksize + vsize].copy_from_slice(&leaf_data[pos + ksize..pos + ksize + vsize]);
        }
        
        // Write ALL element headers FIRST
        let mut tgt_i = 0;
        for i in 0..count {
            if Some(i) == elem_to_skip { continue; }
            let tgt_offset = elem_start + tgt_i * elem_size;
            let (pos, ksize, vsize) = elems[i];
            new_data[tgt_offset..tgt_offset + 4].copy_from_slice(&0u32.to_le_bytes());
            new_data[tgt_offset + 4..tgt_offset + 8].copy_from_slice(&pos.to_le_bytes());
            new_data[tgt_offset + 8..tgt_offset + 12].copy_from_slice(&ksize.to_le_bytes());
            new_data[tgt_offset + 12..tgt_offset + 16].copy_from_slice(&vsize.to_le_bytes());
            tgt_i += 1;
        }
        
        // Write new element header at insert_index
        if !key_exists {
            let new_elem_offset = elem_start + insert_index * elem_size;
            new_data[new_elem_offset..new_elem_offset + 4].copy_from_slice(&flags.to_le_bytes());
            new_data[new_elem_offset + 4..new_elem_offset + 8].copy_from_slice(&(new_kv_pos as u32).to_le_bytes());
            new_data[new_elem_offset + 8..new_elem_offset + 12].copy_from_slice(&(key.len() as u32).to_le_bytes());
            new_data[new_elem_offset + 12..new_elem_offset + 16].copy_from_slice(&(value.len() as u32).to_le_bytes());
        }
        
        // Write new key-value data at the END (after all element headers are written)
        if !key_exists {
            new_data[new_kv_pos..new_kv_pos + key.len()].copy_from_slice(key);
            new_data[new_kv_pos + key.len()..new_kv_pos + key.len() + value.len()].copy_from_slice(value);
        }
        
        self.db.get_pages().lock().unwrap().insert(leaf_pgid, new_data);
        Ok(())
    }

    fn put_with_alloc(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with_flags(key, value, 0)
    }

    /// Find leaf page and insertion index for a key
    fn find_leaf_with_index(&mut self, key: &[u8]) -> Result<(Pgid, Vec<u8>, usize)> {
        let mut pgid = self.root_pgid;
        loop {
            let page_data = self.page_data(pgid).ok_or(Error::PageNotFound(pgid))?;

            // Check page flags at bytes 8-9
            let flags = u16::from_le_bytes([page_data[8], page_data[9]]);

            if flags == PageFlags::LEAF_PAGE_FLAG.bits() {
                // Find index in leaf
                let count = u16::from_le_bytes([page_data[10], page_data[11]]) as usize;
                let mut index = count;
                
                // Leaf elements start at offset 16
                let elem_size = 16usize;
                let elem_start = 16usize;
                
                for i in 0..count {
                    let elem_offset = elem_start + i * elem_size;
                    
                    // Read pos and ksize
                    let pos = u32::from_le_bytes([
                        page_data[elem_offset + 4],
                        page_data[elem_offset + 5],
                        page_data[elem_offset + 6],
                        page_data[elem_offset + 7],
                    ]) as usize;
                    let ksize = u32::from_le_bytes([
                        page_data[elem_offset + 8],
                        page_data[elem_offset + 9],
                        page_data[elem_offset + 10],
                        page_data[elem_offset + 11],
                    ]) as usize;
                    
                    let elem_key = &page_data[pos..pos + ksize];
                    if elem_key.cmp(key) != Ordering::Less {
                        index = i;
                        break;
                    }
                }
                
                return Ok((pgid, page_data, index));
            } else if flags == PageFlags::BRANCH_PAGE_FLAG.bits() {
                // Follow branch
                pgid = self.search_branch(&page_data, key)
                    .ok_or(Error::PageNotFound(pgid))?;
            } else {
                return Err(Error::PageNotFound(pgid));
            }
        }
    }

    /// Delete a key-value pair
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }

        if key.is_empty() {
            return Err(Error::KeyRequired);
        }

        if self.root_pgid == 0 {
            return Ok(()); // Nothing to delete
        }

        // Find and delete the key
        self.delete_from_tree(key)
    }

    /// Delete a key from the B+tree
    fn delete_from_tree(&mut self, key: &[u8]) -> Result<()> {
        let (leaf_pgid, leaf_data, _insert_index) = self.find_leaf_with_index(key)?;
        
        let page_size = self.db.page_size();
        let elem_size = 16usize;
        let elem_start = 16usize;
        let count = u16::from_le_bytes([leaf_data[10], leaf_data[11]]) as usize;
        
        // Find the element to delete
        let mut del_index = None;
        let mut del_pos = 0u32;
        let mut del_ksize = 0u32;
        let mut del_vsize = 0u32;
        
        for i in 0..count {
            let elem_offset = elem_start + i * elem_size;
            let pos = u32::from_le_bytes([leaf_data[elem_offset + 4], leaf_data[elem_offset + 5], leaf_data[elem_offset + 6], leaf_data[elem_offset + 7]]);
            let ksize = u32::from_le_bytes([leaf_data[elem_offset + 8], leaf_data[elem_offset + 9], leaf_data[elem_offset + 10], leaf_data[elem_offset + 11]]);
            let vsize = u32::from_le_bytes([leaf_data[elem_offset + 12], leaf_data[elem_offset + 13], leaf_data[elem_offset + 14], leaf_data[elem_offset + 15]]);
            let elem_key = &leaf_data[pos as usize..pos as usize + ksize as usize];
            
            if elem_key == key {
                del_index = Some(i);
                del_pos = pos;
                del_ksize = ksize;
                del_vsize = vsize;
                break;
            }
        }
        
        if del_index.is_none() {
            return Ok(()); // Key not found
        }
        
        let dIdx = del_index.unwrap();
        
        // Build new page with element removed
        let mut new_data = vec![0u8; page_size];
        new_data[0..16].copy_from_slice(&leaf_data[0..16]);
        new_data[10..12].copy_from_slice(&((count - 1) as u16).to_le_bytes());
        
        // Copy element headers (skip deleted)
        let mut tgt_i = 0usize;
        for i in 0..count {
            if i == dIdx { continue; }
            let src_off = elem_start + i * elem_size;
            let tgt_off = elem_start + tgt_i * elem_size;
            new_data[tgt_off..tgt_off + elem_size].copy_from_slice(&leaf_data[src_off..src_off + elem_size]);
            tgt_i += 1;
        }
        
        // Copy key-value data (compacted)
        let new_data_start = elem_start + (count - 1) * elem_size;
        let del_data_start = del_pos as usize;
        let del_data_end = del_data_start + del_ksize as usize + del_vsize as usize;
        
        // Find all data positions
        let mut all_data_start = elem_start + count * elem_size;
        let mut all_data_end = all_data_start;
        for i in 0..count {
            let elem_offset = elem_start + i * elem_size;
            let pos = u32::from_le_bytes([leaf_data[elem_offset + 4], leaf_data[elem_offset + 5], leaf_data[elem_offset + 6], leaf_data[elem_offset + 7]]);
            let ksize = u32::from_le_bytes([leaf_data[elem_offset + 8], leaf_data[elem_offset + 9], leaf_data[elem_offset + 10], leaf_data[elem_offset + 11]]);
            let vsize = u32::from_le_bytes([leaf_data[elem_offset + 12], leaf_data[elem_offset + 13], leaf_data[elem_offset + 14], leaf_data[elem_offset + 15]]);
            all_data_start = all_data_start.min(pos as usize);
            all_data_end = all_data_end.max((pos + ksize + vsize) as usize);
        }
        
        // Copy data before deleted
        let mut write_pos = elem_start + (count - 1) * elem_size;
        if all_data_start < del_data_start {
            let len = del_data_start - all_data_start as usize;
            new_data[write_pos..write_pos + len].copy_from_slice(&leaf_data[all_data_start as usize..del_data_start]);
            write_pos += len;
        }
        
        // Copy data after deleted
        if del_data_end < all_data_end as usize {
            let len = all_data_end as usize - del_data_end;
            new_data[write_pos..write_pos + len].copy_from_slice(&leaf_data[del_data_end..del_data_end + len]);
        }
        
        // Update page cache
        self.db.get_pages().lock().unwrap().insert(leaf_pgid, new_data);
        
        Ok(())
    }

    /// Iterate over all key-value pairs
    pub fn for_each<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        if self.root_pgid == 0 {
            return Ok(()); // Empty bucket
        }

        let mut cursor = self.cursor();
        while let Some((k, v)) = cursor.next() {
            f(&k, &v)?;
        }
        Ok(())
    }

    /// Sequence returns the current integer sequence for the bucket without incrementing it.
    pub fn sequence(&self) -> u64 {
        self.inbucket.sequence()
    }

    /// SetSequence updates the sequence number for the bucket.
    pub fn set_sequence(&mut self, sequence: u64) -> Result<()> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }
        self.inbucket.set_sequence(sequence);
        Ok(())
    }

    /// NextSequence returns the next integer sequence for the bucket.
    /// The sequence is stored in the InBucket header and is incremented on each call.
    pub fn next_sequence(&mut self) -> Result<u64> {
        if !self.writable {
            return Err(Error::TxNotWritable);
        }

        // Get current sequence
        let mut sequence = self.inbucket.sequence();

        // Increment sequence
        sequence += 1;
        self.inbucket.set_sequence(sequence);

        // Note: The sequence is persisted when the transaction commits
        // by updating the InBucket data in the meta page root
        
        Ok(sequence)
    }

    /// ForEachBucket iterates over all nested buckets in this bucket.
    /// The callback function receives the bucket name and a Bucket reference.
    /// Only entries with the BUCKET_LEAF_FLAG are iterated.
    pub fn for_each_bucket<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], Bucket) -> Result<()>,
    {
        if self.root_pgid == 0 {
            return Ok(()); // Empty bucket - no buckets to iterate
        }

        // Create cursor for this bucket
        let mut cursor = self.cursor();
        
        // Iterate over all entries
        while let Some((k, v, flags)) = cursor.next_with_flags() {
            // Check if this is a bucket entry
            if flags & LeafFlags::BUCKET_LEAF_FLAG.bits() != 0 {
                // Parse InBucket from value
                if v.len() < 16 {
                    continue;
                }
                
                let root_pgid = Pgid::from_le_bytes([
                    v[0], v[1], v[2], v[3],
                    v[4], v[5], v[6], v[7]
                ]);
                let sequence = u64::from_le_bytes([
                    v[8], v[9], v[10], v[11],
                    v[12], v[13], v[14], v[15]
                ]);
                let inbucket = crate::page::InBucket::new(root_pgid, sequence);
                
                // Check if this is an inline bucket
                if root_pgid == 0 && v.len() > 16 {
                    let inline_data = v[16..].to_vec();
                    let bucket = Bucket::with_inline_data(inbucket, self.db.clone(), inline_data);
                    f(&k, bucket)?;
                } else if root_pgid > 0 {
                    let bucket = Bucket::with_db(inbucket, self.db.clone());
                    f(&k, bucket)?;
                }
            }
        }

        Ok(())
    }

    /// Get page data for a page ID
    pub fn page_data(&self, pgid: Pgid) -> Option<Vec<u8>> {
        self.db.page_data(pgid)
    }

    /// Get page info
    pub fn page(&self, pgid: Pgid) -> Page {
        self.db.page(pgid)
    }

    /// Get the InBucket info (root page ID and sequence)
    pub fn inbucket(&self) -> crate::page::InBucket {
        self.inbucket
    }

    /// Set the InBucket info
    pub fn set_inbucket(&mut self, inbucket: crate::page::InBucket) {
        self.inbucket = inbucket;
        self.root_pgid = inbucket.root_pgid();
    }

    /// Stats returns statistics about the bucket.
    pub fn stats(&self) -> BucketStats {
        let mut s = BucketStats::default();
        let page_size = self.db.page_size();
        
        s.bucket_count += 1;
        if self.root_pgid == 0 {
            s.inline_bucket_count += 1;
        }
        
        // Traverse all pages in this bucket
        self.for_each_page(|page_data, depth, is_leaf, overflow| {
            if is_leaf {
                // Count keys
                if page_data.len() >= 16 {
                    let count = u16::from_le_bytes([page_data[10], page_data[11]]) as usize;
                    s.key_count += count;
                }
                
                // Calculate used bytes
                let mut used = 16; // Page header
                if page_data.len() >= 16 {
                    let count = u16::from_le_bytes([page_data[10], page_data[11]]) as usize;
                    if count > 0 {
                        // Element headers
                        used += 16 * count;
                        
                        // Data: use last element's position + sizes
                        let elem_start = 16 + (count - 1) * 16;
                        if elem_start + 16 <= page_data.len() {
                            let pos = u32::from_le_bytes([
                                page_data[elem_start + 4],
                                page_data[elem_start + 5],
                                page_data[elem_start + 6],
                                page_data[elem_start + 7],
                            ]) as usize;
                            let ksize = u32::from_le_bytes([
                                page_data[elem_start + 8],
                                page_data[elem_start + 9],
                                page_data[elem_start + 10],
                                page_data[elem_start + 11],
                            ]) as usize;
                            let vsize = u32::from_le_bytes([
                                page_data[elem_start + 12],
                                page_data[elem_start + 13],
                                page_data[elem_start + 14],
                                page_data[elem_start + 15],
                            ]) as usize;
                            used = pos + ksize + vsize + 16; // +16 for last element header
                        }
                    }
                }
                
                if self.root_pgid == 0 {
                    s.inline_bucket_inuse += used;
                } else {
                    s.leaf_page_count += 1;
                    s.leaf_inuse += used;
                    s.leaf_overflow_count += overflow;
                }
            } else {
                // Branch page
                s.branch_page_count += 1;
                let mut used = 16; // Page header
                if page_data.len() >= 16 {
                    let count = u16::from_le_bytes([page_data[10], page_data[11]]) as usize;
                    if count > 0 {
                        used += 16 * count;
                        let elem_start = 16 + (count - 1) * 16;
                        if elem_start + 16 <= page_data.len() {
                            let pos = u32::from_le_bytes([
                                page_data[elem_start + 4],
                                page_data[elem_start + 5],
                                page_data[elem_start + 6],
                                page_data[elem_start + 7],
                            ]) as usize;
                            let ksize = u32::from_le_bytes([
                                page_data[elem_start + 8],
                                page_data[elem_start + 9],
                                page_data[elem_start + 10],
                                page_data[elem_start + 11],
                            ]) as usize;
                            used = pos + ksize + 16; // +16 for last element header
                        }
                    }
                }
                s.branch_inuse += used;
                s.branch_overflow_count += overflow;
            }
            
            // Track maximum depth
            if depth + 1 > s.depth {
                s.depth = depth + 1;
            }
        });
        
        s
    }

    /// Inspect returns a debug string representation of the bucket.
    pub fn inspect(&self) -> String {
        let stats = self.stats();
        format!(
            "Bucket {{\n  root_pgid: {},\n  depth: {},\n  key_count: {},\n  bucket_count: {},\n  leaf_pages: {},\n  branch_pages: {},\n  inline_buckets: {},\n  inline_bytes: {},\n  leaf_inuse: {},\n  branch_inuse: {},\n}}",
            self.root_pgid,
            stats.depth,
            stats.key_count,
            stats.bucket_count,
            stats.leaf_page_count,
            stats.branch_page_count,
            stats.inline_bucket_count,
            stats.inline_bucket_inuse,
            stats.leaf_inuse,
            stats.branch_inuse,
        )
    }

    /// ForEachPage iterates over all pages in the bucket with a callback.
    fn for_each_page<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], usize, bool, usize),
    {
        if self.root_pgid == 0 {
            // Inline bucket
            if let Some(ref data) = self.inline_data {
                f(data, 0, true, 0);
            }
            return;
        }
        
        self._for_each_page(self.root_pgid, 0, &mut f);
    }

    fn _for_each_page<F>(&self, pgid: Pgid, depth: usize, f: &mut F)
    where
        F: FnMut(&[u8], usize, bool, usize),
    {
        let page_data = match self.page_data(pgid) {
            Some(d) => d,
            None => return,
        };
        
        if page_data.len() < 16 {
            return;
        }
        
        let flags = u16::from_le_bytes([page_data[8], page_data[9]]);
        let overflow = u32::from_le_bytes([page_data[12], page_data[13], page_data[14], page_data[15]]) as usize;
        
        let is_leaf = flags == PageFlags::LEAF_PAGE_FLAG.bits();
        
        // Call the callback
        f(&page_data, depth, is_leaf, overflow);
        
        // Recursively traverse children for branch pages
        if !is_leaf {
            let count = u16::from_le_bytes([page_data[10], page_data[11]]) as usize;
            for i in 0..count {
                let elem_offset = 16 + i * 16;
                if elem_offset + 16 > page_data.len() {
                    break;
                }
                let child_pgid = u64::from_le_bytes([
                    page_data[elem_offset + 8],
                    page_data[elem_offset + 9],
                    page_data[elem_offset + 10],
                    page_data[elem_offset + 11],
                    page_data[elem_offset + 12],
                    page_data[elem_offset + 13],
                    page_data[elem_offset + 14],
                    page_data[elem_offset + 15],
                ]);
                self._for_each_page(child_pgid, depth + 1, f);
            }
        }
    }
}

/// Meta placeholder for Bucket creation
impl Default for crate::page::Meta {
    fn default() -> Self {
        crate::page::Meta {
            magic: 0,
            version: 0,
            page_size: 4096,
            flags: 0,
            root: crate::page::InBucket::default(),
            freelist: 0,
            pgid: 0,
            txid: 0,
            checksum: 0,
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Meta;

    fn create_test_bucket() -> Bucket {
        // Create a simple bucket with no root page
        let meta = Meta::default();
        let db = TxDatabase::new(4096, meta, vec![]);
        Bucket::with_db(crate::page::InBucket::new(0, 0), db)
    }

    fn create_bucket_with_data() -> Bucket {
        // Create a bucket with test data
        let page_size = 4096;
        let mut data = vec![0u8; page_size * 2]; // 2 pages

        // Page 0: meta page
        let meta = Meta::new(page_size as u32, 0, crate::page::InBucket::new(1, 0), 2, 0);
        data[0..8].copy_from_slice(&0u64.to_le_bytes()); // id
        data[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes()); // flags
        data[10..12].copy_from_slice(&0u16.to_le_bytes()); // count
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow
        
        let meta_offset = 16;
        data[meta_offset..meta_offset + 4].copy_from_slice(&meta.magic.to_le_bytes());
        data[meta_offset + 4..meta_offset + 8].copy_from_slice(&meta.version.to_le_bytes());
        data[meta_offset + 8..meta_offset + 12].copy_from_slice(&meta.page_size.to_le_bytes());
        data[meta_offset + 12..meta_offset + 16].copy_from_slice(&meta.flags.to_le_bytes());
        data[meta_offset + 16..meta_offset + 24].copy_from_slice(&meta.root.root.to_le_bytes());
        data[meta_offset + 24..meta_offset + 32].copy_from_slice(&meta.root.sequence.to_le_bytes());
        data[meta_offset + 32..meta_offset + 40].copy_from_slice(&meta.freelist.to_le_bytes());
        data[meta_offset + 40..meta_offset + 48].copy_from_slice(&meta.pgid.to_le_bytes());
        data[meta_offset + 48..meta_offset + 56].copy_from_slice(&meta.txid.to_le_bytes());
        data[meta_offset + 56..meta_offset + 64].copy_from_slice(&meta.checksum.to_le_bytes());

        // Page 1: leaf page with "key1" -> "value1"
        let page_offset = page_size;
        data[page_offset..page_offset + 8].copy_from_slice(&1u64.to_le_bytes()); // id
        data[page_offset + 8..page_offset + 10].copy_from_slice(&PageFlags::LEAF_PAGE_FLAG.bits().to_le_bytes()); // flags
        data[page_offset + 10..page_offset + 12].copy_from_slice(&1u16.to_le_bytes()); // count = 1
        data[page_offset + 12..page_offset + 16].copy_from_slice(&0u32.to_le_bytes()); // overflow

        // Leaf element at offset 16 (PAGE_HEADER_SIZE)
        let elem_offset = page_offset + 16;
        let flags: u32 = 0; // not a bucket entry
        let pos: u32 = 32; // key starts at offset 32
        let ksize: u32 = 4; // "key1"
        let vsize: u32 = 6; // "value1"

        data[elem_offset..elem_offset + 4].copy_from_slice(&flags.to_le_bytes());
        data[elem_offset + 4..elem_offset + 8].copy_from_slice(&pos.to_le_bytes());
        data[elem_offset + 8..elem_offset + 12].copy_from_slice(&ksize.to_le_bytes());
        data[elem_offset + 12..elem_offset + 16].copy_from_slice(&vsize.to_le_bytes());

        // Key data (at page offset 32)
        data[page_offset + 32..page_offset + 36].copy_from_slice(b"key1");
        // Value data (at page offset 36)
        data[page_offset + 36..page_offset + 42].copy_from_slice(b"value1");

        let db = TxDatabase::new(page_size, meta, data);
        Bucket::with_db(crate::page::InBucket::new(1, 0), db)
    }

    #[test]
    fn test_bucket_get_empty() {
        let bucket = create_test_bucket();
        assert_eq!(bucket.get(b"key1"), None);
    }

    #[test]
    fn test_bucket_get_empty_key() {
        let bucket = create_test_bucket();
        assert_eq!(bucket.get(b""), None);
    }

    #[test]
    fn test_bucket_get_not_found() {
        let bucket = create_bucket_with_data();
        assert_eq!(bucket.get(b"notexist"), None);
    }

    #[test]
    fn test_bucket_get_found() {
        let bucket = create_bucket_with_data();
        assert_eq!(bucket.get(b"key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_bucket_put_not_writable() {
        let mut bucket = create_bucket_with_data();
        let result = bucket.put(b"key2", b"value2");
        assert!(matches!(result, Err(Error::TxNotWritable)));
    }

    #[test]
    fn test_bucket_delete_not_writable() {
        let mut bucket = create_bucket_with_data();
        let result = bucket.delete(b"key1");
        assert!(matches!(result, Err(Error::TxNotWritable)));
    }

    #[test]
    fn test_bucket_put_empty_key() {
        let mut bucket = create_bucket_with_data();
        bucket.set_writable(true);
        let result = bucket.put(b"", b"value");
        assert!(matches!(result, Err(Error::KeyRequired)));
    }

    #[test]
    fn test_bucket_delete_empty_key() {
        let mut bucket = create_bucket_with_data();
        bucket.set_writable(true);
        let result = bucket.delete(b"");
        assert!(matches!(result, Err(Error::KeyRequired)));
    }

    fn create_writable_bucket() -> Bucket {
        let page_size = 4096;
        let mut data = vec![0u8; page_size * 2]; // 2 pages
        
        // Meta page at offset 0
        let meta = Meta::new(page_size as u32, 0, crate::page::InBucket::new(1, 0), 2, 0);
        data[0..8].copy_from_slice(&0u64.to_le_bytes());
        data[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes());
        data[10..12].copy_from_slice(&0u16.to_le_bytes());
        data[12..16].copy_from_slice(&0u32.to_le_bytes());
        
        let meta_offset = 16;
        data[meta_offset..meta_offset + 4].copy_from_slice(&meta.magic.to_le_bytes());
        data[meta_offset + 4..meta_offset + 8].copy_from_slice(&meta.version.to_le_bytes());
        data[meta_offset + 8..meta_offset + 12].copy_from_slice(&meta.page_size.to_le_bytes());
        data[meta_offset + 12..meta_offset + 16].copy_from_slice(&meta.flags.to_le_bytes());
        data[meta_offset + 16..meta_offset + 24].copy_from_slice(&meta.root.root.to_le_bytes());
        data[meta_offset + 24..meta_offset + 32].copy_from_slice(&meta.root.sequence.to_le_bytes());
        data[meta_offset + 32..meta_offset + 40].copy_from_slice(&meta.freelist.to_le_bytes());
        data[meta_offset + 40..meta_offset + 48].copy_from_slice(&meta.pgid.to_le_bytes());
        data[meta_offset + 48..meta_offset + 56].copy_from_slice(&meta.txid.to_le_bytes());
        data[meta_offset + 56..meta_offset + 64].copy_from_slice(&meta.checksum.to_le_bytes());
        
        // Page 1: root leaf page at offset 4096
        let page_offset = page_size;
        data[page_offset..page_offset + 8].copy_from_slice(&1u64.to_le_bytes());
        data[page_offset + 8..page_offset + 10].copy_from_slice(&PageFlags::LEAF_PAGE_FLAG.bits().to_le_bytes());
        data[page_offset + 10..page_offset + 12].copy_from_slice(&0u16.to_le_bytes()); // empty
        data[page_offset + 12..page_offset + 16].copy_from_slice(&0u32.to_le_bytes());
        
        let db = TxDatabase::new(page_size, meta, data);
        let mut bucket = Bucket::with_db(crate::page::InBucket::new(1, 0), db);
        bucket.set_writable(true);
        bucket
    }

    #[test]
    fn test_create_bucket() {
        let mut bucket = create_writable_bucket();
        
        let result = bucket.create_bucket(b"subbucket");
        assert!(result.is_ok(), "create_bucket should succeed");
        
        // Verify bucket exists
        assert!(bucket.get_bucket(b"subbucket").is_some());
    }

    #[test]
    fn test_create_bucket_empty_name() {
        let mut bucket = create_writable_bucket();
        
        let result = bucket.create_bucket(b"");
        assert!(matches!(result, Err(Error::BucketNameRequired)));
    }

    #[test]
    fn test_create_bucket_twice() {
        let mut bucket = create_writable_bucket();
        
        assert!(bucket.create_bucket(b"subbucket").is_ok());
        let result = bucket.create_bucket(b"subbucket");
        assert!(matches!(result, Err(Error::BucketExists)));
    }

    #[test]
    fn test_create_bucket_if_not_exists() {
        let mut bucket = create_writable_bucket();
        
        // First create
        let result1 = bucket.create_bucket_if_not_exists(b"subbucket");
        assert!(result1.is_ok());
        
        // Second call should return existing
        let result2 = bucket.create_bucket_if_not_exists(b"subbucket");
        assert!(result2.is_ok());
    }

    #[test]
    fn test_create_bucket_if_not_exists_empty_name() {
        let mut bucket = create_writable_bucket();
        
        let result = bucket.create_bucket_if_not_exists(b"");
        assert!(matches!(result, Err(Error::BucketNameRequired)));
    }

    #[test]
    fn test_get_bucket() {
        let mut bucket = create_writable_bucket();
        
        bucket.create_bucket(b"subbucket").unwrap();
        
        let nested = bucket.get_bucket(b"subbucket");
        assert!(nested.is_some());
    }

    #[test]
    fn test_get_bucket_not_found() {
        let bucket = create_writable_bucket();
        assert!(bucket.get_bucket(b"notexist").is_none());
    }

    #[test]
    fn test_delete_bucket() {
        let mut bucket = create_writable_bucket();
        
        bucket.create_bucket(b"subbucket").unwrap();
        assert!(bucket.get_bucket(b"subbucket").is_some());
        
        let result = bucket.delete_bucket(b"subbucket");
        assert!(result.is_ok());
        
        assert!(bucket.get_bucket(b"subbucket").is_none());
    }

    #[test]
    fn test_delete_bucket_not_found() {
        let mut bucket = create_writable_bucket();
        
        let result = bucket.delete_bucket(b"notexist");
        assert!(matches!(result, Err(Error::BucketNotFound)));
    }

    #[test]
    fn test_delete_bucket_empty_name() {
        let mut bucket = create_writable_bucket();
        
        let result = bucket.delete_bucket(b"");
        assert!(matches!(result, Err(Error::BucketNameRequired)));
    }

    // ============================================
    // Tests for new features (4-6)
    // ============================================

    #[test]
    fn test_sequence_and_set_sequence() {
        let mut bucket = create_writable_bucket();
        
        // Initial sequence is 0
        assert_eq!(bucket.sequence(), 0, "Initial sequence should be 0");
        
        // Set sequence to 100
        bucket.set_sequence(100).unwrap();
        assert_eq!(bucket.sequence(), 100, "Sequence should be 100 after set_sequence");
        
        // Set sequence to 0
        bucket.set_sequence(0).unwrap();
        assert_eq!(bucket.sequence(), 0, "Sequence should be 0 after set_sequence");
    }

    #[test]
    fn test_set_sequence_not_writable() {
        let mut bucket = create_bucket_with_data();
        
        // Setting sequence on non-writable bucket should fail
        let result = bucket.set_sequence(100);
        assert!(result.is_err(), "set_sequence should fail on non-writable bucket");
        assert!(matches!(result.unwrap_err(), Error::TxNotWritable));
    }

    #[test]
    fn test_next_sequence() {
        // Hypothesis: next_sequence() should increment and return the sequence number
        let mut bucket = create_writable_bucket();
        
        // Initial sequence is 0 (from InBucket::new(1, 0))
        // First call should return 1
        let seq1 = bucket.next_sequence().unwrap();
        assert_eq!(seq1, 1, "First sequence should be 1");
        
        // Second call should return 2
        let seq2 = bucket.next_sequence().unwrap();
        assert_eq!(seq2, 2, "Second sequence should be 2");
        
        // Third call should return 3
        let seq3 = bucket.next_sequence().unwrap();
        assert_eq!(seq3, 3, "Third sequence should be 3");
    }

    #[test]
    fn test_for_each_bucket_with_db() {
        // Hypothesis: for_each_bucket() should iterate over nested buckets
        // Use create_bucket_with_data which has proper page setup
        let bucket = create_bucket_with_data();
        
        // Add nested buckets using the bucket
        // Note: This test requires a writable transaction context
        // For unit testing, we verify the method signature and iteration logic
        
        // Empty bucket should iterate 0 times
        let mut count = 0;
        let result = bucket.for_each_bucket(|_name, _subbucket| {
            count += 1;
            Ok(())
        });
        assert!(result.is_ok(), "for_each_bucket should succeed");
        // No buckets created yet, so count should be 0
        assert_eq!(count, 0, "Empty bucket should have 0 nested buckets");
    }

    #[test]
    fn test_for_each_bucket_empty() {
        // Hypothesis: for_each_bucket() on empty bucket should return Ok without calling callback
        let bucket = create_bucket_with_data();
        
        let mut count = 0;
        let result = bucket.for_each_bucket(|_name, _subbucket| {
            count += 1;
            Ok(())
        });
        
        assert!(result.is_ok());
        assert_eq!(count, 0, "No buckets should be found");
    }

    // ============================================
    // Tests for Bucket::stats() and Bucket::inspect()
    // ============================================

    #[test]
    fn test_bucket_stats_empty() {
        // Hypothesis: stats() on empty bucket should return valid stats structure
        let bucket = create_test_bucket();
        let stats = bucket.stats();
        
        assert_eq!(stats.key_count, 0, "Empty bucket should have 0 keys");
        assert_eq!(stats.depth, 0, "Empty bucket should have depth 0");
        assert_eq!(stats.bucket_count, 1, "Should count itself as 1 bucket");
        assert_eq!(stats.inline_bucket_count, 1, "Empty bucket should be inline");
    }

    #[test]
    fn test_bucket_stats_with_data() {
        // Hypothesis: stats() should correctly count keys
        let bucket = create_bucket_with_data();
        let stats = bucket.stats();
        
        // create_bucket_with_data has 1 key ("key1")
        assert_eq!(stats.key_count, 1, "Should have 1 key");
        assert!(stats.depth >= 1, "Should have at least depth 1");
    }

    #[test]
    fn test_bucket_inspect() {
        // Hypothesis: inspect() should return a debug string representation
        let bucket = create_test_bucket();
        let output = bucket.inspect();
        
        // Verify it contains expected fields
        assert!(output.contains("Bucket"), "Should contain 'Bucket'");
        assert!(output.contains("root_pgid:"), "Should contain 'root_pgid:'");
        assert!(output.contains("depth:"), "Should contain 'depth:'");
        assert!(output.contains("key_count:"), "Should contain 'key_count:'");
    }

    #[test]
    fn test_bucket_stats_writable() {
        // Hypothesis: stats() on writable bucket with data should return correct counts
        let mut bucket = create_writable_bucket();
        
        // Add some data
        bucket.put(b"key1", b"value1").unwrap();
        bucket.put(b"key2", b"value2").unwrap();
        
        let stats = bucket.stats();
        
        assert_eq!(stats.key_count, 2, "Should have 2 keys");
        assert!(stats.leaf_page_count >= 1, "Should have at least 1 leaf page");
    }
}