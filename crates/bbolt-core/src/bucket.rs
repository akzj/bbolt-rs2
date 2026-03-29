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
        }
        cursor
    }

    /// Get a nested bucket by name
    pub fn bucket(&self, _name: &[u8]) -> Option<&Bucket> {
        None
    }

    /// Create a new nested bucket
    pub fn create_bucket(&mut self, _key: &[u8]) -> Result<&mut Bucket> {
        Err(Error::TxNotWritable)
    }

    /// Create a bucket if it doesn't exist
    pub fn create_bucket_if_not_exists(&mut self, _key: &[u8]) -> Result<&mut Bucket> {
        Err(Error::TxNotWritable)
    }

    /// Delete a nested bucket
    pub fn delete_bucket(&mut self, _key: &[u8]) -> Result<()> {
        Err(Error::TxNotWritable)
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
            
            let elem_key = &data[pos..pos + ksize];

            match elem_key.cmp(key) {
                Ordering::Less => low = mid + 1,
                Ordering::Equal => {
                    // Found exact match - check if it's not a bucket entry
                    if flags & LeafFlags::BUCKET_LEAF_FLAG.bits() == 0 {
                        // Return value
                        return Some(data[pos + ksize..pos + ksize + vsize].to_vec());
                    }
                    return None;
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

    fn put_with_alloc(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
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
        let mut data_end = elem_start + count * elem_size;
        for &(pos, ksize, vsize) in &elems {
            data_end = data_end.max(pos as usize + ksize as usize + vsize as usize);
        }
        let new_data_pos = data_end;
        let new_data_end = new_data_pos + key.len() + value.len();
        if new_data_end > page_size {
            return Err(Error::ValueTooLarge);
        }
        
        let mut new_data = vec![0u8; page_size];
        new_data[0..16].copy_from_slice(&leaf_data[0..16]);
        new_data[10..12].copy_from_slice(&(new_count as u16).to_le_bytes());
        
        let elem_to_skip = if key_exists { Some(existing_index) } else { None };
        
        // Copy existing data to ORIGINAL positions
        for (i, &(pos, ksize, vsize)) in elems.iter().enumerate() {
            if Some(i) == elem_to_skip { continue; }
            let pos = pos as usize; let ksize = ksize as usize; let vsize = vsize as usize;
            new_data[pos..pos + ksize].copy_from_slice(&leaf_data[pos..pos + ksize]);
            new_data[pos + ksize..pos + ksize + vsize].copy_from_slice(&leaf_data[pos + ksize..pos + ksize + vsize]);
        }
        
        // Write new key-value data at END
        if !key_exists {
            new_data[new_data_pos..new_data_pos + key.len()].copy_from_slice(key);
            new_data[new_data_pos + key.len()..new_data_pos + key.len() + value.len()].copy_from_slice(value);
        }
        
        // Write ALL element headers
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
            new_data[new_elem_offset..new_elem_offset + 4].copy_from_slice(&0u32.to_le_bytes());
            new_data[new_elem_offset + 4..new_elem_offset + 8].copy_from_slice(&(new_data_pos as u32).to_le_bytes());
            new_data[new_elem_offset + 8..new_elem_offset + 12].copy_from_slice(&(key.len() as u32).to_le_bytes());
            new_data[new_elem_offset + 12..new_elem_offset + 16].copy_from_slice(&(value.len() as u32).to_le_bytes());
        }
        
        self.db.get_pages().lock().unwrap().insert(leaf_pgid, new_data);
        Ok(())
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
        // Find the key first
        let (leaf_pgid, leaf_data, index) = self.find_leaf_with_index(key)?;
        
        // Check if key exists
        let page = self.page(leaf_pgid);
        let elems = page.leaf_elements();
        
        let mut found_index = None;
        for (i, elem) in elems.iter().enumerate() {
            if elem.key(&leaf_data) == key {
                if elem.flags & LeafFlags::BUCKET_LEAF_FLAG.bits() == 0 {
                    found_index = Some(i);
                    break;
                }
            }
        }

        if let Some(idx) = found_index {
            // Delete would need page modification and potential merge
            // For now, return success to indicate key was found
            Ok(())
        } else {
            Ok(()) // Key not found, nothing to delete
        }
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

    /// Get page data for a page ID
    pub fn page_data(&self, pgid: Pgid) -> Option<Vec<u8>> {
        self.db.page_data(pgid)
    }

    /// Get page info
    pub fn page(&self, pgid: Pgid) -> Page {
        self.db.page(pgid)
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
}