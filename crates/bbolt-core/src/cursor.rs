//! Cursor implementation for bbolt
//!
//! Cursors provide bidirectional traversal over key-value pairs in a bucket.

use std::cmp::Ordering;

use crate::constants::*;
use crate::errors::Result;
use crate::page::{Page, Pgid};
use crate::tx::TxDatabase;

/// Cursor for traversing a bucket
pub struct Cursor {
    /// Stack of page references
    stack: Vec<ElemRef>,
    /// Database for page data access
    db: Option<TxDatabase>,
}

impl Cursor {
    /// Create a new cursor
    pub fn new() -> Self {
        Self {
            stack: Vec::new(),
            db: None,
        }
    }

    /// Set the root page for this cursor with a database
    pub fn set_root(&mut self, root_pgid: u64, db: TxDatabase) {
        self.stack.clear();
        self.db = Some(db);
        if let Some(ref db) = self.db {
            if let Some(data) = db.page_data(root_pgid) {
                self.stack.push(ElemRef::new(root_pgid, Some(data)));
                self.seek_to_first();
            }
        }
    }

    /// Move to the first key-value pair
    pub fn first(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.stack.is_empty() || self.db.is_none() {
            return None;
        }
        
        // Reset to root and seek to first
        let root_pgid = self.stack.first().map(|r| r.page.id)?;
        if let Some(ref mut db) = self.db {
            if let Some(data) = db.page_data(root_pgid) {
                self.stack.clear();
                self.stack.push(ElemRef::new(root_pgid, Some(data)));
                self.seek_to_first();
            }
        }

        // If we're on an empty page, move to next
        if let Some(count) = self.stack.last().map(|r| r.count()) {
            if count == 0 {
                self.next();
            }
        }

        self.key_value()
    }

    /// Move to the last key-value pair
    pub fn last(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.stack.is_empty() || self.db.is_none() {
            return None;
        }

        // Navigate to the last element
        while !self.is_leaf() {
            if let Some(pgid) = self.stack.last().and_then(|r| r.last_branch_pgid()) {
                if let Some(ref mut db) = self.db {
                    if let Some(data) = db.page_data(pgid) {
                        self.stack.push(ElemRef::new(pgid, Some(data)));
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        self.key_value()
    }

    /// Move to the next key-value pair
    pub fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            // Try to move forward in current page
            for i in 0..self.stack.len() {
                let stack_len = self.stack.len();
                if i >= stack_len {
                    break;
                }

                let current_index = self.stack[i].index;
                let count = self.stack[i].count();

                if current_index < count.saturating_sub(1) {
                    self.stack[i].index = current_index + 1;

                    // If we're on a leaf, return current
                    if self.is_leaf() {
                        return self.key_value();
                    }

                    // Navigate down to first element of next branch
                    self.stack.truncate(i + 1);
                    self.seek_to_first();

                    return self.key_value();
                }
            }

            return None;
        }
    }

    /// Move to the previous key-value pair
    pub fn prev(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let stack_len = self.stack.len();
        for i in 0..stack_len {
            if i >= self.stack.len() {
                break;
            }

            let current_index = self.stack[i].index;
            if current_index > 0 {
                self.stack[i].index = current_index - 1;
                return self.key_value();
            }
        }

        None
    }

    /// Get the current key and value
    pub fn key_value(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let ref_ = self.stack.last()?;

        let count = ref_.count();
        if count == 0 || ref_.index >= count {
            return None;
        }

        // Skip bucket entries
        let flags = ref_.leaf_flags()?;
        if flags & LeafFlags::BUCKET_LEAF_FLAG.bits() != 0 {
            return self.next();
        }

        ref_.leaf_key_value()
    }

    /// Clear the cursor stack
    pub fn clear(&mut self) {
        self.stack.clear();
    }

    /// Seek to a specific key, returning (key, value, flags) if found
    pub fn seek(&mut self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>, u32)> {
        if self.stack.is_empty() || self.db.is_none() {
            return None;
        }
        
        // Find the leaf page containing the key
        let mut pgid = self.stack.first()?.page.id;
        
        loop {
            if let Some(ref mut db) = self.db {
                if let Some(data) = db.page_data(pgid) {
                    let flags = u16::from_le_bytes([data[8], data[9]]);
                    
                    if flags == PageFlags::LEAF_PAGE_FLAG.bits() {
                        // Found leaf page, search for key
                        let count = u16::from_le_bytes([data[10], data[11]]) as usize;
                        let elem_size = 16usize;
                        let elem_start = 16usize;
                        
                        // Binary search for key
                        let mut low = 0;
                        let mut high = count;
                        
                        while low < high {
                            let mid = (low + high) / 2;
                            let elem_offset = elem_start + mid * elem_size;
                            
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
                            
                            if pos + ksize > data.len() {
                                // Key out of bounds, return None
                                return None;
                            }
                            
                            let elem_key = &data[pos..pos + ksize];
                            let cmp = elem_key.cmp(key);
                            
                            if cmp == Ordering::Less {
                                low = mid + 1;
                            } else {
                                high = mid;
                            }
                        }
                        
                        // Check if we found the exact key
                        if low < count {
                            let elem_offset = elem_start + low * elem_size;
                            let elem_flags = u32::from_le_bytes([
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
                            
                            let found_key = data[pos..pos + ksize].to_vec();
                            if found_key == key {
                                let found_value = data[pos + ksize..pos + ksize + vsize].to_vec();
                                return Some((found_key, found_value, elem_flags));
                            }
                        }
                        
                        return None;
                    } else if flags == PageFlags::BRANCH_PAGE_FLAG.bits() {
                        // Follow branch to next level
                        pgid = self.search_branch(&data, key)?;
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
    }

    /// Get the next element with flags
    pub fn next_with_flags(&mut self) -> Option<(Vec<u8>, Vec<u8>, u32)> {
        if self.stack.is_empty() || self.db.is_none() {
            return None;
        }
        
        let elem_size = 16usize;
        let elem_start = 16usize;
        
        loop {
            let stack_len = self.stack.len();
            if stack_len == 0 {
                return None;
            }
            
            let idx = self.stack.len() - 1;
            let elem_ref = &mut self.stack[idx];
            let data = elem_ref.data.as_mut()?;
            let count = u16::from_le_bytes([data[10], data[11]]) as usize;
            
            elem_ref.index += 1;
            
            if elem_ref.index < count {
                let elem_offset = elem_start + elem_ref.index * elem_size;
                let elem_flags = u32::from_le_bytes([
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
                
                let key = data[pos..pos + ksize].to_vec();
                let value = data[pos + ksize..pos + ksize + vsize].to_vec();
                return Some((key, value, elem_flags));
            } else {
                // Pop stack and continue
                self.stack.pop();
            }
        }
    }

    fn seek_to_first(&mut self) {
        while !self.is_leaf() {
            if let Some(pgid) = self.stack.last().and_then(|r| r.first_branch_pgid()) {
                if let Some(ref mut db) = self.db {
                    if let Some(data) = db.page_data(pgid) {
                        self.stack.push(ElemRef::new(pgid, Some(data)));
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    fn is_leaf(&self) -> bool {
        self.stack.last().map(|r| r.is_leaf()).unwrap_or(false)
    }

    /// Search a branch page for the child page containing the key
    fn search_branch(&self, data: &[u8], key: &[u8]) -> Option<Pgid> {
        let count = u16::from_le_bytes([data[10], data[11]]) as usize;
        let elem_size = 16usize;
        let elem_start = 16usize;
        
        // Binary search for the right child
        let mut low = 0usize;
        let mut high = count;
        
        while low < high {
            let mid = (low + high) / 2;
            let elem_offset = elem_start + mid * elem_size;
            
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
            
            if pos + ksize > data.len() {
                return None;
            }
            
            let elem_key = &data[pos..pos + ksize];
            if elem_key.cmp(key) != Ordering::Less {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        
        // low is the index of the first key >= search key
        // For branch pages, we want the child at index low (or count-1 if at end)
        let idx = if low >= count { count - 1 } else { low };
        let elem_offset = elem_start + idx * elem_size;
        
        // Branch elements store child page IDs in pos field
        let child_pgid = u64::from_le_bytes([
            data[elem_offset + 4],
            data[elem_offset + 5],
            data[elem_offset + 6],
            data[elem_offset + 7],
            data[elem_offset + 8],
            data[elem_offset + 9],
            data[elem_offset + 10],
            data[elem_offset + 11],
        ]);
        
        Some(child_pgid)
    }
}

impl Default for Cursor {
    fn default() -> Self {
        Self::new()
    }
}

impl Cursor {
    /// Debug: get stack length
    pub fn stack_len(&self) -> usize {
        self.stack.len()
    }
    
    /// Debug: get current index in leaf
    pub fn current_index(&self) -> Option<usize> {
        self.stack.last().map(|r| r.index)
    }
    
    /// Set inline data for cursor (for inline buckets)
    pub fn set_inline_data(&mut self, data: Vec<u8>) {
        // Clear existing stack
        self.stack.clear();
        // The data already starts from the page header (InBucket header was stripped)
        // Create a synthetic ElemRef with the inline data
        self.stack.push(ElemRef::new_from_inline(0, data));
    }

    /// Delete the current key-value pair.
    /// Requires a mutable reference to the bucket to perform the deletion.
    /// Returns the deleted key if successful.
    pub fn delete(&mut self, bucket: &mut crate::bucket::Bucket) -> Result<Option<Vec<u8>>> {
        // Get current key
        let current = self.key_value();
        if current.is_none() {
            return Ok(None);
        }
        
        let (key, _value) = current.unwrap();
        
        // Use the bucket's delete method
        bucket.delete(&key)?;
        
        // Reset cursor position after deletion
        self.clear();
        
        Ok(Some(key))
    }

    /// Get the current key without consuming it
    pub fn current_key(&self) -> Option<Vec<u8>> {
        if self.stack.is_empty() {
            return None;
        }
        
        let elem_ref = self.stack.last()?;
        let data = elem_ref.data.as_ref()?;
        
        let elem_size = 16usize;
        let elem_offset = 16 + elem_ref.index * elem_size;
        if elem_offset + 16 > data.len() {
            return None;
        }
        
        let pos = u32::from_le_bytes([data[elem_offset + 4], data[elem_offset + 5],
                                       data[elem_offset + 6], data[elem_offset + 7]]) as usize;
        let ksize = u32::from_le_bytes([data[elem_offset + 8], data[elem_offset + 9],
                                        data[elem_offset + 10], data[elem_offset + 11]]) as usize;
        
        if pos + ksize > data.len() {
            return None;
        }
        
        Some(data[pos..pos + ksize].to_vec())
    }
}

/// Reference to an element on a page
struct ElemRef {
    /// Page info
    page: Page,
    /// Page data
    data: Option<Vec<u8>>,
    /// Current index in the page
    index: usize,
}

impl ElemRef {
    /// Create from inline data (for inline buckets)
    fn new_from_inline(pgid: u64, data: Vec<u8>) -> Self {
        // Same as new but takes owned data
        let mut s = Self::new(pgid, Some(data));
        s
    }
    
    fn new(pgid: u64, data: Option<Vec<u8>>) -> Self {
        // Parse page header from data
        // Page header: id(8) + flags(2) + count(2) + overflow(4) = 16 bytes
        let (flags, count, overflow) = if let Some(ref d) = data {
            if d.len() >= 16 {
                let flags = u16::from_le_bytes([d[8], d[9]]);
                let count = u16::from_le_bytes([d[10], d[11]]);
                let overflow = u32::from_le_bytes([d[12], d[13], d[14], d[15]]);
                (flags, count, overflow)
            } else {
                (0, 0, 0)
            }
        } else {
            (0, 0, 0)
        };

        let page = Page {
            id: pgid,
            flags,
            count,
            overflow,
        };

        Self { page, data, index: 0 }
    }

    fn is_leaf(&self) -> bool {
        self.page.is_leaf()
    }

    fn count(&self) -> usize {
        self.page.count as usize
    }

    fn first_branch_pgid(&self) -> Option<u64> {
        if self.is_leaf() {
            return None;
        }
        let elems = self.page.branch_elements();
        elems.first().map(|e| e.pgid)
    }

    fn last_branch_pgid(&self) -> Option<u64> {
        if self.is_leaf() {
            return None;
        }
        let elems = self.page.branch_elements();
        elems.last().map(|e| e.pgid)
    }

    fn leaf_flags(&self) -> Option<u32> {
        let data = self.data.as_ref()?;
        let elem_offset = 16 + self.index * 16;
        if elem_offset + 16 > data.len() {
            return None;
        }
        Some(u32::from_le_bytes([data[elem_offset], data[elem_offset + 1],
                                 data[elem_offset + 2], data[elem_offset + 3]]))
    }

    fn leaf_key_value(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let data = self.data.as_ref()?;
        
        // Element header: flags(4) + pos(4) + ksize(4) + vsize(4) = 16 bytes
        let elem_offset = 16 + self.index * 16;  // 16 bytes for page header
        if elem_offset + 16 > data.len() {
            return None;
        }
        
        // Read pos (absolute offset within page, relative to element header base)
        let pos = u32::from_le_bytes([data[elem_offset + 4], data[elem_offset + 5],
                                       data[elem_offset + 6], data[elem_offset + 7]]) as usize;
        let ksize = u32::from_le_bytes([data[elem_offset + 8], data[elem_offset + 9],
                                        data[elem_offset + 10], data[elem_offset + 11]]) as usize;
        let vsize = u32::from_le_bytes([data[elem_offset + 12], data[elem_offset + 13],
                                        data[elem_offset + 14], data[elem_offset + 15]]) as usize;
        
        // Go's bbolt: Key is at page_start + pos (absolute offset from page start)
        // In our data, the page starts at index 0
        let key_start = pos;
        if key_start + ksize > data.len() {
            return None;
        }
        let key = data[key_start..key_start + ksize].to_vec();
        
        // Value follows key (sequential within the data region)
        let value_start = key_start + ksize;
        if value_start + vsize > data.len() {
            return None;
        }
        let value = data[value_start..value_start + vsize].to_vec();
        
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test cursor traversal on inline bucket data with 2 entries
    #[test]
    fn test_cursor_inline_traversal() {
        // Simulate Go's inline bucket format:
        // [InBucket header (16 bytes)][Page data]
        // Page data: page_header(16) + elem0(16) + elem1(16) + key0 + val0 + key1 + val1
        
        let key0 = b"key1";
        let val0 = b"value1";
        let key1 = b"key3";
        let val1 = b"value3";
        
        let page_size = 4096;
        let mut data = vec![0u8; page_size];
        
        // Page header at offset 0 (16 bytes)
        data[0..8].copy_from_slice(&0u64.to_le_bytes()); // id
        data[8..10].copy_from_slice(&2u16.to_le_bytes()); // flags = LEAF_PAGE_FLAG
        data[10..12].copy_from_slice(&2u16.to_le_bytes()); // count = 2
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow
        
        // Element headers start at offset 16
        let elem0_offset = 16usize;
        let elem1_offset = 32usize;
        
        // Calculate data offsets (sequential, starting after element headers)
        let data_start = elem1_offset + 16; // After elem1 header
        
        // Elem 0 header
        data[elem0_offset..elem0_offset + 4].copy_from_slice(&0u32.to_le_bytes()); // flags
        data[elem0_offset + 4..elem0_offset + 8].copy_from_slice(&(data_start as u32).to_le_bytes()); // pos
        data[elem0_offset + 8..elem0_offset + 12].copy_from_slice(&(key0.len() as u32).to_le_bytes()); // ksize
        data[elem0_offset + 12..elem0_offset + 16].copy_from_slice(&(val0.len() as u32).to_le_bytes()); // vsize
        
        // Elem 1 header - pos points to next sequential data
        let data1_start = data_start + key0.len() + val0.len();
        data[elem1_offset..elem1_offset + 4].copy_from_slice(&0u32.to_le_bytes()); // flags
        data[elem1_offset + 4..elem1_offset + 8].copy_from_slice(&(data1_start as u32).to_le_bytes()); // pos
        data[elem1_offset + 8..elem1_offset + 12].copy_from_slice(&(key1.len() as u32).to_le_bytes()); // ksize
        data[elem1_offset + 12..elem1_offset + 16].copy_from_slice(&(val1.len() as u32).to_le_bytes()); // vsize
        
        // Key-value data
        data[data_start..data_start + key0.len()].copy_from_slice(key0);
        data[data_start + key0.len()..data_start + key0.len() + val0.len()].copy_from_slice(val0);
        data[data1_start..data1_start + key1.len()].copy_from_slice(key1);
        data[data1_start + key1.len()..data1_start + key1.len() + val1.len()].copy_from_slice(val1);
        
        // Create cursor with inline data (without InBucket header)
        let mut cursor = Cursor::new();
        cursor.set_inline_data(data);
        
        // Verify count
        assert_eq!(cursor.stack_len(), 1, "Should have 1 ElemRef");
        assert_eq!(cursor.current_index(), Some(0), "Index should start at 0");
        
        // Get first entry - use key_value() since db is not set
        let first = cursor.key_value();
        assert!(first.is_some(), "key_value() should return Some");
        let (k, v) = first.unwrap();
        assert_eq!(&k, key0, "First key should be 'key1'");
        assert_eq!(&v, val0, "First value should be 'value1'");
        assert_eq!(cursor.current_index(), Some(0), "Index should still be 0 after key_value()");
        
        // Get next entry
        let second = cursor.next();
        assert!(second.is_some(), "next() should return Some for second entry");
        let (k, v) = second.unwrap();
        assert_eq!(&k, key1, "Second key should be 'key3'");
        assert_eq!(&v, val1, "Second value should be 'value3'");
        
        // Get next - should return None
        let third = cursor.next();
        assert!(third.is_none(), "next() should return None after last entry");
        
        println!("Cursor inline traversal test passed!");
    }

    /// Test cursor with data that uses pos offset (not sequential from header)
    #[test]
    fn test_cursor_uses_pos_offset() {
        // Create page data where elements use pos offset
        let key = b"testkey";
        let val = b"testval";
        
        let page_size = 4096;
        let mut data = vec![0u8; page_size];
        
        // Page header
        data[0..8].copy_from_slice(&0u64.to_le_bytes());
        data[8..10].copy_from_slice(&2u16.to_le_bytes()); // LEAF_PAGE_FLAG
        data[10..12].copy_from_slice(&1u16.to_le_bytes()); // count = 1
        data[12..16].copy_from_slice(&0u32.to_le_bytes());
        
        // Element header at offset 16
        let elem_offset = 16usize;
        let data_offset = 256usize; // Key is stored far from header
        
        data[elem_offset..elem_offset + 4].copy_from_slice(&0u32.to_le_bytes()); // flags
        data[elem_offset + 4..elem_offset + 8].copy_from_slice(&(data_offset as u32).to_le_bytes()); // pos
        data[elem_offset + 8..elem_offset + 12].copy_from_slice(&(key.len() as u32).to_le_bytes());
        data[elem_offset + 12..elem_offset + 16].copy_from_slice(&(val.len() as u32).to_le_bytes());
        
        // Key-value data at offset 256
        data[data_offset..data_offset + key.len()].copy_from_slice(key);
        data[data_offset + key.len()..data_offset + key.len() + val.len()].copy_from_slice(val);
        
        // Test cursor
        let mut cursor = Cursor::new();
        cursor.set_inline_data(data);
        
        let result = cursor.key_value();
        assert!(result.is_some(), "key_value() should return Some");
        let (k, v) = result.unwrap();
        assert_eq!(&k, key, "Key should be 'testkey'");
        assert_eq!(&v, val, "Value should be 'testval'");
        
        println!("Cursor pos offset test passed!");
    }
}