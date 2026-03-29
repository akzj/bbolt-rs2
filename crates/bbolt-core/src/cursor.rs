//! Cursor implementation for bbolt
//!
//! Cursors provide bidirectional traversal over key-value pairs in a bucket.

use crate::constants::*;
use crate::page::Page;
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
}

impl Default for Cursor {
    fn default() -> Self {
        Self::new()
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
        if !self.is_leaf() {
            return None;
        }
        let elems = self.page.leaf_elements();
        elems.get(self.index).map(|e| e.flags)
    }

    fn leaf_key_value(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let data = self.data.as_ref()?;
        let elems = self.page.leaf_elements();
        let elem = elems.get(self.index)?;
        Some((elem.key(data).to_vec(), elem.value(data).to_vec()))
    }
}

#[cfg(test)]
mod tests {
    // TODO: Add cursor tests with a test database
}