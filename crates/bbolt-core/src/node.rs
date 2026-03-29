//! B+tree node implementation
//!
//! Nodes are in-memory representations of pages.

use std::cmp::Ordering;

use crate::constants::*;
use crate::page::{Inode, Page, Pgid};

/// A node in the B+tree
#[derive(Debug, Clone)]
pub struct Node {
    /// Whether this is a leaf node
    pub is_leaf: bool,
    /// Whether this node needs rebalancing
    pub unbalanced: bool,
    /// Whether this node has been spilled
    pub spilled: bool,
    /// The first key in this node
    pub key: Vec<u8>,
    /// Page ID (0 for non-persisted nodes)
    pub pgid: Pgid,
    /// Parent node
    pub parent: Option<Box<Node>>,
    /// Child nodes
    pub children: Vec<Box<Node>>,
    /// Inodes (key-value pairs)
    pub inodes: Vec<Inode>,
}

impl Default for Node {
    fn default() -> Self {
        Self::new(false)
    }
}

impl Node {
    /// Create a new empty node
    pub fn new(is_leaf: bool) -> Self {
        Self {
            is_leaf,
            unbalanced: false,
            spilled: false,
            key: Vec::new(),
            pgid: 0,
            parent: None,
            children: Vec::new(),
            inodes: Vec::new(),
        }
    }

    /// Create a new leaf node
    pub fn new_leaf() -> Self {
        Self::new(true)
    }

    /// Create a new branch node
    pub fn new_branch() -> Self {
        Self::new(false)
    }

    /// Get the minimum number of keys
    pub fn min_keys(&self) -> usize {
        if self.is_leaf {
            1
        } else {
            2
        }
    }

    /// Get the size of this node when serialized to a page
    pub fn size(&self) -> usize {
        let elem_size = if self.is_leaf {
            LEAF_PAGE_ELEMENT_SIZE as usize
        } else {
            BRANCH_PAGE_ELEMENT_SIZE as usize
        };

        let mut size = PAGE_HEADER_SIZE as usize + elem_size * self.inodes.len();
        for inode in &self.inodes {
            size += inode.key.len() + inode.value.len();
        }
        size
    }

    /// Check if the node size is less than a threshold
    pub fn size_less_than(&self, threshold: usize) -> bool {
        let elem_size = if self.is_leaf {
            LEAF_PAGE_ELEMENT_SIZE as usize
        } else {
            BRANCH_PAGE_ELEMENT_SIZE as usize
        };

        let mut size = PAGE_HEADER_SIZE as usize + elem_size * self.inodes.len();
        for inode in &self.inodes {
            size += inode.key.len() + inode.value.len();
            if size >= threshold {
                return false;
            }
        }
        true
    }

    /// Get the number of children
    pub fn num_children(&self) -> usize {
        self.inodes.len()
    }

    /// Find the index of a child node
    pub fn child_index(&self, child: &Node) -> usize {
        self.inodes
            .iter()
            .position(|inode| inode.key.cmp(&child.key) != Ordering::Less)
            .unwrap_or(self.inodes.len())
    }

    /// Put a key-value pair into the node
    pub fn put(&mut self, old_key: &[u8], new_key: Vec<u8>, value: Vec<u8>, pgid: Pgid, flags: u32) {
        // Find insertion index
        let index = self
            .inodes
            .iter()
            .position(|inode| inode.key.as_slice().cmp(old_key) != Ordering::Less)
            .unwrap_or(self.inodes.len());

        // Check for exact match
        let exact = self
            .inodes
            .get(index)
            .map(|i| i.key.as_slice() == old_key)
            .unwrap_or(false);

        if !exact {
            self.inodes.insert(
                index,
                if self.is_leaf {
                    Inode::new_leaf(new_key, value, flags)
                } else {
                    Inode::new_branch(new_key, pgid)
                },
            );
        } else {
            // Update existing inode
            let inode = &mut self.inodes[index];
            inode.key = new_key;
            inode.value = value;
            if !self.is_leaf {
                inode.pgid = pgid;
            }
        }

        // Update first key if needed
        if index == 0 && !self.inodes.is_empty() {
            self.key = self.inodes[0].key.clone();
        }
    }

    /// Delete a key from the node
    pub fn del(&mut self, key: &[u8]) {
        let index = self.inodes.iter().position(|i| i.key.as_slice() == key);

        if let Some(index) = index {
            self.inodes.remove(index);
            self.unbalanced = true;

            // Update first key if needed
            if index == 0 && !self.inodes.is_empty() {
                self.key = self.inodes[0].key.clone();
            }
        }
    }

    /// Read a page into this node
    pub fn read(&mut self, page: &Page, data: &[u8]) {
        self.pgid = page.id;
        self.is_leaf = page.is_leaf();
        self.inodes.clear();

        if page.is_leaf() {
            let elems = page.leaf_elements();
            for elem in elems {
                let key = elem.key(data).to_vec();
                let value = elem.value(data).to_vec();
                self.inodes.push(Inode::new_leaf(key, value, elem.flags));
            }
        } else {
            let elems = page.branch_elements();
            for elem in elems {
                let key = elem.key(data).to_vec();
                self.inodes.push(Inode::new_branch(key, elem.pgid));
            }
        }

        // Save first key
        if !self.inodes.is_empty() {
            self.key = self.inodes[0].key.clone();
        } else {
            self.key.clear();
        }
    }

    /// Remove a child from this node
    pub fn remove_child(&mut self, target: &Node) {
        self.children.retain(|c| c.key != target.key);
    }

    /// Rebalance this node
    pub fn rebalance(&mut self) {
        self.unbalanced = false;
        
        // If node has too few keys, try to borrow from siblings
        if self.inodes.len() < self.min_keys() {
            // Request parent to help with rebalancing
            self.unbalanced = true;
        }
    }

    /// Split this node into two at the given index
    /// Returns the new right node
    pub fn split(&mut self, index: usize) -> Node {
        let mut right = Self::new(self.is_leaf);
        
        // Move inodes from index to end to the right node
        right.inodes = self.inodes.split_off(index);
        
        // Update keys
        if !right.inodes.is_empty() {
            right.key = right.inodes[0].key.clone();
        }
        
        // For branch nodes, update parent pointers
        if !self.is_leaf {
            // Children need their parent updated (caller should handle this)
        }
        
        // Mark original node as needing write
        self.unbalanced = true;
        
        right
    }

    /// Find the split index for this node
    pub fn split_index(&self, threshold: usize) -> usize {
        let elem_size = if self.is_leaf {
            LEAF_PAGE_ELEMENT_SIZE as usize
        } else {
            BRANCH_PAGE_ELEMENT_SIZE as usize
        };
        
        let mut size = PAGE_HEADER_SIZE as usize;
        let mut index = 0;
        
        for (i, inode) in self.inodes.iter().enumerate() {
            let elem_space = elem_size + inode.key.len() + inode.value.len();
            if size + elem_space > threshold && i > 0 {
                index = i;
                break;
            }
            size += elem_space;
            index = i + 1;
        }
        
        // Ensure minimum keys on each side
        index.max(1).min(self.inodes.len().saturating_sub(1))
    }

    /// Check if this node needs splitting
    pub fn needs_split(&self, threshold: usize) -> bool {
        !self.size_less_than(threshold)
    }

    /// Merge this node with a right sibling
    pub fn merge(&mut self, right: &mut Node) {
        // Move all inodes from right to self
        self.inodes.extend(right.inodes.drain(..));
        
        // Update first key
        if !self.inodes.is_empty() {
            self.key = self.inodes[0].key.clone();
        }
        
        self.unbalanced = true;
    }

    /// Borrow from left sibling
    pub fn borrow_left(&mut self, left: &mut Node) -> bool {
        if left.inodes.is_empty() || self.inodes.len() <= self.min_keys() {
            return false;
        }
        
        // Take last inode from left
        let borrowed = left.inodes.pop().unwrap();
        
        // Add to front of self
        self.inodes.insert(0, borrowed);
        
        // Update keys
        self.key = self.inodes[0].key.clone();
        if !left.inodes.is_empty() {
            left.key = left.inodes[0].key.clone();
        }
        
        self.unbalanced = true;
        left.unbalanced = true;
        true
    }

    /// Borrow from right sibling
    pub fn borrow_right(&mut self, right: &mut Node) -> bool {
        if right.inodes.is_empty() || self.inodes.len() <= self.min_keys() {
            return false;
        }
        
        // Take first inode from right
        let borrowed = right.inodes.remove(0);
        
        // Add to end of self
        self.inodes.push(borrowed);
        
        // Update keys
        self.key = self.inodes[0].key.clone();
        if !right.inodes.is_empty() {
            right.key = right.inodes[0].key.clone();
        }
        
        self.unbalanced = true;
        right.unbalanced = true;
        true
    }

    /// Get the leftmost key
    pub fn leftmost_key(&self) -> Option<&[u8]> {
        self.inodes.first().map(|i| i.key.as_slice())
    }

    /// Get the rightmost key
    pub fn rightmost_key(&self) -> Option<&[u8]> {
        self.inodes.last().map(|i| i.key.as_slice())
    }

    /// Check if node is underfull
    pub fn is_underfull(&self) -> bool {
        self.inodes.len() < self.min_keys()
    }

    /// Write this node to a page
    pub fn write(&self, page: &mut Page, data: &mut [u8]) {
        page.count = self.inodes.len() as u16;
        
        if self.is_leaf {
            self.write_leaf_elements(page, data);
        } else {
            self.write_branch_elements(page, data);
        }
    }

    fn write_leaf_elements(&self, _page: &mut Page, data: &mut [u8]) {
        let elem_size = 16; // LeafPageElement is 16 bytes
        let mut pos = PAGE_HEADER_SIZE as usize;
        
        for inode in &self.inodes {
            // Write element header (pos, ksize, flags, vsize)
            let key_start = pos + elem_size;
            let value_start = key_start + inode.key.len();
            
            data[pos..pos + 4].copy_from_slice(&(key_start as u32).to_le_bytes()); // pos
            data[pos + 4..pos + 8].copy_from_slice(&(inode.key.len() as u32).to_le_bytes()); // ksize
            data[pos + 8..pos + 12].copy_from_slice(&inode.flags.to_le_bytes()); // flags
            data[pos + 12..pos + 16].copy_from_slice(&(inode.value.len() as u32).to_le_bytes()); // vsize
            
            // Write key
            data[key_start..key_start + inode.key.len()].copy_from_slice(&inode.key);
            
            // Write value
            data[value_start..value_start + inode.value.len()].copy_from_slice(&inode.value);
            
            pos = value_start + inode.value.len();
        }
    }

    fn write_branch_elements(&self, _page: &mut Page, data: &mut [u8]) {
        let elem_size = 16; // BranchPageElement is 16 bytes
        let mut pos = PAGE_HEADER_SIZE as usize;
        
        for inode in &self.inodes {
            // Write element header (pos, ksize, pgid)
            let key_start = pos + elem_size;
            
            data[pos..pos + 4].copy_from_slice(&(key_start as u32).to_le_bytes()); // pos
            data[pos + 4..pos + 8].copy_from_slice(&(inode.key.len() as u32).to_le_bytes()); // ksize
            data[pos + 8..pos + 16].copy_from_slice(&inode.pgid.to_le_bytes()); // pgid
            
            // Write key
            data[key_start..key_start + inode.key.len()].copy_from_slice(&inode.key);
            
            pos = key_start + inode.key.len();
        }
    }

    /// Spill this node to pages
    pub fn spill(&mut self, pgid: Pgid) -> Vec<Vec<u8>> {
        self.spilled = true;
        self.pgid = pgid;
        
        // For simplicity, return one page per node
        let page_size = 4096;
        let mut data = vec![0u8; page_size];
        
        // Create page header (using raw bytes)
        data[0..8].copy_from_slice(&pgid.to_le_bytes()); // id
        let flags: u16 = if self.is_leaf { 0x02 } else { 0x01 };
        data[8..10].copy_from_slice(&flags.to_le_bytes()); // flags
        data[10..12].copy_from_slice(&(self.inodes.len() as u16).to_le_bytes()); // count
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow
        
        // Write node data using Page struct for interpretation
        let page = Page::new(pgid, flags, self.inodes.len() as u16, 0);
        let mut page_ref = page;
        self.write(&mut page_ref, &mut data);
        
        vec![data]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_put_get() {
        let mut node = Node::new_leaf();
        node.put(b"key1", b"key1".to_vec(), b"value1".to_vec(), 0, 0);

        assert_eq!(node.inodes.len(), 1);
        assert_eq!(node.inodes[0].key, b"key1");
        assert_eq!(node.inodes[0].value, b"value1");
    }

    #[test]
    fn test_node_delete() {
        let mut node = Node::new_leaf();
        node.put(b"key1", b"key1".to_vec(), b"value1".to_vec(), 0, 0);
        node.put(b"key2", b"key2".to_vec(), b"value2".to_vec(), 0, 0);

        node.del(b"key1");
        assert_eq!(node.inodes.len(), 1);
        assert_eq!(node.inodes[0].key, b"key2");
    }

    #[test]
    fn test_node_split() {
        let mut node = Node::new_leaf();
        
        // Fill node with many entries
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            node.put(key.as_bytes(), key.as_bytes().to_vec(), value.as_bytes().to_vec(), 0, 0);
        }
        
        assert_eq!(node.inodes.len(), 10);
        
        // Split at index 5
        let right = node.split(5);
        
        assert_eq!(node.inodes.len(), 5);
        assert_eq!(right.inodes.len(), 5);
        
        // Check keys are correctly distributed
        assert_eq!(node.inodes[0].key, b"key0");
        assert_eq!(node.inodes[4].key, b"key4");
        assert_eq!(right.inodes[0].key, b"key5");
        assert_eq!(right.inodes[4].key, b"key9");
    }

    #[test]
    fn test_node_split_index() {
        let mut node = Node::new_leaf();
        
        // Add entries until node needs splitting
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            node.put(key.as_bytes(), key.as_bytes().to_vec(), value.as_bytes().to_vec(), 0, 0);
        }
        
        // Find split index
        let threshold = 4096;
        let index = node.split_index(threshold);
        
        // Index should be valid
        assert!(index > 0);
        assert!(index < node.inodes.len());
    }

    #[test]
    fn test_node_borrow_left() {
        let mut left = Node::new_leaf();
        let mut right = Node::new_leaf();
        
        // Left has more entries (min_keys for leaf is 1)
        left.put(b"key1", b"key1".to_vec(), b"value1".to_vec(), 0, 0);
        left.put(b"key2", b"key2".to_vec(), b"value2".to_vec(), 0, 0);
        left.put(b"key3", b"key3".to_vec(), b"value3".to_vec(), 0, 0);
        
        right.put(b"key4", b"key4".to_vec(), b"value4".to_vec(), 0, 0);
        right.put(b"key5", b"key5".to_vec(), b"value5".to_vec(), 0, 0);
        
        // Borrow from left
        let borrowed = right.borrow_left(&mut left);
        assert!(borrowed);
        
        assert_eq!(left.inodes.len(), 2);
        assert_eq!(right.inodes.len(), 3);
    }

    #[test]
    fn test_node_borrow_right() {
        let mut left = Node::new_leaf();
        let mut right = Node::new_leaf();
        
        left.put(b"key1", b"key1".to_vec(), b"value1".to_vec(), 0, 0);
        left.put(b"key2", b"key2".to_vec(), b"value2".to_vec(), 0, 0);
        
        // Right has more entries
        right.put(b"key3", b"key3".to_vec(), b"value3".to_vec(), 0, 0);
        right.put(b"key4", b"key4".to_vec(), b"value4".to_vec(), 0, 0);
        right.put(b"key5", b"key5".to_vec(), b"value5".to_vec(), 0, 0);
        
        // Borrow from right
        let borrowed = left.borrow_right(&mut right);
        assert!(borrowed);
        
        assert_eq!(left.inodes.len(), 3);
        assert_eq!(right.inodes.len(), 2);
    }

    #[test]
    fn test_node_merge() {
        let mut left = Node::new_leaf();
        let mut right = Node::new_leaf();
        
        left.put(b"key1", b"key1".to_vec(), b"value1".to_vec(), 0, 0);
        right.put(b"key2", b"key2".to_vec(), b"value2".to_vec(), 0, 0);
        
        left.merge(&mut right);
        
        assert_eq!(left.inodes.len(), 2);
        assert_eq!(left.inodes[0].key, b"key1");
        assert_eq!(left.inodes[1].key, b"key2");
    }

    #[test]
    fn test_node_spill() {
        let mut node = Node::new_leaf();
        node.put(b"key1", b"key1".to_vec(), b"value1".to_vec(), 0, 0);
        node.put(b"key2", b"key2".to_vec(), b"value2".to_vec(), 0, 0);
        
        let pages = node.spill(1);
        
        assert!(node.spilled);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].len(), 4096);
        
        // Verify page header is written
        let pgid = u64::from_le_bytes([pages[0][0], pages[0][1], pages[0][2], pages[0][3],
                                        pages[0][4], pages[0][5], pages[0][6], pages[0][7]]);
        assert_eq!(pgid, 1);
    }
}