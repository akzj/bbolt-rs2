//! Page structures for bbolt file format
//!
//! This module defines the low-level page structures that make up the bbolt
//! database file format. These structures are designed to be binary compatible
//! with the Go implementation.

use crate::constants::*;
use crate::errors::{Error, Result};

/// Page ID type
pub type Pgid = u64;

/// Transaction ID type
pub type Txid = u64;

/// Branch page element (used in branch pages)
///
/// Each element contains:
/// - pos: u32 (offset of key data from page start)
/// - ksize: u32 (key size)
/// - pgid: u64 (child page ID)
#[derive(Debug, Clone, Copy)]
pub struct BranchPageElement {
    /// Offset of the key from page start
    pub pos: u32,
    /// Size of the key
    pub ksize: u32,
    /// Child page ID
    pub pgid: Pgid,
}

impl BranchPageElement {
    /// Get the key slice from page data
    pub fn key<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        let start = self.pos as usize;
        &data[start..start + self.ksize as usize]
    }
}

/// Leaf page element (used in leaf pages)
///
/// Each element contains:
/// - flags: u32 (bucket leaf flag)
/// - pos: u32 (offset of key data from page start)
/// - ksize: u32 (key size)
/// - vsize: u32 (value size)
#[derive(Debug, Clone, Copy)]
pub struct LeafPageElement {
    /// Flags (BucketLeafFlag = 0x01)
    pub flags: u32,
    /// Offset of the key from page start
    pub pos: u32,
    /// Size of the key
    pub ksize: u32,
    /// Size of the value
    pub vsize: u32,
}

impl LeafPageElement {
    /// Check if this is a bucket entry
    pub fn is_bucket(&self) -> bool {
        self.flags & LeafFlags::BUCKET_LEAF_FLAG.bits() != 0
    }

    /// Get the key slice from page data
    pub fn key<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        let start = self.pos as usize;
        &data[start..start + self.ksize as usize]
    }

    /// Get the value slice from page data
    pub fn value<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        let start = (self.pos + self.ksize) as usize;
        &data[start..start + self.vsize as usize]
    }
}

/// Meta page structure (stored at the beginning of each meta page)
/// Must use #[repr(C)] for binary compatibility with Go
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Meta {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub flags: u32,
    pub root: InBucket,
    pub freelist: Pgid,
    pub pgid: Pgid,
    pub txid: Txid,
    pub checksum: u64,
}

impl Meta {
    /// Magic number
    pub const MAGIC: u32 = 0xED0CDAED;
    /// Version number
    pub const VERSION: u32 = 2;

    /// Create a new meta page
    pub fn new(page_size: u32, freelist: Pgid, root: InBucket, pgid: Pgid, txid: Txid) -> Self {
        let mut meta = Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            page_size,
            flags: 0,
            root,
            freelist,
            pgid,
            txid,
            checksum: 0,
        };
        meta.checksum = meta.sum64();
        meta
    }

    /// Validate the meta page
    pub fn validate(&self) -> Result<()> {
        if self.magic != Self::MAGIC {
            return Err(Error::Invalid);
        }
        if self.version != Self::VERSION {
            return Err(Error::VersionMismatch);
        }
        if self.checksum != self.sum64() {
            return Err(Error::Checksum);
        }
        Ok(())
    }

    /// Calculate the FNV-1a checksum
    pub fn sum64(&self) -> u64 {
        let ptr = self as *const Meta as *const u8;
        // hash everything except the checksum field (last 8 bytes)
        // With #[repr(C)]: magic(4) + version(4) + page_size(4) + flags(4) + 
        //                  root(16) + freelist(8) + pgid(8) + txid(8) = 56 bytes before checksum
        // But we need to hash up to txid, which ends at byte 47 (0-indexed)
        // So we hash bytes 0-55 = 56 bytes
        let data = unsafe { std::slice::from_raw_parts(ptr, 56) };
        fnv1a(data)
    }

    /// Get the root bucket
    pub fn root(&self) -> &InBucket {
        &self.root
    }

    /// Get the freelist page ID
    pub fn freelist_pgid(&self) -> Pgid {
        self.freelist
    }

    /// Check if freelist is persisted
    pub fn is_freelist_persisted(&self) -> bool {
        self.freelist != PGID_NO_FREELIST
    }

    /// Get the high water mark
    pub fn pgid(&self) -> Pgid {
        self.pgid
    }

    /// Get the transaction ID
    pub fn txid(&self) -> Txid {
        self.txid
    }

    /// Get the page size
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Increment transaction ID
    pub fn inc_txid(&mut self) {
        self.txid += 1;
    }
}

/// InBucket represents the root bucket information stored in meta page
/// Must use #[repr(C)] for binary compatibility with Go
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct InBucket {
    /// Root page ID
    pub root: Pgid,
    /// Sequence number
    pub sequence: u64,
}

impl InBucket {
    /// Create a new InBucket
    pub fn new(root: Pgid, sequence: u64) -> Self {
        Self { root, sequence }
    }

    /// Get the root page ID
    pub fn root_pgid(&self) -> Pgid {
        self.root
    }

    /// Set the root page ID
    pub fn set_root(&mut self, root: Pgid) {
        self.root = root;
    }

    /// Get the sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Set the sequence number
    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }

    /// Increment sequence number
    pub fn inc_sequence(&mut self) {
        self.sequence += 1;
    }
}

/// Inode represents an element in a node
#[derive(Debug, Clone)]
pub struct Inode {
    /// Flags (bucket flag for leaf nodes)
    pub flags: u32,
    /// Page ID (for branch nodes)
    pub pgid: Pgid,
    /// Key bytes
    pub key: Vec<u8>,
    /// Value bytes
    pub value: Vec<u8>,
}

impl Inode {
    /// Create a new inode with key/value for leaf
    pub fn new_leaf(key: Vec<u8>, value: Vec<u8>, flags: u32) -> Self {
        Self {
            flags,
            pgid: 0,
            key,
            value,
        }
    }

    /// Create a new inode for branch node
    pub fn new_branch(key: Vec<u8>, pgid: Pgid) -> Self {
        Self {
            flags: 0,
            pgid,
            key,
            value: Vec::new(),
        }
    }

    /// Check if this is a bucket entry
    pub fn is_bucket(&self) -> bool {
        self.flags & LeafFlags::BUCKET_LEAF_FLAG.bits() != 0
    }
}

/// Page provides read access to a page in the database
/// Must use #[repr(C)] for binary compatibility with Go
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Page {
    /// Page ID
    pub id: Pgid,
    /// Page type flags
    pub flags: u16,
    /// Number of elements on the page
    pub count: u16,
    /// Number of overflow pages
    pub overflow: u32,
}

impl Page {
    /// Create a new page
    pub fn new(id: Pgid, flags: u16, count: u16, overflow: u32) -> Self {
        Self { id, flags, count, overflow }
    }

    /// Check if this is a branch page
    pub fn is_branch(&self) -> bool {
        self.flags == PageFlags::BRANCH_PAGE_FLAG.bits()
    }

    /// Check if this is a leaf page
    pub fn is_leaf(&self) -> bool {
        self.flags == PageFlags::LEAF_PAGE_FLAG.bits()
    }

    /// Check if this is a meta page
    pub fn is_meta(&self) -> bool {
        self.flags == PageFlags::META_PAGE_FLAG.bits()
    }

    /// Check if this is a freelist page
    pub fn is_freelist(&self) -> bool {
        self.flags == PageFlags::FREELIST_PAGE_FLAG.bits()
    }

    /// Get the page type as a string
    pub fn typ(&self) -> &'static str {
        if self.is_branch() {
            "branch"
        } else if self.is_leaf() {
            "leaf"
        } else if self.is_meta() {
            "meta"
        } else if self.is_freelist() {
            "freelist"
        } else {
            "unknown"
        }
    }

    /// Get all branch elements from page data
    pub fn branch_elements(&self) -> &[BranchPageElement] {
        if self.count == 0 {
            return &[];
        }
        let ptr = std::ptr::addr_of!(self) as *const u8;
        let elem_ptr = unsafe { ptr.add(PAGE_HEADER_SIZE as usize) as *const BranchPageElement };
        unsafe { std::slice::from_raw_parts(elem_ptr, self.count as usize) }
    }

    /// Get all leaf elements from page data
    pub fn leaf_elements(&self) -> &[LeafPageElement] {
        if self.count == 0 {
            return &[];
        }
        let ptr = std::ptr::addr_of!(self) as *const u8;
        let elem_ptr = unsafe { ptr.add(PAGE_HEADER_SIZE as usize) as *const LeafPageElement };
        unsafe { std::slice::from_raw_parts(elem_ptr, self.count as usize) }
    }

    /// Get the meta data (only valid for meta pages)
    pub fn meta(&self) -> &Meta {
        let ptr = std::ptr::addr_of!(*self) as *const u8;
        let meta_ptr = unsafe { ptr.add(PAGE_HEADER_SIZE as usize) as *const Meta };
        unsafe { &*meta_ptr }
    }

    /// Get freelist page IDs from a freelist page
    pub fn freelist_ids(&self) -> &[Pgid] {
        if self.count == 0 {
            return &[];
        }
        let ptr = std::ptr::addr_of!(*self) as *const u8;
        let ids_ptr = unsafe { ptr.add(PAGE_HEADER_SIZE as usize) as *const Pgid };
        unsafe { std::slice::from_raw_parts(ids_ptr, self.count as usize) }
    }

    /// Get page info for debugging
    pub fn info(&self) -> PageInfo {
        PageInfo {
            id: self.id,
            typ: self.typ().to_string(),
            count: self.count,
            overflow: self.overflow,
        }
    }
}

/// Page info for debugging/inspection
#[derive(Debug)]
pub struct PageInfo {
    pub id: u64,
    pub typ: String,
    pub count: u16,
    pub overflow: u32,
}

/// Calculate FNV-1a hash
pub fn fnv1a(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_checksum() {
        let meta = Meta::new(4096, 2, InBucket::new(3, 0), 4, 0);
        let checksum = meta.sum64();
        // Checksum should be non-zero
        assert_ne!(checksum, 0);
    }

    #[test]
    fn test_meta_validation() {
        let meta = Meta::new(4096, 2, InBucket::new(3, 0), 4, 0);
        assert!(meta.validate().is_ok());

        // Corrupt magic
        let mut corrupted = meta;
        corrupted.magic = 0;
        assert!(corrupted.validate().is_err());

        // Corrupt checksum
        let mut corrupted = meta;
        corrupted.checksum = 0;
        assert!(corrupted.validate().is_err());
    }

    #[test]
    fn test_fnv1a() {
        // Test known FNV-1a values
        assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
    }

    #[test]
    fn test_meta_serialize_roundtrip() {
        // This tests write_meta -> read_meta round-trip
        let page_size = 4096;
        let mut data = vec![0u8; page_size];
        
        // Create meta and serialize like db.rs write_meta does
        let original = Meta::new(page_size as u32, 2, InBucket::new(3, 0), 4, 0);
        
        // Write page header
        data[0..8].copy_from_slice(&0u64.to_le_bytes()); // id
        data[8..10].copy_from_slice(&PageFlags::META_PAGE_FLAG.bits().to_le_bytes()); // flags
        data[10..12].copy_from_slice(&0u16.to_le_bytes()); // count
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // overflow
        
        // Write meta data (starts at offset 16)
        let meta_offset = 16;
        data[meta_offset..meta_offset + 4].copy_from_slice(&original.magic.to_le_bytes());
        data[meta_offset + 4..meta_offset + 8].copy_from_slice(&original.version.to_le_bytes());
        data[meta_offset + 8..meta_offset + 12].copy_from_slice(&original.page_size.to_le_bytes());
        data[meta_offset + 12..meta_offset + 16].copy_from_slice(&original.flags.to_le_bytes());
        data[meta_offset + 16..meta_offset + 24].copy_from_slice(&original.root.root.to_le_bytes());
        data[meta_offset + 24..meta_offset + 32].copy_from_slice(&original.root.sequence.to_le_bytes());
        data[meta_offset + 32..meta_offset + 40].copy_from_slice(&original.freelist.to_le_bytes());
        data[meta_offset + 40..meta_offset + 48].copy_from_slice(&original.pgid.to_le_bytes());
        data[meta_offset + 48..meta_offset + 56].copy_from_slice(&original.txid.to_le_bytes());
        data[meta_offset + 56..meta_offset + 64].copy_from_slice(&original.checksum.to_le_bytes());
        
        // Now read back like db.rs read_meta does
        let page = unsafe { &*(data.as_ptr() as *const Page) };
        let read_meta = page.meta();
        
        // Validate the read meta
        assert!(read_meta.validate().is_ok(), "Round-trip validation failed");
        
        // Check individual fields
        assert_eq!(read_meta.magic, original.magic, "magic mismatch");
        assert_eq!(read_meta.version, original.version, "version mismatch");
        assert_eq!(read_meta.page_size, original.page_size, "page_size mismatch");
        assert_eq!(read_meta.freelist, original.freelist, "freelist mismatch");
        assert_eq!(read_meta.txid, original.txid, "txid mismatch");
    }
}


#[cfg(test)]
#[test]
fn test_struct_sizes() {
    use std::mem::size_of;
    assert_eq!(size_of::<InBucket>(), 16, "InBucket should be 16 bytes");
    assert_eq!(size_of::<Meta>(), 64, "Meta should be 64 bytes");
    assert_eq!(size_of::<Page>(), 16, "Page should be 16 bytes");
}
