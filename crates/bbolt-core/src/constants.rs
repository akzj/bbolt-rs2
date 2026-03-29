//! Constants and magic numbers for bbolt file format

/// Magic number identifying a bbolt database file
pub const MAGIC: u32 = 0xED0CDAED;

/// Database format version
pub const VERSION: u32 = 2;

/// Page header size in bytes
pub const PAGE_HEADER_SIZE: u64 = 16;

/// Size of a branch page element in bytes
pub const BRANCH_PAGE_ELEMENT_SIZE: u64 = 16;

/// Size of a leaf page element in bytes
pub const LEAF_PAGE_ELEMENT_SIZE: u64 = 16;

/// Minimum keys per page
pub const MIN_KEYS_PER_PAGE: u16 = 2;

/// Default fill percent for nodes
pub const DEFAULT_FILL_PERCENT: f64 = 0.5;

/// Minimum fill percent
pub const MIN_FILL_PERCENT: f64 = 0.1;

/// Maximum fill percent
pub const MAX_FILL_PERCENT: f64 = 1.0;

/// Maximum key size in bytes
pub const MAX_KEY_SIZE: usize = 32768;

/// Maximum value size in bytes
pub const MAX_VALUE_SIZE: usize = (1 << 31) - 2;

/// Special pgid indicating no freelist
pub const PGID_NO_FREELIST: u64 = 0xFFFFFFFFFFFFFFFF;

/// Default page size (will be detected from OS)
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Maximum mmap size
pub const MAX_MAP_SIZE: usize = 1 << 30; // 1GB

/// Maximum mmap step size
pub const MAX_MMAP_STEP: usize = 1 << 30; // 1GB

/// Default allocation size for growing database
pub const DEFAULT_ALLOC_SIZE: usize = 16 * 1024 * 1024; // 16MB

// Page flags
bitflags::bitflags! {
    /// Page type flags
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        const BRANCH_PAGE_FLAG = 0x01;
        const LEAF_PAGE_FLAG = 0x02;
        const META_PAGE_FLAG = 0x04;
        const FREELIST_PAGE_FLAG = 0x10;
    }
}

// Leaf page element flags
bitflags::bitflags! {
    /// Leaf page element flags
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct LeafFlags: u32 {
        const BUCKET_LEAF_FLAG = 0x01;
    }
}