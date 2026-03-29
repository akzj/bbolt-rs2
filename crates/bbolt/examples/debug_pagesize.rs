use bbolt_core::{Db, Page};
use std::path::Path;
use std::io::Read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/gotest/nested_test.db");
    let db = Db::open(db_path, bbolt_core::Options::default())?;
    
    // Read root page (pgid=5) directly
    let mut file = std::fs::File::open(db_path)?;
    let mut page_data = vec![0u8; 16384 * 8];
    file.read_exact(&mut page_data)?;
    
    let page_size = 16384u64;
    let root_offset = 5 * page_size;
    let root_page = &page_data[root_offset as usize..root_offset as usize + page_size as usize];
    
    println!("Root page header:");
    println!("  pgid: {}", u64::from_le_bytes([root_page[0], root_page[1], root_page[2], root_page[3],
                                               root_page[4], root_page[5], root_page[6], root_page[7]]));
    println!("  flags: 0x{:04x}", u16::from_le_bytes([root_page[8], root_page[9]]));
    println!("  count: {}", u16::from_le_bytes([root_page[10], root_page[11]]));
    let count = u16::from_le_bytes([root_page[10], root_page[11]]) as usize;
    
    if count > 0 {
        // Read elements
        for i in 0..count.min(5) {
            let elem_offset = 16 + i * 16;
            let ksize = u32::from_le_bytes([root_page[elem_offset+8], root_page[elem_offset+9],
                                            root_page[elem_offset+10], root_page[elem_offset+11]]);
            let vsize = u32::from_le_bytes([root_page[elem_offset+12], root_page[elem_offset+13],
                                            root_page[elem_offset+14], root_page[elem_offset+15]]);
            let key = String::from_utf8_lossy(&root_page[elem_offset+16..elem_offset+16+ksize as usize]);
            println!("  [{}] ksize={} vsize={} key={:?}", i, ksize, vsize, key);
        }
    }
    
    // Try Rust cursor
    let tx = db.begin(false)?;
    let mut cursor = tx.cursor();
    println!("\nRust cursor:");
    let mut count = 0;
    while let Some((k, v)) = cursor.next() {
        count += 1;
        println!("  [{}] {:?} -> {} bytes", count, k, v.len());
        if count > 10 { break; }
    }
    if count == 0 {
        println!("  (no entries)");
    }
    
    db.close()?;
    Ok(())
}
