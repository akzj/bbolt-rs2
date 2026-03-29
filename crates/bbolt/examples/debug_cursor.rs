use bbolt_core::Db;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/gotest/nested_test.db");
    let db = Db::open(db_path, bbolt_core::Options::default())?;
    let tx = db.begin(false)?;
    
    // Create fresh cursor
    let mut cursor = bbolt_core::Cursor::new();
    println!("Empty cursor stack_len: {}", cursor.stack_len());
    
    // Set root manually to page 5
    let db_handle = tx.get_db();
    cursor.set_root(5, db_handle);
    println!("After set_root(5): stack_len = {}", cursor.stack_len());
    
    // Try first
    if let Some((k, v)) = cursor.first() {
        println!("first() returned: {:?} -> {} bytes", k, v.len());
    } else {
        println!("first() returned None");
    }
    
    // Try next directly (without first)
    if let Some((k, v)) = cursor.next() {
        println!("next() returned: {:?} -> {} bytes", k, v.len());
    } else {
        println!("next() returned None");
    }
    
    db.close()?;
    Ok(())
}
