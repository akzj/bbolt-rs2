// Test: Rust writes data, Go reads it back
use bbolt_core::{Db, Options};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/rust_write_test.db");
    
    // Remove existing file
    let _ = std::fs::remove_file(db_path);
    
    println!("Rust: Creating database...");
    let db = Db::open(db_path, Options::default())?;
    
    // Write transaction
    println!("Rust: Writing key1, key2, key3...");
    let mut tx = db.begin(true)?;
    tx.put(b"key1", b"value1_from_rust")?;
    tx.put(b"key2", b"value2_from_rust")?;
    tx.put(b"key3", b"value3_from_rust")?;
    tx.commit()?;
    db.update_data(tx.committed_data());
    
    println!("Rust: Data written successfully");
    
    db.close()?;
    println!("Rust: Database closed");
    
    Ok(())
}
