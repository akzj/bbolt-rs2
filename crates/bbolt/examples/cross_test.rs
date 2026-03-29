// Test reading Go-generated database
use bbolt_core::{Db, Options};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/gotest/cross_test.db");
    
    println!("Opening Go-generated database: {:?}", db_path);
    let db = Db::open(db_path, Options::default())?;
    
    // Read transaction
    let tx = db.begin(false)?;
    
    // Get values
    if let Some(v) = tx.get(b"key1") {
        println!("Rust Read key1: {:?}", String::from_utf8_lossy(&v));
    } else {
        println!("Rust: key1 NOT FOUND");
    }
    
    if let Some(v) = tx.get(b"key2") {
        println!("Rust Read key2: {:?}", String::from_utf8_lossy(&v));
    } else {
        println!("Rust: key2 NOT FOUND (expected - Go deleted it)");
    }
    
    if let Some(v) = tx.get(b"key3") {
        println!("Rust Read key3: {:?}", String::from_utf8_lossy(&v));
    } else {
        println!("Rust: key3 NOT FOUND");
    }
    
    // Iterate
    println!("\nAll entries:");
    let mut cursor = tx.cursor();
    let mut count = 0;
    while let Some((k, v)) = cursor.next() {
        println!("  {} = {:?}", 
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
        count += 1;
    }
    println!("Total: {} entries", count);
    
    db.close()?;
    Ok(())
}
