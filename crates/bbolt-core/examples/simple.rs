//! Simple example demonstrating bbolt-core usage
//!
//! Run with: cargo run --example simple --release

use bbolt_core::{Db, Error, Options};
use std::env;
use std::path::Path;

fn main() -> Result<(), Error> {
    let db_path = env::args().nth(1).unwrap_or_else(|| "demo.db".to_string());
    if Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).ok();
    }
    
    println!("Opening database: {}", db_path);
    
    let db = Db::open(Path::new(&db_path), Options::default())?;
    
    // Begin write transaction
    let mut tx = db.begin(true)?;
    
    // Put some key-value pairs
    tx.put(b"key1", b"value1")?;
    tx.put(b"key2", b"value2")?;
    tx.put(b"key3", b"value3")?;
    println!("Inserted 3 key-value pairs");
    
    // Get a value
    if let Some(value) = tx.get(b"key1") {
        println!("Got key1: {:?}", std::str::from_utf8(&value));
    }
    
    tx.commit()?;
    println!("Transaction committed");
    
    // Update db with committed data
    db.update_data(tx.committed_data());
    
    // Begin read transaction
    let tx = db.begin(false)?;
    
    // Iterate over all keys
    println!("\nAll entries:");
    let mut cursor = tx.cursor();
    let mut count = 0;
    while let Some((key, value)) = cursor.next() {
        println!("  {} = {:?}", 
            std::str::from_utf8(&key).unwrap_or("?"),
            std::str::from_utf8(&value).unwrap_or("?")
        );
        count += 1;
    }
    println!("Total entries: {}", count);
    
    drop(cursor);
    drop(tx);
    
    // Delete a key
    let mut tx = db.begin(true)?;
    tx.delete(b"key2")?;
    tx.commit()?;
    db.update_data(tx.committed_data());
    println!("\nDeleted key2");
    
    // Verify deletion
    let tx = db.begin(false)?;
    assert!(tx.get(b"key2").is_none(), "key2 should be deleted");
    println!("Verified: key2 is deleted");
    
    println!("\nDemo completed successfully!");
    db.close()?;
    println!("Database closed");
    
    Ok(())
}