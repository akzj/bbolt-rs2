use bbolt_core::{Db, Options};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/gotest/nested_test.db");
    
    println!("Opening Go-generated nested bucket database");
    let db = Db::open(db_path, Options::default())?;
    
    let tx = db.begin(false)?;
    
    // Get 'main' bucket
    if let Some(main) = tx.bucket(b"main") {
        println!("Found 'main' bucket");
        
        let mut cursor = main.cursor();
        while let Some((k, v)) = cursor.next() {
            println!("  {} -> {} bytes", 
                String::from_utf8_lossy(&k),
                v.len()
            );
        }
        
        // Try to get 'subbucket' nested bucket
        if let Some(sub) = main.get_bucket(b"subbucket") {
            println!("\nFound 'subbucket' nested bucket");
            
            let mut cursor = sub.cursor();
            while let Some((k, v)) = cursor.next() {
                println!("  {} = {:?}", 
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
        } else {
            println!("\n'subbucket' not found");
        }
    } else {
        println!("'main' bucket not found");
    }
    
    db.close()?;
    Ok(())
}
