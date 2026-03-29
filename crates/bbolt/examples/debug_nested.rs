use bbolt_core::Db;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/gotest/nested_test.db");
    let db = Db::open(db_path, bbolt_core::Options::default())?;
    let tx = db.begin(false)?;
    
    // Get 'main' bucket
    if let Some(main) = tx.bucket(b"main") {
        println!("Found 'main' bucket");
        
        // Get 'subbucket'
        if let Some(sub) = main.get_bucket(b"subbucket") {
            println!("Found 'subbucket' nested bucket");
            println!("  root_pgid: {}", sub.root_page());
            
            let mut cursor = sub.cursor();
            let mut count = 0;
            while let Some((k, v)) = cursor.next() {
                count += 1;
                println!("  [{}] {:?} = {:?}", count, String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
            }
            if count == 0 {
                println!("  (no entries in subbucket)");
            }
        }
    }
    
    db.close()?;
    Ok(())
}
