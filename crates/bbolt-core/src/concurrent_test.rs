//! Concurrent transaction tests for bbolt-rs2

use std::sync::Arc;
use std::thread;

use crate::db::{Db, Options};
use crate::errors::Result;

/// Test basic write-read cycle using db helpers
#[test]
fn test_basic_write_then_read() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    
    let db = Db::open(path.as_path(), Options::default())?;
    
    // Write using update helper
    db.update(|tx| {
        tx.put(b"key1", b"value1")?;
        Ok(())
    })?;
    
    // Read using view helper
    db.view(|tx| {
        let value = tx.get(b"key1");
        assert_eq!(value, Some(b"value1".to_vec()));
        Ok(())
    })?;
    
    db.close()?;
    Ok(())
}

/// Test single reader after write (in separate thread)
#[test]
fn test_single_reader_after_write() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    
    let db = Arc::new(Db::open(path.as_path(), Options::default())?);
    
    // Write
    db.update(|tx| {
        tx.put(b"test_key", b"test_value")?;
        Ok(())
    })?;
    
    // Read in separate thread
    let db_clone = db.clone();
    let result = thread::spawn(move || {
        db_clone.view(|tx| {
            let value = tx.get(b"test_key");
            assert_eq!(value, Some(b"test_value".to_vec()));
            Ok(())
        })
    }).join().unwrap()?;
    
    db.close()?;
    Ok(())
}

/// Test multiple readers accessing same data
#[test]
fn test_multiple_readers() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    
    let db = Arc::new(Db::open(path.as_path(), Options::default())?);
    
    // Write data
    db.update(|tx| {
        tx.put(b"shared_key", b"shared_value")?;
        Ok(())
    })?;
    
    // Multiple readers access same data
    let mut handles = vec![];
    for i in 0..4 {
        let db = db.clone();
        let handle = thread::spawn(move || {
            db.view(|tx| {
                let value = tx.get(b"shared_key");
                assert_eq!(value, Some(b"shared_value".to_vec()));
                Ok(())
            }).unwrap();
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    db.close()?;
    Ok(())
}

/// Test writer and reader concurrent access
#[test]
fn test_concurrent_read_write() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    
    let db = Arc::new(Db::open(path.as_path(), Options::default())?);
    
    // Write initial data
    db.update(|tx| {
        tx.put(b"counter", b"0")?;
        Ok(())
    })?;
    
    // Spawn writer thread
    let db_writer = db.clone();
    let writer = thread::spawn(move || {
        for i in 1..=10 {
            db_writer.update(|tx| {
                tx.put(b"counter", format!("{}", i).as_bytes())?;
                Ok(())
            }).unwrap();
        }
    });
    
    // Spawn reader threads
    let mut readers = vec![];
    for _ in 0..3 {
        let db = db.clone();
        let reader = thread::spawn(move || {
            for _ in 0..5 {
                db.view(|tx| {
                    let _value = tx.get(b"counter");
                    // Value should be a valid counter
                    Ok(())
                }).unwrap();
            }
        });
        readers.push(reader);
    }
    
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }
    
    db.close()?;
    Ok(())
}