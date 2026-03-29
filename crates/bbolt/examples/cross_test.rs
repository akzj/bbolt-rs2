use bbolt_core::{Db, Options};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("/tmp/gotest/cross_test.db");
    println!("Opening: {:?}", db_path);
    let db = Db::open(db_path, Options::default())?;
    
    let root_pgid = db.meta().root().root_pgid();
    
    if let Some(data) = db.page_data(root_pgid) {
        let count = u16::from_le_bytes([data[10], data[11]]) as usize;
        
        for i in 0..count {
            let elem_start = 16 + i * 16;
            let flags = u32::from_le_bytes([data[elem_start], data[elem_start+1], data[elem_start+2], data[elem_start+3]]);
            let ksize = u32::from_le_bytes([data[elem_start+8], data[elem_start+9], data[elem_start+10], data[elem_start+11]]);
            let vsize = u32::from_le_bytes([data[elem_start+12], data[elem_start+13], data[elem_start+14], data[elem_start+15]]);
            
            let key_off = elem_start + 16;
            let key = String::from_utf8_lossy(&data[key_off..key_off + ksize as usize]);
            println!("Bucket: {}", key);
            
            if flags & 0x01 != 0 {
                let value_off = key_off + ksize as usize;
                let value = &data[value_off..value_off + vsize as usize];
                
                let bucket_header_size = 16usize;
                let inline_count = u16::from_le_bytes([value[bucket_header_size+10], value[bucket_header_size+11]]) as usize;
                
                // Data area starts after: InBucket (16) + Page header (16) + elem headers (16 * count)
                let elem_area_size = 16 * inline_count;
                let data_start = bucket_header_size + 16 + elem_area_size; // = 32 + 16*count
                
                for j in 0..inline_count {
                    let elem_off = bucket_header_size + 16 + j * 16; // elem header offset from value start
                    let ks = u32::from_le_bytes([value[elem_off+8], value[elem_off+9], value[elem_off+10], value[elem_off+11]]);
                    let vs = u32::from_le_bytes([value[elem_off+12], value[elem_off+13], value[elem_off+14], value[elem_off+15]]);
                    
                    // Key at data_start + (cumulative size of previous elements)
                    let mut prev_size = 0u32;
                    for k in 0..j {
                        let pk_off = bucket_header_size + 16 + k * 16;
                        let pks = u32::from_le_bytes([value[pk_off+8], value[pk_off+9], value[pk_off+10], value[pk_off+11]]);
                        let pvs = u32::from_le_bytes([value[pk_off+12], value[pk_off+13], value[pk_off+14], value[pk_off+15]]);
                        prev_size += pks + pvs;
                    }
                    
                    let key_start = data_start + prev_size as usize;
                    let k = String::from_utf8_lossy(&value[key_start..key_start + ks as usize]);
                    let v = String::from_utf8_lossy(&value[key_start + ks as usize..key_start + ks as usize + vs as usize]);
                    println!("  {}={}", k, v);
                }
            }
        }
    }
    
    db.close()?;
    Ok(())
}
