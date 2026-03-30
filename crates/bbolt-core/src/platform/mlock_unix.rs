//! Platform-specific mlock support for UNIX systems (including macOS)

use std::ptr;

/// Lock memory pages containing the given data in memory.
/// 
/// This prevents the data from being swapped to disk, providing
/// security guarantees that sensitive data won't be written to swap.
#[cfg(any(unix, target_os = "macos"))]
pub fn mlock(data: &[u8]) -> Result<(), std::io::Error> {
    if data.is_empty() {
        return Ok(());
    }
    // # Safety: mlock requires the address to be valid and the length to be correct
    unsafe {
        libc::mlock(data.as_ptr() as *const libc::c_void, data.len())
    };
    Ok(())
}

/// Unlock memory pages containing the given data.
#[cfg(any(unix, target_os = "macos"))]
pub fn munlock(data: &[u8]) -> Result<(), std::io::Error> {
    if data.is_empty() || data.as_ptr() == ptr::null() {
        return Ok(());
    }
    // # Safety: munlock requires the address to be valid and the length to be correct
    unsafe {
        libc::munlock(data.as_ptr() as *const libc::c_void, data.len())
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(any(unix, target_os = "macos"))]
    use super::*;

    #[test]
    #[cfg(any(unix, target_os = "macos"))]
    fn test_mlock_unlock_small_data() {
        let data = vec![0u8; 4096];
        assert!(mlock(&data).is_ok());
        assert!(munlock(&data).is_ok());
    }

    #[test]
    #[cfg(any(unix, target_os = "macos"))]
    fn test_munlock_empty_data() {
        let data: Vec<u8> = vec![];
        assert!(munlock(&data).is_ok());
    }
}