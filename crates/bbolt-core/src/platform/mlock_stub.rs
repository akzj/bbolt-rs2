//! Stub mlock for unsupported platforms

/// Stub mlock that always returns Ok
pub fn mlock(_data: &[u8]) -> Result<(), std::io::Error> {
    Ok(())
}

/// Stub munlock that always returns Ok
pub fn munlock(_data: &[u8]) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_mlock_stub() {
        let data = vec![0u8; 4096];
        assert!(mlock(&data).is_ok());
        assert!(munlock(&data).is_ok());
    }
}