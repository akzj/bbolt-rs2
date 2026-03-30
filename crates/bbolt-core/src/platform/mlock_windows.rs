#[cfg(test)]
mod tests {
    #[test]
    fn test_mlock_windows_stub() {
        // On Windows, mlock is a stub that always returns Ok
        let data = vec![0u8; 4096];
        let result = mlock(&data);
        assert!(result.is_ok(), "mlock stub should return Ok on Windows");
        
        let result = munlock(&data);
        assert!(result.is_ok(), "munlock stub should return Ok on Windows");
    }
}