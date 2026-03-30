//! Logger module for bbolt
//!
//! Provides logging functionality similar to Go bbolt's Logger interface.

use std::io::{self, Write};
use std::sync::RwLock;

/// Logger trait - similar to Go bbolt's Logger interface
pub trait Logger: Send + Sync {
    /// Log a debug message
    fn debug(&self, msg: &str);
    /// Log an info message
    fn info(&self, msg: &str);
    /// Log a warning message
    fn warning(&self, msg: &str);
    /// Log an error message
    fn error(&self, msg: &str);
}

/// DiscardLogger - discards all log messages
pub struct DiscardLogger;

impl Logger for DiscardLogger {
    fn debug(&self, _msg: &str) {}
    fn info(&self, _msg: &str) {}
    fn warning(&self, _msg: &str) {}
    fn error(&self, _msg: &str) {}
}

/// DefaultLogger - logs to stderr
pub struct DefaultLogger {
    debug_enabled: bool,
}

impl DefaultLogger {
    /// Create a new DefaultLogger
    pub fn new() -> Self {
        Self { debug_enabled: false }
    }

    /// Enable debug logging
    pub fn enable_debug(&mut self) {
        self.debug_enabled = true;
    }

    fn log(&self, level: &str, msg: &str) {
        let _ = writeln!(io::stderr(), "{}: {}", level, msg);
    }
}

impl Default for DefaultLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger for DefaultLogger {
    fn debug(&self, msg: &str) {
        if self.debug_enabled {
            self.log("DEBUG", msg);
        }
    }

    fn info(&self, msg: &str) {
        self.log("INFO", msg);
    }

    fn warning(&self, msg: &str) {
        self.log("WARNING", msg);
    }

    fn error(&self, msg: &str) {
        self.log("ERROR", msg);
    }
}

/// SimpleLogger - logs to a writer
pub struct SimpleLogger<W: Write + Send + Sync> {
    writer: RwLock<W>,
    debug_enabled: bool,
}

impl<W: Write + Send + Sync> SimpleLogger<W> {
    /// Create a new SimpleLogger with the given writer
    pub fn new(writer: W) -> Self {
        Self {
            writer: RwLock::new(writer),
            debug_enabled: false,
        }
    }

    /// Enable debug logging
    pub fn enable_debug(&mut self) {
        self.debug_enabled = true;
    }
}

impl<W: Write + Send + Sync> Logger for SimpleLogger<W> {
    fn debug(&self, msg: &str) {
        if self.debug_enabled {
            if let Ok(mut w) = self.writer.write() {
                let _ = writeln!(w, "DEBUG: {}", msg);
            }
        }
    }

    fn info(&self, msg: &str) {
        if let Ok(mut w) = self.writer.write() {
            let _ = writeln!(w, "INFO: {}", msg);
        }
    }

    fn warning(&self, msg: &str) {
        if let Ok(mut w) = self.writer.write() {
            let _ = writeln!(w, "WARNING: {}", msg);
        }
    }

    fn error(&self, msg: &str) {
        if let Ok(mut w) = self.writer.write() {
            let _ = writeln!(w, "ERROR: {}", msg);
        }
    }
}

/// Get the global discard logger
pub fn discard_logger() -> &'static dyn Logger {
    &DiscardLogger
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discard_logger() {
        let logger: &dyn Logger = &DiscardLogger;
        logger.debug("should be discarded");
        logger.info("should be discarded");
        logger.warning("should be discarded");
        logger.error("should be discarded");
    }

    #[test]
    fn test_default_logger() {
        let logger = DefaultLogger::new();
        logger.info("test info message");
        
        let mut debug_logger = DefaultLogger::new();
        debug_logger.enable_debug();
        debug_logger.debug("test debug message");
    }
}