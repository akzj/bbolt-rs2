//! Platform-specific code for bbolt

// UNIX-like systems (Linux, macOS, etc.)
#[cfg(any(unix, target_os = "macos"))]
pub mod mlock_unix;

#[cfg(any(unix, target_os = "macos"))]
pub use self::mlock_unix::{mlock, munlock};

// Windows
#[cfg(windows)]
pub mod mlock_windows;

#[cfg(windows)]
pub use self::mlock_windows::{mlock, munlock};

// Other platforms (stub)
#[cfg(all(not(unix), not(target_os = "macos"), not(windows)))]
pub mod mlock_stub;

#[cfg(all(not(unix), not(target_os = "macos"), not(windows)))]
pub use self::mlock_stub::{mlock, munlock};