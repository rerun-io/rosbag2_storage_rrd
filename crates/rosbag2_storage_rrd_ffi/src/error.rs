//! Error reporting across the FFI boundary.
//!
//! Entry points return a plain `0`/`-1`, with the reason left in a thread-local slot for
//! the caller to pick up. rosbag2 drives one storage plugin from one thread, so a
//! thread-local keeps the reporting lock-free without losing messages.

use std::cell::RefCell;
use std::ffi::c_char;

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Records the reason the current entry point is about to fail.
pub(crate) fn set_last_error(message: impl Into<String>) {
    let message = message.into();
    log::error!("{message}");
    LAST_ERROR.with_borrow_mut(|slot| *slot = message);
}

/// Runs `f`, turning both errors and panics into `-1` plus a recorded reason.
///
/// A panic must never unwind into C++, so it is caught here and reported like any other
/// failure. `AssertUnwindSafe` is sound because a caught panic always fails the call: the
/// caller never observes the state the panic left behind.
pub(crate) fn guard(what: &str, f: impl FnOnce() -> anyhow::Result<()>) -> i32 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => {
            set_last_error(format!("{what} failed: {err:#}"));
            -1
        }
        Err(panic) => {
            let reason = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_owned());
            set_last_error(format!("{what} panicked: {reason}"));
            -1
        }
    }
}

/// Copies the last recorded error into `buf` as a NUL-terminated string, and clears it.
///
/// Clearing on read is what stops a stale message from being reported against a later,
/// unrelated failure.
///
/// # Safety
///
/// `buf` must point to at least `buf_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_last_error(buf: *mut c_char, buf_len: usize) -> usize {
    if buf.is_null() || buf_len == 0 {
        return 0;
    }

    let message = LAST_ERROR.with_borrow_mut(std::mem::take);
    let bytes = message.as_bytes();
    let len = bytes.len().min(buf_len - 1);

    // SAFETY: the caller guarantees `buf_len` writable bytes, and `len < buf_len`.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), buf, len);
        *buf.add(len) = 0;
    }

    len
}
