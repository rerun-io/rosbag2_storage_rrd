//! Hands the Rust core's diagnostics to the host.
//!
//! Nothing here prints. This crate logs through `log` and Rerun through `tracing`; one
//! `tracing` subscriber catches both and passes each message to the callback the C++ side
//! registers, which puts it through ROS logging under the plugin's logger name.

use std::ffi::c_char;
use std::fmt::Write as _;
use std::sync::Once;

use tracing_log::NormalizeEvent as _;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};

use crate::error::guard;

/// Log levels as the C side sees them; see `RRD_LOG_*` in the header.
const TRACE: i32 = 0;
const DEBUG: i32 = 1;
const INFO: i32 = 2;
const WARN: i32 = 3;
const ERROR: i32 = 4;

/// Receives one message. `target` is the Rust module it came from. Neither string is
/// NUL-terminated, and both are only valid for the duration of the call.
pub type LogCallback = unsafe extern "C" fn(
    level: i32,
    target: *const c_char,
    target_len: usize,
    message: *const c_char,
    message_len: usize,
);

/// Routes every log message to `callback`, on this and every later call a no-op.
///
/// Messages below `info` are dropped here unless `RUST_LOG` says otherwise, so ROS log
/// levels apply to what gets through and Rerun's debug chatter stays out of the way.
///
/// # Safety
///
/// `callback` must stay valid for the life of the process; it is called from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_set_log_callback(callback: Option<LogCallback>) -> i32 {
    guard("rrd_set_log_callback", || {
        let Some(callback) = callback else {
            anyhow::bail!("null log callback");
        };

        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            // Both can only fail if another Rust component in the process installed its own
            // logging first; then our messages go where it sends them, which is not an error.
            tracing_log::LogTracer::init().ok();

            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
            let subscriber = tracing_subscriber::registry()
                .with(filter)
                .with(Forward(callback));
            tracing::subscriber::set_global_default(subscriber).ok();
        });
        Ok(())
    })
}

/// The one layer of the subscriber: formats an event and hands it over.
struct Forward(LogCallback);

impl<S: tracing::Subscriber> Layer<S> for Forward {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // A message that came in through `log` carries its real origin in `log.*` fields;
        // the event's own metadata points at the bridge.
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());

        let mut message = String::new();
        event.record(&mut Message(&mut message));

        let target = metadata.target();
        // SAFETY: the caller of `rrd_set_log_callback` promised the callback stays valid.
        unsafe {
            (self.0)(
                level(*metadata.level()),
                target.as_ptr().cast(),
                target.len(),
                message.as_ptr().cast(),
                message.len(),
            );
        }
    }
}

fn level(level: tracing::Level) -> i32 {
    match level {
        tracing::Level::TRACE => TRACE,
        tracing::Level::DEBUG => DEBUG,
        tracing::Level::INFO => INFO,
        tracing::Level::WARN => WARN,
        tracing::Level::ERROR => ERROR,
    }
}

/// Collects an event's fields into one line: the message first, then `key=value` pairs.
struct Message<'a>(&'a mut String);

impl tracing::field::Visit for Message<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let name = field.name();
        if name.starts_with("log.") {
            return;
        }
        let written = if name == "message" {
            write!(self.0, "{value:?}")
        } else {
            write!(self.0, " {name}={value:?}")
        };
        written.expect("writing to a String cannot fail");
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        } else {
            self.record_debug(field, &value);
        }
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::*;

    static SEEN: Mutex<Vec<(i32, String, String)>> = Mutex::new(Vec::new());

    unsafe extern "C" fn remember(
        level: i32,
        target: *const c_char,
        target_len: usize,
        message: *const c_char,
        message_len: usize,
    ) {
        // SAFETY: the layer passes valid UTF-8 slices that live for this call.
        let (target, message) = unsafe {
            (
                std::str::from_utf8(std::slice::from_raw_parts(target.cast(), target_len)).unwrap(),
                std::str::from_utf8(std::slice::from_raw_parts(message.cast(), message_len))
                    .unwrap(),
            )
        };
        SEEN.lock()
            .push((level, target.to_owned(), message.to_owned()));
    }

    #[test]
    fn both_log_and_tracing_reach_the_callback_with_their_own_origin() {
        // SAFETY: `remember` is a plain function and lives for the whole process.
        assert_eq!(unsafe { rrd_set_log_callback(Some(remember)) }, 0);

        log::warn!("from log {}", 1);
        tracing::error!(topic = "/x", "from tracing");
        log::debug!("dropped by the default filter");

        let seen = SEEN.lock();
        let ours = |text: &str| seen.iter().find(|(_, _, m)| m.starts_with(text)).cloned();

        let (level, target, message) = ours("from log").expect("log message forwarded");
        assert_eq!(level, WARN);
        assert_eq!(target, module_path!());
        assert_eq!(message, "from log 1");

        let (level, target, message) = ours("from tracing").expect("tracing message forwarded");
        assert_eq!(level, ERROR);
        assert_eq!(target, module_path!());
        assert_eq!(message, "from tracing topic=\"/x\"");

        assert!(ours("dropped").is_none());
    }
}
