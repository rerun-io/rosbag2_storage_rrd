//! Rust core of the `rrd` rosbag2 storage plugin.
//!
//! The C++ side is a thin `pluginlib` shim; everything that touches CDR, Arrow or Rerun
//! happens here. Messages are reflection-decoded from the `.msg` definition text that
//! rosbag2 hands us at `create_topic`, so no ROS typesupport is needed at any point.
//!
//! Every entry point returns `0` on success and `-1` on failure, leaving a
//! human-readable reason for [`rrd_last_error`]. Panics are caught at the boundary rather
//! than unwinding into C++.

mod config;
mod error;
mod index;
mod logging;
mod pipeline;
mod reader;
mod record;
mod replay;
mod statics;
#[cfg(test)]
mod testing;
mod writer;

use std::ffi::c_char;

use anyhow::Context as _;

use crate::config::RecordingConfig;
use crate::error::{guard, set_last_error};
use crate::reader::Reader;
use crate::writer::Writer;

pub use crate::error::rrd_last_error;
pub use crate::logging::rrd_set_log_callback;

/// Borrows a `(pointer, length)` pair as a byte slice.
///
/// # Safety
///
/// `ptr` must point to `len` readable bytes, or be null when `len` is zero.
unsafe fn bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        return &[];
    }
    // SAFETY: guaranteed by the caller.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// Borrows a `(pointer, length)` pair as a string.
///
/// # Safety
///
/// See `bytes`.
unsafe fn text<'a>(ptr: *const c_char, len: usize) -> anyhow::Result<&'a str> {
    // SAFETY: guaranteed by the caller.
    let bytes = unsafe { bytes(ptr.cast::<u8>(), len) };
    Ok(std::str::from_utf8(bytes)?)
}

/// Borrows a writer handle.
///
/// # Safety
///
/// `handle` must be a pointer returned by [`rrd_writer_open`] and not yet closed.
unsafe fn writer<'a>(handle: *mut Writer) -> anyhow::Result<&'a mut Writer> {
    // SAFETY: guaranteed by the caller; the null case is rejected first.
    unsafe { handle.as_mut() }.context_null()
}

/// Turns a null handle into an error rather than undefined behaviour.
trait ContextNull<T> {
    fn context_null(self) -> anyhow::Result<T>;
}

impl<T> ContextNull<T> for Option<T> {
    fn context_null(self) -> anyhow::Result<T> {
        self.ok_or_else(|| anyhow::anyhow!("null writer handle"))
    }
}

/// Opens `path` for writing and returns a handle through `out_writer`.
///
/// `profile` is a `--storage-preset-profile` name; an empty string selects the default.
/// `config_yaml` is the text of the `--storage-config-file`, or empty; see
/// `RecordingConfig::resolve`.
///
/// # Safety
///
/// The string arguments must satisfy `text`, and `out_writer` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_writer_open(
    path: *const c_char,
    path_len: usize,
    profile: *const c_char,
    profile_len: usize,
    config_yaml: *const c_char,
    config_yaml_len: usize,
    out_writer: *mut *mut Writer,
) -> i32 {
    guard("rrd_writer_open", || {
        if out_writer.is_null() {
            anyhow::bail!("null out_writer");
        }

        // SAFETY: guaranteed by the caller.
        let (path, profile, config_yaml) = unsafe {
            (
                text(path, path_len)?,
                text(profile, profile_len)?,
                text(config_yaml, config_yaml_len)?,
            )
        };

        let config = RecordingConfig::resolve(profile, config_yaml)?;
        let writer = Box::new(Writer::open(path, config)?);
        // SAFETY: checked non-null above.
        unsafe { *out_writer = Box::into_raw(writer) };

        Ok(())
    })
}

/// Declares a topic and its message definition.
///
/// # Safety
///
/// See [`rrd_writer_open`] and `writer`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_writer_create_topic(
    handle: *mut Writer,
    topic: *const c_char,
    topic_len: usize,
    type_name: *const c_char,
    type_name_len: usize,
    schema_encoding: *const c_char,
    schema_encoding_len: usize,
    schema_text: *const u8,
    schema_text_len: usize,
    qos_profiles: *const c_char,
    qos_profiles_len: usize,
    type_hash: *const c_char,
    type_hash_len: usize,
    out_topic: *mut usize,
) -> i32 {
    guard("rrd_writer_create_topic", || {
        if out_topic.is_null() {
            anyhow::bail!("null out_topic");
        }

        // SAFETY: guaranteed by the caller.
        unsafe {
            let writer = writer(handle)?;
            let id = writer.create_topic(
                text(topic, topic_len)?,
                text(type_name, type_name_len)?,
                text(schema_encoding, schema_encoding_len)?,
                bytes(schema_text, schema_text_len),
                text(qos_profiles, qos_profiles_len)?,
                text(type_hash, type_hash_len)?,
            );
            *out_topic = id;
        }
        Ok(())
    })
}

/// Records one serialized message.
///
/// # Safety
///
/// See [`rrd_writer_open`] and `writer`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_writer_write(
    handle: *mut Writer,
    topic: usize,
    cdr: *const u8,
    cdr_len: usize,
    recv_timestamp_ns: i64,
    send_timestamp_ns: i64,
) -> i32 {
    guard("rrd_writer_write", || {
        // SAFETY: guaranteed by the caller.
        unsafe {
            writer(handle)?.write(
                topic,
                bytes(cdr, cdr_len),
                recv_timestamp_ns,
                send_timestamp_ns,
            )
        }
    })
}

/// Flushes and forgets a topic.
///
/// # Safety
///
/// See [`rrd_writer_open`] and `writer`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_writer_remove_topic(
    handle: *mut Writer,
    topic: *const c_char,
    topic_len: usize,
) -> i32 {
    guard("rrd_writer_remove_topic", || {
        // SAFETY: guaranteed by the caller.
        unsafe { writer(handle)?.remove_topic(text(topic, topic_len)?) }
    })
}

/// Stores rosbag2's bag metadata, to be written when the bag is closed.
///
/// # Safety
///
/// See [`rrd_writer_open`] and `writer`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_writer_set_metadata(
    handle: *mut Writer,
    yaml: *const c_char,
    yaml_len: usize,
) -> i32 {
    guard("rrd_writer_set_metadata", || {
        // SAFETY: guaranteed by the caller.
        unsafe {
            let writer = writer(handle)?;
            writer.set_metadata(text(yaml, yaml_len)?);
        }
        Ok(())
    })
}

/// Closes the bag and frees the handle, which must not be used again.
///
/// # Safety
///
/// See [`rrd_writer_open`] and `writer`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_writer_close(handle: *mut Writer) -> i32 {
    if handle.is_null() {
        set_last_error("rrd_writer_close called with a null writer handle");
        return -1;
    }

    guard("rrd_writer_close", || {
        // SAFETY: the caller guarantees a live handle from `rrd_writer_open`, and it is
        // consumed here.
        unsafe { Box::from_raw(handle) }.close()
    })
}

// --- Reading -----------------------------------------------------------------

/// A borrowed string owned by the reader.
///
/// Valid until the call that would replace it: topic strings live as long as the reader,
/// a message's strings only until the next [`rrd_reader_next`].
#[repr(C)]
pub struct RrdString {
    pub ptr: *const c_char,
    pub len: usize,
}

impl RrdString {
    fn borrow(s: &str) -> Self {
        Self {
            ptr: s.as_ptr().cast::<c_char>(),
            len: s.len(),
        }
    }

    const EMPTY: Self = Self {
        ptr: std::ptr::null(),
        len: 0,
    };
}

/// A topic as it was recorded.
#[repr(C)]
pub struct RrdTopic {
    pub name: RrdString,
    pub type_name: RrdString,
    pub schema_encoding: RrdString,
    pub schema_text: *const u8,
    pub schema_text_len: usize,
    pub qos_profiles: RrdString,
    pub type_hash: RrdString,
    pub message_count: usize,
}

/// One message being replayed.
#[repr(C)]
pub struct RrdMessage {
    pub topic: RrdString,
    pub data: *const u8,
    pub data_len: usize,
    pub recv_timestamp_ns: i64,
    pub send_timestamp_ns: i64,
}

/// Borrows a reader handle.
///
/// # Safety
///
/// `handle` must come from [`rrd_reader_open`] and not yet be closed.
unsafe fn reader<'a>(handle: *mut Reader) -> anyhow::Result<&'a mut Reader> {
    // SAFETY: guaranteed by the caller; the null case is rejected first.
    unsafe { handle.as_mut() }.ok_or_else(|| anyhow::anyhow!("null reader handle"))
}

/// Opens `path` for reading and indexes it.
///
/// # Safety
///
/// See `text`; `out_reader` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_open(
    path: *const c_char,
    path_len: usize,
    out_reader: *mut *mut Reader,
) -> i32 {
    guard("rrd_reader_open", || {
        if out_reader.is_null() {
            anyhow::bail!("null out_reader");
        }

        // SAFETY: guaranteed by the caller.
        let path = unsafe { text(path, path_len)? };
        let reader = Box::new(Reader::open(path)?);
        // SAFETY: checked non-null above.
        unsafe { *out_reader = Box::into_raw(reader) };

        Ok(())
    })
}

/// The number of topics in the recording.
///
/// # Safety
///
/// See `reader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_topic_count(handle: *mut Reader) -> usize {
    // SAFETY: guaranteed by the caller.
    unsafe { reader(handle) }.map_or(0, |reader| reader.topics().len())
}

/// Describes the topic at `index`, whose strings live as long as the reader.
///
/// # Safety
///
/// See `reader`; `out_topic` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_topic(
    handle: *mut Reader,
    index: usize,
    out_topic: *mut RrdTopic,
) -> i32 {
    guard("rrd_reader_topic", || {
        if out_topic.is_null() {
            anyhow::bail!("null out_topic");
        }

        // SAFETY: guaranteed by the caller.
        let reader = unsafe { reader(handle)? };
        let topic = reader
            .topics()
            .get(index)
            .with_context(|| format!("no topic at index {index}"))?;

        // SAFETY: checked non-null above.
        unsafe {
            *out_topic = RrdTopic {
                name: RrdString::borrow(&topic.name),
                type_name: RrdString::borrow(&topic.type_name),
                schema_encoding: RrdString::borrow(&topic.schema_encoding),
                schema_text: topic.schema_text.as_ptr(),
                schema_text_len: topic.schema_text.len(),
                qos_profiles: RrdString::borrow(&topic.qos_profiles),
                type_hash: RrdString::borrow(&topic.type_hash),
                message_count: topic.message_count,
            };
        }

        Ok(())
    })
}

/// The stored rosbag2 metadata document, or an empty string if the bag has none.
///
/// # Safety
///
/// See `reader`; `out_yaml` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_metadata(handle: *mut Reader, out_yaml: *mut RrdString) -> i32 {
    guard("rrd_reader_metadata", || {
        if out_yaml.is_null() {
            anyhow::bail!("null out_yaml");
        }

        // SAFETY: guaranteed by the caller.
        let reader = unsafe { reader(handle)? };
        // SAFETY: checked non-null above.
        unsafe {
            *out_yaml = reader
                .metadata_yaml()
                .map_or(RrdString::EMPTY, RrdString::borrow);
        }

        Ok(())
    })
}

/// The Rerun recording id the file was written under.
///
/// # Safety
///
/// See `reader`; `out_id` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_recording_id(
    handle: *mut Reader,
    out_id: *mut RrdString,
) -> i32 {
    guard("rrd_reader_recording_id", || {
        if out_id.is_null() {
            anyhow::bail!("null out_id");
        }

        // SAFETY: guaranteed by the caller.
        let reader = unsafe { reader(handle)? };
        // SAFETY: checked non-null above.
        unsafe { *out_id = RrdString::borrow(reader.recording_id()) };

        Ok(())
    })
}

/// The receive time of the first and last message. Returns `0` for an empty recording.
///
/// # Safety
///
/// See `reader`; both out-pointers must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_time_range(
    handle: *mut Reader,
    out_start_ns: *mut i64,
    out_end_ns: *mut i64,
    out_message_count: *mut usize,
) -> i32 {
    guard("rrd_reader_time_range", || {
        if out_start_ns.is_null() || out_end_ns.is_null() || out_message_count.is_null() {
            anyhow::bail!("null out pointer");
        }

        // SAFETY: guaranteed by the caller.
        let reader = unsafe { reader(handle)? };
        let (start, end) = reader.time_range().unwrap_or((0, 0));

        // SAFETY: checked non-null above.
        unsafe {
            *out_start_ns = start;
            *out_end_ns = end;
            *out_message_count = reader.message_count();
        }

        Ok(())
    })
}

/// Restricts playback to the newline-separated topics in `topics`; empty replays all.
///
/// # Safety
///
/// See `reader` and `text`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_set_filter(
    handle: *mut Reader,
    topics: *const c_char,
    topics_len: usize,
) -> i32 {
    guard("rrd_reader_set_filter", || {
        // SAFETY: guaranteed by the caller.
        unsafe {
            let names = text(topics, topics_len)?;
            let names = names
                .split('\n')
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect();
            reader(handle)?.set_filter(names)
        }
    })
}

/// Sets the replay direction. Reverse holds the whole recording in memory.
///
/// # Safety
///
/// See `reader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_set_reverse(handle: *mut Reader, reverse: i32) -> i32 {
    guard("rrd_reader_set_reverse", || {
        // SAFETY: guaranteed by the caller.
        unsafe { reader(handle)? }.set_reverse(reverse != 0)
    })
}

/// Moves the read head to the first message at or after `timestamp_ns`.
///
/// # Safety
///
/// See `reader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_seek(handle: *mut Reader, timestamp_ns: i64) -> i32 {
    guard("rrd_reader_seek", || {
        // SAFETY: guaranteed by the caller.
        unsafe { reader(handle)? }.seek(timestamp_ns)
    })
}

/// Reads the next message.
///
/// Returns `1` and fills `out_message` when there was one, `0` at the end of the
/// recording, and `-1` on failure. The message's pointers stay valid until the next call.
///
/// # Safety
///
/// See `reader`; `out_message` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_next(handle: *mut Reader, out_message: *mut RrdMessage) -> i32 {
    let mut found = false;

    let status = guard("rrd_reader_next", || {
        if out_message.is_null() {
            anyhow::bail!("null out_message");
        }

        // SAFETY: guaranteed by the caller.
        let reader = unsafe { reader(handle)? };
        let Some(message) = reader.read_next_borrowed()? else {
            return Ok(());
        };

        // SAFETY: checked non-null above.
        unsafe {
            *out_message = RrdMessage {
                topic: RrdString::borrow(&message.topic_name),
                data: message.data.as_ptr(),
                data_len: message.data.len(),
                recv_timestamp_ns: message.recv_timestamp,
                send_timestamp_ns: message.send_timestamp,
            };
        }
        found = true;

        Ok(())
    });

    if status != 0 {
        return status;
    }
    i32::from(found)
}

/// Closes the recording and frees `handle`, which must not be used again.
///
/// # Safety
///
/// See `reader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rrd_reader_close(handle: *mut Reader) {
    if !handle.is_null() {
        // SAFETY: the caller guarantees a live handle, consumed here.
        drop(unsafe { Box::from_raw(handle) });
    }
}
