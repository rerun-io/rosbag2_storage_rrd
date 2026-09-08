#ifndef ROSBAG2_STORAGE_RRD__RRD_FFI_H_
#define ROSBAG2_STORAGE_RRD__RRD_FFI_H_

#include <stddef.h>
#include <stdint.h>

/// The Rust core of the `rrd` storage plugin.
///
/// Everything that touches CDR, Arrow or Rerun lives behind this boundary. Strings and
/// buffers are passed as explicit `(pointer, length)` pairs and are only borrowed for the
/// duration of the call.
///
/// Every function returns `0` on success and `-1` on failure, leaving a reason for
/// `rrd_last_error`. Rust panics are caught at the boundary and reported the same way, so
/// no unwinding ever crosses back into C++.
#ifdef __cplusplus
extern "C" {
#endif

/// A string borrowed from the Rust side.
///
/// Topic strings are valid for the lifetime of the reader; a message's strings only until
/// the next `rrd_reader_next`. A writer's strings live as long as the writer.
typedef struct RrdString
{
  const char * ptr;
  size_t len;
} RrdString;

/// An open bag being written. Opaque; owned by the Rust side.
typedef struct RrdWriter RrdWriter;

/// Copies the reason the last call on this thread failed into `buf` as a NUL-terminated
/// string, and clears it. Returns the number of bytes written, excluding the NUL.
///
/// Clearing on read is what stops a stale message being reported against a later,
/// unrelated failure.
size_t rrd_last_error(char * buf, size_t buf_len);

/// Levels of the messages `RrdLogCallback` receives.
enum
{
  RRD_LOG_TRACE = 0,
  RRD_LOG_DEBUG = 1,
  RRD_LOG_INFO = 2,
  RRD_LOG_WARN = 3,
  RRD_LOG_ERROR = 4,
};

/// Receives one diagnostic from the Rust side, including Rerun's own. `target` is the Rust
/// module it came from. Neither string is NUL-terminated; both are borrowed for the call.
/// Called from any thread.
typedef void (* RrdLogCallback)(
  int32_t level,
  const char * target, size_t target_len,
  const char * message, size_t message_len);

/// Routes every diagnostic to `callback`. The first call installs it for the process;
/// later calls do nothing. Messages below `RRD_LOG_INFO` are dropped unless `RUST_LOG`
/// asks for them.
int32_t rrd_set_log_callback(RrdLogCallback callback);

/// Opens `path` for writing.
///
/// `profile` names a preset (`""`, `none`, `low_latency`, `ultra_low_latency`, or
/// `high_throughput`); an unknown name fails. `config_yaml` is the text of the
/// `--storage-config-file`, or empty; its keys override the preset and may name the
/// recording id to write under. See the README for the format.
int32_t rrd_writer_open(
  const char * path, size_t path_len,
  const char * profile, size_t profile_len,
  const char * config_yaml, size_t config_yaml_len,
  RrdWriter ** out_writer);

/// Declares a topic and hands over its ROS 2 message definition.
///
/// `schema_text` is the definition exactly as rosbag2 supplies it. A definition that
/// cannot be reflected is not an error: the topic records as raw CDR blobs instead, and
/// the definition is stored either way so it can be decoded later.
int32_t rrd_writer_create_topic(
  RrdWriter * writer,
  const char * topic, size_t topic_len,
  const char * type_name, size_t type_name_len,
  const char * schema_encoding, size_t schema_encoding_len,
  const uint8_t * schema_text, size_t schema_text_len,
  const char * qos_profiles, size_t qos_profiles_len,
  const char * type_hash, size_t type_hash_len,
  size_t * out_topic);

/// Records one serialized message.
///
/// `topic` is the index handed back by `rrd_writer_create_topic`, not a name: recording a
/// message should not cost a string hash.
int32_t rrd_writer_write(
  RrdWriter * writer,
  size_t topic,
  const uint8_t * cdr, size_t cdr_len,
  int64_t recv_timestamp_ns, int64_t send_timestamp_ns);

/// Flushes and forgets a topic.
int32_t rrd_writer_remove_topic(
  RrdWriter * writer, const char * topic, size_t topic_len);

/// Stores rosbag2's bag metadata, written when the bag is closed.
///
/// Deferring it to close is deliberate: rosbag2 hands this over before the first message,
/// so the message counts and time range in it are only final at the end.
int32_t rrd_writer_set_metadata(
  RrdWriter * writer, const char * yaml, size_t yaml_len);

/// Closes the bag and frees `writer`, which must not be used again.
int32_t rrd_writer_close(RrdWriter * writer);

/// A recording opened for reading. Opaque; owned by the Rust side.
typedef struct RrdReader RrdReader;

/// A topic as it was recorded.
typedef struct RrdTopic
{
  RrdString name;
  RrdString type_name;
  RrdString schema_encoding;
  const uint8_t * schema_text;
  size_t schema_text_len;
  RrdString qos_profiles;
  RrdString type_hash;
  size_t message_count;
} RrdTopic;

/// One message being replayed.
typedef struct RrdMessage
{
  RrdString topic;
  const uint8_t * data;
  size_t data_len;
  int64_t recv_timestamp_ns;
  int64_t send_timestamp_ns;
} RrdMessage;

/// Opens `path` and indexes every message in it.
int32_t rrd_reader_open(const char * path, size_t path_len, RrdReader ** out_reader);

/// The number of topics in the recording.
size_t rrd_reader_topic_count(RrdReader * reader);

/// Describes the topic at `index`.
int32_t rrd_reader_topic(RrdReader * reader, size_t index, RrdTopic * out_topic);

/// The stored rosbag2 metadata document, empty if the bag carries none.
int32_t rrd_reader_metadata(RrdReader * reader, RrdString * out_yaml);

/// The Rerun recording id the file was written under. A recording made with the same id
/// (`recording_id` in the storage config file) is a layer on top of this bag.
int32_t rrd_reader_recording_id(RrdReader * reader, RrdString * out_id);

/// The receive time of the first and last message, and how many there are.
int32_t rrd_reader_time_range(
  RrdReader * reader, int64_t * out_start_ns, int64_t * out_end_ns, size_t * out_message_count);

/// Restricts playback to the newline-separated topics in `topics`; empty replays all.
int32_t rrd_reader_set_filter(RrdReader * reader, const char * topics, size_t topics_len);

/// Sets the replay direction. Reverse holds the whole recording in memory.
int32_t rrd_reader_set_reverse(RrdReader * reader, int32_t reverse);

/// Moves the read head to the first message at or after `timestamp_ns`.
int32_t rrd_reader_seek(RrdReader * reader, int64_t timestamp_ns);

/// Reads the next message.
///
/// Returns `1` and fills `out_message` when there was one, `0` at the end of the
/// recording, and `-1` on failure.
int32_t rrd_reader_next(RrdReader * reader, RrdMessage * out_message);

/// Closes the recording and frees `reader`, which must not be used again.
void rrd_reader_close(RrdReader * reader);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // ROSBAG2_STORAGE_RRD__RRD_FFI_H_
