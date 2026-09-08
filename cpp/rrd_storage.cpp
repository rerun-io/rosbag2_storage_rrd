/// A rosbag2 storage plugin writing Rerun `.rrd` files.
///
/// This file is only the `pluginlib` boundary: it translates rosbag2's interface into
/// calls on the Rust core (`rrd_ffi.h`), which does the CDR decoding, the Arrow building
/// and the Rerun writing. Nothing Rerun-facing appears here, which is what keeps the
/// plugin to a single linkage of the SDK.

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "pluginlib/class_list_macros.hpp"
#include "rcutils/logging_macros.h"
#include "rosbag2_storage/message_definition.hpp"
#include "rosbag2_storage/qos.hpp"
#include "rosbag2_storage/ros_helper.hpp"
#include "rosbag2_storage/storage_interfaces/read_write_interface.hpp"
#include "rosbag2_storage/storage_options.hpp"
#include "rosbag2_storage/yaml.hpp"

#include "rosbag2_storage_rrd/rrd_ffi.h"

namespace rosbag2_storage_plugins
{
namespace
{
constexpr char LOG_NAME[] = "rosbag2_storage_rrd";

/// `custom_data` key carrying the Rerun recording id, so a script can read it from the bag
/// metadata and put it in a storage config file to record a layer on top of the bag.
constexpr char RECORDING_ID_KEY[] = "rerun.recording_id";

/// Puts a diagnostic from the Rust side through ROS logging, under this plugin's logger
/// name and with the Rust module in place of the function name.
void log_from_rust(
  int32_t level, const char * target, size_t target_len, const char * message,
  size_t message_len)
{
  int severity = RCUTILS_LOG_SEVERITY_INFO;
  switch (level) {
    case RRD_LOG_TRACE:
    case RRD_LOG_DEBUG:
      severity = RCUTILS_LOG_SEVERITY_DEBUG;
      break;
    case RRD_LOG_INFO:
      severity = RCUTILS_LOG_SEVERITY_INFO;
      break;
    case RRD_LOG_WARN:
      severity = RCUTILS_LOG_SEVERITY_WARN;
      break;
    case RRD_LOG_ERROR:
      severity = RCUTILS_LOG_SEVERITY_ERROR;
      break;
  }

  const std::string function(target, target_len);
  const rcutils_log_location_t location{function.c_str(), "", 0};
  RCUTILS_LOGGING_AUTOINIT;
  rcutils_log(&location, severity, LOG_NAME, "%.*s", static_cast<int>(message_len), message);
}

/// rosbag2 polls `get_bagfile_size()` on every write while size-based splitting is on, so
/// the size is estimated from what we have handed over rather than by flushing the file.
/// The estimate ignores Arrow framing and compression, so a split lands slightly late.
constexpr uint64_t kPerMessageOverhead = 256;

/// Even an empty recording carries a header and a footer, so refuse split thresholds
/// smaller than this.
constexpr uint64_t kMinimumSplitFileSize = 1024;

/// Raises the reason the Rust core gave for a failure.
[[noreturn]] void throw_last_error(const char * what)
{
  char buffer[1024];
  const size_t len = rrd_last_error(buffer, sizeof(buffer));
  throw std::runtime_error(
          std::string(what) + ": " + (len > 0 ? buffer : "no further detail"));
}

/// Runs an FFI call, raising on failure.
void check(int32_t status, const char * what)
{
  if (status != 0) {
    throw_last_error(what);
  }
}

/// Borrows an `RrdString` without copying beyond what `std::string` needs.
std::string to_string(const RrdString & s)
{
  return s.ptr == nullptr ? std::string() : std::string(s.ptr, s.len);
}

/// Runs an FFI call, logging rather than raising.
///
/// Used where rosbag2's interface gives us nowhere to report a failure.
void check_logged(int32_t status, const char * what)
{
  if (status != 0) {
    char buffer[1024];
    const size_t len = rrd_last_error(buffer, sizeof(buffer));
    RCUTILS_LOG_ERROR_NAMED(LOG_NAME, "%s: %s", what, len > 0 ? buffer : "no further detail");
  }
}

}  // namespace

/// A rosbag2 storage plugin writing Rerun `.rrd` files.
class RrdStorage : public rosbag2_storage::storage_interfaces::ReadWriteInterface
{
public:
  RrdStorage()
  {
    check_logged(rrd_set_log_callback(&log_from_rust), "Failed to route Rust logging");
    metadata_.storage_identifier = get_storage_identifier();
    metadata_.message_count = 0;
  }

  ~RrdStorage() override
  {
    if (writer_ != nullptr) {
      check_logged(rrd_writer_close(writer_), "Failed to close the recording");
      writer_ = nullptr;
    }
    if (reader_ != nullptr) {
      rrd_reader_close(reader_);
      reader_ = nullptr;
    }
  }

  RrdStorage(const RrdStorage &) = delete;
  RrdStorage & operator=(const RrdStorage &) = delete;

  /** BaseIOInterface **/

  void open(
    const rosbag2_storage::StorageOptions & storage_options,
    rosbag2_storage::storage_interfaces::IOFlag io_flag =
    rosbag2_storage::storage_interfaces::IOFlag::READ_WRITE) override
  {
    using rosbag2_storage::storage_interfaces::IOFlag;

    if (io_flag == IOFlag::READ_ONLY) {
      open_for_reading(storage_options.uri);
      return;
    }

    // APPEND would have to reopen an existing recording and continue it, which the
    // recording stream cannot do; silently truncating instead would lose a bag.
    if (io_flag == IOFlag::APPEND) {
      throw std::runtime_error("The 'rrd' storage plugin does not support APPEND mode");
    }

    const std::string uri = storage_options.uri;
    const auto parent = std::filesystem::path(uri).parent_path();
    if (!parent.empty()) {
      std::filesystem::create_directories(parent);
    }
    relative_path_ = uri + ".rrd";

    splitting_enabled_ = storage_options.max_bagfile_size > 0;

    // The config file is read here and interpreted on the Rust side, so there is one
    // place that knows its keys and one error path back to rosbag2.
    std::string config_yaml;
    if (!storage_options.storage_config_uri.empty()) {
      std::ifstream in(storage_options.storage_config_uri);
      if (!in) {
        throw std::runtime_error(
                "Failed to read the storage config file '" +
                storage_options.storage_config_uri + "'");
      }
      config_yaml.assign(std::istreambuf_iterator<char>(in), {});
    }

    const std::string & profile = storage_options.storage_preset_profile;
    check(
      rrd_writer_open(
        relative_path_.c_str(), relative_path_.size(),
        profile.c_str(), profile.size(),
        config_yaml.c_str(), config_yaml.size(),
        &writer_),
      "Failed to open the recording");

    metadata_.relative_file_paths.push_back(relative_path_);
  }

  /** BaseInfoInterface **/

  rosbag2_storage::BagMetadata get_metadata() override
  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (reader_ != nullptr) {
      return metadata_;
    }

    metadata_.bag_size = bagfile_size_locked();
    metadata_.topics_with_message_count.clear();
    for (const auto & [_, info] : topic_metadata_) {
      metadata_.topics_with_message_count.push_back(info);
    }
    return metadata_;
  }

  std::string get_relative_file_path() const override
  {
    return relative_path_;
  }

  uint64_t get_bagfile_size() const override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return bagfile_size_locked();
  }

  std::string get_storage_identifier() const override
  {
    return "rrd";
  }

  /** ReadInterface **/

  bool set_read_order(const rosbag2_storage::ReadOrder & read_order) override
  {
    if (read_order.sort_by != rosbag2_storage::ReadOrder::ReceivedTimestamp) {
      return false;
    }
    if (reader_ == nullptr) {
      return true;
    }

    next_.reset();
    if (rrd_reader_set_reverse(reader_, read_order.reverse ? 1 : 0) != 0) {
      return false;
    }
    return true;
  }

  bool has_next() override
  {
    if (next_) {
      return true;
    }
    if (reader_ == nullptr) {
      return false;
    }

    RrdMessage message;
    const int32_t status = rrd_reader_next(reader_, &message);
    if (status < 0) {
      throw_last_error("Failed to read the next message");
    }
    if (status == 0) {
      return false;
    }

    next_ = std::make_shared<rosbag2_storage::SerializedBagMessage>();
    next_->topic_name = to_string(message.topic);
    next_->recv_timestamp = message.recv_timestamp_ns;
    next_->send_timestamp = message.send_timestamp_ns;
    next_->serialized_data =
      rosbag2_storage::make_serialized_message(message.data, message.data_len);
    return true;
  }

  std::shared_ptr<rosbag2_storage::SerializedBagMessage> read_next() override
  {
    if (!has_next()) {
      throw std::runtime_error("No more messages to read");
    }
    auto message = std::move(next_);
    next_.reset();
    return message;
  }

  std::vector<rosbag2_storage::TopicMetadata> get_all_topics_and_types() override
  {
    std::vector<rosbag2_storage::TopicMetadata> topics;
    topics.reserve(read_topics_.size());
    for (const auto & info : read_topics_) {
      topics.push_back(info.topic_metadata);
    }
    return topics;
  }

  void get_all_message_definitions(
    std::vector<rosbag2_storage::MessageDefinition> & definitions) override
  {
    definitions = read_definitions_;
  }

  void set_filter(const rosbag2_storage::StorageFilter & storage_filter) override
  {
    if (reader_ == nullptr) {
      return;
    }

    // Newline-separated rather than one call per topic: topic names cannot contain one.
    std::string joined;
    for (const auto & topic : storage_filter.topics) {
      if (!joined.empty()) {
        joined.push_back('\n');
      }
      joined += topic;
    }

    // A prefetched message the new filter still admits must survive: the reader has
    // already advanced past it, so dropping it here would lose it entirely.
    if (next_ != nullptr && !storage_filter.topics.empty()) {
      const auto & topics = storage_filter.topics;
      if (std::find(topics.begin(), topics.end(), next_->topic_name) == topics.end()) {
        next_.reset();
      }
    }
    check(
      rrd_reader_set_filter(reader_, joined.c_str(), joined.size()),
      "Failed to set the topic filter");
  }

  void reset_filter() override
  {
    set_filter(rosbag2_storage::StorageFilter{});
  }

  void seek(const rcutils_time_point_value_t & timestamp) override
  {
    if (reader_ == nullptr) {
      return;
    }
    next_.reset();
    check(rrd_reader_seek(reader_, timestamp), "Failed to seek");
  }

  /** WriteInterface **/

  uint64_t get_minimum_split_file_size() const override
  {
    return kMinimumSplitFileSize;
  }

  void write(std::shared_ptr<const rosbag2_storage::SerializedBagMessage> msg) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    write_locked(msg);
  }

  void write(
    const std::vector<std::shared_ptr<const rosbag2_storage::SerializedBagMessage>> & msgs) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto & msg : msgs) {
      write_locked(msg);
    }
  }

#ifdef RRD_HAS_WRITE_MESSAGES
  // rosbag2_storage >= 0.33. This plugin never silently drops: a message is either
  // recorded or the failure is raised, so success is always total.

  bool write_message(std::shared_ptr<const rosbag2_storage::SerializedBagMessage> msg) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    write_locked(msg);
    return true;
  }

  std::vector<size_t> write_messages(const rosbag2_storage::SerializedBagMessages & msgs) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto & msg : msgs) {
      write_locked(msg);
    }
    return {};
  }
#endif

  void create_topic(
    const rosbag2_storage::TopicMetadata & topic,
    const rosbag2_storage::MessageDefinition & message_definition) override
  {
    std::lock_guard<std::mutex> lock(mutex_);

    topic_metadata_.emplace(topic.name, rosbag2_storage::TopicInformation{topic, 0});
    size_t topic_index = 0;

    // QoS has to survive the round trip: `ros2 bag reindex` compares it, and a replayed
    // bag should offer what the original publisher did. Encoding always uses the linked
    // rosbag2's current metadata version; the version travels in the stored bag metadata,
    // which is what the read side decodes against.
    const std::string qos =
      rosbag2_storage::serialize_rclcpp_qos_vector(topic.offered_qos_profiles);

    // The definition travels into the recording, so the bag can be decoded later without
    // the ROS packages that produced it.
    check_logged(
      rrd_writer_create_topic(
        writer_,
        topic.name.c_str(), topic.name.size(),
        topic.type.c_str(), topic.type.size(),
        message_definition.encoding.c_str(), message_definition.encoding.size(),
        reinterpret_cast<const uint8_t *>(message_definition.encoded_message_definition.data()),
        message_definition.encoded_message_definition.size(),
        qos.c_str(), qos.size(),
        topic.type_description_hash.c_str(), topic.type_description_hash.size(),
        &topic_index),
      ("Failed to create topic '" + topic.name + "'").c_str());

    topic_ids_.emplace(topic.name, topic_index);
  }

  void remove_topic(const rosbag2_storage::TopicMetadata & topic) override
  {
    std::lock_guard<std::mutex> lock(mutex_);

    topic_metadata_.erase(topic.name);
    topic_ids_.erase(topic.name);
    check_logged(
      rrd_writer_remove_topic(writer_, topic.name.c_str(), topic.name.size()),
      ("Failed to remove topic '" + topic.name + "'").c_str());
  }

  void update_metadata(const rosbag2_storage::BagMetadata & bag_metadata) override
  {
    std::lock_guard<std::mutex> lock(mutex_);

    // rosbag2 hands this over before the first message, so its counts and time range are
    // not final. Keep only the fields rosbag2 alone knows; the rest we fill in ourselves
    // as messages arrive, and the Rust core writes the document at close.
    metadata_.custom_data = bag_metadata.custom_data;
    metadata_.ros_distro = bag_metadata.ros_distro;

    YAML::Node node;
    node = metadata_;
    const std::string yaml = YAML::Dump(node);
    check_logged(
      rrd_writer_set_metadata(writer_, yaml.c_str(), yaml.size()),
      "Failed to record bag metadata");
  }

private:
  /// The body of `get_bagfile_size`, for callers already holding the mutex.
  uint64_t bagfile_size_locked() const
  {
    if (splitting_enabled_) {
      return size_estimate_;
    }

    std::error_code ec;
    const auto size = std::filesystem::file_size(relative_path_, ec);
    return ec ? size_estimate_ : size;
  }

  /// Opens a recording and rebuilds the bag description rosbag2 expects from it.
  void open_for_reading(const std::string & uri)
  {
    relative_path_ = std::filesystem::exists(uri) ? uri : uri + ".rrd";

    check(
      rrd_reader_open(relative_path_.c_str(), relative_path_.size(), &reader_),
      "Failed to open the recording for reading");

    // The stored document carries what only rosbag2 knows — including the metadata
    // version the QoS profiles below were encoded with. A bag without one (or with one
    // we cannot parse) was written before anything newer than version 9 existed.
    int metadata_version = 9;
    RrdString yaml;
    check(rrd_reader_metadata(reader_, &yaml), "Failed to read the bag metadata");
    if (yaml.ptr != nullptr && yaml.len > 0) {
      try {
        const auto stored = YAML::Load(to_string(yaml)).as<rosbag2_storage::BagMetadata>();
        metadata_version = stored.version;
        metadata_.custom_data = stored.custom_data;
        metadata_.ros_distro = stored.ros_distro;
      } catch (const YAML::Exception & e) {
        RCUTILS_LOG_WARN_NAMED(LOG_NAME, "Could not parse the stored bag metadata: %s", e.what());
      }
    }

    RrdString recording_id;
    check(rrd_reader_recording_id(reader_, &recording_id), "Failed to read the recording id");
    metadata_.custom_data[RECORDING_ID_KEY] = to_string(recording_id);

    const size_t count = rrd_reader_topic_count(reader_);
    for (size_t i = 0; i < count; ++i) {
      RrdTopic topic;
      check(rrd_reader_topic(reader_, i, &topic), "Failed to read topic info");

      rosbag2_storage::TopicMetadata metadata;
      metadata.name = to_string(topic.name);
      metadata.type = to_string(topic.type_name);
      metadata.serialization_format = "cdr";
      metadata.type_description_hash = to_string(topic.type_hash);
      const std::string qos = to_string(topic.qos_profiles);
      if (!qos.empty()) {
        try {
          metadata.offered_qos_profiles =
            rosbag2_storage::to_rclcpp_qos_vector(qos, metadata_version);
        } catch (const std::exception & e) {
          RCUTILS_LOG_WARN_NAMED(
            LOG_NAME, "Could not parse QoS for topic '%s': %s", metadata.name.c_str(), e.what());
        }
      }

      rosbag2_storage::MessageDefinition definition;
      definition.topic_type = metadata.type;
      definition.type_hash = metadata.type_description_hash;
      definition.encoding = to_string(topic.schema_encoding);
      definition.encoded_message_definition.assign(
        reinterpret_cast<const char *>(topic.schema_text), topic.schema_text_len);

      read_topics_.push_back(rosbag2_storage::TopicInformation{metadata, topic.message_count});
      read_definitions_.push_back(definition);
    }

    int64_t start_ns = 0;
    int64_t end_ns = 0;
    size_t message_count = 0;
    check(
      rrd_reader_time_range(reader_, &start_ns, &end_ns, &message_count),
      "Failed to read the recording's time range");

    metadata_.message_count = message_count;
    metadata_.starting_time = std::chrono::time_point<std::chrono::high_resolution_clock>(
      std::chrono::nanoseconds(start_ns));
    metadata_.duration = std::chrono::nanoseconds(end_ns - start_ns);
    metadata_.relative_file_paths = {relative_path_};
    metadata_.bag_size = get_bagfile_size();

    for (auto & info : read_topics_) {
      metadata_.topics_with_message_count.push_back(info);
    }
  }

  void write_locked(const std::shared_ptr<const rosbag2_storage::SerializedBagMessage> & msg)
  {
    const auto id = topic_ids_.find(msg->topic_name);
    if (id == topic_ids_.end()) {
      throw std::runtime_error("Write to topic '" + msg->topic_name + "' before it was created");
    }

    const auto & buffer = msg->serialized_data;
    check(
      rrd_writer_write(
        writer_,
        id->second,
        buffer->buffer, buffer->buffer_length,
        msg->recv_timestamp, msg->send_timestamp),
      "Failed to record a message");

    update_write_metadata(msg);
  }

  void update_write_metadata(
    const std::shared_ptr<const rosbag2_storage::SerializedBagMessage> & msg)
  {
    size_estimate_ += msg->serialized_data->buffer_length + kPerMessageOverhead;

    const auto it = topic_metadata_.find(msg->topic_name);
    if (it != topic_metadata_.end()) {
      it->second.message_count++;
    }
    metadata_.message_count++;

    const auto stamp = std::chrono::time_point<std::chrono::high_resolution_clock>(
      std::chrono::nanoseconds(msg->recv_timestamp));
    if (metadata_.message_count == 1) {
      metadata_.starting_time = stamp;
      metadata_.duration = std::chrono::nanoseconds(0);
    } else {
      const auto end = metadata_.starting_time + metadata_.duration;
      if (stamp > end) {
        metadata_.duration = stamp - metadata_.starting_time;
      }
      if (stamp < metadata_.starting_time) {
        metadata_.duration += metadata_.starting_time - stamp;
        metadata_.starting_time = stamp;
      }
    }
  }

  mutable std::mutex mutex_;

  RrdWriter * writer_ = nullptr;
  std::string relative_path_;
  bool splitting_enabled_ = false;
  uint64_t size_estimate_ = 0;

  RrdReader * reader_ = nullptr;
  std::shared_ptr<rosbag2_storage::SerializedBagMessage> next_;
  std::vector<rosbag2_storage::TopicInformation> read_topics_;
  std::vector<rosbag2_storage::MessageDefinition> read_definitions_;

  rosbag2_storage::BagMetadata metadata_;
  std::unordered_map<std::string, rosbag2_storage::TopicInformation> topic_metadata_;
  std::unordered_map<std::string, size_t> topic_ids_;
};

}  // namespace rosbag2_storage_plugins

PLUGINLIB_EXPORT_CLASS(
  rosbag2_storage_plugins::RrdStorage,
  rosbag2_storage::storage_interfaces::ReadWriteInterface)
