#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <map>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "pluginlib/class_loader.hpp"
#include "rclcpp/serialization.hpp"
#include "rclcpp/serialized_message.hpp"
#include "rosbag2_storage/bag_metadata.hpp"
#include "rosbag2_storage/ros_helper.hpp"
#include "rosbag2_storage/storage_factory.hpp"
#include "rosbag2_storage/storage_filter.hpp"
#include "rosbag2_storage/storage_interfaces/read_write_interface.hpp"
#include "rosbag2_storage/storage_options.hpp"
#include "std_msgs/msg/string.hpp"

namespace
{

constexpr int kMessagesPerTopic = 10;
constexpr int64_t kStartTimeNs = 1'000'000'000;
constexpr int64_t kStepNs = 1'000'000;

std::shared_ptr<rosbag2_storage::SerializedBagMessage> make_string_message(
  const std::string & topic, const std::string & data, int64_t recv_ns, int64_t send_ns)
{
  std_msgs::msg::String msg;
  msg.data = data;
  rclcpp::Serialization<std_msgs::msg::String> serialization;
  rclcpp::SerializedMessage serialized;
  serialization.serialize_message(&msg, &serialized);

  auto bag_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
  bag_msg->topic_name = topic;
  bag_msg->recv_timestamp = recv_ns;
  bag_msg->send_timestamp = send_ns;
  bag_msg->serialized_data = rosbag2_storage::make_serialized_message(
    serialized.get_rcl_serialized_message().buffer, serialized.size());
  return bag_msg;
}

std::string string_payload(
  const std::shared_ptr<rosbag2_storage::SerializedBagMessage> & bag_msg)
{
  rclcpp::Serialization<std_msgs::msg::String> serialization;
  const rclcpp::SerializedMessage serialized(*bag_msg->serialized_data);
  std_msgs::msg::String msg;
  serialization.deserialize_message(&serialized, &msg);
  return msg.data;
}

std::vector<uint8_t> payload_bytes(
  const std::shared_ptr<rosbag2_storage::SerializedBagMessage> & bag_msg)
{
  return std::vector<uint8_t>(
    bag_msg->serialized_data->buffer,
    bag_msg->serialized_data->buffer + bag_msg->serialized_data->buffer_length);
}

/// A bag written through the plugin: one decodable std_msgs/String topic and one
/// unreflectable topic with an unknown type, with interleaved timestamps.
///
/// `config_yaml`, if not empty, is written to a `--storage-config-file` beside the bag.
struct WrittenBag
{
  WrittenBag(const std::filesystem::path & dir, const std::string & config_yaml)
  {
    std::filesystem::create_directories(dir);
    options.uri = (dir / "bag").string();
    options.storage_id = "rrd";
    if (!config_yaml.empty()) {
      const auto config_path = dir / "storage_config.yaml";
      std::ofstream(config_path) << config_yaml;
      options.storage_config_uri = config_path.string();
    }

    rosbag2_storage::StorageFactory factory;
    auto writer = factory.open_read_write(options);
    if (!writer) {
      throw std::runtime_error("failed to open the bag for writing");
    }

    rosbag2_storage::TopicMetadata chatter;
    chatter.name = "/chatter";
    chatter.type = "std_msgs/msg/String";
    chatter.serialization_format = "cdr";
    writer->create_topic(chatter, {"std_msgs/msg/String", "ros2msg", "string data\n", ""});

    rosbag2_storage::TopicMetadata unknown;
    unknown.name = "/unknown";
    unknown.type = "nonexistent_msgs/msg/Mystery";
    unknown.serialization_format = "cdr";
    writer->create_topic(unknown, {"nonexistent_msgs/msg/Mystery", "ros2msg", "", ""});

    for (int i = 0; i < kMessagesPerTopic; ++i) {
      const int64_t recv = kStartTimeNs + 2 * i * kStepNs;
      auto chatter_msg =
        make_string_message("/chatter", "hello_" + std::to_string(i), recv, recv - kStepNs);
      chatter_payloads.push_back(payload_bytes(chatter_msg));
      writer->write(chatter_msg);

      auto raw = make_string_message(
        "/unknown", "raw_" + std::to_string(i), recv + kStepNs, recv);
      raw_payloads.push_back(payload_bytes(raw));
      writer->write(raw);
    }
    relative_path = writer->get_relative_file_path();

    // What rosbag2's SequentialWriter does: hand the storage the bag metadata only rosbag2
    // knows, which the plugin writes into the recording at close.
    rosbag2_storage::BagMetadata metadata;
    metadata.storage_identifier = options.storage_id;
    metadata.relative_file_paths = {relative_path};
    writer->update_metadata(metadata);

    writer.reset();  // Close flushes everything to disk.
  }

  std::shared_ptr<rosbag2_storage::storage_interfaces::ReadOnlyInterface> open_for_reading()
  const
  {
    rosbag2_storage::StorageOptions read_options = options;
    read_options.uri = relative_path;
    rosbag2_storage::StorageFactory factory;
    return factory.open_read_only(read_options);
  }

  rosbag2_storage::StorageOptions options;
  std::string relative_path;
  std::vector<std::vector<uint8_t>> chatter_payloads;
  std::vector<std::vector<uint8_t>> raw_payloads;
};

std::filesystem::path unique_temp_dir(const std::string & name)
{
  return std::filesystem::temp_directory_path() /
         ("rrd_storage_test_" + name + "_" + std::to_string(::getpid()));
}

/// The replayable representation sets. Every test in the suite runs against each, since
/// what is stored may differ but what comes back must not.
struct RepresentationCase
{
  const char * name;
  const char * config_yaml;
};

constexpr RepresentationCase kReplayableCases[] = {
  {"reflected", ""},  // The default: no config file at all.
  {"raw", "representations: [raw]\n"},
  {"raw_reflected", "representations: [raw, reflected]\n"},
  {"raw_lenses", "representations: [raw, lenses]\n"},
  {"reflected_lenses", "representations: [reflected, lenses]\n"},
};

class RrdStorageRoundTrip : public ::testing::TestWithParam<RepresentationCase>
{
protected:
  void SetUp() override
  {
    bag_dir_ = unique_temp_dir(GetParam().name);
    bag_ = std::make_unique<WrittenBag>(bag_dir_, GetParam().config_yaml);
  }

  void TearDown() override
  {
    bag_.reset();
    std::error_code ec;
    std::filesystem::remove_all(bag_dir_, ec);
  }

  std::filesystem::path bag_dir_;
  std::unique_ptr<WrittenBag> bag_;
};

INSTANTIATE_TEST_SUITE_P(
  Representations, RrdStorageRoundTrip, ::testing::ValuesIn(kReplayableCases),
  [](const ::testing::TestParamInfo<RepresentationCase> & info) {return info.param.name;});

TEST_P(RrdStorageRoundTrip, topics_and_metadata_round_trip)
{
  auto reader = bag_->open_for_reading();
  ASSERT_NE(reader, nullptr);

  auto topics = reader->get_all_topics_and_types();
  ASSERT_EQ(topics.size(), 2u);

  std::map<std::string, std::string> types;
  for (const auto & topic : topics) {
    types[topic.name] = topic.type;
    EXPECT_EQ(topic.serialization_format, "cdr");
  }
  EXPECT_EQ(types["/chatter"], "std_msgs/msg/String");
  EXPECT_EQ(types["/unknown"], "nonexistent_msgs/msg/Mystery");

  // A message kept in several representations is still one message.
  const auto metadata = reader->get_metadata();
  EXPECT_EQ(metadata.message_count, 2 * kMessagesPerTopic);
  EXPECT_EQ(metadata.starting_time.time_since_epoch().count(), kStartTimeNs);
  for (const auto & topic : metadata.topics_with_message_count) {
    EXPECT_EQ(topic.message_count, kMessagesPerTopic) << topic.topic_metadata.name;
  }
  EXPECT_FALSE(metadata.custom_data.at("rerun.recording_id").empty());

  std::vector<rosbag2_storage::MessageDefinition> definitions;
  reader->get_all_message_definitions(definitions);
  EXPECT_EQ(definitions.size(), 2u);
}

TEST_P(RrdStorageRoundTrip, messages_replay_in_order_and_byte_exact)
{
  auto reader = bag_->open_for_reading();
  ASSERT_NE(reader, nullptr);

  std::vector<std::shared_ptr<rosbag2_storage::SerializedBagMessage>> messages;
  while (reader->has_next()) {
    messages.push_back(reader->read_next());
  }
  ASSERT_EQ(messages.size(), 2u * kMessagesPerTopic);

  // Receive-time order across topics, which is what `ros2 bag play` relies on.
  for (size_t i = 1; i < messages.size(); ++i) {
    EXPECT_LE(messages[i - 1]->recv_timestamp, messages[i]->recv_timestamp);
  }

  const std::string config = GetParam().config_yaml;
  const bool chatter_is_raw = config.find("raw") != std::string::npos;

  int chatter = 0;
  int unknown = 0;
  for (const auto & msg : messages) {
    if (msg->topic_name == "/chatter") {
      // Decoded and re-encoded: the value must survive, byte-for-byte identity need not.
      // Kept raw: the bytes themselves must.
      EXPECT_EQ(string_payload(msg), "hello_" + std::to_string(chatter));
      if (chatter_is_raw) {
        EXPECT_EQ(payload_bytes(msg), bag_->chatter_payloads[chatter]);
      }
      EXPECT_EQ(msg->send_timestamp, msg->recv_timestamp - kStepNs);
      ++chatter;
    } else {
      // Never decoded, so these must come back exactly as they went in.
      EXPECT_EQ(payload_bytes(msg), bag_->raw_payloads[unknown]);
      ++unknown;
    }
  }
  EXPECT_EQ(chatter, kMessagesPerTopic);
  EXPECT_EQ(unknown, kMessagesPerTopic);
}

TEST_P(RrdStorageRoundTrip, seek_repositions_playback)
{
  auto reader = bag_->open_for_reading();
  ASSERT_NE(reader, nullptr);

  const int64_t midpoint = kStartTimeNs + kMessagesPerTopic * kStepNs;
  reader->seek(midpoint);

  int seen = 0;
  while (reader->has_next()) {
    auto msg = reader->read_next();
    EXPECT_GE(msg->recv_timestamp, midpoint);
    ++seen;
  }
  EXPECT_GT(seen, 0);
  EXPECT_LT(seen, 2 * kMessagesPerTopic);

  reader->seek(0);
  int all = 0;
  while (reader->has_next()) {
    reader->read_next();
    ++all;
  }
  EXPECT_EQ(all, 2 * kMessagesPerTopic);
}

TEST_P(RrdStorageRoundTrip, filter_restricts_topics)
{
  auto reader = bag_->open_for_reading();
  ASSERT_NE(reader, nullptr);

  rosbag2_storage::StorageFilter filter;
  filter.topics = {"/chatter"};
  reader->set_filter(filter);

  int seen = 0;
  while (reader->has_next()) {
    EXPECT_EQ(reader->read_next()->topic_name, "/chatter");
    ++seen;
  }
  EXPECT_EQ(seen, kMessagesPerTopic);

  reader->seek(0);
  reader->reset_filter();
  int all = 0;
  while (reader->has_next()) {
    reader->read_next();
    ++all;
  }
  EXPECT_EQ(all, 2 * kMessagesPerTopic);
}

TEST_P(RrdStorageRoundTrip, widening_the_filter_mid_playback_loses_nothing)
{
  auto reader = bag_->open_for_reading();
  ASSERT_NE(reader, nullptr);

  rosbag2_storage::StorageFilter filter;
  filter.topics = {"/chatter"};
  reader->set_filter(filter);

  // Read half the chatter messages; the last one read is at kStartTimeNs + 8 * kStepNs.
  int64_t last_read = 0;
  for (int i = 0; i < kMessagesPerTopic / 2; ++i) {
    ASSERT_TRUE(reader->has_next());
    last_read = reader->read_next()->recv_timestamp;
  }
  EXPECT_EQ(last_read, kStartTimeNs + 8 * kStepNs);

  // Widening the filter must deliver every remaining message on both topics — including
  // /unknown messages the old filter excluded — with none repeated and none skipped.
  reader->reset_filter();

  std::vector<std::shared_ptr<rosbag2_storage::SerializedBagMessage>> remaining;
  while (reader->has_next()) {
    remaining.push_back(reader->read_next());
  }

  // After kStartTimeNs + 8 * kStepNs: chatter 5..9 and unknown 4..9.
  ASSERT_EQ(remaining.size(), 11u);
  EXPECT_EQ(remaining.front()->topic_name, "/unknown");
  EXPECT_EQ(remaining.front()->recv_timestamp, kStartTimeNs + 9 * kStepNs);
  for (size_t i = 0; i < remaining.size(); ++i) {
    EXPECT_GT(remaining[i]->recv_timestamp, last_read);
    if (i > 0) {
      EXPECT_GT(remaining[i]->recv_timestamp, remaining[i - 1]->recv_timestamp);
    }
  }
}

TEST_P(RrdStorageRoundTrip, reverse_playback_mirrors_forward)
{
  std::vector<int64_t> forward;
  {
    auto reader = bag_->open_for_reading();
    ASSERT_NE(reader, nullptr);
    while (reader->has_next()) {
      forward.push_back(reader->read_next()->recv_timestamp);
    }
  }
  ASSERT_EQ(forward.size(), 2u * kMessagesPerTopic);

  auto reader = bag_->open_for_reading();
  ASSERT_NE(reader, nullptr);
  rosbag2_storage::ReadOrder order;
  order.reverse = true;
  ASSERT_TRUE(reader->set_read_order(order));

  std::vector<int64_t> reversed;
  while (reader->has_next()) {
    reversed.push_back(reader->read_next()->recv_timestamp);
  }

  std::reverse(reversed.begin(), reversed.end());
  EXPECT_EQ(reversed, forward);
}

TEST_P(RrdStorageRoundTrip, truncated_recording_falls_back_to_scanning)
{
  // A recording killed mid-write ends in anything but a valid footer. Chop the tail off
  // a complete recording to stand in for that: the reader must still open it and replay
  // what survives, in order.
  const auto truncated = bag_dir_ / "truncated.rrd";
  std::filesystem::copy_file(bag_->relative_path, truncated);
  const auto size = std::filesystem::file_size(truncated);
  ASSERT_GT(size, 512u);
  std::filesystem::resize_file(truncated, size - 512);

  rosbag2_storage::StorageOptions read_options = bag_->options;
  read_options.uri = truncated.string();
  rosbag2_storage::StorageFactory factory;
  auto reader = factory.open_read_only(read_options);
  ASSERT_NE(reader, nullptr);
  EXPECT_FALSE(reader->get_metadata().custom_data.at("rerun.recording_id").empty());

  int64_t previous = 0;
  size_t seen = 0;
  while (reader->has_next()) {
    const auto msg = reader->read_next();
    EXPECT_GE(msg->recv_timestamp, previous);
    previous = msg->recv_timestamp;
    ++seen;
  }
  EXPECT_GT(seen, 0u);
  EXPECT_LE(seen, 2u * kMessagesPerTopic);
}

TEST(RrdStorage, a_layer_is_written_under_the_recording_id_the_config_names)
{
  const auto dir = unique_temp_dir("layer");
  {
    const WrittenBag layer(
      dir, "representations: [lenses]\nunmatched: drop\nrecording_id: layer-test-id\n");

    auto reader = layer.open_for_reading();
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->get_metadata().custom_data.at("rerun.recording_id"), "layer-test-id");
  }
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST(RrdStorage, lenses_only_plays_nothing_for_a_topic_a_lens_consumed)
{
  const auto dir = unique_temp_dir("lenses_only");
  {
    const WrittenBag bag(dir, "representations: [lenses]\n");

    auto reader = bag.open_for_reading();
    ASSERT_NE(reader, nullptr);

    // `ros2 bag info` still describes both topics.
    EXPECT_EQ(reader->get_all_topics_and_types().size(), 2u);

    // /chatter went through the lenses and has no CDR left; /unknown could not be
    // reflected and was kept raw regardless, so it alone counts.
    EXPECT_EQ(reader->get_metadata().message_count, kMessagesPerTopic);

    // Playback yields what has CDR: every /unknown message, in order, and nothing else.
    int seen = 0;
    while (reader->has_next()) {
      const auto msg = reader->read_next();
      EXPECT_EQ(msg->topic_name, "/unknown");
      EXPECT_EQ(payload_bytes(msg), bag.raw_payloads[seen]);
      ++seen;
    }
    EXPECT_EQ(seen, kMessagesPerTopic);
  }
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

/// Loads the plugin directly rather than through StorageFactory, which turns a failed
/// open into a null pointer instead of letting the reason escape.
auto load_plugin(
  pluginlib::ClassLoader<rosbag2_storage::storage_interfaces::ReadWriteInterface> & loader)
{
  return loader.createUniqueInstance("rrd");
}

TEST(RrdStorage, unknown_preset_profile_throws)
{
  pluginlib::ClassLoader<rosbag2_storage::storage_interfaces::ReadWriteInterface> loader(
    "rosbag2_storage", "rosbag2_storage::storage_interfaces::ReadWriteInterface");
  auto storage = load_plugin(loader);

  rosbag2_storage::StorageOptions options;
  options.uri = (std::filesystem::temp_directory_path() / "rrd_bad_profile").string();
  options.storage_id = "rrd";
  options.storage_preset_profile = "not_a_profile";

  EXPECT_THROW(storage->open(options), std::runtime_error);
}

TEST(RrdStorage, unusable_config_file_throws)
{
  pluginlib::ClassLoader<rosbag2_storage::storage_interfaces::ReadWriteInterface> loader(
    "rosbag2_storage", "rosbag2_storage::storage_interfaces::ReadWriteInterface");

  const auto dir = unique_temp_dir("bad_config");
  std::filesystem::create_directories(dir);

  rosbag2_storage::StorageOptions options;
  options.uri = (dir / "bag").string();
  options.storage_id = "rrd";

  // A file that is not there.
  options.storage_config_uri = (dir / "missing.yaml").string();
  EXPECT_THROW(load_plugin(loader)->open(options), std::runtime_error);

  // A key the plugin does not know, an empty set, and a name that is not a
  // representation: each is refused rather than quietly recording the default.
  for (const char * bad : {"chunkSize: 4\n", "representations: []\n",
      "representations: [blobs]\n", "representations: [raw\n"})
  {
    const auto path = dir / "storage_config.yaml";
    std::ofstream(path) << bad;
    options.storage_config_uri = path.string();
    EXPECT_THROW(load_plugin(loader)->open(options), std::runtime_error) << bad;
  }

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST(RrdStorage, server_uri_without_a_server_throws)
{
  pluginlib::ClassLoader<rosbag2_storage::storage_interfaces::ReadWriteInterface> loader(
    "rosbag2_storage", "rosbag2_storage::storage_interfaces::ReadWriteInterface");

  const auto dir = unique_temp_dir("no_server");
  std::filesystem::create_directories(dir);
  const auto path = dir / "storage_config.yaml";
  std::ofstream(path) << "server_uri: rerun+http://127.0.0.1:1/proxy\n";

  rosbag2_storage::StorageOptions options;
  options.uri = (dir / "bag").string();
  options.storage_id = "rrd";
  options.storage_config_uri = path.string();

  // Nothing listens on port 1. The SDK's client keeps retrying rather than reporting a
  // refused connection, so this waits out the plugin's connect timeout before throwing.
  EXPECT_THROW(load_plugin(loader)->open(options), std::runtime_error);

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST(RrdStorage, append_mode_is_rejected)
{
  pluginlib::ClassLoader<rosbag2_storage::storage_interfaces::ReadWriteInterface> loader(
    "rosbag2_storage", "rosbag2_storage::storage_interfaces::ReadWriteInterface");

  auto storage = load_plugin(loader);
  rosbag2_storage::StorageOptions options;
  options.uri = (std::filesystem::temp_directory_path() / "rrd_append").string();
  options.storage_id = "rrd";

  EXPECT_THROW(
    storage->open(options, rosbag2_storage::storage_interfaces::IOFlag::APPEND),
    std::runtime_error);
}

}  // namespace
