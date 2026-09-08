# rosbag2_storage_rrd

An experimental [rosbag2](https://github.com/ros2/rosbag2) storage plugin that records
ROS 2 bags as Rerun [`.rrd`](https://rerun.io/docs/concepts/logging-and-ingestion/rrd-format)
files.

```bash
ros2 bag record -s rrd
```

> [!WARNING]
> 🚧 **Experimental.** 🚧 The `.rrd` layout and the config file keys may change without a
> migration path. ⚠️ Expect breaking changes between releases. Feedback and bug reports are
> welcome in [issues](https://github.com/rerun-io/rosbag2_storage_rrd/issues).

## Highlights

- **Live.** Stream to a running viewer while recording.
  See [Watching a recording live](#watching-a-recording-live).
- **Optimize for recording or analysis.** Raw bytes for recording speed, Rerun
  [archetypes](https://rerun.io/docs/reference/types/archetypes) for the fastest queries
  and visualization, or reflected [Arrow](https://arrow.apache.org/) columns as the
  lossless middle ground. See [Representations](#representations).
- **Reprocess.** `ros2 bag convert` adds or drops representations after recording.
- **Self-describing.** Message definitions, QoS and type hashes travel in the file.
- **Layers.** Set the recording id to make the bag one
  [layer](https://rerun.io/docs/howto/logging-and-ingestion/layers) of a
  [segment](https://rerun.io/docs/concepts/query-and-transform/properties-and-segments).
  Add layers later without touching the original.

## Status

ROS 2 Lyrical, tested on macOS (arm64) and Linux (x86_64, aarch64).

- Rust dependencies are a git pin to a `rerun-io/rerun` main commit until the next Rerun
  release. The viewer must be at least as new as the pin to open the files. See
  [Building](#building).
- Not supported: reopening a bag in `APPEND` mode, and `wstring` fields (a definition
  containing one is kept as raw CDR).
- A topic whose definition cannot be decoded (for example `ros2idl` types) is kept as raw
  CDR: it replays byte-for-byte but is opaque in the viewer.

## Quickstart

Build the package in your workspace (see [Building](#building)), then:

```bash
ros2 bag record -s rrd --all              # writes rosbag2_<date>-<time>/*.rrd
ros2 bag info rosbag2_*                   # topics, counts, duration
rerun rosbag2_*/*.rrd                     # open in the Rerun viewer
ros2 bag play rosbag2_*                   # replay to the ROS graph
```

`ros2 bag record -s rrd --help` lists the plugin's presets. Change settings in a
[storage config file](#storage-config-file).

## Watching a recording live

Start the viewer first, then record with a `server_uri`:

```bash
rerun    # hosts a server at rerun+http://127.0.0.1:9876/proxy
```

```yaml
# live.yaml
server_uri: rerun+http://127.0.0.1:9876/proxy
representations: [reflected, lenses]
```

```bash
ros2 bag record -s rrd --all --storage-config-file live.yaml --storage-preset-profile low_latency
```

Every chunk goes to the file and to the viewer. Rules:

- The server has to be up when the recording opens; otherwise opening fails.
- The `--storage-preset-profile` that bounds the file's flush latency bounds the live
  view's too. For live viewing `low_latency` or `ultra_low_latency` is suggested.

## Flush presets

`--storage-preset-profile` trades recording throughput against latency:

| Profile | Behavior |
| --- | --- |
| `none` (default) | Flush at 64 rows, 8 MiB, or 200 ms |
| `low_latency` | Flush every frame, ~30 Hz max latency |
| `ultra_low_latency` | Flush every frame, ~60 Hz max latency |
| `high_throughput` | Flush at 1024 rows or 8 MiB, no latency cap |

## Representations

A message can be kept in three forms. They differ in where compute cost is paid (at record
time or at query and visualization time) and in how much of the original message survives.

| Representation | Stored as | Record cost | Lossless | Replayable | In Rerun |
| --- | --- | --- | --- | --- | --- |
| `raw` | The CDR bytes, verbatim | lowest | yes | byte-for-byte | Opaque: nothing to query or draw until something decodes it |
| `reflected` (default) | An Arrow struct reflected from the `.msg` definition | medium | yes | by re-encoding | Queryable as columns, in the original message structure; scalars plot, most other data needs a lens to draw |
| `lenses` | Rerun archetypes derived by the built-in ROS 2 [lenses](https://rerun.io/docs/concepts/query-and-transform/lenses) | highest | no | **no** | Rerun-native: images, transforms and sensors draw and query with no setup, in Rerun's semantics rather than the message's |

**`raw`** is the cheapest to write and the least useful as-is. It exists for byte-exact
replay and for deferring every other decision.

**`reflected`** is lossless and still useful in Rerun: the bag becomes a
[dataframe](https://rerun.io/docs/howto/dataframe-api) whose columns are the message
fields, and `ros2 bag play` re-encodes it exactly. The viewer shows the fields as values
and plots the numeric ones, but it does not know that a `sensor_msgs/msg/Image` is an
image, so there is nothing to draw until a lens says so.

**`lenses`** converts [messages Rerun understands](https://rerun.io/docs/concepts/logging-and-ingestion/mcap/message-formats)
into Rerun archetypes at record time. That
is the most expensive form to write and the cheapest to use: the viewer draws it and the
dataframe API queries it like any native recording. The trade-off: a query sees Rerun's
model of the data, not the original message layout, some fields do not survive, and a
topic kept only this way cannot be replayed (`ros2 bag info` reports zero messages
for it; `ros2 bag play` skips it). A topic no lens understands keeps its reflected form
instead, so nothing is dropped.

Any combination is a valid recording. The usual choices: `reflected` alone when you want
one lossless, queryable file; `raw` plus `lenses` when record-time budget is tight but the
result must draw immediately and replay exactly; `reflected` plus `lenses` when the bag
should do everything. Whatever was not paid for at record time can be added afterwards,
see [Adding representations after recording](#adding-representations-after-recording).

## Storage config file

`--storage-config-file` points at a YAML file. Every key is optional and overrides the
preset:

```yaml
representations: [raw, lenses]   # any set of raw, reflected, lenses; default [reflected]
unmatched: keep                  # or drop: record only what the lenses derived (see Layers)
recording_id: 5f3a…              # an existing bag's id, to record a layer on top of it
server_uri: rerun+http://127.0.0.1:9876/proxy   # also stream to a running Rerun server
max_rows: 64                     # flush after this many buffered rows; 0 = no row limit
max_bytes: 8388608               # flush after this many buffered bytes; 0 = no byte limit
max_latency_ms: 200              # flush after a row has waited this long; 0 = no time limit
```

## Adding representations after recording

A bag recorded as `raw` or `reflected` is complete and lossless, so more representations can
be added later with `ros2 bag convert`, which replays the bag through this plugin into a
new one. Two ways to use it:

**Make a richer copy.** The new bag has the same messages in more forms.

```yaml
# richer_config.yaml
representations: [reflected, lenses]
```

```yaml
# output.yaml
output_bags:
  - uri: richer
    storage_id: rrd
    all_topics: true
    storage_config_uri: richer_config.yaml
```

```bash
ros2 bag convert -i recording.rrd -o output.yaml
```

The copy replays exactly like the original: `raw` replays the same bytes, `reflected`
re-encodes to the same values.

**Add a layer.** Instead of a copy, make a second `.rrd` that holds only the lens output.
Rerun shows two `.rrd` files with the same
[recording id](https://rerun.io/docs/concepts/logging-and-ingestion/recordings) as one
recording, and a catalog stores them as one
[segment](https://rerun.io/docs/concepts/query-and-transform/properties-and-segments). The
original keeps the messages; the layer adds the visualization.

1. Find the original's recording id: select the recording in the viewer and read
   *Recording ID*, or read `custom_data: rerun.recording_id` from the bag's
   `metadata.yaml`.
2. Write the layer's config and convert as above with `storage_config_uri` pointing at it:

   ```yaml
   # layer_config.yaml
   representations: [lenses]
   unmatched: drop         # topics no lens understands are left out, not copied
   recording_id: 5f3a…     # the original's id, so Rerun pairs the two files
   ```

The layer has nothing to replay on its own. Open both files in the viewer, or register
both to a dataset under different layer names, and play the original with `ros2 bag play`.

## How it works

A thin C++ [`pluginlib`](https://github.com/ros/pluginlib) shim (`cpp/rrd_storage.cpp`)
over a Rust core (`crates/rosbag2_storage_rrd_ffi`), linked as a staticlib so the plugin is
one shared library with no extra runtime artifacts.

On the write path, each topic buffers its messages and flushes them as columnar Arrow
chunks, in whichever representations the recording asks for, through the Rerun SDK's
`RecordingStream`. With a `server_uri`, a second stream carries the same chunks to the
viewer, so a server that stops answering can only ever produce a warning, never a bag
error. On the read path, the file's footer index plans playback and seeks.

Rerun's ROS 2 message reflection (`re_ros_msg`) and lenses (`re_lenses`), the same code
behind Rerun's [MCAP support](https://rerun.io/docs/reference/mcap), do the decoding and
visualization; the plugin owns buffering, flushing, and the rosbag2 contract.

## Building

Build it inside a ROS 2 workspace as an ament package:

```bash
cd ~/your_ws/src
git clone https://github.com/rerun-io/rosbag2_storage_rrd.git
cd ~/your_ws
colcon build --packages-select rosbag2_storage_rrd
colcon test  --packages-select rosbag2_storage_rrd
```

CMake drives `cargo build --release` for the Rust core; a Rust toolchain (see
`rust-toolchain`) is the only dependency beyond the ROS 2 ones in `package.xml`.

The Rust dependencies are a git pin to a `rerun-io/rerun` main commit until the next Rerun
release; `crates/rosbag2_storage_rrd_ffi/Cargo.toml` names the rev and how to bump it. A
viewer at least that new opens the files; install one with
`cargo install --git https://github.com/rerun-io/rerun.git --rev <rev> rerun-cli`.

Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
