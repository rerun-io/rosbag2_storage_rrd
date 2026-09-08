//! What a recording is asked to be: which representations of each message to keep, and
//! when buffered rows are flushed.
//!
//! rosbag2 offers two knobs, and they compose the way its own plugins use them:
//! `--storage-preset-profile` names a bundle of settings, and `--storage-config-file`
//! overrides individual keys on top of it. The Rust side owns both, so there is one place
//! that validates a configuration and one error path back to rosbag2.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context as _;
use rerun::external::re_uri::ProxyUri;
use serde::Deserialize;

/// One way a message can be stored.
///
/// A recording keeps a set of these. Everything is derivable from [`Self::Raw`] and nothing
/// is derivable from [`Self::Lenses`], so the order here is the ladder a bag can be moved
/// up (by replaying it through `ros2 bag convert` into a recording that asks for more).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Representation {
    /// The CDR bytes exactly as they went over the wire. Lossless, replays byte-for-byte,
    /// and the cheapest to write. Queryable only once something reflects them.
    Raw,

    /// The message reflected into an Arrow struct from its `.msg` definition. Lossless and
    /// replays by re-encoding. Queryable as columns; scalars plot, most other data needs a
    /// lens to be visualized.
    Reflected,

    /// Rerun archetypes derived from the reflected message by the built-in ROS 2 lenses.
    /// Visualizable as-is, but lossy: a topic recorded only this way cannot be replayed.
    /// A topic no lens matches keeps its reflected form instead, so nothing is dropped.
    Lenses,
}

impl Representation {
    /// The name used in configuration files.
    pub fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Reflected => "reflected",
            Self::Lenses => "lenses",
        }
    }
}

impl std::fmt::Display for Representation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// What a recording does with a topic no lens matched.
///
/// Only meaningful alongside [`Representation::Lenses`]. The default keeps such a topic's
/// reflected struct so a bag loses nothing. [`Self::Drop`] keeps only what the lenses
/// derived, so the recording can be stacked as a layer on top of one that already holds
/// the messages; a topic no lens matches then records nothing at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unmatched {
    /// Keep the reflected struct of a topic no lens matched.
    Keep,

    /// Record only lens output, dropping everything a lens did not produce.
    Drop,
}

/// The representations a recording keeps; never empty once validated.
pub type Representations = BTreeSet<Representation>;

/// `raw,lenses` — how a set is shown in messages.
pub fn format_representations(representations: &Representations) -> String {
    representations
        .iter()
        .map(|r| r.name())
        .collect::<Vec<_>>()
        .join(",")
}

/// When a topic's buffered rows are handed to the recording stream.
///
/// All three limits are checked per message, and whichever is reached first wins. The byte
/// limit is what keeps a topic of large messages (point clouds, images) from holding many
/// megabytes while it waits for a row count that will take a long time to reach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushPolicy {
    /// Buffered rows to allow, or `0` for no row limit.
    pub max_rows: usize,

    /// Buffered payload bytes to allow, or `0` for no byte limit.
    ///
    /// This sets the chunk size, and peak memory is a U-curve in it. Too small and the
    /// per-chunk metadata the file sink accumulates for the footer dominates; too large
    /// and the buffers being filled, compressed and held in flight do. Measured on 900 KiB
    /// images: 2 MiB → 1046 MB peak RSS, 8 MiB → 818 MB, 32 MiB → 1023 MB, with the same
    /// shape on 256 KiB point clouds.
    ///
    /// Smaller chunks are somewhat faster (2748 vs 2178 MB/s on images at 2 MiB), but they
    /// also make for a larger manifest and worse query performance downstream, so the
    /// memory optimum is the right place to sit.
    pub max_bytes: usize,

    /// How long a row may wait, or `None` for no time limit.
    ///
    /// Enforced on the topic's own writes and by the writer's cross-topic sweep, which
    /// also runs at this cadence — so a topic that goes quiet still flushes within about
    /// twice this bound, as long as anything else is being recorded.
    pub max_latency: Option<Duration>,
}

/// Everything `rrd_writer_open` needs to know about the recording it is asked to make.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingConfig {
    pub flush: FlushPolicy,
    pub representations: Representations,

    /// What becomes of a topic no lens matched; only consulted when the recording asks
    /// for [`Representation::Lenses`] and not [`Representation::Reflected`].
    pub unmatched: Unmatched,

    /// The Rerun recording id to write under, or `None` for a fresh random one.
    ///
    /// Rerun merges `.rrd` files that share a recording id into one recording, and a
    /// catalog uses it as the segment id. Setting it to an existing bag's id makes this
    /// recording a layer on top of that bag.
    pub recording_id: Option<String>,

    /// A running Rerun server (the viewer, or `rerun server`) that every chunk is also
    /// streamed to, so the recording can be watched as it is written; `None` writes the
    /// file only.
    ///
    /// The server has to be up when the recording opens: the SDK's client queues at most
    /// a hundred messages until it connects, and that backpressure would stall the bag.
    pub server_uri: Option<ProxyUri>,
}

impl RecordingConfig {
    /// Resolves a `--storage-preset-profile` name.
    ///
    /// The latency-bound profiles exist for watching a recording live, where a row that
    /// arrives late is worse than a chunk that is small. The row- and byte-bound profiles
    /// are for recording throughput, where the opposite is true. Every profile records the
    /// reflected form; the config file is where a different set is asked for.
    pub fn from_profile(name: &str) -> anyhow::Result<Self> {
        const MIB: usize = 1024 * 1024;

        let flush = match name {
            "" | "none" => FlushPolicy {
                max_rows: 64,
                max_bytes: 8 * MIB,
                max_latency: Some(Duration::from_millis(200)),
            },
            "low_latency" => FlushPolicy {
                max_rows: 0,
                max_bytes: 8 * MIB,
                max_latency: Some(Duration::from_nanos(1_000_000_000 / 30)),
            },
            "ultra_low_latency" => FlushPolicy {
                max_rows: 0,
                max_bytes: 8 * MIB,
                max_latency: Some(Duration::from_nanos(1_000_000_000 / 60)),
            },
            "high_throughput" => FlushPolicy {
                max_rows: 1024,
                max_bytes: 8 * MIB,
                max_latency: None,
            },
            unknown => anyhow::bail!(
                "unknown storage preset profile '{unknown}', expected one of: \
                 none, low_latency, ultra_low_latency, high_throughput"
            ),
        };

        Ok(Self {
            flush,
            representations: Representations::from([Representation::Reflected]),
            unmatched: Unmatched::Keep,
            recording_id: None,
            server_uri: None,
        })
    }

    /// Resolves a profile and applies the `--storage-config-file` on top, if there is one.
    pub fn resolve(profile: &str, config_yaml: &str) -> anyhow::Result<Self> {
        let mut config = Self::from_profile(profile)?;
        if config_yaml.trim().is_empty() {
            return Ok(config);
        }

        let file: ConfigFile = serde_yaml_ng::from_str(config_yaml)
            .context("failed to parse the storage config file")?;
        file.apply(&mut config)?;

        Ok(config)
    }
}

/// The keys a `--storage-config-file` may set. Each one overrides the profile's value.
///
/// ```yaml
/// representations: [raw, lenses]
/// unmatched: keep       # or `drop`: record only what the lenses derived
/// recording_id: 5f3a…   # an existing bag's id, to record a layer on top of it
/// server_uri: rerun+http://127.0.0.1:9876/proxy   # also stream to a running Rerun server
/// max_rows: 64          # 0 = no row limit
/// max_bytes: 8388608    # 0 = no byte limit
/// max_latency_ms: 200   # 0 = no time limit
/// ```
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    representations: Option<Vec<Representation>>,
    unmatched: Option<Unmatched>,
    recording_id: Option<String>,
    server_uri: Option<String>,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    max_latency_ms: Option<u64>,
}

impl ConfigFile {
    fn apply(self, config: &mut RecordingConfig) -> anyhow::Result<()> {
        if let Some(representations) = self.representations {
            anyhow::ensure!(
                !representations.is_empty(),
                "the storage config file asks for no representations at all; \
                 choose at least one of: raw, reflected, lenses"
            );
            config.representations = representations.into_iter().collect();
        }
        if let Some(unmatched) = self.unmatched {
            config.unmatched = unmatched;
        }

        // `drop` discards everything the lenses did not produce, so asking for it together
        // with a representation that is not derived from lenses contradicts itself. Raw is
        // exempt: it is written as its own chunk and never passes through the lenses.
        anyhow::ensure!(
            config.unmatched == Unmatched::Keep
                || (config.representations.contains(&Representation::Lenses)
                    && !config.representations.contains(&Representation::Reflected)),
            "'unmatched: drop' keeps only what the lenses derive, so it needs \
             representations to include 'lenses' and not 'reflected'; got [{}]",
            format_representations(&config.representations)
        );
        if let Some(recording_id) = self.recording_id {
            anyhow::ensure!(
                !recording_id.trim().is_empty(),
                "the storage config file sets an empty recording_id"
            );
            config.recording_id = Some(recording_id);
        }
        if let Some(server_uri) = self.server_uri {
            let uri = server_uri.parse::<ProxyUri>().with_context(|| {
                format!(
                    "the storage config file's server_uri '{server_uri}' is not a Rerun \
                     proxy URL like rerun+http://127.0.0.1:9876/proxy"
                )
            })?;
            config.server_uri = Some(uri);
        }
        if let Some(max_rows) = self.max_rows {
            config.flush.max_rows = max_rows;
        }
        if let Some(max_bytes) = self.max_bytes {
            config.flush.max_bytes = max_bytes;
        }
        if let Some(ms) = self.max_latency_ms {
            config.flush.max_latency = (ms > 0).then(|| Duration::from_millis(ms));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::reps;

    #[test]
    fn every_profile_records_reflected() {
        for name in [
            "",
            "none",
            "low_latency",
            "ultra_low_latency",
            "high_throughput",
        ] {
            let config = RecordingConfig::from_profile(name).unwrap();
            assert_eq!(config.representations, reps(&[Representation::Reflected]));
        }
        assert!(RecordingConfig::from_profile("not_a_profile").is_err());
    }

    #[test]
    fn empty_config_file_changes_nothing() {
        let plain = RecordingConfig::from_profile("low_latency").unwrap();
        assert_eq!(RecordingConfig::resolve("low_latency", "").unwrap(), plain);
        assert_eq!(
            RecordingConfig::resolve("low_latency", "  \n").unwrap(),
            plain
        );
    }

    #[test]
    fn config_file_overrides_each_key() {
        let config = RecordingConfig::resolve(
            "none",
            "representations: [lenses, raw]\nmax_rows: 7\nmax_bytes: 0\nmax_latency_ms: 0\n",
        )
        .unwrap();
        assert_eq!(
            config.representations,
            reps(&[Representation::Raw, Representation::Lenses])
        );
        assert_eq!(config.flush.max_rows, 7);
        assert_eq!(config.flush.max_bytes, 0);
        assert_eq!(config.flush.max_latency, None);

        let partial = RecordingConfig::resolve("none", "max_latency_ms: 50\n").unwrap();
        assert_eq!(partial.representations, reps(&[Representation::Reflected]));
        assert_eq!(partial.flush.max_rows, 64);
        assert_eq!(partial.flush.max_latency, Some(Duration::from_millis(50)));
        assert_eq!(partial.recording_id, None);

        let layer = RecordingConfig::resolve("none", "recording_id: abc-123\n").unwrap();
        assert_eq!(layer.recording_id.as_deref(), Some("abc-123"));

        let live =
            RecordingConfig::resolve("none", "server_uri: rerun+http://127.0.0.1:9876/proxy\n")
                .unwrap();
        assert_eq!(
            live.server_uri.as_ref().map(ToString::to_string).as_deref(),
            Some("rerun+http://127.0.0.1:9876/proxy")
        );
        assert_eq!(partial.server_uri, None);
    }

    #[test]
    fn config_file_mistakes_are_errors() {
        assert!(RecordingConfig::resolve("none", "representations: []\n").is_err());
        assert!(RecordingConfig::resolve("none", "representations: [blobs]\n").is_err());
        assert!(RecordingConfig::resolve("none", "recording_id: ''\n").is_err());
        assert!(RecordingConfig::resolve("none", "server_uri: ''\n").is_err());
        assert!(RecordingConfig::resolve("none", "server_uri: localhost:9876\n").is_err());
        assert!(
            RecordingConfig::resolve("none", "server_uri: rerun+http://127.0.0.1:9876\n").is_err()
        );
        assert!(RecordingConfig::resolve("none", "maxRows: 3\n").is_err());
        assert!(RecordingConfig::resolve("none", "not: [valid\n").is_err());
    }

    #[test]
    fn representations_format_in_ladder_order() {
        let all = reps(&[
            Representation::Lenses,
            Representation::Raw,
            Representation::Reflected,
        ]);
        assert_eq!(format_representations(&all), "raw,reflected,lenses");
    }
}
