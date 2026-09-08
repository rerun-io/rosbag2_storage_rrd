//! The chunk index read out of an `.rrd` footer.
//!
//! A complete recording ends with a manifest listing every chunk: its entity, its time
//! range, its row count, and where its bytes are. That is enough to plan playback without
//! reading any message data — which is what lets the reader open in constant time, seek by
//! lookup rather than by rescanning, and replay backwards without holding the whole
//! recording.
//!
//! A recording that was killed mid-write has no footer. That is not an error here: the
//! reader falls back to scanning, the same way `re_mcap` recovers a truncated MCAP.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context as _;
use re_sdk_types::ComponentIdentifier;
use rerun::EntityPath;
use rerun::external::re_log_encoding::{self, RrdManifest};
use rerun::log::{Chunk, ChunkId};

/// Timeline the index is ordered by — the same one rosbag2 replays in.
use crate::pipeline::RECV_TIMESTAMP;

/// Where one chunk sits in the recording, and what it covers.
pub struct ChunkRef {
    pub id: ChunkId,
    pub entity: EntityPath,
    pub num_rows: u64,

    /// Earliest and latest `recv_timestamp` in the chunk.
    pub start: i64,
    pub end: i64,

    /// The components the chunk carries; they tell its representation apart.
    pub components: Vec<ComponentIdentifier>,
}

/// Every chunk in a recording, ordered by start time.
pub struct ChunkIndex {
    file: std::fs::File,
    manifest: Arc<RrdManifest>,

    /// The Rerun recording id the file was written under.
    pub recording_id: String,

    /// Message chunks, ascending by `(start, end)`.
    pub chunks: Vec<ChunkRef>,

    /// How many non-static chunks the recording holds, on any timeline — including ones
    /// `chunks` leaves out because they are not on the timeline rosbag2 replays.
    pub temporal_chunks: usize,

    /// `max_end_through[i]` is the latest message time in any of `chunks[..=i]`.
    ///
    /// The chunks are sorted by start time, so a chunk early in the list can still end
    /// after every chunk that follows it. Reverse playback needs the latest time still
    /// unread, and that is a prefix maximum over ends, not any single chunk's `end`.
    pub max_end_through: Vec<i64>,

    /// Chunks holding static data — channel and schema info, and the bag metadata.
    pub statics: Vec<ChunkId>,
}

impl ChunkIndex {
    /// Reads the footer and builds the index, or `None` if the recording has no footer.
    pub fn open(path: &str) -> anyhow::Result<Option<Self>> {
        let file = std::fs::File::open(path).with_context(|| format!("failed to open '{path}'"))?;

        // A footer that cannot be read is treated the same as one that is absent: a
        // recording killed mid-write can end in anything, and the scan fallback exists
        // precisely to recover it. Only a file that cannot be opened at all is an error.
        let footer = match pollster::block_on(re_log_encoding::read_rrd_footer(&file)) {
            Ok(footer) => footer,
            Err(err) => {
                log::warn!(
                    "Could not read the footer of '{path}', falling back to scanning: {err}"
                );
                None
            }
        };
        let Some(footer) = footer else {
            return Ok(None);
        };

        // A bag is one recording; if a file somehow holds several, the first is ours.
        let Some((store_id, raw)) = footer.manifests.into_iter().next() else {
            return Ok(None);
        };
        let manifest = Arc::new(
            RrdManifest::try_new(&raw).context("failed to interpret the recording's manifest")?,
        );

        let mut summaries = summarize(&manifest);

        let ids = manifest.col_chunk_ids();
        let entities: Vec<EntityPath> = manifest.col_chunk_entity_path_iter().collect();
        let is_static: Vec<bool> = manifest.col_chunk_is_static_iter().collect();
        let num_rows = manifest.col_chunk_num_rows();

        let mut chunks = Vec::new();
        let mut statics = Vec::new();
        let mut temporal_chunks = 0;

        for (i, &id) in ids.iter().enumerate() {
            if is_static.get(i).copied().unwrap_or(false) {
                statics.push(id);
                continue;
            }
            temporal_chunks += 1;

            // A chunk with no `recv_timestamp` is not a bag message.
            let Some(summary) = summaries.remove(&id) else {
                continue;
            };
            let Some(entity) = entities.get(i).cloned() else {
                continue;
            };
            chunks.push(ChunkRef {
                id,
                entity,
                num_rows: num_rows.get(i).copied().unwrap_or(0),
                start: summary.start,
                end: summary.end,
                components: summary.components,
            });
        }

        chunks.sort_by_key(|chunk| (chunk.start, chunk.end));

        let mut index = Self {
            file,
            manifest,
            recording_id: store_id.recording_id().as_str().to_owned(),
            chunks,
            temporal_chunks,
            max_end_through: Vec::new(),
            statics,
        };
        index.recompute_max_end_through();
        Ok(Some(index))
    }

    /// Drops every chunk `keep` rejects, as if it had never been in the recording.
    pub fn retain(&mut self, keep: impl FnMut(&ChunkRef) -> bool) {
        self.chunks.retain(keep);
        self.recompute_max_end_through();
    }

    fn recompute_max_end_through(&mut self) {
        let mut max_end = i64::MIN;
        self.max_end_through = self
            .chunks
            .iter()
            .map(|chunk| {
                max_end = max_end.max(chunk.end);
                max_end
            })
            .collect();
    }

    /// Loads the given chunks by seeking straight to their bytes.
    pub fn load(&self, ids: &[ChunkId]) -> anyhow::Result<Vec<Arc<Chunk>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        pollster::block_on(re_log_encoding::read_chunks(
            &self.file,
            &self.manifest,
            ids,
        ))
        .context("failed to read chunks from the recording")
    }

    /// Total messages across every topic.
    pub fn message_count(&self) -> usize {
        let total: u64 = self.chunks.iter().map(|chunk| chunk.num_rows).sum();
        usize::try_from(total).unwrap_or(usize::MAX)
    }

    /// Earliest and latest message time, or `None` for a recording with no messages.
    pub fn time_range(&self) -> Option<(i64, i64)> {
        let start = self.chunks.first()?.start;
        let end = self.chunks.iter().map(|chunk| chunk.end).max()?;
        Some((start, end))
    }
}

/// What the manifest says about one chunk, on the one timeline rosbag2 replays.
struct ChunkSummary {
    start: i64,
    end: i64,
    components: Vec<ComponentIdentifier>,
}

/// Collapses the manifest's per-component entries into one summary per chunk.
///
/// The manifest indexes time by entity, timeline and component; a bag only cares about
/// when each chunk's messages happened and which components it carries.
// Min/max accumulation plus a component list that is only ever searched, so hash
// iteration order cannot affect the result.
#[expect(clippy::iter_over_hash_type)]
fn summarize(manifest: &RrdManifest) -> HashMap<ChunkId, ChunkSummary> {
    let mut summaries: HashMap<ChunkId, ChunkSummary> = HashMap::new();

    for timelines in manifest.temporal_map().values() {
        for (timeline, components) in timelines {
            if timeline.name().as_str() != RECV_TIMESTAMP {
                continue;
            }
            for (component, entries) in components {
                for (id, entry) in entries {
                    let start = entry.time_range.min().as_i64();
                    let end = entry.time_range.max().as_i64();
                    let summary = summaries.entry(*id).or_insert(ChunkSummary {
                        start,
                        end,
                        components: Vec::new(),
                    });
                    summary.start = summary.start.min(start);
                    summary.end = summary.end.max(end);
                    summary.components.push(*component);
                }
            }
        }
    }

    summaries
}
