//! The read path: an `.rrd` back out as rosbag2 messages.
//!
//! Playback streams. The file is scanned once when the bag is opened to learn its topics,
//! message count and time range — keeping only that metadata, never the message data —
//! and then re-opened and walked forward as messages are read.
//!
//! Chunks are written grouped by topic, but rosbag2 replays in receive-time order across
//! all of them, so reading is a merge. Only the chunks whose time ranges straddle the
//! current playback position are held in memory; each is dropped as soon as its last row
//! is emitted. That keeps resident memory proportional to how far topics are interleaved
//! rather than to the size of the bag.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context as _;
use arrow::array::{Array as _, ListArray, StructArray};
use re_ros_msg::MessageSchema;
use re_ros_msg::reflection::MessageDecodePlan;
use re_sdk_types::ComponentIdentifier;
use rerun::external::re_log_encoding;
use rerun::external::re_log_types::LogMsg;
use rerun::log::Chunk;
use rerun::{EntityPath, TimelineName};

use crate::index::ChunkIndex;
use crate::pipeline::{Columns, RECV_TIMESTAMP, SEND_TIMESTAMP};
use crate::replay;
use crate::statics::{self, TopicStatics, blob_at, column};

/// What a topic looked like when it was recorded.
pub struct TopicInfo {
    pub name: String,
    pub type_name: String,
    pub schema_encoding: String,
    pub schema_text: Vec<u8>,
    pub qos_profiles: String,
    pub type_hash: String,

    /// Messages recorded on this topic, which `ros2 bag reindex` checks.
    pub message_count: usize,

    /// The components its chunks are told apart by.
    columns: Columns,

    /// How to re-encode the reflected struct; built only for a topic that has struct rows
    /// to re-encode. Every row without one is a blob and replays verbatim.
    plan: Option<Arc<MessageDecodePlan>>,
}

/// What a scan learned about one message chunk without keeping it.
struct ScannedChunk {
    entity: EntityPath,
    components: Vec<ComponentIdentifier>,
    num_rows: usize,

    /// Earliest `recv_timestamp`, or `i64::MAX` for a chunk with none.
    earliest: i64,
    time_range: Option<(i64, i64)>,
}

impl ScannedChunk {
    fn new(chunk: &Chunk) -> Self {
        let mut earliest = i64::MAX;
        let mut time_range: Option<(i64, i64)> = None;
        for time in times(chunk, RECV_TIMESTAMP).unwrap_or_default() {
            earliest = earliest.min(time);
            time_range = Some(match time_range {
                None => (time, time),
                Some((start, end)) => (start.min(time), end.max(time)),
            });
        }
        Self {
            entity: chunk.entity_path().clone(),
            components: chunk.components().keys().copied().collect(),
            num_rows: chunk.num_rows(),
            earliest,
            time_range,
        }
    }
}

/// The message columns of one chunk. Every row has at least one of the two forms: the raw
/// bytes where they were kept, the decoded struct where the message decoded.
struct ChunkData {
    /// Raw CDR, replayed verbatim.
    blobs: Option<ListArray>,

    /// Decoded message structs, re-encoded to CDR as they are read.
    messages: Option<Arc<StructArray>>,
}

/// Total order over rows: receive time, then chunk position, then row within the chunk.
///
/// Messages sharing a timestamp keep the order they were written in, and reverse
/// playback is an exact mirror of forward.
type SortKey = (i64, usize, usize);

/// One chunk in the merge window, with a cursor over the rows still to be emitted.
struct Window {
    topic: usize,
    recv_timestamps: Vec<i64>,
    send_timestamps: Vec<i64>,
    data: ChunkData,
    next: usize,
    rows_reversed: bool,

    /// Where this chunk sits in the recording; the tie-breaker in [`SortKey`].
    seq: usize,
}

impl Window {
    /// The receive time of the next row, or `None` once the chunk is drained.
    fn head(&self) -> Option<(i64, i64)> {
        let recv = *self.recv_timestamps.get(self.next)?;
        Some((
            recv,
            self.send_timestamps.get(self.next).copied().unwrap_or(recv),
        ))
    }

    /// The [`SortKey`] of the next row, or `None` once the chunk is drained.
    fn key(&self) -> Option<SortKey> {
        let recv = *self.recv_timestamps.get(self.next)?;
        Some((recv, self.seq, self.data_row(self.next)))
    }

    /// Rows are held in file order; reverse playback walks them from the end.
    fn reversed(&mut self) {
        self.recv_timestamps.reverse();
        self.send_timestamps.reverse();
        self.rows_reversed = true;
    }

    /// The row index into the chunk's data for the cursor's current position.
    fn data_row(&self, row: usize) -> usize {
        if self.rows_reversed {
            self.recv_timestamps.len() - 1 - row
        } else {
            row
        }
    }
}

/// One `.rrd` opened for reading.
pub struct Reader {
    path: String,

    /// The footer's chunk index, when the recording has one.
    ///
    /// With it, playback is planned from the index and chunks are loaded by offset. A
    /// recording killed mid-write has no footer, and then `source` scans instead.
    index: Option<ChunkIndex>,

    /// Position in `index.chunks` for the indexed path.
    at: usize,

    /// Forward pass over the file, used only when there is no footer.
    source: Option<Box<dyn Iterator<Item = LogMsg>>>,

    topics: Vec<TopicInfo>,
    topic_index: BTreeMap<EntityPath, usize>,

    /// Chunks straddling the current playback position.
    window: Vec<Window>,

    /// Receive time of the most recent row emitted, so a seek knows which way it is going.
    position: i64,

    /// Topics to replay; empty means all of them.
    filter: Vec<String>,

    /// [`SortKey`] of the last row handed to the caller, so a filter change can rebuild
    /// the pass and resume exactly after it. `None` until something is read, and cleared
    /// by a seek, which makes `position` the resume boundary instead.
    cursor: Option<SortKey>,

    metadata_yaml: Option<String>,
    recording_id: String,
    message_count: usize,
    time_range: Option<(i64, i64)>,

    /// `earliest_remaining[i]` is the earliest message time in any chunk from `i` onwards,
    /// in file order. Read-ahead stops as soon as this exceeds the candidate row's time.
    earliest_remaining: Vec<i64>,

    /// How many message chunks the forward pass has consumed.
    next_chunk: usize,

    /// Replay newest-first.
    ///
    /// The file can only be read forwards, so reverse playback holds every chunk at once
    /// rather than a window. That cost is paid only when reverse playback is requested.
    reverse: bool,

    /// The message handed out by the last read, kept alive so the pointers the C++ side
    /// received stay valid until it asks for the next one.
    last_message: Option<Message>,
}

impl Reader {
    /// Opens `path`, learning its shape without keeping any message data.
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let mut index = ChunkIndex::open(path)?;

        let mut found: BTreeMap<EntityPath, TopicStatics> = BTreeMap::new();
        let mut metadata_yaml = None;
        let mut scanned = Vec::new();
        let mut recording_id = None;

        // With a footer, everything the bag needs to describe itself is in the manifest,
        // and only the handful of static chunks have to be read. Without one, fall back
        // to reading the whole thing to learn its shape.
        if let Some(index) = &index {
            recording_id = Some(index.recording_id.clone());
            for chunk in index.load(&index.statics)? {
                statics::collect(&chunk, &mut found, &mut metadata_yaml);
            }
        } else {
            for msg in Self::scan(path)? {
                recording_id.get_or_insert_with(|| msg.store_id().recording_id().to_string());
                let Some(chunk) = chunk_of(&msg) else {
                    continue;
                };
                if chunk.is_static() {
                    statics::collect(&chunk, &mut found, &mut metadata_yaml);
                } else {
                    scanned.push(ScannedChunk::new(&chunk));
                }
            }
        }
        let recording_id =
            recording_id.with_context(|| format!("'{path}' holds no recording at all"))?;

        // A recording written before `rosbag2.TopicMetadata` replaced the MCAP-shaped
        // channel/schema pair describes no topics we recognize, so no chunk would match a
        // topic and the bag would open empty. Say what happened instead. Such a recording
        // keeps its chunks on a differently named timeline, so count chunks on any.
        let holds_data = index
            .as_ref()
            .map_or_else(|| !scanned.is_empty(), |index| index.temporal_chunks > 0);
        anyhow::ensure!(
            !found.is_empty() || !holds_data,
            "'{path}' holds data but describes no topics; it was written by a version of \
             this plugin that recorded them as MCAP channel and schema pairs. Re-record it, \
             or convert it with the version that wrote it."
        );

        // Topic order is the order rosbag2 reports them in, so keep it stable.
        let mut topics = Vec::new();
        let mut topic_index = BTreeMap::new();
        for (entity, statics) in found {
            let Some(info) = topic_info(statics) else {
                continue;
            };
            topic_index.insert(entity, topics.len());
            topics.push(info);
        }

        // Only chunks that hold messages count, and every row of one is a message; see
        // [`Columns::holds_messages`]. Topics with struct rows are noted along the way, so
        // that only they pay for a decode plan.
        let mut counts = vec![0_usize; topics.len()];
        let mut re_encodes = vec![false; topics.len()];
        let mut earliest_remaining = Vec::new();
        let (message_count, time_range) = {
            let mut count = |entity: &EntityPath, components: &[ComponentIdentifier], rows| {
                let Some(&topic) = topic_index.get(entity) else {
                    return false;
                };
                let columns = &topics[topic].columns;
                if !columns.holds_messages(components) {
                    return false;
                }
                counts[topic] += rows;
                re_encodes[topic] |= columns.holds_structs(components);
                true
            };

            if let Some(index) = &mut index {
                index.retain(|chunk| {
                    count(
                        &chunk.entity,
                        &chunk.components,
                        usize::try_from(chunk.num_rows).unwrap_or(usize::MAX),
                    )
                });
                (index.message_count(), index.time_range())
            } else {
                let mut message_count = 0;
                let mut time_range: Option<(i64, i64)> = None;
                for chunk in &scanned {
                    if !count(&chunk.entity, &chunk.components, chunk.num_rows) {
                        continue;
                    }
                    message_count += chunk.num_rows;
                    if let Some((start, end)) = chunk.time_range {
                        time_range = Some(match time_range {
                            None => (start, end),
                            Some((s, e)) => (s.min(start), e.max(end)),
                        });
                    }
                }

                // Suffix minima give read-ahead an exact stopping rule in the absence of
                // an index: chunks are written in flush order, which is not time order.
                // Every message chunk is included, replayed or not, because the forward
                // pass walks them all.
                earliest_remaining = vec![i64::MAX; scanned.len() + 1];
                for i in (0..scanned.len()).rev() {
                    earliest_remaining[i] = scanned[i].earliest.min(earliest_remaining[i + 1]);
                }

                (message_count, time_range)
            }
        };

        for (topic, info) in topics.iter_mut().enumerate() {
            info.message_count = counts[topic];
            if re_encodes[topic] {
                info.plan = decode_plan(&info.type_name, &info.schema_text);
            }
        }

        let mut reader = Self {
            path: path.to_owned(),
            index,
            at: 0,
            source: None,
            topics,
            topic_index,
            window: Vec::new(),
            position: i64::MIN,
            filter: Vec::new(),
            cursor: None,
            metadata_yaml,
            recording_id,
            message_count,
            time_range,
            earliest_remaining,
            next_chunk: 0,
            reverse: false,
            last_message: None,
        };
        reader.restart()?;

        Ok(reader)
    }

    /// A fresh forward pass over the file, decoding messages as they are pulled.
    fn scan(path: &str) -> anyhow::Result<impl Iterator<Item = LogMsg>> {
        let file = std::fs::File::open(path).with_context(|| format!("failed to open '{path}'"))?;
        let decoder = re_log_encoding::DecoderApp::decode_eager(std::io::BufReader::new(file))
            .with_context(|| format!("'{path}' is not a readable .rrd file"))?;

        Ok(decoder.filter_map(Result::ok))
    }

    /// Drops the merge state and starts the forward pass again from the beginning.
    fn restart(&mut self) -> anyhow::Result<()> {
        self.window.clear();
        self.next_chunk = 0;
        self.position = i64::MIN;
        self.cursor = None;

        if self.index.is_some() {
            self.at = 0;
            return Ok(());
        }

        let file = std::fs::File::open(&self.path)
            .with_context(|| format!("failed to open '{}'", self.path))?;
        let decoder = re_log_encoding::DecoderApp::decode_eager(std::io::BufReader::new(file))
            .with_context(|| format!("'{}' is not a readable .rrd file", self.path))?;

        self.source = Some(Box::new(decoder.filter_map(|msg| msg.ok())));

        Ok(())
    }

    pub fn topics(&self) -> &[TopicInfo] {
        &self.topics
    }

    pub fn metadata_yaml(&self) -> Option<&str> {
        self.metadata_yaml.as_deref()
    }

    /// The Rerun recording id the file was written under; a layer on top of this bag is
    /// recorded with the same one.
    pub fn recording_id(&self) -> &str {
        &self.recording_id
    }

    pub fn time_range(&self) -> Option<(i64, i64)> {
        self.time_range
    }

    pub fn message_count(&self) -> usize {
        self.message_count
    }

    /// Sets the replay direction, restarting the pass.
    pub fn set_reverse(&mut self, reverse: bool) -> anyhow::Result<()> {
        if self.reverse != reverse {
            self.reverse = reverse;
            self.restart()?;
        }
        Ok(())
    }

    /// Restricts playback to `topics`; an empty list replays everything.
    ///
    /// rosbag2's contract: a filter change takes effect in place, so messages after the
    /// last-read position that the old filter excluded must now be delivered. The scan
    /// path filters per row and satisfies that for free. The indexed path skips
    /// filtered-out chunks entirely, so it rebuilds the pass and winds back to the
    /// cursor — the same way the sqlite3 and mcap plugins re-prepare their iterators.
    pub fn set_filter(&mut self, topics: Vec<String>) -> anyhow::Result<()> {
        if topics == self.filter {
            return Ok(());
        }
        self.filter = topics;
        if self.index.is_some() {
            self.resume()?;
        }
        Ok(())
    }

    /// Rebuilds the pass and winds it back to just after the last row handed out.
    ///
    /// The resume boundary is the cursor when something has been read, and `position` —
    /// the last seek target — when nothing has. Winding compares full sort keys, so
    /// rows sharing the cursor's timestamp are neither re-delivered nor skipped.
    fn resume(&mut self) -> anyhow::Result<()> {
        let cursor = self.cursor;
        let position = self.position;

        // Nothing read and nothing sought: a fresh pass is already in the right place.
        // (`i64::MIN` is "before the start" only going forwards, so the general winding
        // below would drain a reverse pass completely.)
        if cursor.is_none() && position == i64::MIN {
            return self.restart();
        }

        self.restart()?;
        self.position = position;
        self.skip_index_to(position);

        while let Some((index, _)) = self.next_row()? {
            let Some(key) = self.window[index].key() else {
                break;
            };
            let passed = match cursor {
                Some(cursor) if self.reverse => key < cursor,
                Some(cursor) => key > cursor,
                None if self.reverse => key.0 <= position,
                None => key.0 >= position,
            };
            if passed {
                break;
            }
            self.advance(index);
        }

        self.position = position;
        self.cursor = cursor;
        Ok(())
    }

    /// Whether a topic is being replayed at all.
    fn wanted(&self, topic: usize) -> bool {
        self.filter.is_empty() || self.filter.contains(&self.topics[topic].name)
    }

    /// Moves the read head to the first message at or after `timestamp`.
    pub fn seek(&mut self, timestamp: i64) -> anyhow::Result<()> {
        self.restart()?;
        self.skip_index_to(timestamp);

        while let Some((index, row)) = self.next_row()? {
            let Some(recv_timestamp) = self.window[index].recv_timestamps.get(row).copied() else {
                break;
            };
            let reached = if self.reverse {
                recv_timestamp <= timestamp
            } else {
                recv_timestamp >= timestamp
            };
            if reached {
                break;
            }
            self.advance(index);
        }
        self.position = timestamp;
        self.cursor = None;

        Ok(())
    }

    /// Advances the index cursor past chunks that cannot contain `timestamp`.
    ///
    /// The index exists for this: a seek skips chunks without reading
    /// them, rather than decoding and discarding every message along the way.
    fn skip_index_to(&mut self, timestamp: i64) {
        let Some(index) = self.index.as_ref() else {
            return;
        };

        self.at = if self.reverse {
            index
                .chunks
                .iter()
                .rev()
                .position(|chunk| chunk.start <= timestamp)
                .unwrap_or(index.chunks.len())
        } else {
            index
                .chunks
                .iter()
                .position(|chunk| chunk.end >= timestamp)
                .unwrap_or(index.chunks.len())
        };
    }

    /// Pulls the next chunk out of the file and turns it into a window.
    fn pull(&mut self) -> anyhow::Result<Option<Window>> {
        if self.index.is_some() {
            return self.pull_indexed();
        }

        let Some(mut source) = self.source.take() else {
            return Ok(None);
        };

        while let Some(msg) = source.next() {
            let Some(chunk) = chunk_of(&msg).filter(|chunk| !chunk.is_static()) else {
                continue;
            };
            let Some(&topic) = self.topic_index.get(chunk.entity_path()) else {
                // A chunk on an entity with no channel info is not a bag message.
                continue;
            };

            self.next_chunk += 1;

            match load_window(&chunk, topic, &self.topics[topic], self.next_chunk - 1) {
                Ok(Some(mut window)) => {
                    if self.reverse {
                        window.reversed();
                    }
                    self.source = Some(source);
                    return Ok(Some(window));
                }
                Ok(None) => {}
                Err(err) => {
                    self.source = Some(source);
                    return Err(err);
                }
            }
        }

        Ok(None)
    }

    /// Loads the next chunk named by the index, in playback order.
    fn pull_indexed(&mut self) -> anyhow::Result<Option<Window>> {
        loop {
            let index = self.index.as_ref().expect("checked by the caller");
            if self.at >= index.chunks.len() {
                return Ok(None);
            }

            // Forwards walks the index from the start, backwards from the end.
            let position = if self.reverse {
                index.chunks.len() - 1 - self.at
            } else {
                self.at
            };
            self.at += 1;

            let chunk_ref = &index.chunks[position];
            let Some(&topic) = self.topic_index.get(&chunk_ref.entity) else {
                continue;
            };

            // Filtering here costs nothing: a skipped chunk is never read off disk, and a
            // widened filter re-consults the index rather than rewinding.
            if !self.wanted(topic) {
                continue;
            }

            let id = chunk_ref.id;
            let loaded = index.load(&[id])?;
            let Some(chunk) = loaded.into_iter().next() else {
                continue;
            };

            if let Some(mut window) = load_window(&chunk, topic, &self.topics[topic], position)? {
                if self.reverse {
                    window.reversed();
                }
                return Ok(Some(window));
            }
        }
    }

    /// The earliest message time in any chunk the index has not yet handed out.
    fn earliest_unread(&self) -> i64 {
        let Some(index) = self.index.as_ref() else {
            return self
                .earliest_remaining
                .get(self.next_chunk)
                .copied()
                .unwrap_or(i64::MAX);
        };

        if self.at >= index.chunks.len() {
            return if self.reverse { i64::MIN } else { i64::MAX };
        }

        // The index is sorted by start time, so going forwards the next chunk bounds
        // everything after it. Going backwards the bound is the latest end among the
        // unread prefix — a prefix maximum, since an early wide chunk can outlast every
        // chunk that follows it.
        if self.reverse {
            index.max_end_through[index.chunks.len() - 1 - self.at]
        } else {
            index.chunks[self.at].start
        }
    }

    /// Reads ahead until every chunk that could hold the next message is in the window.
    ///
    /// A row may be emitted only once no unread chunk can still hold an earlier one.
    /// `earliest_remaining` answers that exactly, so this reads the minimum needed and
    /// keeps resident memory to the chunks that genuinely overlap the current position.
    fn fill(&mut self) -> anyhow::Result<()> {
        // Without an index there is no way to know what a later chunk holds without
        // reading it, so reverse has to materialise everything.
        if self.reverse && self.index.is_none() {
            while let Some(window) = self.pull()? {
                self.window.push(window);
            }
            return Ok(());
        }

        loop {
            let candidate = if self.reverse {
                self.window.iter().filter_map(Window::head).max()
            } else {
                self.window.iter().filter_map(Window::head).min()
            };
            let unread = self.earliest_unread();

            let settled = match candidate {
                Some((recv_timestamp, _)) if self.reverse => unread < recv_timestamp,
                Some((recv_timestamp, _)) => unread > recv_timestamp,
                None => false,
            };
            if settled {
                return Ok(());
            }

            let exhausted = if self.reverse {
                unread == i64::MIN
            } else {
                unread == i64::MAX
            };
            if exhausted && candidate.is_some() {
                return Ok(());
            }

            let Some(window) = self.pull()? else {
                return Ok(());
            };
            self.window.push(window);
        }
    }

    /// The window holding the next message, and the row within it.
    fn next_row(&mut self) -> anyhow::Result<Option<(usize, usize)>> {
        self.fill()?;

        let keys = self
            .window
            .iter()
            .enumerate()
            .filter_map(|(i, w)| w.key().map(|key| (i, key)));

        let chosen = if self.reverse {
            keys.max_by_key(|(_, key)| *key)
        } else {
            keys.min_by_key(|(_, key)| *key)
        };

        Ok(chosen.map(|(i, _)| (i, self.window[i].next)))
    }

    /// Advances past the row at `index`, dropping the chunk once it is drained.
    ///
    /// Dropping as soon as the last row is out is what bounds resident memory.
    fn advance(&mut self, index: usize) {
        let window = &mut self.window[index];
        window.next += 1;
        self.position = window
            .recv_timestamps
            .get(window.next.saturating_sub(1))
            .copied()
            .unwrap_or(self.position);

        if self.window[index].head().is_none() {
            self.window.swap_remove(index);
        }
    }

    /// Consumes the next message in receive-time order.
    fn take(&mut self) -> anyhow::Result<Option<TakenRow>> {
        let Some((index, row)) = self.next_row()? else {
            return Ok(None);
        };

        let window = &self.window[index];
        let topic = window.topic;
        let recv_timestamp = window.recv_timestamps.get(row).copied().unwrap_or(0);
        let send_timestamp = window
            .send_timestamps
            .get(row)
            .copied()
            .unwrap_or(recv_timestamp);
        let data_row = window.data_row(row);
        let key = (recv_timestamp, window.seq, data_row);

        // The raw bytes where they were kept — exact, and no re-encoding — otherwise the
        // struct. A row has at least one of the two.
        let blob = window
            .data
            .blobs
            .as_ref()
            .and_then(|blobs| blob_at(blobs, data_row));
        let data = if let Some(bytes) = blob {
            bytes
        } else {
            let name = &self.topics[topic].name;
            let messages = window.data.messages.as_ref().with_context(|| {
                format!("topic '{name}' has neither bytes nor a struct at row {data_row}")
            })?;
            let plan = self.topics[topic]
                .plan
                .as_ref()
                .with_context(|| format!("topic '{name}' has struct rows but no decode plan"))?;
            replay::encode(plan, messages, data_row)?
        };

        self.advance(index);
        self.cursor = Some(key);
        Ok(Some((topic, recv_timestamp, send_timestamp, data)))
    }

    /// Reads the next message that passes the filter.
    ///
    /// The filter is applied before the row is materialised, so a filtered-out message
    /// costs a cursor step rather than a full CDR re-encode.
    pub fn read_next(&mut self) -> anyhow::Result<Option<Message>> {
        loop {
            let Some((index, _)) = self.next_row()? else {
                return Ok(None);
            };

            let topic = self.window[index].topic;
            if !self.wanted(topic) {
                self.advance(index);
                continue;
            }

            let Some((topic, recv_timestamp, send_timestamp, data)) = self.take()? else {
                return Ok(None);
            };

            return Ok(Some(Message {
                topic_name: self.topics[topic].name.clone(),
                recv_timestamp,
                send_timestamp,
                data,
            }));
        }
    }

    /// Reads the next message and keeps it alive for the caller to borrow.
    pub fn read_next_borrowed(&mut self) -> anyhow::Result<Option<&Message>> {
        self.last_message = self.read_next()?;
        Ok(self.last_message.as_ref())
    }
}

/// Topic index, receive time, send time and payload of one consumed row.
type TakenRow = (usize, i64, i64, Vec<u8>);

/// One message on its way back to rosbag2.
#[derive(Debug)]
pub struct Message {
    pub topic_name: String,
    pub recv_timestamp: i64,
    pub send_timestamp: i64,
    pub data: Vec<u8>,
}

/// Turns one chunk into a window over its rows, or `None` if it holds no messages.
fn load_window(
    chunk: &Chunk,
    topic: usize,
    info: &TopicInfo,
    seq: usize,
) -> anyhow::Result<Option<Window>> {
    let recv_timestamps = times(chunk, RECV_TIMESTAMP).unwrap_or_default();
    let send_timestamps = times(chunk, SEND_TIMESTAMP).unwrap_or_else(|| recv_timestamps.clone());

    let blobs = column(chunk, info.columns.raw());
    let messages = info
        .columns
        .reflected()
        .and_then(|reflected| column(chunk, reflected))
        .map(|list| {
            list.values()
                .as_any()
                .downcast_ref::<StructArray>()
                .cloned()
                .map(Arc::new)
                .context("decoded message column is not a struct array")
        })
        .transpose()?;

    // The index already filtered on this; the scan path relies on it.
    if blobs.is_none() && messages.is_none() {
        return Ok(None);
    }
    let data = ChunkData { blobs, messages };

    Ok(Some(Window {
        topic,
        recv_timestamps,
        send_timestamps,
        data,
        next: 0,
        rows_reversed: false,
        seq,
    }))
}

/// Turns what was found on an entity into a topic, if it is one.
///
/// The decode plan is filled in later, once it is known whether the topic has any struct
/// rows to re-encode; see [`Reader::open`].
fn topic_info(statics: TopicStatics) -> Option<TopicInfo> {
    let name = statics.topic?;
    let type_name = statics.type_name.unwrap_or_default();
    Some(TopicInfo {
        columns: Columns::for_type(&type_name),
        name,
        type_name,
        schema_encoding: statics.schema_encoding.unwrap_or_default(),
        schema_text: statics.schema_text.unwrap_or_default(),
        qos_profiles: statics.qos_profiles.unwrap_or_default(),
        type_hash: statics.type_hash.unwrap_or_default(),
        message_count: 0,
        plan: None,
    })
}

/// How to re-encode a topic's struct rows, or `None` if the definition cannot be reflected.
///
/// An empty definition is rejected for the same reason as on the write side: it parses into
/// a spec that describes nothing.
fn decode_plan(type_name: &str, schema_text: &[u8]) -> Option<Arc<MessageDecodePlan>> {
    std::str::from_utf8(schema_text)
        .ok()
        .filter(|text| !text.trim().is_empty())
        .and_then(|text| MessageSchema::parse(type_name, text).ok())
        .and_then(|schema| MessageDecodePlan::from_schema(&schema).ok())
        .map(Arc::new)
}

/// The chunk a log message carries, if it carries one.
fn chunk_of(msg: &LogMsg) -> Option<Chunk> {
    let LogMsg::ArrowMsg(_, arrow_msg) = msg else {
        return None;
    };
    Chunk::from_arrow_msg(arrow_msg).ok()
}

fn times(chunk: &Chunk, name: &'static str) -> Option<Vec<i64>> {
    chunk
        .timelines()
        .get(&TimelineName::from(name))
        .map(|column| column.times_raw().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::RecordingConfig;
    use crate::testing::cdr_string;
    use crate::writer::Writer;

    const CHATTER: &str = "/chatter";
    const MYSTERY: &str = "/mystery";
    const MESSAGES: usize = 10;
    const QOS: &str = "- history: 3\n  depth: 0\n";
    const TYPE_HASH: &str = "RIHS01_deadbeef";

    /// Declares the reflectable topic every test records on.
    fn create_chatter(writer: &mut Writer) -> usize {
        writer.create_topic(
            CHATTER,
            "std_msgs/msg/String",
            "ros2msg",
            b"string data\n",
            QOS,
            TYPE_HASH,
        )
    }

    fn open_writer(dir: &std::path::Path, representations: &str) -> (String, Writer) {
        let path = dir.join("bag.rrd").to_string_lossy().into_owned();
        let config =
            RecordingConfig::resolve("none", &format!("representations: [{representations}]\n"))
                .unwrap();
        let writer = Writer::open(&path, config).unwrap();
        (path, writer)
    }

    /// Records a reflectable topic and an unreflectable one, interleaved, in
    /// `representations`, handing over a metadata document the way `update_metadata` does.
    fn record(dir: &std::path::Path, representations: &str) -> String {
        record_with(dir, representations, Some("version: 9\n"))
    }

    fn record_with(
        dir: &std::path::Path,
        representations: &str,
        metadata_yaml: Option<&str>,
    ) -> String {
        let (path, mut writer) = open_writer(dir, representations);
        let chatter = create_chatter(&mut writer);
        let mystery = writer.create_topic(
            MYSTERY,
            "nonexistent_msgs/msg/Mystery",
            "ros2msg",
            b"",
            QOS,
            TYPE_HASH,
        );
        for i in 0..MESSAGES {
            let t = 1_000_000_000 + 2 * i64::try_from(i).unwrap() * 1_000_000;
            writer
                .write(chatter, &cdr_string(&format!("hello_{i}")), t, t - 1)
                .unwrap();
            writer
                .write(mystery, &cdr_string(&format!("raw_{i}")), t + 1_000_000, t)
                .unwrap();
        }
        if let Some(yaml) = metadata_yaml {
            writer.set_metadata(yaml);
        }
        writer.close().unwrap();
        path
    }

    fn drain(reader: &mut Reader) -> Vec<Message> {
        let mut out = Vec::new();
        while let Some(message) = reader.read_next().unwrap() {
            out.push(message);
        }
        out
    }

    #[test]
    fn every_replayable_set_replays_each_message_once() {
        for representations in [
            "raw",
            "reflected",
            "raw, reflected",
            "raw, lenses",
            "reflected, lenses",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = record(dir.path(), representations);
            let mut reader = Reader::open(&path).unwrap();

            assert_eq!(reader.message_count(), 2 * MESSAGES, "{representations}");
            for topic in reader.topics() {
                assert_eq!(topic.qos_profiles, QOS, "{representations}: {}", topic.name);
                assert_eq!(
                    topic.type_hash, TYPE_HASH,
                    "{representations}: {}",
                    topic.name
                );
                assert_eq!(
                    topic.message_count, MESSAGES,
                    "{representations}: {}",
                    topic.name
                );
            }

            let messages = drain(&mut reader);
            assert_eq!(messages.len(), 2 * MESSAGES, "{representations}");
            let chatter: Vec<_> = messages
                .iter()
                .filter(|m| m.topic_name == CHATTER)
                .collect();
            for (i, message) in chatter.iter().enumerate() {
                assert_eq!(
                    message.data,
                    cdr_string(&format!("hello_{i}")),
                    "{representations}"
                );
            }
        }
    }

    /// A forward seek from the middle of a pass must not hand out a chunk that was already
    /// resident a second time.
    #[test]
    fn seeking_forward_mid_pass_replays_each_remaining_message_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = record(dir.path(), "raw, reflected");
        let rows = |messages: Vec<Message>| -> Vec<(String, i64, Vec<u8>)> {
            messages
                .into_iter()
                .map(|m| (m.topic_name, m.recv_timestamp, m.data))
                .collect()
        };

        let all = rows(drain(&mut Reader::open(&path).unwrap()));

        let mut reader = Reader::open(&path).unwrap();
        reader.read_next().unwrap();
        reader.read_next().unwrap();
        reader.seek(all[3].1).unwrap();

        assert_eq!(rows(drain(&mut reader)), all[3..]);
    }

    /// A recording killed mid-write has no metadata document. How each topic is stored is
    /// on the topic itself, so a message kept in two forms is still replayed once.
    #[test]
    fn a_bag_without_a_metadata_document_still_replays_each_message_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = record_with(dir.path(), "raw, reflected", None);
        let mut reader = Reader::open(&path).unwrap();

        assert!(reader.metadata_yaml().is_none());
        assert_eq!(reader.message_count(), 2 * MESSAGES);
        assert_eq!(drain(&mut reader).len(), 2 * MESSAGES);
    }

    /// No lens matches `std_msgs/msg/Int32`, so under `lenses` alone it keeps its reflected
    /// struct — and that is what it replays from.
    #[test]
    fn a_topic_no_lens_matches_replays_from_its_kept_struct() {
        fn cdr_int32(value: i32) -> Vec<u8> {
            let mut out = vec![0x00, 0x01, 0x00, 0x00];
            out.extend_from_slice(&value.to_le_bytes());
            out
        }

        let dir = tempfile::tempdir().unwrap();
        let (path, mut writer) = open_writer(dir.path(), "lenses");
        let count = writer.create_topic(
            "/count",
            "std_msgs/msg/Int32",
            "ros2msg",
            b"int32 data\n",
            QOS,
            TYPE_HASH,
        );
        for i in 0..MESSAGES {
            let t = 1_000_000_000 + i64::try_from(i).unwrap();
            writer
                .write(count, &cdr_int32(i32::try_from(i).unwrap()), t, t)
                .unwrap();
        }
        writer.close().unwrap();

        let mut reader = Reader::open(&path).unwrap();
        assert_eq!(reader.message_count(), MESSAGES);
        let messages = drain(&mut reader);
        assert_eq!(messages.len(), MESSAGES);
        for (i, message) in messages.iter().enumerate() {
            assert_eq!(message.data, cdr_int32(i32::try_from(i).unwrap()));
        }
    }

    /// A lens consumes `std_msgs/msg/String`, so under `lenses` alone `/chatter` holds
    /// nothing to replay and plays nothing; the unreflectable topic, kept raw, still does.
    #[test]
    fn a_topic_a_lens_consumed_is_described_but_has_nothing_to_play() {
        let dir = tempfile::tempdir().unwrap();
        let path = record(dir.path(), "lenses");
        let mut reader = Reader::open(&path).unwrap();

        assert_eq!(reader.topics().len(), 2);
        assert_eq!(reader.message_count(), MESSAGES);
        for topic in reader.topics() {
            let expected = if topic.name == CHATTER { 0 } else { MESSAGES };
            assert_eq!(topic.message_count, expected, "{}", topic.name);
        }

        let messages = drain(&mut reader);
        assert_eq!(messages.len(), MESSAGES);
        assert!(messages.iter().all(|m| m.topic_name == MYSTERY));
    }

    /// A layer is a second recording written under the source's recording id, holding only
    /// what the lenses derived; Rerun merges the two.
    #[test]
    fn a_layer_shares_its_source_recording_id() {
        let dir = tempfile::tempdir().unwrap();
        let source = record(dir.path(), "raw");
        let source_id = Reader::open(&source).unwrap().recording_id().to_owned();
        assert!(!source_id.is_empty());

        let path = dir.path().join("layer.rrd").to_string_lossy().into_owned();
        let config = RecordingConfig::resolve(
            "none",
            &format!("representations: [lenses]\nunmatched: drop\nrecording_id: {source_id}\n"),
        )
        .unwrap();
        let mut writer = Writer::open(&path, config).unwrap();
        let chatter = create_chatter(&mut writer);
        writer.write(chatter, &cdr_string("hello"), 1, 1).unwrap();
        writer.close().unwrap();

        let layer = Reader::open(&path).unwrap();
        assert_eq!(layer.recording_id(), source_id);
        assert_eq!(layer.message_count(), 0);
    }

    /// A message that fails to decode keeps its bytes in the row its struct would have
    /// filled, and comes back byte-for-byte among the rest.
    #[test]
    fn a_message_that_failed_to_decode_replays_verbatim_among_the_rest() {
        let corrupt = vec![0x00, 0x01, 0x00, 0x00, 0xff, 0xff, 0xff, 0x7f];
        let payloads: Vec<Vec<u8>> = (0..MESSAGES)
            .map(|i| {
                if i == 3 {
                    corrupt.clone()
                } else {
                    cdr_string(&format!("hello_{i}"))
                }
            })
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let (path, mut writer) = open_writer(dir.path(), "reflected");
        let chatter = create_chatter(&mut writer);
        for (i, payload) in payloads.iter().enumerate() {
            let t = 1_000_000_000 + i64::try_from(i).unwrap();
            writer.write(chatter, payload, t, t).unwrap();
        }
        writer.close().unwrap();

        let mut reader = Reader::open(&path).unwrap();
        assert_eq!(reader.message_count(), MESSAGES);
        let replayed: Vec<Vec<u8>> = drain(&mut reader).into_iter().map(|m| m.data).collect();
        assert_eq!(replayed, payloads);
    }
}
