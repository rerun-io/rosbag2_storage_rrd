//! From ROS 2 messages to the chunks a recording asked for.
//!
//! A topic pushes each message into its [`Pipeline`] as it arrives, and asks for the
//! buffered rows back as a chunk when its flush policy says so. Each message is one row of
//! that chunk, and each form the recording keeps of it — the raw bytes as a blob, the
//! reflected Arrow struct — is a column; a form the message does not have is null at its
//! row. The ROS 2 lenses then derive archetypes from the struct.
//!
//! Because a message is one row whatever forms it has, playback replays every row of every
//! chunk that carries either column exactly once, without knowing what the recording was
//! asked for.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Context as _;
use arrow::array::{
    Array as _, FixedSizeListArray, FixedSizeListBuilder, ListArray, ListBuilder, UInt8Builder,
    UInt32Array,
};
use arrow::datatypes::{DataType, Field};
use re_lenses::{LensError, Lenses, OutputMode, Runtime};
use re_ros_msg::MessageSchema;
use re_ros_msg::reflection::{CdrArrowDecoder, CdrDecodeError, MessageDecodePlan};
use re_sdk_types::reflection::ComponentDescriptorExt as _;
use re_sdk_types::{
    ArchetypeName, ArrowDataType as _, ComponentDescriptor, ComponentIdentifier, components,
};
use rerun::EntityPath;
use rerun::log::{Chunk, ChunkId, TimeColumn};

use crate::config::{Representation, Representations, Unmatched};

// The three timelines every message carries are named for the
// `rosbag2_storage::SerializedBagMessage` fields they are filled from; see
// `cpp/rrd_storage.cpp`, which assigns `recv_timestamp` and `send_timestamp` from the
// times rosbag2 hands across the FFI.

/// Bag receive time — `SerializedBagMessage::recv_timestamp`.
pub const RECV_TIMESTAMP: &str = "recv_timestamp";

/// Publisher send time — `SerializedBagMessage::send_timestamp`.
pub const SEND_TIMESTAMP: &str = "send_timestamp";

/// Write order across the whole bag — `SerializedBagMessage::sequence_number`.
pub const SEQUENCE_NUMBER: &str = "sequence_number";

/// The message's own `header.stamp`, present only for types that carry one.
///
/// Not a `rosbag2` field — it belongs to the message, not the bag — so it is named for the
/// two `std_msgs/Header` fields it is read from: `header` and `stamp`. `re_mcap` calls the
/// same timeline `ros2_timestamp`, so a bag recorded here and the same data imported from
/// MCAP do not share this axis.
pub const HEADER_STAMP: &str = "header_stamp";

/// The message definition encoding we can reflect. rosbag2 also emits `ros2idl` (for types
/// declared in `.idl`) and `unknown`; both record as raw until `re_ros_msg` parses IDL.
const REFLECTABLE_ENCODING: &str = "ros2msg";

/// Time columns buffered alongside the message columns.
#[derive(Default)]
struct Times {
    recv: Vec<i64>,
    send: Vec<i64>,
    sequence: Vec<i64>,
}

impl Times {
    fn push(&mut self, recv: i64, send: i64, sequence: i64) {
        self.recv.push(recv);
        self.send.push(send);
        self.sequence.push(sequence);
    }

    fn len(&self) -> usize {
        self.recv.len()
    }

    /// The three timelines every row carries, whatever its type.
    fn columns(&self) -> Vec<TimeColumn> {
        vec![
            TimeColumn::new_timestamp_nanos_since_epoch(RECV_TIMESTAMP, self.recv.iter().copied()),
            TimeColumn::new_timestamp_nanos_since_epoch(SEND_TIMESTAMP, self.send.iter().copied()),
            TimeColumn::new_sequence(SEQUENCE_NUMBER, self.sequence.iter().copied()),
        ]
    }
}

/// The rows buffered since the last flush: one per message, holding each form the
/// recording keeps of it as a column.
///
/// A form a message does not have — the struct of one that failed to decode, the raw copy
/// of one that decoded when raw was not asked for — is null at its row, so the columns
/// always cover the same rows.
struct Rows {
    times: Times,

    /// Raw CDR: every row when raw is kept, otherwise only the rows that failed to decode.
    blobs: FixedSizeListBuilder<ListBuilder<UInt8Builder>>,
    blob_rows: usize,

    /// Per row, whether the decoder holds it. The decoder drops the rows it cancelled when
    /// it finishes; [`Pipeline::take`] puts them back as nulls from this.
    decoded: Vec<bool>,
}

impl Default for Rows {
    fn default() -> Self {
        Self {
            times: Times::default(),
            blobs: blob_list_builder(),
            blob_rows: 0,
            decoded: Vec::new(),
        }
    }
}

impl Rows {
    fn len(&self) -> usize {
        self.times.len()
    }

    /// Appends one row, with its raw bytes if they are being kept.
    fn push(&mut self, blob: Option<&[u8]>, decoded: bool, recv: i64, send: i64, sequence: i64) {
        self.times.push(recv, send, sequence);
        self.decoded.push(decoded);
        if let Some(cdr) = blob {
            self.blobs.values().values().append_slice(cdr);
            self.blobs.values().append(true);
            self.blobs.append(true);
            self.blob_rows += 1;
        } else {
            self.blobs.values().append_null();
            self.blobs.append(false);
        }
    }
}

/// The decoder for a topic whose `.msg` definition could be reflected.
struct Reflection {
    plan: Arc<MessageDecodePlan>,
    decoder: CdrArrowDecoder,

    /// `<pkg>.msg.<Type>:message` — what the ROS 2 lenses match on.
    descriptor: ComponentDescriptor,
}

impl Reflection {
    fn new(type_name: &str, schema_encoding: &str, schema_text: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            schema_encoding == REFLECTABLE_ENCODING,
            "schema encoding '{schema_encoding}' is not '{REFLECTABLE_ENCODING}'"
        );

        let schema_text =
            std::str::from_utf8(schema_text).context("message definition is not valid UTF-8")?;

        // An empty definition parses into a spec that looks usable but describes nothing,
        // so decoding against it would silently truncate every message. rosbag2 leaves the
        // definition empty whenever it cannot find the type.
        anyhow::ensure!(
            !schema_text.trim().is_empty(),
            "message definition is empty"
        );

        // rosbag2 writes a `.msg` with its top-level block first and a separator line only
        // before each dependency. A service or action definition is delimited from its
        // first line, and parsed as a `.msg` it describes the request alone.
        anyhow::ensure!(
            !schema_text
                .lines()
                .find(|line| !line.trim().is_empty())
                .is_some_and(re_ros_msg::is_schema_separator),
            "definition is a delimited interface (.srv or .action), not a .msg"
        );
        let schema = MessageSchema::parse(type_name, schema_text)
            .context("failed to parse the ROS 2 message definition")?;
        let plan = Arc::new(
            MessageDecodePlan::from_schema(&schema)
                .context("failed to resolve the message definition into a decode plan")?,
        );

        Ok(Self {
            decoder: CdrArrowDecoder::new(Arc::clone(&plan), 0),
            plan,
            descriptor: reflected_descriptor(type_name)?,
        })
    }
}

/// How one topic's messages become chunks, fixed when the topic is created.
pub struct Pipeline {
    entity: EntityPath,

    /// Whether every message keeps its raw bytes; otherwise only the ones that fail to
    /// decode do.
    keep_raw: bool,

    /// `None` when the recording never asks for anything reflected, when the definition
    /// could not be reflected, or after decoding failed unrecoverably. Without it every
    /// message is kept raw, whatever was asked for: nothing is dropped.
    reflection: Option<Reflection>,

    rows: Rows,

    /// Shared by every topic; `None` unless the recording asks for lenses.
    lenses: Option<Arc<LensSet>>,
    runtime: Arc<Runtime>,

    /// What this topic actually holds; see [`Self::representations`].
    stored: Representations,

    bytes: usize,
}

impl Pipeline {
    /// Prepares a topic, reflecting its `.msg` definition if anything asked for needs it.
    ///
    /// A definition we cannot reflect is not an error: the topic still records, raw. The
    /// `.msg` text is stored either way (see [`crate::statics`]), so such a topic can be
    /// reflected later by a tool that understands it.
    pub fn new(
        entity: EntityPath,
        representations: &Representations,
        type_name: &str,
        schema_encoding: &str,
        schema_text: &[u8],
        lenses: Option<Arc<LensSet>>,
        runtime: Arc<Runtime>,
    ) -> Self {
        let needs_reflection = representations.contains(&Representation::Reflected)
            || representations.contains(&Representation::Lenses);

        let reflection = if needs_reflection {
            match Reflection::new(type_name, schema_encoding, schema_text) {
                Ok(reflection) => Some(reflection),
                Err(err) => {
                    log::warn!(
                        "Topic '{entity}' ({type_name}, schema encoding '{schema_encoding}') \
                         will be recorded raw: {err:#}"
                    );
                    None
                }
            }
        } else {
            None
        };

        let stored = stored(representations, reflection.as_ref(), lenses.as_deref());

        Self {
            entity,
            keep_raw: representations.contains(&Representation::Raw),
            reflection,
            rows: Rows::default(),
            lenses,
            runtime,
            stored,
            bytes: 0,
        }
    }

    /// What this topic actually holds.
    ///
    /// This differs from what the recording asked for where the topic cannot oblige: a
    /// definition that cannot be reflected is kept raw whatever was asked, and a topic no
    /// lens matches keeps its reflected form in place of lens output — or nothing at all
    /// under [`Unmatched::Drop`].
    pub fn representations(&self) -> &Representations {
        &self.stored
    }

    /// Whether this topic is being kept in a form that can be replayed: raw or reflected.
    pub fn is_replayable(&self) -> bool {
        self.stored.contains(&Representation::Raw)
            || self.stored.contains(&Representation::Reflected)
    }

    /// Whether this topic records nothing at all: no lens matches it and the recording asks
    /// for [`Unmatched::Drop`].
    pub fn records_nothing(&self) -> bool {
        self.stored.is_empty()
    }

    /// Rows buffered since the last flush.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.len() == 0
    }

    /// Payload bytes buffered since the last flush, which the byte limit counts.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Buffers one message as one row, in every form the recording keeps of it.
    pub fn push(&mut self, cdr: &[u8], recv: i64, send: i64, sequence: i64) {
        let decoded = match self
            .reflection
            .as_mut()
            .map(|reflection| reflection.decoder.decode_message(cdr))
        {
            None => false,
            Some(Ok(())) => true,

            // The row is already cancelled inside the decoder, so the next message decodes
            // as usual; this one is kept verbatim instead.
            Some(Err(CdrDecodeError::Message(err))) => {
                log::warn!(
                    "Keeping a message on '{}' raw, it failed to decode: {err:#}",
                    self.entity
                );
                false
            }

            // The Arrow builders could not be returned to a row boundary, so this decoder
            // is finished and the rows it held are gone. Without raw copies, the rows
            // buffered alongside them go too. Keep the topic recording, raw.
            Some(Err(CdrDecodeError::Unrecoverable(err))) => {
                self.reflection = None;
                let lost = if self.keep_raw {
                    0
                } else {
                    self.bytes = 0;
                    std::mem::take(&mut self.rows).len()
                };
                log::error!(
                    "Recording '{}' raw from here on, its Arrow builders could not be \
                     recovered ({lost} buffered rows lost): {err}",
                    self.entity
                );
                false
            }
        };

        let blob = (self.keep_raw || !decoded).then_some(cdr);
        self.rows.push(blob, decoded, recv, send, sequence);
        self.bytes += cdr.len();
    }

    /// Takes every buffered row out as chunks: the one holding the messages, plus whatever
    /// the lenses derive from it.
    pub fn flush(&mut self) -> anyhow::Result<Vec<Chunk>> {
        Ok(match self.take()? {
            Some(chunk) => self.apply_lenses(chunk),
            None => Vec::new(),
        })
    }

    /// Takes the buffered rows out as one chunk, or `None` if there are none.
    fn take(&mut self) -> anyhow::Result<Option<Chunk>> {
        if self.rows.len() == 0 {
            return Ok(None);
        }
        // Rows and builders leave together, so a failure below cannot leave one behind
        // without the other.
        let mut rows = std::mem::take(&mut self.rows);
        self.bytes = 0;

        let mut time_columns = rows.times.columns();
        let mut columns: Vec<(ComponentDescriptor, ListArray)> = Vec::new();

        if rows.blob_rows > 0 {
            columns.push((raw_descriptor(), rows.blobs.finish().into()));
        }

        if let Some(reflection) = self.reflection.as_mut() {
            let messages = reflection.decoder.finish();
            if !messages.is_empty() {
                // A row that failed to decode has no stamp to give, so a chunk holding one
                // goes without the timeline rather than inventing a value for it.
                let complete = rows.decoded.iter().all(|&decoded| decoded);
                if complete && let Some(stamps) = header_stamps(&reflection.plan, &messages) {
                    time_columns.push(TimeColumn::new_timestamp_nanos_since_epoch(
                        HEADER_STAMP,
                        stamps,
                    ));
                }
                let messages = scatter(messages, &rows.decoded)?;
                columns.push((reflection.descriptor.clone(), messages.into()));
            }
        }

        let chunk = Chunk::from_auto_row_ids(
            ChunkId::new(),
            self.entity.clone(),
            timelines(time_columns),
            columns.into_iter().collect(),
        )
        .context("failed to build a chunk of messages")?;
        Ok(Some(chunk))
    }

    /// Derives archetypes from the message chunk, if the recording asked for them.
    ///
    /// With lenses, the chunk is never sent as-is: what of it survives is decided by the
    /// output mode chosen in [`lenses_for`].
    fn apply_lenses(&self, chunk: Chunk) -> Vec<Chunk> {
        let Some(lenses) = &self.lenses else {
            return vec![chunk];
        };

        let mut out = Vec::new();
        for result in lenses.apply(&chunk, &self.runtime) {
            match result {
                Ok(chunk) => out.push(chunk),
                Err(partial) => {
                    for err in partial.errors() {
                        log::error!("Lens failed on '{}': {err}", self.entity);
                    }
                    if let Some(chunk) = partial.partial_chunk() {
                        out.push(chunk);
                    }
                }
            }
        }
        out
    }
}

/// Puts the rows the decoder dropped back as nulls, so the struct column lines up with
/// the others. `dense` holds one row per `true` in `decoded`, in order.
fn scatter(dense: FixedSizeListArray, decoded: &[bool]) -> anyhow::Result<FixedSizeListArray> {
    if decoded.iter().all(|&decoded| decoded) {
        return Ok(dense);
    }

    let mut next = 0;
    let indices: UInt32Array = decoded
        .iter()
        .map(|&decoded| {
            decoded.then(|| {
                let index = next;
                next += 1;
                index
            })
        })
        .collect();
    let scattered = arrow::compute::take(&dense, &indices, None)
        .context("failed to line the decoded messages up with their rows")?;
    scattered
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .cloned()
        .context("lining the decoded messages up with their rows changed their type")
}

/// What a topic will actually hold, given what was asked for; see
/// [`Pipeline::representations`].
fn stored(
    asked: &Representations,
    reflection: Option<&Reflection>,
    lenses: Option<&LensSet>,
) -> Representations {
    let mut stored = Representations::new();

    let Some(reflection) = reflection else {
        stored.insert(Representation::Raw);
        return stored;
    };
    if asked.contains(&Representation::Raw) {
        stored.insert(Representation::Raw);
    }
    if asked.contains(&Representation::Reflected) {
        stored.insert(Representation::Reflected);
    }
    if let Some(lenses) = lenses {
        if lenses.matches(&reflection.descriptor) {
            stored.insert(Representation::Lenses);
        } else if lenses.keeps_unmatched {
            stored.insert(Representation::Reflected);
        }
    }
    stored
}

/// The ROS 2 lenses a recording applies, and what they consume.
pub struct LensSet {
    lenses: Lenses,

    /// The reflected message types some lens takes as input. A topic whose type is not
    /// among them is one no lens matches.
    inputs: BTreeSet<ComponentIdentifier>,

    /// Whether the struct of a topic no lens matched is forwarded; `false` only under
    /// [`Unmatched::Drop`].
    keeps_unmatched: bool,
}

impl LensSet {
    /// Whether some lens consumes this reflected message type.
    fn matches(&self, reflected: &ComponentDescriptor) -> bool {
        self.inputs.contains(&reflected.component)
    }

    fn apply<'a>(
        &'a self,
        chunk: &'a Chunk,
        runtime: &'a Runtime,
    ) -> impl Iterator<Item = Result<Chunk, LensError>> + 'a {
        self.lenses.apply(chunk, runtime)
    }
}

/// The lenses a recording applies, or `None` if it does not ask for any.
///
/// When the reflected form is also kept, the lenses forward the struct column alongside
/// what they derive, so one chunk carries both. Otherwise only the outputs are forwarded,
/// plus the struct column of any topic no lens matched, unless the recording asked for
/// [`Unmatched::Drop`]. The blob column is no lens's input and is forwarded in every mode
/// but that one.
pub fn lenses_for(
    representations: &Representations,
    unmatched: Unmatched,
) -> anyhow::Result<Option<LensSet>> {
    if !representations.contains(&Representation::Lenses) {
        return Ok(None);
    }
    let mode = match unmatched {
        // The reflected form was asked for in its own right, so it is forwarded whether or
        // not a lens consumed it.
        _ if representations.contains(&Representation::Reflected) => OutputMode::ForwardAll,

        // Nothing is lost: a topic no lens matched keeps its struct.
        Unmatched::Keep => OutputMode::ForwardUnmatched,

        // Only what the lenses derived, so the recording can be stacked as a layer on top
        // of one that already holds the messages. A topic no lens matched records nothing.
        Unmatched::Drop => OutputMode::DropUnmatched,
    };

    let mut lenses = Lenses::new(mode);
    let mut inputs = BTreeSet::new();
    for lens in re_lenses::semantic::ros2msg::all().context("failed to build the ROS 2 lenses")? {
        inputs.insert(lens.input());
        lenses = lenses.add_lens(lens);
    }
    Ok(Some(LensSet {
        lenses,
        inputs,
        keeps_unmatched: mode != OutputMode::DropUnmatched,
    }))
}

/// `rosbag2.SerializedBagMessage:serialized_data` — the CDR bytes exactly as they went
/// over the wire.
///
/// Named for the `rosbag2_storage` struct the bytes arrive in and the field they arrive as.
/// That struct's other fields are already recorded elsewhere: `recv_timestamp`,
/// `send_timestamp` and `sequence_number` are timelines, `topic_name` is the entity path.
pub fn raw_descriptor() -> ComponentDescriptor {
    ComponentDescriptor::partial("serialized_data")
        .with_builtin_archetype(ArchetypeName::from("rosbag2.SerializedBagMessage"))
}

/// `sensor_msgs/msg/Imu` → `sensor_msgs.msg.Imu:message`.
///
/// This matches what the MCAP importer writes, so the same lenses apply to bags recorded
/// either way.
pub fn reflected_descriptor(type_name: &str) -> anyhow::Result<ComponentDescriptor> {
    let archetype_name = ArchetypeName::try_new(type_name.replace('/', "."))
        .context("ROS type name is not a usable archetype name")?;
    Ok(ComponentDescriptor::partial("message").with_builtin_archetype(archetype_name))
}

fn timelines(
    columns: Vec<TimeColumn>,
) -> rerun::external::nohash_hasher::IntMap<rerun::TimelineName, TimeColumn> {
    columns
        .into_iter()
        .map(|column| (*column.timeline().name(), column))
        .collect()
}

/// A builder for a column of `Blob`s, one per message.
fn blob_list_builder() -> FixedSizeListBuilder<ListBuilder<UInt8Builder>> {
    // The bytes of a blob are always present, matching `components::Blob::arrow_data_type()`.
    let bytes = ListBuilder::<UInt8Builder>::default()
        .with_field(Arc::new(Field::new_list_field(DataType::UInt8, false)));

    // The outer list is the per-row component list of a chunk column, and Rerun's
    // canonical form for that is a nullable `item` field.
    let component_list_field = Field::new_list_field(components::Blob::arrow_data_type(), true);
    FixedSizeListBuilder::new(bytes, 1).with_field(Arc::new(component_list_field))
}

/// Reads the `header_stamp` timeline off the decoded Arrow columns.
///
/// Returns `None` for a type that carries no `header.stamp`, and for the rare case of a
/// stamp that cannot be represented — dropping the timeline rather than the messages.
fn header_stamps(plan: &MessageDecodePlan, messages: &FixedSizeListArray) -> Option<Vec<i64>> {
    let nanos = plan.timestamp_nanos(messages).ok().flatten()?;
    nanos.into_iter().map(|n| i64::try_from(n).ok()).collect()
}

/// The components a topic's messages are stored under, one per replayable form.
///
/// Built once per topic, so telling its chunks apart costs a comparison of interned
/// identifiers rather than a descriptor per chunk.
pub struct Columns {
    raw: ComponentIdentifier,

    /// `None` for a type name that cannot be an archetype name; every chunk on such a
    /// topic is raw.
    reflected: Option<ComponentIdentifier>,
}

impl Columns {
    pub fn for_type(type_name: &str) -> Self {
        Self {
            raw: raw_descriptor().component,
            reflected: reflected_descriptor(type_name)
                .ok()
                .map(|descriptor| descriptor.component),
        }
    }

    pub fn raw(&self) -> ComponentIdentifier {
        self.raw
    }

    pub fn reflected(&self) -> Option<ComponentIdentifier> {
        self.reflected
    }

    /// Whether a chunk holds messages: it carries the blob column, the struct column, or
    /// both. Anything else on a topic's entity is lens output.
    pub fn holds_messages<'a>(
        &self,
        components: impl IntoIterator<Item = &'a ComponentIdentifier>,
    ) -> bool {
        components
            .into_iter()
            .any(|component| *component == self.raw || Some(*component) == self.reflected)
    }

    /// Whether a chunk carries the struct column, and so has rows that replay by
    /// re-encoding.
    pub fn holds_structs<'a>(
        &self,
        components: impl IntoIterator<Item = &'a ComponentIdentifier>,
    ) -> bool {
        components
            .into_iter()
            .any(|component| Some(*component) == self.reflected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::statics::blob_at;
    use crate::testing::{cdr_string, reps};

    const TYPE: &str = "std_msgs/msg/String";
    const DEFINITION: &[u8] = b"string data\n";

    /// A length that runs past the end of the buffer.
    const CORRUPT: [u8; 8] = [0x00, 0x01, 0x00, 0x00, 0xff, 0xff, 0xff, 0x7f];

    fn pipeline_for(asked: &[Representation], type_name: &str, definition: &[u8]) -> Pipeline {
        let asked = reps(asked);
        let lenses = lenses_for(&asked, Unmatched::Keep).unwrap().map(Arc::new);
        Pipeline::new(
            EntityPath::from("/topic"),
            &asked,
            type_name,
            "ros2msg",
            definition,
            lenses,
            re_lenses::default_runtime(),
        )
    }

    fn pipeline(reps: &[Representation]) -> Pipeline {
        pipeline_for(reps, TYPE, DEFINITION)
    }

    /// Pushes `payloads` and flushes.
    fn run(pipeline: &mut Pipeline, payloads: &[Vec<u8>]) -> Vec<Chunk> {
        for (i, payload) in payloads.iter().enumerate() {
            let i = i64::try_from(i).unwrap();
            pipeline.push(payload, 1_000 + i, 900 + i, i);
        }
        pipeline.flush().unwrap()
    }

    fn strings(texts: &[&str]) -> Vec<Vec<u8>> {
        texts.iter().map(|text| cdr_string(text)).collect()
    }

    /// How many rows of `chunk` hold a blob and how many a struct; `None` where the column
    /// is absent altogether.
    fn forms(chunk: &Chunk) -> (Option<usize>, Option<usize>) {
        let columns = Columns::for_type(TYPE);
        let rows_with = |component: Option<ComponentIdentifier>| {
            component
                .and_then(|component| chunk.components().get(component))
                .map(|column| column.list_array.len() - column.list_array.null_count())
        };
        (
            rows_with(Some(columns.raw())),
            rows_with(columns.reflected()),
        )
    }

    fn has_component(chunk: &Chunk, identifier: &str) -> bool {
        chunk
            .components()
            .keys()
            .any(|component| component.as_str() == identifier)
    }

    #[test]
    fn raw_round_trips_every_message_byte_for_byte() {
        let chunks = run(
            &mut pipeline(&[Representation::Raw]),
            &strings(&["a", "bb", "ccc"]),
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (Some(3), None));

        let column = chunks[0]
            .components()
            .get(raw_descriptor().component)
            .unwrap()
            .list_array
            .clone();
        for (row, text) in ["a", "bb", "ccc"].iter().enumerate() {
            assert_eq!(blob_at(&column, row).unwrap(), cdr_string(text));
        }
    }

    #[test]
    fn reflected_re_encodes_to_the_same_bytes() {
        let chunks = run(
            &mut pipeline(&[Representation::Reflected]),
            &strings(&["hello", "world"]),
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (None, Some(2)));

        let plan = Arc::new(
            MessageDecodePlan::from_schema(
                &MessageSchema::parse(TYPE, std::str::from_utf8(DEFINITION).unwrap()).unwrap(),
            )
            .unwrap(),
        );
        let column = chunks[0]
            .components()
            .get(reflected_descriptor(TYPE).unwrap().component)
            .unwrap()
            .list_array
            .clone();
        let structs = column
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        assert_eq!(
            crate::replay::encode(&plan, structs, 0).unwrap(),
            cdr_string("hello")
        );
        assert_eq!(
            crate::replay::encode(&plan, structs, 1).unwrap(),
            cdr_string("world")
        );
    }

    #[test]
    fn a_message_kept_in_two_forms_is_one_row() {
        let mut pipeline = pipeline(&[Representation::Raw, Representation::Reflected]);
        assert!(pipeline.is_empty());
        let first = run(&mut pipeline, &strings(&["one", "two"]));
        assert_eq!(pipeline.len(), 0);
        assert_eq!(pipeline.bytes(), 0);
        let second = run(&mut pipeline, &strings(&["three"]));

        assert_eq!(first.len(), 1);
        assert_eq!(first[0].num_rows(), 2);
        assert_eq!(forms(&first[0]), (Some(2), Some(2)));
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].num_rows(), 1);
        assert_eq!(forms(&second[0]), (Some(1), Some(1)));
        assert!(pipeline.flush().unwrap().is_empty());
    }

    #[test]
    fn a_corrupt_message_keeps_its_bytes_in_the_row_its_struct_would_have_filled() {
        let payloads = vec![cdr_string("fine"), CORRUPT.to_vec(), cdr_string("fine too")];

        let chunks = run(&mut pipeline(&[Representation::Reflected]), &payloads);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 3);
        assert_eq!(forms(&chunks[0]), (Some(1), Some(2)));
        let blobs = chunks[0]
            .components()
            .get(raw_descriptor().component)
            .unwrap()
            .list_array
            .clone();
        assert_eq!(blob_at(&blobs, 0), None);
        assert_eq!(blob_at(&blobs, 1).unwrap(), CORRUPT);
        assert_eq!(blob_at(&blobs, 2), None);

        // With raw kept anyway, the corrupt message is simply a row with no struct.
        let chunks = run(
            &mut pipeline(&[Representation::Raw, Representation::Reflected]),
            &payloads[..2],
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 2);
        assert_eq!(forms(&chunks[0]), (Some(2), Some(1)));
    }

    #[test]
    fn lenses_pass_a_row_without_a_struct_through() {
        let payloads = vec![cdr_string("fine"), CORRUPT.to_vec(), cdr_string("fine too")];
        let chunks = run(
            &mut pipeline(&[Representation::Reflected, Representation::Lenses]),
            &payloads,
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 3);
        assert_eq!(forms(&chunks[0]), (Some(1), Some(2)));
        assert!(has_component(&chunks[0], "TextDocument:text"));
    }

    #[test]
    fn an_unreflectable_topic_is_kept_raw_whatever_was_asked() {
        let mut pipeline = pipeline_for(
            &[Representation::Lenses],
            "nonexistent_msgs/msg/Mystery",
            b"",
        );
        assert!(pipeline.is_replayable());
        assert_eq!(pipeline.representations(), &reps(&[Representation::Raw]));
        let chunks = run(&mut pipeline, &strings(&["x"]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (Some(1), None));
    }

    #[test]
    fn a_service_event_topic_is_kept_raw() {
        let mut pipeline = pipeline_for(
            &[Representation::Lenses],
            "test_msgs/srv/BasicTypes_Event",
            b"================================================================================\n\
              SRV: test_msgs/srv/BasicTypes\n\
              bool request_value\n\
              ---\n\
              bool response_value\n",
        );
        assert!(pipeline.is_replayable());
        assert_eq!(pipeline.representations(), &reps(&[Representation::Raw]));
        let chunks = run(&mut pipeline, &strings(&["x"]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (Some(1), None));
    }

    #[test]
    fn lenses_derive_archetypes_and_keep_the_struct_only_when_asked() {
        // A lens matches `std_msgs/msg/String`, so lenses alone leave nothing to replay.
        let mut lenses_only = pipeline(&[Representation::Lenses]);
        assert!(!lenses_only.is_replayable());
        assert_eq!(
            lenses_only.representations(),
            &reps(&[Representation::Lenses])
        );
        let chunks = run(&mut lenses_only, &strings(&["hello"]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (None, None));
        assert!(has_component(&chunks[0], "TextDocument:text"));

        let mut both = pipeline(&[Representation::Reflected, Representation::Lenses]);
        assert!(both.is_replayable());
        assert_eq!(
            both.representations(),
            &reps(&[Representation::Reflected, Representation::Lenses])
        );
        let chunks = run(&mut both, &strings(&["hello"]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (None, Some(1)));
        assert!(has_component(&chunks[0], "TextDocument:text"));

        // The blob is no lens's input, so it rides along with what the lenses derived.
        let chunks = run(
            &mut pipeline(&[Representation::Raw, Representation::Lenses]),
            &strings(&["hello"]),
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(forms(&chunks[0]), (Some(1), None));
        assert!(has_component(&chunks[0], "TextDocument:text"));
    }

    /// Three `float64`s behind the 4-byte CDR encapsulation header.
    fn vec3_cdr() -> Vec<u8> {
        let mut cdr = vec![0x00, 0x01, 0x00, 0x00];
        for value in [1.0_f64, 2.0, 3.0] {
            cdr.extend_from_slice(&value.to_le_bytes());
        }
        cdr
    }

    #[test]
    fn dropping_the_unmatched_leaves_a_topic_no_lens_matches_with_nothing() {
        // `geometry_msgs/msg/Vector3` reflects fine but no ROS 2 lens matches it, so it is
        // exactly the topic the two modes disagree about.
        const VEC3: &str = "geometry_msgs/msg/Vector3";
        const VEC3_DEF: &[u8] = b"float64 x\nfloat64 y\nfloat64 z\n";

        for (unmatched, kept) in [
            (Unmatched::Keep, reps(&[Representation::Reflected])),
            (Unmatched::Drop, reps(&[])),
        ] {
            let asked = reps(&[Representation::Lenses]);
            let lenses = lenses_for(&asked, unmatched).unwrap().map(Arc::new);
            let mut pipeline = Pipeline::new(
                EntityPath::from("/unmatched"),
                &asked,
                VEC3,
                "ros2msg",
                VEC3_DEF,
                lenses,
                re_lenses::default_runtime(),
            );
            assert_eq!(pipeline.representations(), &kept, "{unmatched:?}");
            assert_eq!(pipeline.records_nothing(), kept.is_empty(), "{unmatched:?}");

            pipeline.push(&vec3_cdr(), 1, 1, 0);
            let chunks = pipeline.flush().unwrap();
            assert_eq!(
                !chunks.is_empty(),
                !kept.is_empty(),
                "{unmatched:?} produced {} chunks",
                chunks.len()
            );
        }
    }

    #[test]
    fn a_topic_no_lens_matches_keeps_its_struct() {
        let mut pipeline = pipeline_for(
            &[Representation::Lenses],
            "std_msgs/msg/Int32",
            b"int32 data\n",
        );
        assert!(pipeline.is_replayable());
        assert_eq!(
            pipeline.representations(),
            &reps(&[Representation::Reflected])
        );
        pipeline.push(&[0x00, 0x01, 0x00, 0x00, 7, 0, 0, 0], 1, 1, 0);
        let chunks = pipeline.flush().unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(has_component(&chunks[0], "std_msgs.msg.Int32:message"));
    }
}
