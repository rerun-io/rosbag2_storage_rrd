//! What a topic is, recorded as static data on its entity.
//!
//! This is what lets a bare `.rrd` answer topic, type and definition queries without
//! `metadata.yaml`. The writer builds it and the reader takes it apart; keeping both here
//! is what keeps them in step.
//!
//! One component per `rosbag2_storage` struct rosbag2 hands across the FFI —
//! `rosbag2.TopicMetadata:metadata` and `rosbag2.MessageDefinition:definition` — each
//! holding that struct whole. A struct per component, not a component per field, matches
//! how a reflected ROS message is stored (`sensor_msgs.msg.Image:message`). The struct is
//! the unit that arrived and the unit the reader wants back, and a chunk manifest gains a
//! column per entity/component pair, so a field per column would widen it by topics times
//! fields.
//!
//! Descriptors are minted here rather than generated from archetype definitions, as
//! [`crate::pipeline::reflected_descriptor`] mints one per ROS type. Nothing visualizes or
//! consumes these archetypes; they are addresses.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, FixedSizeListArray, ListArray, StringArray, StructArray,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Fields};
use re_sdk_types::reflection::ComponentDescriptorExt as _;
use re_sdk_types::{ArchetypeName, ComponentDescriptor, ComponentIdentifier};
use rerun::EntityPath;
use rerun::log::{Chunk, ChunkId};

/// Recording property holding rosbag2's own `BagMetadata`, so the `.rrd` describes itself
/// and playback needs no `metadata.yaml` beside it.
pub const METADATA_PROPERTY: &str = "rosbag2";

/// Archetype names, one per `rosbag2_storage` struct the record comes from.
const TOPIC_METADATA: &str = "rosbag2.TopicMetadata";
const MESSAGE_DEFINITION: &str = "rosbag2.MessageDefinition";

/// `rosbag2_storage::TopicMetadata`'s fields, minus the storage-assigned `id`.
const FIELD_NAME: &str = "name";
const FIELD_TYPE: &str = "type";
const FIELD_SERIALIZATION_FORMAT: &str = "serialization_format";
const FIELD_OFFERED_QOS_PROFILES: &str = "offered_qos_profiles";
const FIELD_TYPE_DESCRIPTION_HASH: &str = "type_description_hash";

/// `rosbag2_storage::MessageDefinition`'s fields, minus `topic_type` and `type_hash`:
/// `cpp/rrd_storage.cpp` fills both from the topic's own `type` and `type_description_hash`
/// on the way back out, so storing them would duplicate the metadata struct.
const FIELD_ENCODING: &str = "encoding";
const FIELD_ENCODED_MESSAGE_DEFINITION: &str = "encoded_message_definition";

/// `rosbag2.TopicMetadata:metadata` — the whole struct as one component.
pub fn metadata_descriptor() -> ComponentDescriptor {
    ComponentDescriptor::partial("metadata")
        .with_builtin_archetype(ArchetypeName::from(TOPIC_METADATA))
}

/// `rosbag2.MessageDefinition:definition` — the `.msg` text and how it is encoded.
pub fn definition_descriptor() -> ComponentDescriptor {
    ComponentDescriptor::partial("definition")
        .with_builtin_archetype(ArchetypeName::from(MESSAGE_DEFINITION))
}

/// What rosbag2's `create_topic` says a topic is, borrowed for as long as it takes to
/// write it down.
pub struct TopicRecord<'a> {
    pub topic: &'a str,
    pub type_name: &'a str,
    pub schema_encoding: &'a str,
    pub schema_text: &'a [u8],
    pub qos_profiles: &'a str,
    pub type_hash: &'a str,
}

/// One row of a struct column: the struct, wrapped in the per-row instance list a chunk
/// column is made of.
fn one_row(fields: Vec<Arc<Field>>, columns: Vec<ArrayRef>) -> anyhow::Result<FixedSizeListArray> {
    let record = StructArray::try_new(Fields::from(fields), columns, None)?;

    // Rerun's canonical form for a component list is a nullable `item` field.
    let item = Field::new_list_field(record.data_type().clone(), true);
    Ok(FixedSizeListArray::new(
        Arc::new(item),
        1,
        Arc::new(record) as ArrayRef,
        None,
    ))
}

fn utf8_row(values: &[(&str, &str)]) -> anyhow::Result<FixedSizeListArray> {
    let fields = values
        .iter()
        .map(|(name, _)| Arc::new(Field::new(*name, DataType::Utf8, false)))
        .collect();
    let columns = values
        .iter()
        .map(|(_, value)| Arc::new(StringArray::from(vec![*value])) as ArrayRef)
        .collect();
    one_row(fields, columns)
}

/// Everything known about a topic at `create_topic` time, as one static chunk.
///
/// `offered_qos_profiles` and `type_description_hash` are here because rosbag2 wants them
/// back: `ros2 bag reindex` compares them. How the topic's messages are stored is *not*
/// here: the message chunks say that themselves, by the columns they carry (see
/// [`crate::pipeline`]).
pub fn topic_info(entity: &EntityPath, record: &TopicRecord<'_>) -> anyhow::Result<Chunk> {
    let metadata = utf8_row(&[
        (FIELD_NAME, record.topic),
        (FIELD_TYPE, record.type_name),
        (FIELD_SERIALIZATION_FORMAT, "cdr"),
        (FIELD_OFFERED_QOS_PROFILES, record.qos_profiles),
        (FIELD_TYPE_DESCRIPTION_HASH, record.type_hash),
    ])?;

    // The definition is bytes, not text: rosbag2 hands it over as an encoded blob and an
    // `ros2idl` definition is not required to be UTF-8.
    let definition = one_row(
        vec![
            Arc::new(Field::new(FIELD_ENCODING, DataType::Utf8, false)),
            Arc::new(Field::new(
                FIELD_ENCODED_MESSAGE_DEFINITION,
                DataType::Binary,
                false,
            )),
        ],
        vec![
            Arc::new(StringArray::from(vec![record.schema_encoding])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![record.schema_text])) as ArrayRef,
        ],
    )?;

    Chunk::from_auto_row_ids(
        ChunkId::new(),
        entity.clone(),
        Default::default(),
        [
            (metadata_descriptor(), metadata.into()),
            (definition_descriptor(), definition.into()),
        ]
        .into_iter()
        .collect(),
    )
    .map_err(Into::into)
}

/// The static components found on one entity while reading a recording.
#[derive(Default)]
pub struct TopicStatics {
    pub topic: Option<String>,
    pub type_name: Option<String>,
    pub schema_encoding: Option<String>,
    pub schema_text: Option<Vec<u8>>,
    pub qos_profiles: Option<String>,
    pub type_hash: Option<String>,
}

/// Reads a topic's static record, or the bag metadata, out of a static chunk.
pub fn collect(
    chunk: &Chunk,
    statics: &mut BTreeMap<EntityPath, TopicStatics>,
    metadata_yaml: &mut Option<String>,
) {
    if let Some(yaml) = read_metadata_property(chunk) {
        *metadata_yaml = Some(yaml);
        return;
    }

    let metadata = struct_row(chunk, &metadata_descriptor());
    let definition = struct_row(chunk, &definition_descriptor());
    if metadata.is_none() && definition.is_none() {
        return;
    }

    let entry = statics.entry(chunk.entity_path().clone()).or_default();
    if let Some(metadata) = metadata {
        entry.topic = struct_string(&metadata, FIELD_NAME);
        entry.type_name = struct_string(&metadata, FIELD_TYPE);
        entry.qos_profiles = struct_string(&metadata, FIELD_OFFERED_QOS_PROFILES);
        entry.type_hash = struct_string(&metadata, FIELD_TYPE_DESCRIPTION_HASH);
    }
    if let Some(definition) = definition {
        entry.schema_encoding = struct_string(&definition, FIELD_ENCODING);
        entry.schema_text = struct_bytes(&definition, FIELD_ENCODED_MESSAGE_DEFINITION);
    }
}

/// The blob at `row` of a `Blob` column, or `None` if there is none.
pub fn blob_at(list: &ListArray, row: usize) -> Option<Vec<u8>> {
    if row >= list.len() || list.is_null(row) {
        return None;
    }
    let inner = list.value(row);
    let bytes = inner.as_any().downcast_ref::<ListArray>()?;
    if bytes.is_empty() {
        return None;
    }
    let values = bytes.value(0);
    let values = values.as_any().downcast_ref::<UInt8Array>()?;
    Some(values.values().to_vec())
}

pub fn column(chunk: &Chunk, component: ComponentIdentifier) -> Option<ListArray> {
    chunk
        .components()
        .get(component)
        .map(|batch| batch.list_array.clone())
}

/// The one-row struct a struct-valued component holds, if this chunk has that component.
fn struct_row(chunk: &Chunk, descriptor: &ComponentDescriptor) -> Option<StructArray> {
    let list = column(chunk, descriptor.component)?;
    if list.is_empty() || list.is_null(0) {
        return None;
    }
    let instances = list.value(0);
    let record = instances.as_any().downcast_ref::<StructArray>()?;
    (!record.is_empty()).then(|| record.clone())
}

fn struct_field<'a>(record: &'a StructArray, name: &str) -> Option<&'a ArrayRef> {
    let (index, _) = record.fields().find(name)?;
    record.columns().get(index)
}

fn struct_string(record: &StructArray, name: &str) -> Option<String> {
    let column = struct_field(record, name)?;
    let strings = column.as_any().downcast_ref::<StringArray>()?;
    (!strings.is_empty() && !strings.is_null(0)).then(|| strings.value(0).to_owned())
}

fn struct_bytes(record: &StructArray, name: &str) -> Option<Vec<u8>> {
    let column = struct_field(record, name)?;
    let bytes = column.as_any().downcast_ref::<BinaryArray>()?;
    (!bytes.is_empty() && !bytes.is_null(0)).then(|| bytes.value(0).to_vec())
}

/// Reads the `rosbag2` recording property, if this chunk is it.
fn read_metadata_property(chunk: &Chunk) -> Option<String> {
    // Exactly where `send_property` puts it: `/__properties/rosbag2`. A recorded topic
    // is never under `__properties`, so nothing else can match.
    let property_path = EntityPath::properties().join(&EntityPath::from(METADATA_PROPERTY));
    if chunk.entity_path() != &property_path {
        return None;
    }
    let descriptor = ComponentIdentifier::from("TextDocument:text");
    let list = chunk.components().get(descriptor)?.list_array.clone();
    let inner = list.value(0);
    let strings = inner.as_any().downcast_ref::<StringArray>()?;
    (!strings.is_empty()).then(|| strings.value(0).to_owned())
}
