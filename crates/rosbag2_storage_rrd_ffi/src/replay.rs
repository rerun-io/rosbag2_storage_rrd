//! The playback path: decoded Arrow rows back out as CDR.
//!
//! This mirrors `re_ros_msg`'s CDR-to-Arrow decoding, walking the same
//! [`MessageDecodePlan`] in the same field order so that what comes out is what went in.
//!
//! Messages recorded as raw blobs never come through here — those bytes are replayed
//! exactly as they arrived, whatever their encapsulation.

use anyhow::Context as _;
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    ListArray, StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use re_cdr::{CdrWriter, LittleEndian};
use re_ros_msg::message_spec::{ArraySize, BuiltInType};
use re_ros_msg::reflection::{MessageDecodePlan, ValueLayout};

/// Classic CDR, little-endian, options zeroed — what ROS 2 publishes over DDS.
///
/// A message originally received big-endian is re-encoded little-endian rather than
/// reproduced byte for byte. The values are identical and every ROS 2 subscriber accepts
/// either, so this is a faithful replay but not always an identical one.
const ENCAPSULATION: [u8; 4] = [0x00, 0x01, 0x00, 0x00];

/// Encodes one row of a decoded message column back into a CDR message.
pub fn encode(plan: &MessageDecodePlan, root: &StructArray, row: usize) -> anyhow::Result<Vec<u8>> {
    // CDR alignment is measured from the start of the body, so the body is built on its
    // own and the encapsulation header prepended afterwards.
    let mut body = Vec::new();
    {
        let mut writer = CdrWriter::<LittleEndian>::new(&mut body);
        encode_message(plan, MessageDecodePlan::ROOT_ID, root, row, &mut writer)
            .with_context(|| format!("failed to re-encode {}", plan.schema_name()))?;
    }

    let mut message = Vec::with_capacity(ENCAPSULATION.len() + body.len());
    message.extend_from_slice(&ENCAPSULATION);
    message.append(&mut body);
    Ok(message)
}

/// Writes every field of one message, in ROS declaration order.
fn encode_message(
    plan: &MessageDecodePlan,
    message_id: usize,
    message: &StructArray,
    row: usize,
    writer: &mut CdrWriter<'_, LittleEndian>,
) -> anyhow::Result<()> {
    let layout = plan.message(message_id);
    anyhow::ensure!(
        layout.fields().len() == message.num_columns(),
        "message has {} Arrow columns but its plan declares {} fields",
        message.num_columns(),
        layout.fields().len()
    );

    for (field, column) in std::iter::zip(layout.fields(), message.columns()) {
        encode_value(plan, field.value(), column.as_ref(), row, writer)
            .with_context(|| format!("field `{}`", field.name()))?;
    }

    Ok(())
}

/// Writes one value: a scalar, a nested message, or an array of either.
fn encode_value(
    plan: &MessageDecodePlan,
    layout: &ValueLayout,
    array: &dyn Array,
    row: usize,
    writer: &mut CdrWriter<'_, LittleEndian>,
) -> anyhow::Result<()> {
    match layout {
        ValueLayout::BuiltIn(ty) => encode_builtin(ty, array, row, writer),

        ValueLayout::Message(message_id) => {
            let nested = downcast::<StructArray>(array, "struct")?;
            encode_message(plan, *message_id, nested, row, writer)
        }

        ValueLayout::Array { element, size } => {
            let list = downcast::<ListArray>(array, "list")?;
            let values = list.value(row);
            let len = values.len();

            // A fixed-size ROS array carries no length on the wire; everything else does.
            match size {
                ArraySize::Fixed(expected) => anyhow::ensure!(
                    len == *expected,
                    "fixed-size array holds {len} elements, expected {expected}"
                ),
                ArraySize::Bounded(_) | ArraySize::Unbounded => writer.write_sequence_length(len),
            }

            // Arrays of built-ins are the bulk of every real message — point cloud data,
            // image pixels, laser ranges. They get one downcast and, where the layout
            // allows, one memcpy.
            if let ValueLayout::BuiltIn(ty) = element.as_ref() {
                return encode_builtin_array(ty, values.as_ref(), writer);
            }

            for index in 0..len {
                encode_value(plan, element, values.as_ref(), index, writer)?;
            }
            Ok(())
        }
    }
}

/// Writes a whole array of ROS built-ins.
///
/// The point of this over looping [`encode_value`] is that the array is downcast **once**
/// rather than once per element: a 256 KiB point cloud is ~65k elements, and paying a
/// dynamic downcast for each one dominated everything else.
///
/// The elements themselves are still written one at a time. `re_cdr` 0.1.0 has
/// `read_numeric_vec` for bulk decoding but no bulk write, so the encode side stays
/// asymmetric until that lands; this is where `write_numeric_slice` would go.
fn encode_builtin_array(
    ty: &BuiltInType,
    array: &dyn Array,
    writer: &mut CdrWriter<'_, LittleEndian>,
) -> anyhow::Result<()> {
    /// Downcast once, then write each element.
    macro_rules! write_each {
        ($arrow:ty, $label:literal, $write:ident) => {{
            let values = downcast::<$arrow>(array, $label)?;
            for index in 0..values.len() {
                writer.$write(values.value(index));
            }
        }};
    }

    match ty {
        BuiltInType::Byte | BuiltInType::Char | BuiltInType::UInt8 => {
            write_each!(UInt8Array, "uint8", write_u8);
        }
        BuiltInType::Int8 => write_each!(Int8Array, "int8", write_i8),
        BuiltInType::Int16 => write_each!(Int16Array, "int16", write_i16),
        BuiltInType::UInt16 => write_each!(UInt16Array, "uint16", write_u16),
        BuiltInType::Int32 => write_each!(Int32Array, "int32", write_i32),
        BuiltInType::UInt32 => write_each!(UInt32Array, "uint32", write_u32),
        BuiltInType::Int64 => write_each!(Int64Array, "int64", write_i64),
        BuiltInType::UInt64 => write_each!(UInt64Array, "uint64", write_u64),
        BuiltInType::Float32 => write_each!(Float32Array, "float32", write_f32),
        BuiltInType::Float64 => write_each!(Float64Array, "float64", write_f64),
        BuiltInType::Bool => write_each!(BooleanArray, "bool", write_bool),
        BuiltInType::String(_) => write_each!(StringArray, "string", write_string),

        BuiltInType::WString(_) => anyhow::bail!("ROS 2 `wstring` is not supported"),
    }

    Ok(())
}

/// Writes one ROS built-in scalar.
fn encode_builtin(
    ty: &BuiltInType,
    array: &dyn Array,
    row: usize,
    writer: &mut CdrWriter<'_, LittleEndian>,
) -> anyhow::Result<()> {
    match ty {
        BuiltInType::Bool => writer.write_bool(downcast::<BooleanArray>(array, "bool")?.value(row)),
        BuiltInType::Byte | BuiltInType::Char | BuiltInType::UInt8 => {
            writer.write_u8(downcast::<UInt8Array>(array, "uint8")?.value(row));
        }
        BuiltInType::Int8 => writer.write_i8(downcast::<Int8Array>(array, "int8")?.value(row)),
        BuiltInType::Int16 => writer.write_i16(downcast::<Int16Array>(array, "int16")?.value(row)),
        BuiltInType::UInt16 => {
            writer.write_u16(downcast::<UInt16Array>(array, "uint16")?.value(row));
        }
        BuiltInType::Int32 => writer.write_i32(downcast::<Int32Array>(array, "int32")?.value(row)),
        BuiltInType::UInt32 => {
            writer.write_u32(downcast::<UInt32Array>(array, "uint32")?.value(row));
        }
        BuiltInType::Int64 => writer.write_i64(downcast::<Int64Array>(array, "int64")?.value(row)),
        BuiltInType::UInt64 => {
            writer.write_u64(downcast::<UInt64Array>(array, "uint64")?.value(row));
        }
        BuiltInType::Float32 => {
            writer.write_f32(downcast::<Float32Array>(array, "float32")?.value(row));
        }
        BuiltInType::Float64 => {
            writer.write_f64(downcast::<Float64Array>(array, "float64")?.value(row));
        }
        BuiltInType::String(_) => {
            writer.write_string(downcast::<StringArray>(array, "string")?.value(row));
        }

        // Never recorded in the first place: a schema containing one is gated to blobs.
        BuiltInType::WString(_) => anyhow::bail!("ROS 2 `wstring` is not supported"),
    }

    Ok(())
}

/// Reads an Arrow array as the type its layout calls for.
fn downcast<'a, T: Array + 'static>(array: &'a dyn Array, expected: &str) -> anyhow::Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("expected a {expected} column, found {}", array.data_type()))
}
