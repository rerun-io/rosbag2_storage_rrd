//! Helpers shared by the unit tests.

use crate::config::{Representation, Representations};

/// A `std_msgs/msg/String` as rmw serializes it: encapsulation header, length including
/// the NUL, the bytes, the NUL. CDR pads before a field, never after the last one.
pub fn cdr_string(text: &str) -> Vec<u8> {
    let mut out = vec![0x00, 0x01, 0x00, 0x00];
    let len = u32::try_from(text.len() + 1).unwrap();
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    out.push(0);
    out
}

pub fn reps(list: &[Representation]) -> Representations {
    list.iter().copied().collect()
}
