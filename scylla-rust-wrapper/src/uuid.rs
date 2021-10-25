use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use std::os::raw::c_char;
use uuid::Uuid;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct CassUuid {
    pub time_and_version: cass_uint64_t,
    pub clock_seq_and_node: cass_uint64_t,
}

// Implementation directly ported from Cpp Driver implementation:

const TIME_OFFSET_BETWEEN_UTC_AND_EPOCH: u64 = 0x01B21DD213814000; // Nanoseconds
const MIN_CLOCK_SEQ_AND_NODE: u64 = 0x8080808080808080;
const MAX_CLOCK_SEQ_AND_NODE: u64 = 0x7f7f7f7f7f7f7f7f;

fn to_milliseconds(timestamp: u64) -> u64 {
    timestamp / 10000
}

fn from_unix_timestamp(timestamp: u64) -> u64 {
    (timestamp * 10000).wrapping_add(TIME_OFFSET_BETWEEN_UTC_AND_EPOCH)
}

fn set_version(timestamp: u64, version: u8) -> u64 {
    (timestamp & 0x0FFFFFFFFFFFFFFF) | ((version as u64) << 60)
}

#[no_mangle]
pub extern "C" fn cass_uuid_version(uuid: CassUuid) -> cass_uint8_t {
    ((uuid.time_and_version >> 60) & 0x0F) as cass_uint8_t
}

#[no_mangle]
pub extern "C" fn cass_uuid_timestamp(uuid: CassUuid) -> cass_uint64_t {
    let timestamp: u64 = uuid.time_and_version & 0x0FFFFFFFFFFFFFFF;
    to_milliseconds(timestamp.wrapping_sub(TIME_OFFSET_BETWEEN_UTC_AND_EPOCH))
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_min_from_time(
    timestamp: cass_uint64_t,
    output_raw: *mut CassUuid,
) {
    let output = ptr_to_ref_mut(output_raw);

    output.time_and_version = set_version(from_unix_timestamp(timestamp), 1);
    output.clock_seq_and_node = MIN_CLOCK_SEQ_AND_NODE;
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_max_from_time(
    timestamp: cass_uint64_t,
    output_raw: *mut CassUuid,
) {
    let output = ptr_to_ref_mut(output_raw);

    output.time_and_version = set_version(from_unix_timestamp(timestamp), 1);
    output.clock_seq_and_node = MAX_CLOCK_SEQ_AND_NODE;
}

// Implemented ourselves:

impl From<CassUuid> for Uuid {
    fn from(uuid: CassUuid) -> Self {
        // This is a strange representation that Cpp driver
        // employs. "Recovered" and validated it empirically...
        let time_and_version_msb = uuid.time_and_version & 0xFFFFFFFF;
        let time_and_version_lsb =
            (((uuid.time_and_version & 0xFFFFFFFF00000000) >> 32) as u32).rotate_left(16) as u64;

        let msb = ((time_and_version_msb << 32) | time_and_version_lsb) as u128;
        let lsb = uuid.clock_seq_and_node as u128;

        Uuid::from_u128((msb << 64) | lsb)
    }
}

impl From<Uuid> for CassUuid {
    fn from(uuid: Uuid) -> Self {
        let u128_representation = uuid.as_u128();
        let upper_u64 = (u128_representation >> 64) as u64;
        let lower_u64 = (u128_representation & 0xFFFFFFFFFFFFFFFF) as u64;

        // This is a strange representation that Cpp driver
        // employs. "Recovered" and validated it empirically...
        let msb = ((upper_u64 & 0xFFFFFFFF) as u32).rotate_left(16) as u64;
        let lsb = (upper_u64 & 0xFFFFFFFF00000000) >> 32;

        CassUuid {
            time_and_version: (msb << 32) | lsb,
            clock_seq_and_node: lower_u64,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_string(uuid_raw: CassUuid, output: *mut c_char) {
    let uuid: Uuid = uuid_raw.into();

    let string_representation = uuid.to_hyphenated().to_string();
    std::ptr::copy_nonoverlapping(
        string_representation.as_ptr(),
        output as *mut u8,
        string_representation.len(),
    );

    // Null-terminate
    let null_byte = output.add(string_representation.len()) as *mut c_char;
    *null_byte = 0;
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_from_string(
    value: *const c_char,
    output: *mut CassUuid,
) -> CassError {
    let value_str = ptr_to_cstr(value).unwrap();
    let value_length = value_str.len();

    cass_uuid_from_string_n(value, value_length as size_t, output)
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_from_string_n(
    value: *const c_char,
    value_length: size_t,
    output_raw: *mut CassUuid,
) -> CassError {
    let output = ptr_to_ref_mut(output_raw);
    let value_str = ptr_to_cstr_n(value, value_length);

    match value_str {
        Some(value_str) => {
            Uuid::parse_str(value_str).map_or(crate::cass_error::LIB_BAD_PARAMS, |parsed_uuid| {
                *output = parsed_uuid.into();
                crate::cass_error::OK
            })
        }
        None => crate::cass_error::LIB_BAD_PARAMS,
    }
}
