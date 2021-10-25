#![allow(non_camel_case_types)]

use std::convert::TryFrom;

use scylla::batch::BatchType;
use scylla::statement::Consistency;

// Definition for size_t (and possibly other types in the future)
include!(concat!(env!("OUT_DIR"), "/basic_types.rs"));

pub type cass_bool_t = ::std::os::raw::c_uint;
pub type cass_float_t = f32;
pub type cass_double_t = f64;
pub type cass_int8_t = i8;
pub type cass_uint8_t = u8;
pub type cass_int16_t = i16;
pub type cass_uint16_t = u16;
pub type cass_int32_t = i32;
pub type cass_uint32_t = u32;
pub type cass_int64_t = i64;
pub type cass_uint64_t = u64;
pub type cass_byte_t = cass_uint8_t;
pub type cass_duration_t = cass_uint64_t;

// Implementation directly ported from Cpp Driver implementation:

const NUM_SECONDS_PER_DAY: i64 = 24 * 60 * 60;
const CASS_DATE_EPOCH: u64 = 2147483648; // 2^31
const CASS_TIME_NANOSECONDS_PER_SECOND: i64 = 1_000_000_000;

// All type conversions (between i32, u64, i64) based on original Cpp Driver implementation
// and C++ implicit type promotion rules.

#[no_mangle]
pub unsafe extern "C" fn cass_date_from_epoch(epoch_secs: cass_int64_t) -> cass_uint32_t {
    ((epoch_secs / NUM_SECONDS_PER_DAY) + (CASS_DATE_EPOCH as i64)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn cass_time_from_epoch(epoch_secs: cass_int64_t) -> cass_int64_t {
    CASS_TIME_NANOSECONDS_PER_SECOND * (epoch_secs % NUM_SECONDS_PER_DAY)
}

#[no_mangle]
pub unsafe extern "C" fn cass_date_time_to_epoch(
    date: cass_uint32_t,
    time: cass_int64_t,
) -> cass_int64_t {
    (((date as u64) - CASS_DATE_EPOCH) * (NUM_SECONDS_PER_DAY as u64)
        + ((time / CASS_TIME_NANOSECONDS_PER_SECOND) as u64)) as i64
}

#[allow(non_upper_case_globals)]
pub const cass_false: cass_bool_t = 0;
#[allow(non_upper_case_globals)]
pub const cass_true: cass_bool_t = 1;

//TODO: generate this and other enums at compile time using bindgen
#[repr(u32)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum CassConsistency {
    CASS_CONSISTENCY_UNKNOWN = 65535,
    CASS_CONSISTENCY_ANY = 0,
    CASS_CONSISTENCY_ONE = 1,
    CASS_CONSISTENCY_TWO = 2,
    CASS_CONSISTENCY_THREE = 3,
    CASS_CONSISTENCY_QUORUM = 4,
    CASS_CONSISTENCY_ALL = 5,
    CASS_CONSISTENCY_LOCAL_QUORUM = 6,
    CASS_CONSISTENCY_EACH_QUORUM = 7,
    CASS_CONSISTENCY_SERIAL = 8,
    CASS_CONSISTENCY_LOCAL_SERIAL = 9,
    CASS_CONSISTENCY_LOCAL_ONE = 10,
}

impl From<Consistency> for CassConsistency {
    fn from(c: Consistency) -> CassConsistency {
        match c {
            Consistency::Any => CassConsistency::CASS_CONSISTENCY_ANY,
            Consistency::One => CassConsistency::CASS_CONSISTENCY_ONE,
            Consistency::Two => CassConsistency::CASS_CONSISTENCY_TWO,
            Consistency::Three => CassConsistency::CASS_CONSISTENCY_THREE,
            Consistency::Quorum => CassConsistency::CASS_CONSISTENCY_QUORUM,
            Consistency::All => CassConsistency::CASS_CONSISTENCY_ALL,
            Consistency::LocalQuorum => CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM,
            Consistency::EachQuorum => CassConsistency::CASS_CONSISTENCY_EACH_QUORUM,
            Consistency::Serial => CassConsistency::CASS_CONSISTENCY_SERIAL,
            Consistency::LocalSerial => CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL,
            Consistency::LocalOne => CassConsistency::CASS_CONSISTENCY_LOCAL_ONE,
        }
    }
}

impl TryFrom<CassConsistency> for Consistency {
    type Error = ();

    fn try_from(c: CassConsistency) -> Result<Consistency, ()> {
        match c {
            CassConsistency::CASS_CONSISTENCY_ANY => Ok(Consistency::Any),
            CassConsistency::CASS_CONSISTENCY_ONE => Ok(Consistency::One),
            CassConsistency::CASS_CONSISTENCY_TWO => Ok(Consistency::Two),
            CassConsistency::CASS_CONSISTENCY_THREE => Ok(Consistency::Three),
            CassConsistency::CASS_CONSISTENCY_QUORUM => Ok(Consistency::Quorum),
            CassConsistency::CASS_CONSISTENCY_ALL => Ok(Consistency::All),
            CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Ok(Consistency::LocalQuorum),
            CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Ok(Consistency::EachQuorum),
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(Consistency::Serial),
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Ok(Consistency::LocalSerial),
            CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Ok(Consistency::LocalOne),
            _ => Err(()),
        }
    }
}

pub type CassBatchType = std::os::raw::c_uint;

pub const CASS_BATCH_TYPE_LOGGED: CassBatchType = 0;
pub const CASS_BATCH_TYPE_UNLOGGED: CassBatchType = 1;
pub const CASS_BATCH_TYPE_COUNTER: CassBatchType = 2;

pub fn make_batch_type(type_: CassBatchType) -> Option<BatchType> {
    match type_ {
        CASS_BATCH_TYPE_LOGGED => Some(BatchType::Logged),
        CASS_BATCH_TYPE_UNLOGGED => Some(BatchType::Unlogged),
        CASS_BATCH_TYPE_COUNTER => Some(BatchType::Counter),
        _ => None,
    }
}
