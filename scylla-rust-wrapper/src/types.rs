#![allow(non_camel_case_types)]

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
pub type size_t = cass_uint64_t;

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
