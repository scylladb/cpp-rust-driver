use crate::types::{cass_int64_t, cass_uint32_t};

// Implementation directly ported from Cpp Driver implementation:

const NUM_SECONDS_PER_DAY: i64 = 24 * 60 * 60;
const CASS_DATE_EPOCH: u64 = 2147483648; // 2^31
const CASS_TIME_NANOSECONDS_PER_SECOND: i64 = 1_000_000_000;

// All type conversions (between i32, u64, i64) based on original Cpp Driver implementation
// and C++ implicit type promotion rules.

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_date_from_epoch(epoch_secs: cass_int64_t) -> cass_uint32_t {
    ((epoch_secs / NUM_SECONDS_PER_DAY) + (CASS_DATE_EPOCH as i64)) as u32
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_time_from_epoch(epoch_secs: cass_int64_t) -> cass_int64_t {
    CASS_TIME_NANOSECONDS_PER_SECOND * (epoch_secs % NUM_SECONDS_PER_DAY)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_date_time_to_epoch(
    date: cass_uint32_t,
    time: cass_int64_t,
) -> cass_int64_t {
    (((date as u64) - CASS_DATE_EPOCH) * (NUM_SECONDS_PER_DAY as u64)
        + ((time / CASS_TIME_NANOSECONDS_PER_SECOND) as u64)) as i64
}
