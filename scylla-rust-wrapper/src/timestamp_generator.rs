use std::sync::Arc;
use std::time::Duration;

use scylla::policies::timestamp_generator::MonotonicTimestampGenerator;

use crate::argconv::{BoxFFI, CMut, CassOwnedExclusivePtr, FFI, FromBox};

pub enum CassTimestampGen {
    ServerSide,
    Monotonic(Arc<MonotonicTimestampGenerator>),
}

impl FFI for CassTimestampGen {
    type Origin = FromBox;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_timestamp_gen_server_side_new()
-> CassOwnedExclusivePtr<CassTimestampGen, CMut> {
    BoxFFI::into_ptr(Box::new(CassTimestampGen::ServerSide))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_timestamp_gen_monotonic_new()
-> CassOwnedExclusivePtr<CassTimestampGen, CMut> {
    BoxFFI::into_ptr(Box::new(CassTimestampGen::Monotonic(Arc::new(
        // Generator with default settings (warning_threshold=1s, warning_interval=1s)
        MonotonicTimestampGenerator::new(),
    ))))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_timestamp_gen_monotonic_new_with_settings(
    warning_threshold_us: i64,
    warning_interval_ms: i64,
) -> CassOwnedExclusivePtr<CassTimestampGen, CMut> {
    let generator = if warning_threshold_us <= 0 {
        // If threshold is <= 0, we disable the warnings.
        MonotonicTimestampGenerator::new().without_warnings()
    } else {
        let warning_threshold = Duration::from_micros(warning_threshold_us as u64);
        let warning_interval = if warning_interval_ms <= 0 {
            // Inverval <= 0 fallbacks to 1ms.
            Duration::from_millis(1)
        } else {
            Duration::from_millis(warning_interval_ms as u64)
        };

        MonotonicTimestampGenerator::new().with_warning_times(warning_threshold, warning_interval)
    };

    BoxFFI::into_ptr(Box::new(CassTimestampGen::Monotonic(Arc::new(generator))))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_timestamp_gen_free(
    timestamp_gen_raw: CassOwnedExclusivePtr<CassTimestampGen, CMut>,
) {
    BoxFFI::free(timestamp_gen_raw)
}
