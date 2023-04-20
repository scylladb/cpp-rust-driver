use std::ffi::c_char;

use crate::argconv::free_boxed;
use crate::batch::CassBatch;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cluster::CassCluster;
use crate::retry_policy::CassRetryPolicy;
use crate::statement::CassStatement;
use crate::types::{cass_bool_t, cass_int32_t, cass_int64_t, cass_uint32_t, cass_uint64_t, size_t};

pub struct CassExecProfile;

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_new() -> *mut CassExecProfile {
    Box::into_raw(Box::new(CassExecProfile))
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_free(profile: *mut CassExecProfile) {
    free_boxed(profile);
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_consistency(
    profile: *mut CassExecProfile,
    consistency: CassConsistency,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_constant_speculative_execution_policy(
    profile: *mut CassExecProfile,
    constant_delay_ms: cass_int64_t,
    max_speculative_executions: cass_int32_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing(
    profile: *mut CassExecProfile,
    enabled: cass_bool_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware(
    profile: *mut CassExecProfile,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware_n(
    profile: *mut CassExecProfile,
    local_dc: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_round_robin(
    profile: *mut CassExecProfile,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_request_timeout(
    profile: *mut CassExecProfile,
    timeout_ms: cass_uint64_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_retry_policy(
    profile: *mut CassExecProfile,
    retry_policy: *const CassRetryPolicy,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_serial_consistency(
    profile: *mut CassExecProfile,
    serial_consistency: CassConsistency,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_token_aware_routing(
    profile: *mut CassExecProfile,
    enabled: cass_bool_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_execution_profile(
    statement: *mut CassStatement,
    name: *const c_char,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_execution_profile_n(
    statement: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile(
    batch: *mut CassBatch,
    name: *const c_char,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile_n(
    batch: *mut CassBatch,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_execution_profile(
    cluster: *mut CassCluster,
    name: *const c_char,
    profile: *const CassExecProfile,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_execution_profile_n(
    cluster: *mut CassCluster,
    name: *const c_char,
    name_length: size_t,
    profile: *const CassExecProfile,
) -> CassError {
    unimplemented!()
}
