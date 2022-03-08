use crate::cass_types::CassDataType;
use crate::cluster::CassCluster;
use crate::future::CassFuture;
use crate::query_error::CassConsistency;
use crate::query_result::{CassIterator, CassResult, CassRow, CassValue};
use crate::session::CassSession;
use crate::types::*;
use crate::{cass_error::CassError, statement::CassStatement};

pub enum CassCustomPayload {}
pub enum CassExecProfile {}

pub enum CassValueType {}

pub enum CassLogCallback {}

pub enum CassLogLevel {}

#[no_mangle]
pub unsafe extern "C" fn cass_error_desc(error: CassError) -> *const ::std::os::raw::c_char {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_consistency(
    statement: *mut CassStatement,
    consistency: CassConsistency,
) -> CassError {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_custom_payload_new() -> *mut CassCustomPayload {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_custom_payload_item_count(future: *mut CassFuture) -> size_t {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_custom_payload_item(
    future: *mut CassFuture,
    index: size_t,
    name: *mut *const ::std::os::raw::c_char,
    name_length: *mut size_t,
    value: *mut *const cass_byte_t,
    value_size: *mut size_t,
) -> CassError {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_row(row: *const CassRow) -> *mut CassIterator {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column(
    iterator: *const CassIterator,
) -> *const CassValue {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_row_count(result: *const CassResult) -> size_t {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_first_row(result: *const CassResult) -> *const CassRow {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_close(session: *mut CassSession) -> *mut CassFuture {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect_keyspace(
    session: *mut CassSession,
    cluster: *const CassCluster,
    keyspace: *const ::std::os::raw::c_char,
) -> *mut CassFuture {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_use_beta_protocol_version(
    cluster: *mut CassCluster,
    enable: cass_bool_t,
) -> CassError {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_execution_profile(
    cluster: *mut CassCluster,
    name: *const ::std::os::raw::c_char,
    profile: *mut CassExecProfile,
) -> CassError {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_use_randomized_contact_points(
    cluster: *mut CassCluster,
    enabled: cass_bool_t,
) -> CassError {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_use_schema(
    cluster: *mut CassCluster,
    enabled: cass_bool_t,
) {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_type(value: *const CassValue) -> CassValueType {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_data_type(value: *const CassValue) -> *const CassDataType {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_custom_payload_free(payload: *mut CassCustomPayload) {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_set_callback(
    callback: CassLogCallback,
    data: *mut ::std::os::raw::c_void,
) {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_set_level(log_level: CassLogLevel) {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_get_callback_and_data(
    callback_out: *mut CassLogCallback,
    data_out: *mut *mut ::std::os::raw::c_void,
) {
    panic!();
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_level_string(
    log_level: CassLogLevel,
) -> *const ::std::os::raw::c_char {
    panic!();
}
