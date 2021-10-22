use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::*;
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use scylla::query::Query;
use std::os::raw::c_char;

pub struct CassStatement {
    pub query: Query,
    pub bound_values: Vec<MaybeUnset<Option<CqlValue>>>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new(
    query: *const c_char,
    parameter_count: size_t,
) -> *mut CassStatement {
    // TODO: error handling
    let query_str = ptr_to_cstr(query).unwrap();
    let query_length = query_str.len();

    cass_statement_new_n(query, query_length as size_t, parameter_count)
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new_n(
    query: *const c_char,
    query_length: size_t,
    parameter_count: size_t,
) -> *mut CassStatement {
    // TODO: error handling
    let query_str = ptr_to_cstr_n(query, query_length).unwrap();

    Box::into_raw(Box::new(CassStatement {
        query: Query::new(query_str.to_string()),
        bound_values: vec![Unset; parameter_count as usize],
    }))
}

// TODO: Bind methods currently not implemented:
// cass_statement_bind_decimal
//
// cass_statement_bind_duration - DURATION not implemented in Rust Driver
//
// (methods requiring implementing cpp driver data structures)
// cass_statement_bind_user_type
// cass_statement_bind_collection
// cass_statement_bind_custom
// cass_statement_bind_custom_n
// cass_statement_bind_tuple
// cass_statement_bind_uuid
// cass_statement_bind_inet
//
// Variants of all methods with by_name, by_name_n

unsafe fn cass_statement_bind_maybe_unset(
    statement_raw: *mut CassStatement,
    index: size_t,
    value: MaybeUnset<Option<CqlValue>>,
) -> CassError {
    // FIXME: Bounds check
    let statement = ptr_to_ref_mut(statement_raw);
    statement.bound_values[index as usize] = value;

    crate::cass_error::OK
}

unsafe fn cass_statement_bind_cql_value(
    statement: *mut CassStatement,
    index: size_t,
    value: CqlValue,
) -> CassError {
    cass_statement_bind_maybe_unset(statement, index, Set(Some(value)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_null(
    statement: *mut CassStatement,
    index: size_t,
) -> CassError {
    cass_statement_bind_maybe_unset(statement, index, Set(None))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_int8(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_int8_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, TinyInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_int16(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_int16_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, SmallInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_int32(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_int32_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, Int(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_uint32(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_uint32_t,
) -> CassError {
    // cass_statement_bind_uint32 is only used to set a DATE.
    cass_statement_bind_cql_value(statement, index, Date(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_int64(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_int64_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, BigInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_float(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_float_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, Float(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_double(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_double_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, Double(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_bool(
    statement: *mut CassStatement,
    index: size_t,
    value: cass_bool_t,
) -> CassError {
    cass_statement_bind_cql_value(statement, index, Boolean(value != 0))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_string(
    statement: *mut CassStatement,
    index: size_t,
    value: *const c_char,
) -> CassError {
    let value_str = ptr_to_cstr(value).unwrap();
    let value_length = value_str.len();

    cass_statement_bind_string_n(statement, index, value, value_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_string_n(
    statement: *mut CassStatement,
    index: size_t,
    value: *const c_char,
    value_length: size_t,
) -> CassError {
    // TODO: Error handling
    let value_string = ptr_to_cstr_n(value, value_length).unwrap().to_string();
    cass_statement_bind_cql_value(statement, index, Text(value_string))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_bytes(
    statement: *mut CassStatement,
    index: size_t,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_statement_bind_cql_value(statement, index, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    free_boxed(statement_raw);
}
