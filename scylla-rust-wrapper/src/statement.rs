use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::size_t;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::Int;
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

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_int32(
    statement_raw: *mut CassStatement,
    index: size_t,
    value: i32,
) -> CassError {
    // FIXME: Bounds check
    let statement = ptr_to_ref_mut(statement_raw);
    statement.bound_values[index as usize] = Set(Some(Int(value)));

    crate::cass_error::OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    free_boxed(statement_raw);
}
