use crate::argconv::*;
use crate::types::size_t;
use scylla::query::Query;
use std::os::raw::c_char;

pub struct CassStatement {
    pub query: Query,
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

    assert!(
        parameter_count == 0,
        "parameter_count > 0 not implemented yet"
    );

    Box::into_raw(Box::new(CassStatement {
        query: Query::new(query_str.to_string()),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    free_boxed(statement_raw);
}
