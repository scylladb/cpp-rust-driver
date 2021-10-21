use crate::size_t;
use scylla::query::Query;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::Arc;

pub struct CassStatement_ {
    pub query: Query,
}

pub type CassStatement = Arc<CassStatement_>;

#[no_mangle]
pub extern "C" fn cass_statement_new(
    query: *const c_char,
    parameter_count: size_t,
) -> *mut CassStatement {
    let query_cstr = unsafe { CStr::from_ptr(query) };
    let query_length = query_cstr.to_str().unwrap().len();

    cass_statement_new_n(query, query_length, parameter_count)
}

#[no_mangle]
pub extern "C" fn cass_statement_new_n(
    query: *const c_char,
    query_length: size_t,
    parameter_count: size_t,
) -> *mut CassStatement {
    let query_str = unsafe {
        std::str::from_utf8(std::slice::from_raw_parts(query as *const u8, query_length)).unwrap()
    };

    assert!(
        parameter_count == 0,
        "parameter_count > 0 not implemented yet"
    );

    Box::into_raw(Box::new(Arc::new(CassStatement_ {
        query: Query::new(query_str.to_string()),
    })))
}

#[no_mangle]
pub extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    if !statement_raw.is_null() {
        let ptr = unsafe { Box::from_raw(statement_raw) };
        drop(ptr); // Explicit drop, to make function clearer
    }
}
