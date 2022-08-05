use crate::cass_error::*;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn cass_error_desc(_error: *const CassError) -> *const c_char {
    // FIXME: add proper implementation
    let error = "my_custom_error\0";
    error.as_ptr() as *const c_char
}
