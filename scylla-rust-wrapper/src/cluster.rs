use crate::cass_error::{self, CassError};
use scylla::SessionBuilder;
use std::ffi::CStr;
use std::os::raw::c_char;

pub struct CassCluster(pub SessionBuilder);

#[no_mangle]
pub extern "C" fn cass_cluster_new() -> *mut CassCluster {
    Box::into_raw(Box::new(CassCluster(SessionBuilder::new())))
}

#[no_mangle]
pub extern "C" fn cass_cluster_free(session_builder_raw: *mut CassCluster) {
    if !session_builder_raw.is_null() {
        let ptr = unsafe { Box::from_raw(session_builder_raw) };
        drop(ptr);
    }
}

#[no_mangle]
pub extern "C" fn cass_cluster_set_contact_points(
    session_builder_raw: *mut CassCluster,
    uri_raw: *const c_char,
) -> CassError {
    let session_builder: &mut CassCluster = unsafe { session_builder_raw.as_mut().unwrap() };
    let uri_cstr = unsafe { CStr::from_ptr(uri_raw) };

    if let Ok(uri) = uri_cstr.to_str() {
        session_builder.0.config.add_known_node(uri);
        cass_error::OK
    } else {
        cass_error::LIB_BAD_PARAMS
    }
}
