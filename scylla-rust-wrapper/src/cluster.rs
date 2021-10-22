use crate::argconv::*;
use crate::cass_error::{self, CassError};
use scylla::SessionBuilder;
use std::os::raw::c_char;

pub struct CassCluster(pub SessionBuilder);

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_new() -> *mut CassCluster {
    Box::into_raw(Box::new(CassCluster(SessionBuilder::new())))
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_free(session_builder_raw: *mut CassCluster) {
    free_boxed(session_builder_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_contact_points(
    session_builder_raw: *mut CassCluster,
    uri_raw: *const c_char,
) -> CassError {
    match cluster_set_contact_points(session_builder_raw, uri_raw) {
        Ok(()) => cass_error::OK,
        Err(err) => err,
    }
}

unsafe fn cluster_set_contact_points(
    session_builder_raw: *mut CassCluster,
    uri_raw: *const c_char,
) -> Result<(), CassError> {
    let session_builder = ptr_to_ref_mut(session_builder_raw);
    let uri = ptr_to_cstr(uri_raw).ok_or(cass_error::LIB_BAD_PARAMS)?;
    session_builder.0.config.add_known_node(uri);
    Ok(())
}
