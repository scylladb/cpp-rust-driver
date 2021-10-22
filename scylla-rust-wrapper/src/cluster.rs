use crate::argconv::*;
use crate::cass_error::{self, CassError};
use scylla::SessionBuilder;
use std::os::raw::c_char;

pub struct CassCluster {
    session_builder: SessionBuilder,

    contact_points: Vec<String>,
    port: u16,
}

pub fn build_session_builder(cluster: &CassCluster) -> SessionBuilder {
    let known_nodes: Vec<_> = cluster
        .contact_points
        .clone()
        .into_iter()
        .map(|cp| format!("{}:{}", cp, cluster.port))
        .collect();
    cluster
        .session_builder
        .clone()
        .known_nodes(&known_nodes)
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_new() -> *mut CassCluster {
    Box::into_raw(Box::new(CassCluster {
        session_builder: SessionBuilder::new(),
        port: 9042,
        contact_points: Vec::new(),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_free(cluster: *mut CassCluster) {
    free_boxed(cluster);
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_contact_points(
    cluster: *mut CassCluster,
    contact_points: *const c_char,
) -> CassError {
    let contact_points_str = ptr_to_cstr(contact_points).unwrap();
    let contact_points_length = contact_points_str.len();

    cass_cluster_set_contact_points_n(cluster, contact_points, contact_points_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_contact_points_n(
    cluster: *mut CassCluster,
    contact_points: *const c_char,
    contact_points_length: size_t,
) -> CassError {
    match cluster_set_contact_points(cluster, contact_points, contact_points_length) {
        Ok(()) => cass_error::OK,
        Err(err) => err,
    }
}

unsafe fn cluster_set_contact_points(
    cluster_raw: *mut CassCluster,
    contact_points_raw: *const c_char,
    contact_points_length: size_t,
) -> Result<(), CassError> {
    let cluster = ptr_to_ref_mut(cluster_raw);
    let mut contact_points = ptr_to_cstr_n(contact_points_raw, contact_points_length)
        .ok_or(cass_error::LIB_BAD_PARAMS)?
        .split(',')
        .peekable();

    if contact_points.peek().is_none() {
        // If cass_cluster_set_contact_points() is called with empty
        // set of contact points, the contact points should be cleared.
        cluster.contact_points.clear();
        return Ok(());
    }

    // cass_cluster_set_contact_points() will append
    // in subsequent calls, not overwrite.
    cluster.contact_points.extend(
        contact_points
            .map(|cp| cp.trim().to_string())
            .filter(|cp| !cp.is_empty()),
    );
    Ok(())
}
#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_port(
    cluster_raw: *mut CassCluster,
    port: c_int,
) -> CassError {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.port = port as u16; // FIXME: validate port number
    cass_error::OK
}

