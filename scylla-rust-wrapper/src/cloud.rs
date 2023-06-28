use crate::argconv::{ptr_to_cstr_n, ptr_to_ref_mut};
use crate::cass_error::CassError;
use crate::cluster::CassCluster;
use crate::types::*;
use libc::strlen;
use std::convert::TryInto;
use std::os::raw::c_char;
use std::path::Path;

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle(
    cluster: *mut CassCluster,
    path: *const c_char,
) -> CassError {
    cass_cluster_set_cloud_secure_connection_bundle_n(
        cluster,
        path,
        strlen(path).try_into().unwrap(),
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle_n(
    cluster: *mut CassCluster,
    path: *const c_char,
    path_length: size_t,
) -> CassError {
    openssl_sys::init();
    cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init_n(cluster, path, path_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
    cluster: *mut CassCluster,
    path: *const c_char,
) -> CassError {
    cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init_n(
        cluster,
        path,
        strlen(path).try_into().unwrap(),
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init_n(
    cluster: *mut CassCluster,
    path: *const c_char,
    path_length: size_t,
) -> CassError {
    let cluster = ptr_to_ref_mut(cluster);
    let config_path = ptr_to_cstr_n(path, path_length);

    if let Some(config_path) = config_path {
        if !Path::new(config_path).exists() {
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }

        cluster.connection_bundle_path = Some(config_path.to_string());
        CassError::CASS_OK
    } else {
        CassError::CASS_ERROR_LIB_BAD_PARAMS
    }
}
