use crate::argconv::{free_boxed, ptr_to_ref, write_str_to_c};
use crate::cluster::CassCluster;
use crate::types::*;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn testing_get_connect_timeout_from_cluster(
    cluster: *mut CassCluster,
) -> cass_uint16_t {
    let cluster = ptr_to_ref(cluster);
    cluster.session_builder.config.connect_timeout.as_millis() as cass_uint16_t
}

#[no_mangle]
pub unsafe extern "C" fn testing_get_contact_points_from_cluster(
    cluster: *mut CassCluster,
    contact_points: *mut *const c_char,
    contact_points_length: *mut size_t,
    contact_points_boxed: *mut *const String,
) {
    let cluster = ptr_to_ref(cluster);
    let str = Box::new(cluster.contact_points.join(","));
    let result = str.as_str();

    write_str_to_c(result, contact_points, contact_points_length);
    *contact_points_boxed = Box::into_raw(str);
}

#[no_mangle]
pub unsafe extern "C" fn testing_free_contact_points_string(contact_points_boxed: *mut String) {
    free_boxed(contact_points_boxed)
}

#[no_mangle]
pub unsafe extern "C" fn testing_get_port_from_cluster(cluster: *mut CassCluster) -> cass_int32_t {
    let cluster = ptr_to_ref(cluster);

    cluster.port as cass_int32_t
}
