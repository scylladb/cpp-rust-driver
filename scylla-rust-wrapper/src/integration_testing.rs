use std::ffi::{CString, c_char};

use crate::argconv::{BoxFFI, CMut, CassBorrowedExclusivePtr};
use crate::cluster::CassCluster;
use crate::types::{cass_int32_t, cass_uint16_t, size_t};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_cluster_get_connect_timeout(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
) -> cass_uint16_t {
    let cluster = BoxFFI::as_ref(cluster_raw).unwrap();

    cluster.get_session_config().connect_timeout.as_millis() as cass_uint16_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_cluster_get_port(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
) -> cass_int32_t {
    let cluster = BoxFFI::as_ref(cluster_raw).unwrap();

    cluster.get_port() as cass_int32_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_cluster_get_contact_points(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    contact_points: *mut *mut c_char,
    contact_points_length: *mut size_t,
) {
    let cluster = BoxFFI::as_ref(cluster_raw).unwrap();

    let contact_points_string = cluster.get_contact_points().join(",");
    let length = contact_points_string.len();

    match CString::new(contact_points_string) {
        Ok(cstring) => {
            unsafe {
                *contact_points = cstring.into_raw();
                *contact_points_length = length as size_t
            };
        }
        Err(_) => {
            // The contact points string contained a nul byte in the middle.
            unsafe {
                *contact_points = std::ptr::null_mut();
                *contact_points_length = 0
            };
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_free_contact_points(contact_points: *mut c_char) {
    let _ = unsafe { CString::from_raw(contact_points) };
}
