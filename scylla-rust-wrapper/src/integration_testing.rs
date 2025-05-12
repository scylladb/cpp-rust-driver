use std::ffi::{CString, c_char};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::errors::{RequestAttemptError, RequestError};
use scylla::observability::history::{AttemptId, HistoryListener, RequestId, SpeculativeId};
use scylla::policies::retry::RetryDecision;

use crate::argconv::{
    ArcFFI, BoxFFI, CConst, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr,
};
use crate::batch::CassBatch;
use crate::cluster::CassCluster;
use crate::future::{CassFuture, CassResultValue};
use crate::statement::{BoundStatement, CassStatement};
use crate::types::{cass_int32_t, cass_uint16_t, cass_uint64_t, size_t};

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_future_get_host(
    future_raw: CassBorrowedSharedPtr<CassFuture, CConst>,
    host: *mut *mut c_char,
    host_length: *mut size_t,
) {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to testing_future_get_host!");
        unsafe {
            *host = std::ptr::null_mut();
            *host_length = 0;
        };
        return;
    };

    future.with_waited_result(|r| match r {
        Ok(CassResultValue::QueryResult(result)) => {
            // unwrap: Coordinator is none only for unit tests.
            let coordinator = result.coordinator.as_ref().unwrap();

            let ip_addr_str = coordinator.node().address.ip().to_string();
            let length = ip_addr_str.len();

            let ip_addr_cstr = CString::new(ip_addr_str).expect(
                "String obtained from IpAddr::to_string() should not contain any nul bytes!",
            );

            unsafe {
                *host = ip_addr_cstr.into_raw();
                *host_length = length as size_t
            };
        }
        _ => unsafe {
            *host = std::ptr::null_mut();
            *host_length = 0;
        },
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_free_host(host: *mut c_char) {
    let _ = unsafe { CString::from_raw(host) };
}

#[derive(Debug)]
struct SleepingHistoryListener(Duration);

impl HistoryListener for SleepingHistoryListener {
    fn log_request_start(&self) -> RequestId {
        RequestId(0)
    }

    fn log_request_success(&self, _request_id: RequestId) {}

    fn log_request_error(&self, _request_id: RequestId, _error: &RequestError) {}

    fn log_new_speculative_fiber(&self, _request_id: RequestId) -> SpeculativeId {
        SpeculativeId(0)
    }

    fn log_attempt_start(
        &self,
        _request_id: RequestId,
        _speculative_id: Option<SpeculativeId>,
        _node_addr: SocketAddr,
    ) -> AttemptId {
        // Sleep to simulate a delay in the request
        std::thread::sleep(self.0);
        AttemptId(0)
    }

    fn log_attempt_success(&self, _attempt_id: AttemptId) {}

    fn log_attempt_error(
        &self,
        _attempt_id: AttemptId,
        _error: &RequestAttemptError,
        _retry_decision: &RetryDecision,
    ) {
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_statement_set_sleeping_history_listener(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    sleep_time_ms: cass_uint64_t,
) {
    let sleep_time = Duration::from_millis(sleep_time_ms);
    let history_listener = Arc::new(SleepingHistoryListener(sleep_time));

    match &mut BoxFFI::as_mut_ref(statement_raw).unwrap().statement {
        BoundStatement::Simple(inner) => inner.query.set_history_listener(history_listener),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_history_listener(history_listener),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_batch_set_sleeping_history_listener(
    batch_raw: CassBorrowedExclusivePtr<CassBatch, CMut>,
    sleep_time_ms: cass_uint64_t,
) {
    let sleep_time = Duration::from_millis(sleep_time_ms);
    let history_listener = Arc::new(SleepingHistoryListener(sleep_time));

    let batch = BoxFFI::as_mut_ref(batch_raw).unwrap();

    Arc::make_mut(&mut batch.state)
        .batch
        .set_history_listener(history_listener)
}
