use std::ffi::{CString, c_char};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::errors::{RequestAttemptError, RequestError};
use scylla::observability::history::{AttemptId, HistoryListener, RequestId, SpeculativeId};
use scylla::policies::retry::RetryDecision;

use crate::argconv::{
    ArcFFI, BoxFFI, CConst, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr,
    CassOwnedSharedPtr,
};
use crate::batch::CassBatch;
use crate::cluster::CassCluster;
use crate::future::{CassFuture, CassResultValue};
use crate::retry_policy::CassRetryPolicy;
use crate::statement::{BoundStatement, CassStatement};
use crate::types::{cass_bool_t, cass_int32_t, cass_uint16_t, cass_uint64_t, size_t};

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
pub unsafe extern "C" fn testing_free_cstring(s: *mut c_char) {
    let _ = unsafe { CString::from_raw(s) };
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

/// Used to record attempted hosts during a request.
/// This is useful for testing purposes and is used in integration tests.
/// This is enabled by `testing_statement_set_recording_history_listener`
/// and can be queried using `testing_future_get_attempted_hosts`.
#[derive(Debug)]
pub(crate) struct RecordingHistoryListener {
    attempted_hosts: std::sync::Mutex<Vec<SocketAddr>>,
}

impl RecordingHistoryListener {
    pub(crate) fn new() -> Self {
        RecordingHistoryListener {
            attempted_hosts: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn get_attempted_hosts(&self) -> Vec<SocketAddr> {
        self.attempted_hosts.lock().unwrap().clone()
    }
}

impl HistoryListener for RecordingHistoryListener {
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
        node_addr: SocketAddr,
    ) -> AttemptId {
        // Record the host that was attempted.
        self.attempted_hosts.lock().unwrap().push(node_addr);

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

/// Enables recording of attempted hosts in the statement's history listener.
/// This is useful for testing purposes, allowing us to verify which hosts were attempted
/// during a request.
/// Later, `testing_future_get_attempted_hosts` can be used to retrieve this information.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_statement_set_recording_history_listener(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    enable: cass_bool_t,
) {
    let statement = &mut BoxFFI::as_mut_ref(statement_raw).unwrap();

    statement.record_hosts = enable != 0;
}

/// Retrieves hosts that were attempted during the execution of a future.
/// In order for this to work, the statement must have been configured with
/// `testing_statement_set_recording_history_listener`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_future_get_attempted_hosts(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> *mut c_char {
    // This function should return a list of attempted hosts.
    // Care should be taken to ensure that the list is properly allocated and freed.
    // The problem is that the return type must be understandable by C code.
    // See testing.cpp:53 (get_attempted_hosts_from_future()) for an example of how
    // this is done in C++.
    //
    // Idea: Create a concatenated string of attempted hosts.
    // Return a pointer to that string. Caller is responsible for freeing it.

    let future: &CassFuture = ArcFFI::as_ref(future_raw).unwrap();

    let attempted_hosts = future.attempted_hosts();
    let concatenated_hosts = attempted_hosts.iter().fold(String::new(), |mut acc, host| {
        // Convert the SocketAddr to a string.
        // Delimit address strings with '\n' to enable easy parsing in C.

        write!(&mut acc, "{}\n", host.ip()).unwrap();
        acc
    });

    // The caller is responsible for freeing this memory, by calling `testing_free_cstring`.
    unsafe { CString::from_vec_unchecked(concatenated_hosts.into_bytes()) }.into_raw()
}

/// Ensures that the `testing_future_get_attempted_hosts` function
/// behaves correctly, i.e., it returns a list of attempted hosts as a concatenated string.
#[test]
fn test_future_get_attempted_hosts() {
    use scylla::observability::history::HistoryListener as _;

    let listener = Arc::new(RecordingHistoryListener::new());
    let future = CassFuture::new_from_future(std::future::pending(), Some(listener.clone()));

    fn assert_attempted_hosts_eq(future: &Arc<CassFuture>, hosts: &[String]) {
        let hosts_str = unsafe { testing_future_get_attempted_hosts(ArcFFI::as_ptr(future)) };
        let hosts_cstr = unsafe { CString::from_raw(hosts_str) };
        let hosts_string = hosts_cstr.to_str().unwrap();

        // Split the string by '\n' and collect into a Vec<&str>.
        let attempted_hosts: Vec<&str> =
            hosts_string.split('\n').filter(|s| !s.is_empty()).collect();

        assert_eq!(attempted_hosts, hosts);
    }

    // 1. Test with no attempted hosts.
    {
        assert_attempted_hosts_eq(&future, &[]);
    }

    let addr1: SocketAddr = SocketAddr::from(([127, 0, 0, 1], 9042));
    let addr2: SocketAddr = SocketAddr::from(([127, 0, 0, 2], 9042));

    // 2. Attempt two hosts and see if they are recorded correctly, in order.
    {
        listener.log_attempt_start(RequestId(0), None, addr1);
        listener.log_attempt_start(RequestId(0), None, addr2);
        assert_attempted_hosts_eq(&future, &[addr1, addr2].map(|addr| addr.ip().to_string()))
    }

    let addr3: SocketAddr = SocketAddr::from(([127, 0, 0, 3], 9042));

    // 3. Attempt one more host and see if all hosts are present, in order.
    {
        listener.log_attempt_start(RequestId(0), None, addr3);
        assert_attempted_hosts_eq(
            &future,
            &[addr1, addr2, addr3].map(|addr| addr.ip().to_string()),
        )
    }
}

/// A retry policy that always ignores all errors.
///
/// Useful for testing purposes.
#[derive(Debug)]
pub(crate) struct IgnoringRetryPolicy;

#[derive(Debug)]
struct IgnoringRetrySession;

impl scylla::policies::retry::RetryPolicy for IgnoringRetryPolicy {
    fn new_session(&self) -> Box<dyn scylla::policies::retry::RetrySession> {
        Box::new(IgnoringRetrySession)
    }
}

impl scylla::policies::retry::RetrySession for IgnoringRetrySession {
    fn decide_should_retry(
        &mut self,
        _request_info: scylla::policies::retry::RequestInfo,
    ) -> RetryDecision {
        RetryDecision::IgnoreWriteError
    }

    fn reset(&mut self) {}
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn testing_retry_policy_ignoring_new()
-> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(CassRetryPolicy::Ignoring(Arc::new(
        IgnoringRetryPolicy,
    ))))
}
