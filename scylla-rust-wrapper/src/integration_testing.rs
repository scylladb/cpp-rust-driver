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

        writeln!(&mut acc, "{}", host.ip()).unwrap();
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
pub struct IgnoringRetryPolicy;

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

/// Stubs of functions that must be implemented for the integration tests
/// or examples to compile, but the proper implementation is not needed for
/// the tests/examples to run, and at the same time the functions are not
/// yet implemented in the wrapper.
pub(crate) mod stubs {
    use std::ffi::c_void;

    use super::*;
    use crate::argconv::{CassOwnedExclusivePtr, CassPtr, ptr_to_cstr_n, strlen};
    use crate::cass_authenticator_types::{
        CassAuthenticatorCallbacks, CassAuthenticatorDataCleanupCallback,
    };
    use crate::cass_error_types::CassError;
    use crate::cass_host_listener_types::CassHostListenerCallback;
    use crate::cass_version_types::CassVersion;
    use crate::iterator::CassIterator;
    use crate::metadata::{CassColumnMeta, CassKeyspaceMeta, CassSchemaMeta, CassTableMeta};
    use crate::query_result::CassValue;
    use crate::types::cass_byte_t;

    pub struct CassCustomPayload;

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn cass_cluster_set_use_randomized_contact_points(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        _enabled: cass_bool_t,
    ) -> CassError {
        // FIXME: should set `use_randomized_contact_points` flag in cluster config

        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle(
        cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        path: *const c_char,
    ) -> CassError {
        unsafe {
            cass_cluster_set_cloud_secure_connection_bundle_n(cluster_raw, path, strlen(path))
        }
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle_n(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        path: *const c_char,
        path_length: size_t,
    ) -> CassError {
        // FIXME: Should unzip file associated with the path
        let zip_file = unsafe { ptr_to_cstr_n(path, path_length) }.unwrap();

        if zip_file == "invalid_filename" {
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }

        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn cass_cluster_set_exponential_reconnect(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        base_delay_ms: cass_uint64_t,
        max_delay_ms: cass_uint64_t,
    ) -> CassError {
        if base_delay_ms <= 1 {
            // Base delay must be greater than 1
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }

        if max_delay_ms <= 1 {
            // Max delay must be greater than 1
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }

        if max_delay_ms < base_delay_ms {
            // Max delay cannot be less than base delay
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }

        // FIXME: should set exponential reconnect with base_delay_ms and max_delay_ms
        /*
        cluster->config().set_exponential_reconnect(base_delay_ms, max_delay_ms);
        */

        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_custom_payload_new() -> *const CassCustomPayload {
        // FIXME: should create a new custom payload that must be freed
        std::ptr::null()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_future_custom_payload_item(
        _future: CassBorrowedExclusivePtr<CassFuture, CMut>,
        _i: size_t,
        _name: *const c_char,
        _name_length: size_t,
        _value: *const cass_byte_t,
        _value_size: size_t,
    ) -> CassError {
        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_future_custom_payload_item_count(
        _future: CassBorrowedExclusivePtr<CassFuture, CMut>,
    ) -> size_t {
        0
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_cluster_set_authenticator_callbacks(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        _callbacks: CassBorrowedSharedPtr<CassAuthenticatorCallbacks, CConst>,
        _cleanup_callback: CassAuthenticatorDataCleanupCallback,
        _data: *mut c_void,
    ) -> CassError {
        CassError::CASS_OK
    }

    pub struct CassAuthenticator;

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_authenticator_response(
        _auth: CassBorrowedExclusivePtr<CassAuthenticator, CMut>,
        _size: size_t,
    ) -> *mut c_char {
        std::ptr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_statement_add_key_index(
        _statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
        _index: size_t,
    ) -> CassError {
        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_statement_set_keyspace(
        _statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
        _keyspace: *const c_char,
    ) -> CassError {
        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_cluster_set_host_listener_callback(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        _callback: CassHostListenerCallback,
        _data: *mut c_void,
    ) -> CassError {
        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_cluster_set_num_threads_io(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        _num_threads: u32,
    ) -> CassError {
        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_cluster_set_queue_size_io(
        _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
        _queue_size: u32,
    ) -> CassError {
        CassError::CASS_OK
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_get_meta_field_name(
        _cass_iterator: CassBorrowedSharedPtr<CassIterator, CConst>,
        _name: *mut *const c_char,
        _name_length: *mut size_t,
    ) {
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_schema_meta_version(
        _schema_meta: CassBorrowedSharedPtr<CassSchemaMeta, CConst>,
    ) -> CassVersion {
        CassVersion {
            major_version: 2,
            minor_version: 1,
            patch_version: 3,
        }
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_get_meta_field_value<'schema>(
        _cass_iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
    ) -> CassOwnedExclusivePtr<CassValue<'schema>, CMut> {
        CassPtr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_indexes_from_table_meta(
        _cass_table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_fields_from_column_meta(
        _cass_column_meta: CassBorrowedSharedPtr<CassColumnMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_fields_from_table_meta(
        _cass_table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_fields_from_keyspace_meta(
        _cass_keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    pub struct CassFunctionMeta;

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_function_meta_name(
        _cass_function_meta: CassBorrowedSharedPtr<CassFunctionMeta, CConst>,
        _name: *mut *const c_char,
        _name_length: *mut size_t,
    ) {
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_fields_from_function_meta(
        _cass_function_meta: CassBorrowedSharedPtr<CassFunctionMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    pub struct CassIndexMeta;

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_get_index_meta<'schema>(
        _cass_iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
    ) -> CassBorrowedSharedPtr<'schema, CassIndexMeta, CConst> {
        CassPtr::null()
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_index_meta_name(
        _cass_index_meta: CassBorrowedSharedPtr<CassIndexMeta, CConst>,
        _name: *mut *const c_char,
        _name_length: *mut size_t,
    ) {
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_fields_from_index_meta(
        _cass_index_meta: CassBorrowedSharedPtr<CassIndexMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn cass_keyspace_meta_function_by_name(
        _keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
        _function: *const c_char,
    ) -> CassBorrowedSharedPtr<CassFunctionMeta, CConst> {
        CassPtr::null()
    }

    pub struct CassAggregateMeta;

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_aggregate_meta_name(
        _cass_aggregate_meta: CassBorrowedSharedPtr<CassAggregateMeta, CConst>,
        _name: *mut *const c_char,
        _name_length: *mut size_t,
    ) {
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn cass_iterator_fields_from_aggregate_meta(
        _cass_aggregate_meta: CassBorrowedSharedPtr<CassAggregateMeta, CConst>,
    ) -> CassOwnedExclusivePtr<CassIterator, CMut> {
        CassPtr::null_mut()
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn cass_keyspace_meta_aggregate_by_name(
        _keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
        _aggregate: *const c_char,
    ) -> CassBorrowedSharedPtr<CassAggregateMeta, CConst> {
        CassPtr::null()
    }
}
