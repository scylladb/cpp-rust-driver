use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::config_value::MaybeUnsetConfig;
use crate::exec_profile::{CassExecProfile, ExecProfileName, exec_profile_builder_modify};
use crate::load_balancing::{
    CassHostFilter, DcRestriction, LoadBalancingConfig, LoadBalancingKind,
};
use crate::retry_policy::CassRetryPolicy;
use crate::ssl::CassSsl;
use crate::timestamp_generator::CassTimestampGen;
use crate::types::*;
use crate::uuid::CassUuid;
use openssl::ssl::SslContextBuilder;
use openssl_sys::SSL_CTX_up_ref;
use scylla::client::execution_profile::ExecutionProfileBuilder;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::{PoolSize, SelfIdentity, WriteCoalescingDelay};
use scylla::frame::Compression;
use scylla::policies::host_filter::HostFilter;
use scylla::policies::load_balancing::LatencyAwarenessBuilder;
use scylla::policies::retry::{DefaultRetryPolicy, RetryPolicy};
use scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::policies::timestamp_generator::{MonotonicTimestampGenerator, TimestampGenerator};
use scylla::routing::ShardAwarePortRange;
use scylla::statement::{Consistency, SerialConsistency};
use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;
use std::net::IpAddr;
use std::num::{NonZero, NonZeroUsize};
use std::os::raw::{c_char, c_int, c_uint};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::cass_compression_types::CassCompressionType;

// According to `cassandra.h` the defaults for
// - consistency for statements is LOCAL_ONE,
pub(crate) const DEFAULT_CONSISTENCY: Consistency = Consistency::LocalOne;
// - serial consistency for statements is ANY, which corresponds to None in Rust Driver.
const DEFAULT_SERIAL_CONSISTENCY: Option<SerialConsistency> = None;
// - request client timeout is 12000 millis,
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(12000);
// - fetching schema metadata is true
const DEFAULT_DO_FETCH_SCHEMA_METADATA: bool = true;
// - schema agreement timeout is 10000 millis,
const DEFAULT_MAX_SCHEMA_WAIT_TIME: Duration = Duration::from_millis(10000);
// - schema agreement interval is 200 millis.
// This default is taken from rust-driver, since this option is an extension to cpp-rust-driver.
const DEFAULT_SCHEMA_AGREEMENT_INTERVAL: Duration = Duration::from_millis(200);
// - setting TCP_NODELAY is true
const DEFAULT_SET_TCP_NO_DELAY: bool = true;
// - connection pool size is 1 per shard
const DEFAULT_CONNECTION_POOL_SIZE: PoolSize = PoolSize::PerShard(NonZeroUsize::new(1).unwrap());
// - enabling write coalescing
const DEFAULT_ENABLE_WRITE_COALESCING: bool = true;
// - write coalescing delay
const DEFAULT_WRITE_COALESCING_DELAY: WriteCoalescingDelay =
    WriteCoalescingDelay::SmallNondeterministic;
// - connect timeout is 5000 millis
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(5000);
// - keepalive interval is 30 secs
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
// - keepalive timeout is 60 secs
const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(60);
// - TCP keepalive interval (actually: delay before the first keepalive
//   probe is sent) is 0 sec in the CPP Driver. However, libuv started
//   to reject such configuration on purpose since 1.49, so let's follow
//   its reasoning and set it to more.
//   1 sec could make sense, but Rust driver warns if it's not greater
//   than 1 sec (for performance reasons), so let's set 2 secs..
const DEFAULT_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2);
// - default local ip address is arbitrary
const DEFAULT_LOCAL_IP_ADDRESS: Option<IpAddr> = None;
// - default shard aware local port range is ephemeral range
const DEFAULT_SHARD_AWARE_LOCAL_PORT_RANGE: ShardAwarePortRange =
    ShardAwarePortRange::EPHEMERAL_PORT_RANGE;

const DRIVER_NAME: &str = "ScyllaDB Cpp-Rust Driver";
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
pub struct CassCluster {
    session_builder: SessionBuilder,
    default_execution_profile_builder: ExecutionProfileBuilder,
    execution_profile_map: HashMap<ExecProfileName, CassExecProfile>,

    contact_points: Vec<String>,
    port: u16,

    load_balancing_config: LoadBalancingConfig,

    use_beta_protocol_version: bool,
    auth_username: Option<String>,
    auth_password: Option<String>,

    client_id: Option<uuid::Uuid>,
}

impl CassCluster {
    pub(crate) fn execution_profile_map(&self) -> &HashMap<ExecProfileName, CassExecProfile> {
        &self.execution_profile_map
    }

    #[inline]
    pub(crate) fn get_client_id(&self) -> Option<uuid::Uuid> {
        self.client_id
    }

    pub(crate) fn build_host_filter(&self) -> Arc<dyn HostFilter> {
        CassHostFilter::new_from_lbp_configs(
            std::iter::once(&self.load_balancing_config).chain(
                self.execution_profile_map
                    .values()
                    // Filter out the profiles that do not have specified base LBP.
                    // If base LBP is not specified, the extensions such as filtering
                    // are simply ignored - default (cluster) LBP is used.
                    .filter_map(|exec_profile| {
                        exec_profile
                            .load_balancing_config
                            .load_balancing_kind
                            .as_ref()
                            .map(|_| &exec_profile.load_balancing_config)
                    }),
            ),
        )
    }
}

// Utilities for integration testing
#[cfg(cpp_integration_testing)]
impl CassCluster {
    #[inline]
    pub(crate) fn get_session_config(&self) -> &scylla::client::session::SessionConfig {
        &self.session_builder.config
    }

    #[inline]
    pub(crate) fn get_port(&self) -> u16 {
        self.port
    }

    #[inline]
    pub(crate) fn get_contact_points(&self) -> &[String] {
        &self.contact_points
    }
}

impl FFI for CassCluster {
    type Origin = FromBox;
}

impl CassCluster {
    // We want to make sure that the returned future does not depend
    // on the provided &CassCluster, hence the `static here.
    pub(crate) fn build_session_builder(&self) -> impl Future<Output = SessionBuilder> + 'static {
        let known_nodes = self
            .contact_points
            .iter()
            .map(|cp| format!("{}:{}", cp, self.port));
        let mut execution_profile_builder = self.default_execution_profile_builder.clone();
        let load_balancing_config = self.load_balancing_config.clone();
        let mut session_builder = self.session_builder.clone().known_nodes(known_nodes);
        if let (Some(username), Some(password)) = (&self.auth_username, &self.auth_password) {
            session_builder = session_builder.user(username, password)
        }

        async move {
            let load_balancing = load_balancing_config.clone().build().await;
            execution_profile_builder =
                execution_profile_builder.load_balancing_policy(load_balancing);
            session_builder
                .default_execution_profile_handle(execution_profile_builder.build().into_handle())
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_new() -> CassOwnedExclusivePtr<CassCluster, CMut> {
    let default_execution_profile_builder = ExecutionProfileBuilder::default()
        .consistency(DEFAULT_CONSISTENCY)
        .serial_consistency(DEFAULT_SERIAL_CONSISTENCY)
        .request_timeout(Some(DEFAULT_REQUEST_TIMEOUT))
        .retry_policy(Arc::new(DefaultRetryPolicy::new()))
        .speculative_execution_policy(None);
    // Load balancing is set in LoadBalancingConfig::build().
    // The default load balancing policy is DCAware in CPP Driver.
    // NOTE: CPP Driver initializes the local DC to the first node the client
    // connects to. This is tricky, a possible footgun, and hard to be done with
    // the current Rust driver implementation, which is why do don't use DC awareness
    // by default.

    /* Default config options - according to `cassandra.h`:
     * ```c++
     * // Cluster-level defaults
     * #define CASS_DEFAULT_CONNECT_TIMEOUT_MS 5000
     * #define CASS_DEFAULT_HEARTBEAT_INTERVAL_SECS 30
     * #define CASS_DEFAULT_HOSTNAME_RESOLUTION_ENABLED false
     * #define CASS_DEFAULT_IDLE_TIMEOUT_SECS 60
     * #define CASS_DEFAULT_LOG_LEVEL CASS_LOG_WARN
     * #define CASS_DEFAULT_MAX_PREPARES_PER_FLUSH 128
     * #define CASS_DEFAULT_MAX_REUSABLE_WRITE_OBJECTS UINT_MAX
     * #define CASS_DEFAULT_MAX_SCHEMA_WAIT_TIME_MS 10000
     * #define CASS_DEFAULT_NUM_CONNECTIONS_PER_HOST 1
     * #define CASS_DEFAULT_PREPARE_ON_ALL_HOSTS true
     * #define CASS_DEFAULT_PREPARE_ON_UP_OR_ADD_HOST true
     * #define CASS_DEFAULT_PORT 9042
     * #define CASS_DEFAULT_QUEUE_SIZE_IO 8192
     * #define CASS_DEFAULT_CONSTANT_RECONNECT_WAIT_TIME_MS 2000u
     * #define CASS_DEFAULT_EXPONENTIAL_RECONNECT_BASE_DELAY_MS \
     *   CASS_DEFAULT_CONSTANT_RECONNECT_WAIT_TIME_MS
     * #define CASS_DEFAULT_EXPONENTIAL_RECONNECT_MAX_DELAY_MS 600000u // 10 minutes
     * #define CASS_DEFAULT_RESOLVE_TIMEOUT_MS 5000
     * #define CASS_DEFAULT_TCP_KEEPALIVE_DELAY_SECS 0
     * #define CASS_DEFAULT_TCP_KEEPALIVE_ENABLED true
     * #define CASS_DEFAULT_TCP_NO_DELAY_ENABLED true
     * #define CASS_DEFAULT_THREAD_COUNT_IO 1
     * #define CASS_DEFAULT_USE_TOKEN_AWARE_ROUTING true
     * #define CASS_DEFAULT_USE_SNI_ROUTING false
     * #define CASS_DEFAULT_USE_BETA_PROTOCOL_VERSION false
     * #define CASS_DEFAULT_USE_RANDOMIZED_CONTACT_POINTS true
     * #define CASS_DEFAULT_USE_SCHEMA true
     * #define CASS_DEFAULT_COALESCE_DELAY 200
     * #define CASS_DEFAULT_NEW_REQUEST_RATIO 50
     * #define CASS_DEFAULT_NO_COMPACT false
     * #define CASS_DEFAULT_CQL_VERSION "3.0.0"
     * #define CASS_DEFAULT_MAX_TRACING_DATA_WAIT_TIME_MS 15
     * #define CASS_DEFAULT_RETRY_TRACING_DATA_WAIT_TIME_MS 3
     * #define CASS_DEFAULT_TRACING_CONSISTENCY CASS_CONSISTENCY_ONE
     * #define CASS_DEFAULT_HISTOGRAM_REFRESH_INTERVAL_NO_REFRESH 0
     *
     * // Request-level defaults
     * #define CASS_DEFAULT_CONSISTENCY CASS_CONSISTENCY_LOCAL_ONE
     * #define CASS_DEFAULT_REQUEST_TIMEOUT_MS 12000u
     * #define CASS_DEFAULT_SERIAL_CONSISTENCY CASS_CONSISTENCY_ANY
     *
     *  Config()
     *    : port_(CASS_DEFAULT_PORT)
     *    , protocol_version_(ProtocolVersion::highest_supported())
     *    , use_beta_protocol_version_(CASS_DEFAULT_USE_BETA_PROTOCOL_VERSION)
     *    , thread_count_io_(CASS_DEFAULT_THREAD_COUNT_IO)
     *    , queue_size_io_(CASS_DEFAULT_QUEUE_SIZE_IO)
     *    , core_connections_per_host_(CASS_DEFAULT_NUM_CONNECTIONS_PER_HOST)
     *    , reconnection_policy_(new ExponentialReconnectionPolicy())
     *    , connect_timeout_ms_(CASS_DEFAULT_CONNECT_TIMEOUT_MS)
     *    , resolve_timeout_ms_(CASS_DEFAULT_RESOLVE_TIMEOUT_MS)
     *    , max_schema_wait_time_ms_(CASS_DEFAULT_MAX_SCHEMA_WAIT_TIME_MS)
     *    , max_tracing_wait_time_ms_(CASS_DEFAULT_MAX_TRACING_DATA_WAIT_TIME_MS)
     *    , retry_tracing_wait_time_ms_(CASS_DEFAULT_RETRY_TRACING_DATA_WAIT_TIME_MS)
     *    , tracing_consistency_(CASS_DEFAULT_TRACING_CONSISTENCY)
     *    , coalesce_delay_us_(CASS_DEFAULT_COALESCE_DELAY)
     *    , new_request_ratio_(CASS_DEFAULT_NEW_REQUEST_RATIO)
     *    , log_level_(CASS_DEFAULT_LOG_LEVEL)
     *    , log_callback_(stderr_log_callback)
     *    , log_data_(NULL)
     *    , auth_provider_(new AuthProvider())
     *    , tcp_nodelay_enable_(CASS_DEFAULT_TCP_NO_DELAY_ENABLED)
     *    , tcp_keepalive_enable_(CASS_DEFAULT_TCP_KEEPALIVE_ENABLED)
     *    , tcp_keepalive_delay_secs_(CASS_DEFAULT_TCP_KEEPALIVE_DELAY_SECS)
     *    , connection_idle_timeout_secs_(CASS_DEFAULT_IDLE_TIMEOUT_SECS)
     *    , connection_heartbeat_interval_secs_(CASS_DEFAULT_HEARTBEAT_INTERVAL_SECS)
     *    , timestamp_gen_(new MonotonicTimestampGenerator())
     *    , use_schema_(CASS_DEFAULT_USE_SCHEMA)
     *    , use_hostname_resolution_(CASS_DEFAULT_HOSTNAME_RESOLUTION_ENABLED)
     *    , use_randomized_contact_points_(CASS_DEFAULT_USE_RANDOMIZED_CONTACT_POINTS)
     *    , max_reusable_write_objects_(CASS_DEFAULT_MAX_REUSABLE_WRITE_OBJECTS)
     *    , prepare_on_all_hosts_(CASS_DEFAULT_PREPARE_ON_ALL_HOSTS)
     *    , prepare_on_up_or_add_host_(CASS_DEFAULT_PREPARE_ON_UP_OR_ADD_HOST)
     *    , no_compact_(CASS_DEFAULT_NO_COMPACT)
     *    , is_client_id_set_(false)
     *    , host_listener_(new DefaultHostListener())
     *    , monitor_reporting_interval_secs_(CASS_DEFAULT_CLIENT_MONITOR_EVENTS_INTERVAL_SECS)
     *    , cluster_metadata_resolver_factory_(new DefaultClusterMetadataResolverFactory())
     *    , histogram_refresh_interval_(CASS_DEFAULT_HISTOGRAM_REFRESH_INTERVAL_NO_REFRESH) {
     *    profiles_.set_empty_key(String());
     *
     *    // Assign the defaults to the cluster profile
     *    default_profile_.set_serial_consistency(CASS_DEFAULT_SERIAL_CONSISTENCY);
     *    default_profile_.set_request_timeout(CASS_DEFAULT_REQUEST_TIMEOUT_MS);
     *    default_profile_.set_load_balancing_policy(new DCAwarePolicy());
     *    default_profile_.set_retry_policy(new DefaultRetryPolicy());
     *    default_profile_.set_speculative_execution_policy(new NoSpeculativeExecutionPolicy());
     *  }
     * ```
     */
    let default_session_builder = {
        // Set DRIVER_NAME and DRIVER_VERSION of cpp-rust driver.
        let custom_identity = SelfIdentity::new()
            .with_custom_driver_name(DRIVER_NAME)
            .with_custom_driver_version(DRIVER_VERSION);

        SessionBuilder::new()
            .custom_identity(custom_identity)
            .fetch_schema_metadata(DEFAULT_DO_FETCH_SCHEMA_METADATA)
            .schema_agreement_timeout(DEFAULT_MAX_SCHEMA_WAIT_TIME)
            .schema_agreement_interval(DEFAULT_SCHEMA_AGREEMENT_INTERVAL)
            .tcp_nodelay(DEFAULT_SET_TCP_NO_DELAY)
            .tcp_keepalive_interval(DEFAULT_TCP_KEEPALIVE_INTERVAL)
            .connection_timeout(DEFAULT_CONNECT_TIMEOUT)
            .pool_size(DEFAULT_CONNECTION_POOL_SIZE)
            .write_coalescing(DEFAULT_ENABLE_WRITE_COALESCING)
            .write_coalescing_delay(DEFAULT_WRITE_COALESCING_DELAY)
            .keepalive_interval(DEFAULT_KEEPALIVE_INTERVAL)
            .keepalive_timeout(DEFAULT_KEEPALIVE_TIMEOUT)
            .local_ip_address(DEFAULT_LOCAL_IP_ADDRESS)
            .shard_aware_local_port_range(DEFAULT_SHARD_AWARE_LOCAL_PORT_RANGE)
            .timestamp_generator(Arc::new(MonotonicTimestampGenerator::new()))
    };

    BoxFFI::into_ptr(Box::new(CassCluster {
        session_builder: default_session_builder,
        port: 9042,
        contact_points: Vec::new(),
        // Per DataStax documentation: Without additional configuration the C/C++ driver
        // defaults to using Datacenter-aware load balancing with token-aware routing.
        use_beta_protocol_version: false,
        auth_username: None,
        auth_password: None,
        default_execution_profile_builder,
        execution_profile_map: Default::default(),
        load_balancing_config: Default::default(),
        client_id: None,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_free(cluster: CassOwnedExclusivePtr<CassCluster, CMut>) {
    BoxFFI::free(cluster);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_contact_points(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    contact_points: *const c_char,
) -> CassError {
    unsafe { cass_cluster_set_contact_points_n(cluster, contact_points, strlen(contact_points)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_contact_points_n(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    contact_points: *const c_char,
    contact_points_length: size_t,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_contact_points_n!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match unsafe {
        update_comma_delimited_list(
            &mut cluster.contact_points,
            contact_points,
            contact_points_length,
            // Ignore empty contact points.
            |s| (!s.is_empty()).then(|| s.to_string()),
        )
    } {
        Ok(()) => CassError::CASS_OK,
        Err(err) => err,
    }
}

/// A utility method to parse a comma-delimited list of items,
/// and update the provided vector accordingly.
///
/// If the provided string is null or empty, the list is cleared.
/// Otherwise, string is splitted by a comma, each item is trimmed of
/// whitespace, and passed to the provided `convert` function. Resulting
/// items are then appended to the list.
///
/// `convert` function should filter out invalid items - they will be ignored.
pub(crate) unsafe fn update_comma_delimited_list<T, F>(
    list: &mut Vec<T>,
    item_ptr: *const c_char,
    item_length: size_t,
    convert: F,
) -> Result<(), CassError>
where
    F: Fn(&str) -> Option<T>,
{
    // item_ptr is null if the user provided a null string.
    // null string is equivalent to empty string in this case - it clears the list.
    let item_str = if item_ptr.is_null() {
        None
    } else {
        match unsafe { ptr_to_cstr_n(item_ptr, item_length) } {
            Some(h) => Some(h),
            None => {
                tracing::error!("Provided non-utf8 string representing a comma-delimited list");
                return Err(CassError::CASS_ERROR_LIB_BAD_PARAMS);
            }
        }
    };

    let item_iter = item_str.and_then(|non_null_item_str| {
        // Check for string emptiness **before** splitting and parsing the entries.
        // This is what cpp-driver does.
        // For example, if user provides: "foo,bar,baz" (invalid IP addresses), we don't
        // want to clear the list, but simply ignore bad entries.
        (!non_null_item_str.is_empty()).then(|| {
            non_null_item_str
                .split(',')
                .map(|s| s.trim())
                .filter_map(convert)
        })
    });

    if let Some(item_iter) = item_iter {
        // We should append new entries.
        list.extend(item_iter);
    } else {
        // If the string is empty or null, we should clear the list.
        list.clear();
    }

    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_application_name(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    app_name: *const c_char,
) {
    unsafe { cass_cluster_set_application_name_n(cluster_raw, app_name, strlen(app_name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_application_name_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    app_name: *const c_char,
    app_name_len: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_application_name_n!");
        return;
    };
    let app_name = unsafe { ptr_to_cstr_n(app_name, app_name_len) }
        .unwrap()
        .to_string();

    cluster
        .session_builder
        .config
        .identity
        .set_application_name(app_name)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_application_version(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    app_version: *const c_char,
) {
    unsafe { cass_cluster_set_application_version_n(cluster_raw, app_version, strlen(app_version)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_application_version_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    app_version: *const c_char,
    app_version_len: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_application_version_n!");
        return;
    };
    let app_version = unsafe { ptr_to_cstr_n(app_version, app_version_len) }
        .unwrap()
        .to_string();

    cluster
        .session_builder
        .config
        .identity
        .set_application_version(app_version);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_client_id(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    client_id: CassUuid,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_client_id!");
        return;
    };

    let client_uuid: uuid::Uuid = client_id.into();
    let client_uuid_str = client_uuid.to_string();

    cluster.client_id = Some(client_uuid);
    cluster
        .session_builder
        .config
        .identity
        .set_client_id(client_uuid_str)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_use_schema(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_use_schema!");
        return;
    };

    cluster.session_builder.config.fetch_schema_metadata = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_tcp_nodelay(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_tcp_nodelay!");
        return;
    };

    cluster.session_builder.config.tcp_nodelay = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_tcp_keepalive(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
    delay_secs: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_tcp_keepalive!");
        return;
    };

    let enabled = enabled != 0;
    let tcp_keepalive_interval = enabled.then(|| Duration::from_secs(delay_secs as u64));

    cluster.session_builder.config.tcp_keepalive_interval = tcp_keepalive_interval;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_timestamp_gen(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    timestamp_gen_raw: CassBorrowedSharedPtr<CassTimestampGen, CMut>,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_timestamp_gen!");
        return;
    };
    let Some(timestamp_gen) = BoxFFI::as_ref(timestamp_gen_raw) else {
        tracing::error!(
            "Provided null timestamp generator pointer to cass_cluster_set_timestamp_gen!"
        );
        return;
    };

    let rust_timestamp_gen: Option<Arc<dyn TimestampGenerator>> = match timestamp_gen {
        // In rust-driver, `None` is equivalent to using server-side timestamp generator.
        CassTimestampGen::ServerSide => None,
        CassTimestampGen::Monotonic(monotonic_timestamp_generator) => {
            Some(Arc::clone(monotonic_timestamp_generator) as _)
        }
    };

    cluster.session_builder.config.timestamp_generator = rust_timestamp_gen;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_connection_heartbeat_interval(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    interval_secs: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_connection_heartbeat_interval!"
        );
        return;
    };

    let keepalive_interval = (interval_secs > 0).then(|| Duration::from_secs(interval_secs as u64));

    cluster.session_builder.config.keepalive_interval = keepalive_interval;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_connection_idle_timeout(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    timeout_secs: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_connection_idle_timeout!"
        );
        return;
    };

    let keepalive_timeout = (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs as u64));

    cluster.session_builder.config.keepalive_timeout = keepalive_timeout;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_connect_timeout(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    timeout_ms: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_connect_timeout!");
        return;
    };

    cluster.session_builder.config.connect_timeout = Duration::from_millis(timeout_ms.into());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_core_connections_per_host(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    num_connections: c_uint,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_core_connections_per_host!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match NonZeroUsize::new(num_connections as usize) {
        Some(non_zero_conns) => {
            cluster.session_builder.config.connection_pool_size = PoolSize::PerHost(non_zero_conns);
            CassError::CASS_OK
        }
        None => {
            tracing::error!(
                "Provided zero connections to cass_cluster_set_core_connections_per_host!"
            );
            CassError::CASS_ERROR_LIB_BAD_PARAMS
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_core_connections_per_shard(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    num_connections: c_uint,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_core_connections_per_shard!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match NonZeroUsize::new(num_connections as usize) {
        Some(non_zero_conns) => {
            cluster.session_builder.config.connection_pool_size =
                PoolSize::PerShard(non_zero_conns);
            CassError::CASS_OK
        }
        None => {
            tracing::error!(
                "Provided zero connections to cass_cluster_set_core_connections_per_shard!"
            );
            CassError::CASS_ERROR_LIB_BAD_PARAMS
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_coalesce_delay(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    delay_us: cass_int64_t,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_coalesce_delay!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let delay = match delay_us.cmp(&0) {
        std::cmp::Ordering::Less => {
            tracing::error!("Provided negative delay to cass_cluster_set_coalesce_delay!");
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        std::cmp::Ordering::Equal => None,
        std::cmp::Ordering::Greater => match NonZero::new((delay_us / 1000) as u64) {
            Some(non_zero_delay) => Some(WriteCoalescingDelay::Milliseconds(non_zero_delay)),
            // This means that 0 < delay_us < 1000.
            None => Some(WriteCoalescingDelay::SmallNondeterministic),
        },
    };

    match delay {
        Some(d) => {
            cluster.session_builder.config.enable_write_coalescing = true;
            cluster.session_builder.config.write_coalescing_delay = d;
        }
        None => cluster.session_builder.config.enable_write_coalescing = false,
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_request_timeout(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    timeout_ms: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_request_timeout!");
        return;
    };

    exec_profile_builder_modify(&mut cluster.default_execution_profile_builder, |builder| {
        // 0 -> no timeout
        builder.request_timeout((timeout_ms > 0).then(|| Duration::from_millis(timeout_ms.into())))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_max_schema_wait_time(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    wait_time_ms: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_max_schema_wait_time!");
        return;
    };

    cluster.session_builder.config.schema_agreement_timeout =
        Duration::from_millis(wait_time_ms.into());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_schema_agreement_interval(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    interval_ms: c_uint,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_schema_agreement_interval!"
        );
        return;
    };

    cluster.session_builder.config.schema_agreement_interval =
        Duration::from_millis(interval_ms.into());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_port(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    port: c_int,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_port!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let Ok(port): Result<u16, _> = port.try_into() else {
        tracing::error!("Provided invalid port number to cass_cluster_set_port!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    cluster.port = port;
    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_local_address(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    ip: *const c_char,
) -> CassError {
    // Safety: We assume that string is null-terminated.
    unsafe { cass_cluster_set_local_address_n(cluster_raw, ip, strlen(ip)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_local_address_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    ip: *const c_char,
    ip_length: size_t,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_local_address_n!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    // Semantics from cpp-driver - if pointer is null or length is 0, use the
    // arbitrary address (INADDR_ANY, or in6addr_any).
    let local_addr: Option<IpAddr> = if ip.is_null() || ip_length == 0 {
        None
    } else {
        // SAFETY: We assume that user provides valid pointer and length.
        match unsafe { ptr_to_cstr_n(ip, ip_length) } {
            Some(ip_str) => match IpAddr::from_str(ip_str) {
                Ok(addr) => Some(addr),
                Err(err) => {
                    tracing::error!("Failed to parse ip address <{}>: {}", ip_str, err);
                    return CassError::CASS_ERROR_LIB_BAD_PARAMS;
                }
            },
            None => {
                tracing::error!("Provided non-utf8 ip string to cass_cluster_set_local_address_n!");
                return CassError::CASS_ERROR_LIB_BAD_PARAMS;
            }
        }
    };

    cluster.session_builder.config.local_ip_address = local_addr;

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_local_port_range(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    lo: c_int,
    hi: c_int,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_local_port_range!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    fn compute_range_from_raw(lo: i32, hi: i32) -> Result<ShardAwarePortRange, CassError> {
        let start: u16 = lo
            .try_into()
            .map_err(|_| CassError::CASS_ERROR_LIB_BAD_PARAMS)?;
        // In cpp-driver, the `hi` is exluded from the port range.
        // In rust-driver, OTOH, we include the upper bound of the range - thus -1.
        let end: u16 = hi
            .checked_sub(1)
            .ok_or(CassError::CASS_ERROR_LIB_BAD_PARAMS)?
            .try_into()
            .map_err(|_| CassError::CASS_ERROR_LIB_BAD_PARAMS)?;

        // Further validation is performed by the constructor.
        ShardAwarePortRange::new(start..=end).map_err(|_| CassError::CASS_ERROR_LIB_BAD_PARAMS)
    }

    let range: ShardAwarePortRange = match compute_range_from_raw(lo, hi) {
        Ok(range) => range,
        Err(cass_error) => {
            // Let's use the error message from cpp-driver.
            tracing::error!("Invalid local port range. Expected: 1024 < lo <= hi < 65536.");
            return cass_error;
        }
    };

    cluster.session_builder.config.shard_aware_local_port_range = range;

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_credentials(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    username: *const c_char,
    password: *const c_char,
) {
    unsafe {
        cass_cluster_set_credentials_n(
            cluster,
            username,
            strlen(username),
            password,
            strlen(password),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_credentials_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    username_raw: *const c_char,
    username_length: size_t,
    password_raw: *const c_char,
    password_length: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_credentials_n!");
        return;
    };
    // TODO: string error handling
    let username = unsafe { ptr_to_cstr_n(username_raw, username_length) }.unwrap();
    let password = unsafe { ptr_to_cstr_n(password_raw, password_length) }.unwrap();

    cluster.auth_username = Some(username.to_string());
    cluster.auth_password = Some(password.to_string());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_load_balance_round_robin(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_load_balance_round_robin!"
        );
        return;
    };

    cluster.load_balancing_config.load_balancing_kind = Some(LoadBalancingKind::RoundRobin);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_load_balance_dc_aware(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: c_uint,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    unsafe {
        cass_cluster_set_load_balance_dc_aware_n(
            cluster,
            local_dc,
            strlen(local_dc),
            used_hosts_per_remote_dc,
            allow_remote_dcs_for_local_cl,
        )
    }
}

pub(crate) unsafe fn set_load_balance_dc_aware_n(
    load_balancing_config: &mut LoadBalancingConfig,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: c_uint,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    let Some(local_dc) = (unsafe { ptr_to_cstr_n(local_dc_raw, local_dc_length) }) else {
        tracing::error!(
            "Provided null or non-UTF-8 local DC name to cass_*_set_load_balance_dc_aware(_n)!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    if local_dc_length == 0 {
        tracing::error!("Provided empty local DC name to cass_*_set_load_balance_dc_aware(_n)!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let permit_dc_failover = if used_hosts_per_remote_dc > 0 {
        // TODO: update cassandra.h documentation to reflect this behaviour.
        tracing::warn!(
            "cass_*_set_load_balance_dc_aware(_n): `used_hosts_per_remote_dc` parameter is only partially \
            supported in the driver: `0` is supported correctly, and any value `>0` has the semantics of \"+inf\", \
            which means no limit on the number of hosts per remote DC. This is different from the original cpp-driver! \
            To clarify, you can understand this parameter as \"permit_dc_failover\", with `0` being `false` and `>0` \
            being `true`."
        );
        true
    } else {
        false
    };

    let allow_remote_dcs_for_local_cl = allow_remote_dcs_for_local_cl != 0;

    load_balancing_config.load_balancing_kind = Some(LoadBalancingKind::DcAware {
        local_dc: local_dc.to_owned(),
        permit_dc_failover,
        allow_remote_dcs_for_local_cl,
    });
    load_balancing_config.filtering.dc_restriction = if permit_dc_failover {
        DcRestriction::None
    } else {
        DcRestriction::Local(local_dc.to_owned())
    };

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_load_balance_dc_aware_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: c_uint,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_load_balance_dc_aware_n!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    unsafe {
        set_load_balance_dc_aware_n(
            &mut cluster.load_balancing_config,
            local_dc_raw,
            local_dc_length,
            used_hosts_per_remote_dc,
            allow_remote_dcs_for_local_cl,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_load_balance_rack_aware(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    local_dc_raw: *const c_char,
    local_rack_raw: *const c_char,
) -> CassError {
    unsafe {
        cass_cluster_set_load_balance_rack_aware_n(
            cluster_raw,
            local_dc_raw,
            strlen(local_dc_raw),
            local_rack_raw,
            strlen(local_rack_raw),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_load_balance_rack_aware_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    local_rack_raw: *const c_char,
    local_rack_length: size_t,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_load_balance_rack_aware_n!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    unsafe {
        set_load_balance_rack_aware_n(
            &mut cluster.load_balancing_config,
            local_dc_raw,
            local_dc_length,
            local_rack_raw,
            local_rack_length,
        )
    }
}

pub(crate) unsafe fn set_load_balance_rack_aware_n(
    load_balancing_config: &mut LoadBalancingConfig,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    local_rack_raw: *const c_char,
    local_rack_length: size_t,
) -> CassError {
    let (local_dc, local_rack) = match (
        unsafe { ptr_to_cstr_n(local_dc_raw, local_dc_length) },
        unsafe { ptr_to_cstr_n(local_rack_raw, local_rack_length) },
    ) {
        (Some(local_dc_str), Some(local_rack_str))
            if local_dc_length > 0 && local_rack_length > 0 =>
        {
            (local_dc_str.to_owned(), local_rack_str.to_owned())
        }
        // One of them either is a null pointer, is an empty string or is not a proper utf-8.
        _ => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    load_balancing_config.load_balancing_kind = Some(LoadBalancingKind::RackAware {
        local_dc,
        local_rack,
    });

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_use_beta_protocol_version(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enable: cass_bool_t,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_use_beta_protocol_version!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    cluster.use_beta_protocol_version = enable == cass_true;

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_protocol_version(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    protocol_version: c_int,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_protocol_version!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    if protocol_version == 4 && !cluster.use_beta_protocol_version {
        // Rust Driver supports only protocol version 4
        CassError::CASS_OK
    } else {
        CassError::CASS_ERROR_LIB_BAD_PARAMS
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_cluster_set_queue_size_event(
    _cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    _queue_size: c_uint,
) -> CassError {
    // In Cpp Driver this function is also a no-op...
    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_constant_speculative_execution_policy(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    constant_delay_ms: cass_int64_t,
    max_speculative_executions: c_int,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_constant_speculative_execution_policy!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    if constant_delay_ms < 0 || max_speculative_executions < 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let policy = SimpleSpeculativeExecutionPolicy {
        max_retry_count: max_speculative_executions as usize,
        retry_interval: Duration::from_millis(constant_delay_ms as u64),
    };

    exec_profile_builder_modify(&mut cluster.default_execution_profile_builder, |builder| {
        builder.speculative_execution_policy(Some(Arc::new(policy)))
    });

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_no_speculative_execution_policy(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_no_speculative_execution_policy!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    exec_profile_builder_modify(&mut cluster.default_execution_profile_builder, |builder| {
        builder.speculative_execution_policy(None)
    });

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_token_aware_routing(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_token_aware_routing!");
        return;
    };

    cluster.load_balancing_config.token_awareness_enabled = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_token_aware_routing_shuffle_replicas(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_token_aware_routing_shuffle_replicas!"
        );
        return;
    };

    cluster
        .load_balancing_config
        .token_aware_shuffling_replicas_enabled = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_retry_policy(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    cass_retry_policy: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_retry_policy!");
        return;
    };

    let maybe_unset_cass_retry_policy = ArcFFI::as_ref(cass_retry_policy);
    let MaybeUnsetConfig::Set(retry_policy): MaybeUnsetConfig<_, Arc<dyn RetryPolicy>> =
        MaybeUnsetConfig::from_c_value_infallible(maybe_unset_cass_retry_policy)
    else {
        tracing::error!("Provided null retry policy pointer to cass_cluster_set_retry_policy!");
        return;
    };

    exec_profile_builder_modify(&mut cluster.default_execution_profile_builder, |builder| {
        builder.retry_policy(retry_policy)
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_ssl(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    ssl: CassBorrowedSharedPtr<CassSsl, CMut>,
) {
    let Some(cluster_from_raw) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_ssl!");
        return;
    };
    let Some(cass_ssl) = ArcFFI::as_ref(ssl) else {
        tracing::error!("Provided null ssl pointer to cass_cluster_set_ssl!");
        return;
    };

    let ssl_context_builder = unsafe { SslContextBuilder::from_ptr(cass_ssl.ssl_context) };
    // Reference count is increased as tokio_openssl will try to free `ssl_context` when calling `SSL_free`.
    unsafe { SSL_CTX_up_ref(cass_ssl.ssl_context) };

    cluster_from_raw.session_builder.config.tls_context = Some(ssl_context_builder.build().into());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_compression(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    compression_type: CassCompressionType,
) {
    let Some(cluster_from_raw) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_compression!");
        return;
    };

    let compression = match compression_type {
        CassCompressionType::CASS_COMPRESSION_LZ4 => Some(Compression::Lz4),
        CassCompressionType::CASS_COMPRESSION_SNAPPY => Some(Compression::Snappy),
        CassCompressionType::CASS_COMPRESSION_NONE => None,
        _ => None,
    };

    cluster_from_raw.session_builder.config.compression = compression;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_latency_aware_routing(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_latency_aware_routing!");
        return;
    };

    cluster.load_balancing_config.latency_awareness_enabled = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_latency_aware_routing_settings(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    exclusion_threshold: cass_double_t,
    scale_ms: cass_uint64_t,
    retry_period_ms: cass_uint64_t,
    update_rate_ms: cass_uint64_t,
    min_measured: cass_uint64_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_latency_aware_routing_settings!"
        );
        return;
    };

    cluster.load_balancing_config.latency_awareness_builder = LatencyAwarenessBuilder::new()
        .exclusion_threshold(exclusion_threshold)
        .scale(Duration::from_millis(scale_ms))
        .retry_period(Duration::from_millis(retry_period_ms))
        .update_rate(Duration::from_millis(update_rate_ms))
        .minimum_measurements(min_measured as usize);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_whitelist_filtering(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    hosts: *const c_char,
) {
    unsafe { cass_cluster_set_whitelist_filtering_n(cluster_raw, hosts, strlen(hosts)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_whitelist_filtering_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    hosts: *const c_char,
    hosts_size: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_whitelist_filtering_n!");
        return;
    };

    unsafe {
        // Ignore the result - for some reason cluster methods do not return CassError,
        // while exec profile methods do.
        let _ = update_comma_delimited_list(
            &mut cluster.load_balancing_config.filtering.whitelist_hosts,
            hosts,
            hosts_size,
            |s| match IpAddr::from_str(s) {
                Ok(ip) => Some(ip),
                Err(err) => {
                    tracing::error!("Failed to parse ip address <{}>: {}", s, err);
                    None
                }
            },
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_blacklist_filtering(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    hosts: *const c_char,
) {
    unsafe { cass_cluster_set_blacklist_filtering_n(cluster_raw, hosts, strlen(hosts)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_blacklist_filtering_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    hosts: *const c_char,
    hosts_size: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_blacklist_filtering_n!");
        return;
    };

    unsafe {
        let _ = update_comma_delimited_list(
            &mut cluster.load_balancing_config.filtering.blacklist_hosts,
            hosts,
            hosts_size,
            |s| match IpAddr::from_str(s) {
                Ok(ip) => Some(ip),
                Err(err) => {
                    tracing::error!("Failed to parse ip address <{}>: {}", s, err);
                    None
                }
            },
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_whitelist_dc_filtering(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    dcs: *const c_char,
) {
    unsafe { cass_cluster_set_whitelist_dc_filtering_n(cluster_raw, dcs, strlen(dcs)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_whitelist_dc_filtering_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    dcs: *const c_char,
    dcs_size: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_whitelist_dc_filtering!"
        );
        return;
    };

    unsafe {
        let _ = update_comma_delimited_list(
            &mut cluster.load_balancing_config.filtering.whitelist_dc,
            dcs,
            dcs_size,
            // Filter out empty dcs.
            |s| (!s.is_empty()).then(|| s.to_owned()),
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_blacklist_dc_filtering(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    dcs: *const c_char,
) {
    unsafe { cass_cluster_set_blacklist_dc_filtering_n(cluster_raw, dcs, strlen(dcs)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_blacklist_dc_filtering_n(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    dcs: *const c_char,
    dcs_size: size_t,
) {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster_raw) else {
        tracing::error!(
            "Provided null cluster pointer to cass_cluster_set_blacklist_dc_filtering_n!"
        );
        return;
    };

    unsafe {
        let _ = update_comma_delimited_list(
            &mut cluster.load_balancing_config.filtering.blacklist_dc,
            dcs,
            dcs_size,
            // Filter out empty dcs.
            |s| (!s.is_empty()).then(|| s.to_owned()),
        );
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_consistency(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    consistency: CassConsistency,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_consistency!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Ok(maybe_set_consistency) = MaybeUnsetConfig::<_, Consistency>::from_c_value(consistency)
    else {
        // Invalid consistency value provided.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match maybe_set_consistency {
        MaybeUnsetConfig::Unset(_) => {
            // `CASS_CONSISTENCY_UNKNOWN` is not supported in the cluster settings.
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        MaybeUnsetConfig::Set(consistency) => {
            exec_profile_builder_modify(
                &mut cluster.default_execution_profile_builder,
                |builder| builder.consistency(consistency),
            );
        }
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_serial_consistency(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    serial_consistency: CassConsistency,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_serial_consistency!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Ok(maybe_set_serial_consistency) =
        MaybeUnsetConfig::<_, Option<SerialConsistency>>::from_c_value(serial_consistency)
    else {
        // Invalid serial consistency value provided.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match maybe_set_serial_consistency {
        MaybeUnsetConfig::Unset(_) => {
            // `CASS_CONSISTENCY_UNKNOWN` is not supported in the cluster settings.
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        MaybeUnsetConfig::Set(serial_consistency) => {
            exec_profile_builder_modify(
                &mut cluster.default_execution_profile_builder,
                |builder| builder.serial_consistency(serial_consistency),
            );
        }
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_execution_profile(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    name: *const c_char,
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
) -> CassError {
    unsafe { cass_cluster_set_execution_profile_n(cluster, name, strlen(name), profile) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_execution_profile_n(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    name: *const c_char,
    name_length: size_t,
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
) -> CassError {
    let Some(cluster) = BoxFFI::as_mut_ref(cluster) else {
        tracing::error!("Provided null cluster pointer to cass_cluster_set_execution_profile_n!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let name = if let Some(name) =
        unsafe { ptr_to_cstr_n(name, name_length) }.and_then(|name| name.to_owned().try_into().ok())
    {
        name
    } else {
        // Got NULL or empty string, which is invalid name for a profile.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let profile = if let Some(profile) = BoxFFI::as_ref(profile) {
        profile.clone()
    } else {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    cluster.execution_profile_map.insert(name, profile);

    CassError::CASS_OK
}

#[cfg(test)]
mod tests {
    use crate::testing::{assert_cass_error_eq, setup_tracing};

    use super::*;
    use crate::{
        argconv::make_c_str,
        cass_error::CassError,
        exec_profile::{cass_execution_profile_free, cass_execution_profile_new},
    };
    use assert_matches::assert_matches;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::{
        collections::HashSet,
        convert::{TryFrom, TryInto},
        os::raw::c_char,
    };

    #[test]
    fn test_local_ip_address() {
        unsafe {
            let mut cluster_raw = cass_cluster_new();

            // Check default address
            {
                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.local_ip_address.is_none());
            }

            // null ip pointer
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_address(cluster_raw.borrow_mut(), std::ptr::null()),
                    CassError::CASS_OK
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.local_ip_address.is_none());
            }

            // empty string
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_address(cluster_raw.borrow_mut(), c"".as_ptr()),
                    CassError::CASS_OK
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.local_ip_address.is_none());
            }

            // valid ipv4 address
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_address(cluster_raw.borrow_mut(), c"1.2.3.4".as_ptr()),
                    CassError::CASS_OK
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert_eq!(
                    cluster.session_builder.config.local_ip_address,
                    Some(Ipv4Addr::new(1, 2, 3, 4).into())
                );
            }

            // valid ipv6 address
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_address(
                        cluster_raw.borrow_mut(),
                        c"2001:db8::8a2e:370:7334".as_ptr()
                    ),
                    CassError::CASS_OK
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert_eq!(
                    cluster.session_builder.config.local_ip_address,
                    Some(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0x8a2e, 0x0370, 0x7334,).into())
                );
            }

            // non-numeric address
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_address(cluster_raw.borrow_mut(), c"foo".as_ptr()),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // non-valid-utf8 slice
            {
                let non_utf8_slice: &[u8] = &[0xF0, 0x28, 0x8C, 0x28, 0x00];
                assert_cass_error_eq!(
                    cass_cluster_set_local_address(
                        cluster_raw.borrow_mut(),
                        non_utf8_slice.as_ptr() as *const c_char
                    ),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            cass_cluster_free(cluster_raw);
        }
    }

    #[test]
    fn test_local_port_range() {
        // TODO: Currently no way to compare the `ShardAwarePortRange`. Either implement `PartialEq`
        // or expose a getter for underlying range on rust-driver side. We can test the validation, though.

        unsafe {
            let mut cluster_raw = cass_cluster_new();

            // negative value
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), -1, 1025),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // start (inclusive) == end (exclusive)
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), 5555, 5555),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // start == end - 1
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), 5556, 5556),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // start > end
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), 5556, 5555),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // 0 <= start,end < 1024
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), 1, 3),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // end is i32::MIN - check that does not panic due to overflow
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), 5555, i32::MIN),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // some valid port range
            {
                assert_cass_error_eq!(
                    cass_cluster_set_local_port_range(cluster_raw.borrow_mut(), 5555, 5557),
                    CassError::CASS_OK
                );
            }

            cass_cluster_free(cluster_raw);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_coalescing_delay() {
        #[derive(Debug)]
        struct DelayEqWrapper<'a>(&'a WriteCoalescingDelay);
        impl PartialEq for DelayEqWrapper<'_> {
            fn eq(&self, other: &Self) -> bool {
                match (self.0, other.0) {
                    (
                        WriteCoalescingDelay::SmallNondeterministic,
                        WriteCoalescingDelay::SmallNondeterministic,
                    ) => true,
                    (
                        WriteCoalescingDelay::Milliseconds(delay_self),
                        WriteCoalescingDelay::Milliseconds(delay_other),
                    ) => delay_self == delay_other,
                    _ => false,
                }
            }
        }

        unsafe {
            let mut cluster_raw = cass_cluster_new();

            // Check the defaults
            {
                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.enable_write_coalescing);
                assert_eq!(
                    DelayEqWrapper(&cluster.session_builder.config.write_coalescing_delay),
                    DelayEqWrapper(&WriteCoalescingDelay::SmallNondeterministic)
                );
            }

            // Provide negative delay
            {
                assert_cass_error_eq!(
                    cass_cluster_set_coalesce_delay(cluster_raw.borrow_mut(), -1),
                    CassError::CASS_ERROR_LIB_BAD_PARAMS
                );
            }

            // Provide zero delay (disables write coalescing)
            {
                assert_cass_error_eq!(
                    cass_cluster_set_coalesce_delay(cluster_raw.borrow_mut(), 0),
                    CassError::CASS_OK,
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(!cluster.session_builder.config.enable_write_coalescing);
            }

            // Provide sub-millisecond delay
            {
                assert_cass_error_eq!(
                    cass_cluster_set_coalesce_delay(cluster_raw.borrow_mut(), 420),
                    CassError::CASS_OK,
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.enable_write_coalescing);
                assert_eq!(
                    DelayEqWrapper(&cluster.session_builder.config.write_coalescing_delay),
                    DelayEqWrapper(&WriteCoalescingDelay::SmallNondeterministic)
                );
            }

            // Provide millisecond delay
            {
                assert_cass_error_eq!(
                    cass_cluster_set_coalesce_delay(cluster_raw.borrow_mut(), 1000),
                    CassError::CASS_OK,
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.enable_write_coalescing);
                assert_eq!(
                    DelayEqWrapper(&cluster.session_builder.config.write_coalescing_delay),
                    DelayEqWrapper(&WriteCoalescingDelay::Milliseconds(
                        NonZero::new(1).unwrap()
                    ))
                );
            }

            // Provide delay with some microseconds remainder - this should take the floor of (micros as f64 / 1000.0)
            {
                assert_cass_error_eq!(
                    cass_cluster_set_coalesce_delay(cluster_raw.borrow_mut(), 2137),
                    CassError::CASS_OK,
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(cluster.session_builder.config.enable_write_coalescing);
                assert_eq!(
                    DelayEqWrapper(&cluster.session_builder.config.write_coalescing_delay),
                    DelayEqWrapper(&WriteCoalescingDelay::Milliseconds(
                        NonZero::new(2).unwrap()
                    ))
                );
            }

            cass_cluster_free(cluster_raw);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_load_balancing_config() {
        unsafe {
            let mut cluster_raw = cass_cluster_new();
            {
                /* Test valid configurations */
                {
                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    assert_matches!(cluster.load_balancing_config.load_balancing_kind, None);
                    assert!(cluster.load_balancing_config.token_awareness_enabled);
                    assert!(!cluster.load_balancing_config.latency_awareness_enabled);
                }
                {
                    cass_cluster_set_token_aware_routing(cluster_raw.borrow_mut(), 0);
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_dc_aware(
                            cluster_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            0, // forbid DC failover
                            0
                        ),
                        CassError::CASS_OK
                    );
                    cass_cluster_set_latency_aware_routing(cluster_raw.borrow_mut(), 1);
                    // These values cannot currently be tested to be set properly in the latency awareness builder,
                    // but at least we test that the function completed successfully.
                    cass_cluster_set_latency_aware_routing_settings(
                        cluster_raw.borrow_mut(),
                        2.,
                        1,
                        2000,
                        100,
                        40,
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    let load_balancing_kind = &cluster.load_balancing_config.load_balancing_kind;
                    match load_balancing_kind {
                        Some(LoadBalancingKind::DcAware {
                            local_dc,
                            permit_dc_failover,
                            allow_remote_dcs_for_local_cl,
                        }) => {
                            assert_eq!(local_dc, "eu");
                            assert!(!permit_dc_failover);
                            assert!(!allow_remote_dcs_for_local_cl);
                        }
                        _ => panic!("Expected preferred dc"),
                    }
                    assert!(!cluster.load_balancing_config.token_awareness_enabled);
                    assert!(cluster.load_balancing_config.latency_awareness_enabled);

                    // set preferred rack+dc
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_rack_aware(
                            cluster_raw.borrow_mut(),
                            c"eu-east".as_ptr(),
                            c"rack1".as_ptr(),
                        ),
                        CassError::CASS_OK
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    let node_location_preference =
                        &cluster.load_balancing_config.load_balancing_kind;
                    match node_location_preference {
                        Some(LoadBalancingKind::RackAware {
                            local_dc,
                            local_rack,
                        }) => {
                            assert_eq!(local_dc, "eu-east");
                            assert_eq!(local_rack, "rack1");
                        }
                        _ => panic!("Expected preferred dc and rack"),
                    }

                    // set back to preferred dc
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_dc_aware(
                            cluster_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            42,        // allow DC failover
                            cass_true  // allow remote DCs for local CL
                        ),
                        CassError::CASS_OK
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    let node_location_preference =
                        &cluster.load_balancing_config.load_balancing_kind;
                    match node_location_preference {
                        Some(LoadBalancingKind::DcAware {
                            local_dc,
                            permit_dc_failover,
                            allow_remote_dcs_for_local_cl,
                        }) => {
                            assert_eq!(local_dc, "eu");
                            assert!(permit_dc_failover);
                            assert!(allow_remote_dcs_for_local_cl);
                        }
                        _ => panic!("Expected preferred dc"),
                    }
                }
                /* Test invalid configurations */
                {
                    // null pointers
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_dc_aware(
                            cluster_raw.borrow_mut(),
                            std::ptr::null(),
                            0,
                            0
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_rack_aware(
                            cluster_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            std::ptr::null(),
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_rack_aware(
                            cluster_raw.borrow_mut(),
                            std::ptr::null(),
                            c"rack".as_ptr(),
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );

                    // empty strings
                    // Allow it, since c"" somehow clashes with #[ntest] proc macro...
                    #[allow(clippy::manual_c_str_literals)]
                    let empty_str = "\0".as_ptr() as *const i8;
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_dc_aware(
                            cluster_raw.borrow_mut(),
                            std::ptr::null(),
                            0,
                            0
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_rack_aware(
                            cluster_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            empty_str,
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_rack_aware(
                            cluster_raw.borrow_mut(),
                            empty_str,
                            c"rack".as_ptr(),
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                }
            }

            cass_cluster_free(cluster_raw);
        }
    }

    #[test]
    fn test_cluster_whitelist_blacklist_filtering_config() {
        setup_tracing();

        unsafe {
            let mut cluster_raw = cass_cluster_new();

            // Check the defaults
            {
                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(
                    cluster
                        .load_balancing_config
                        .filtering
                        .whitelist_hosts
                        .is_empty()
                );
                assert!(
                    cluster
                        .load_balancing_config
                        .filtering
                        .blacklist_hosts
                        .is_empty()
                );
                assert!(
                    cluster
                        .load_balancing_config
                        .filtering
                        .whitelist_dc
                        .is_empty()
                );
                assert!(
                    cluster
                        .load_balancing_config
                        .filtering
                        .blacklist_dc
                        .is_empty()
                );
            }

            // add some addresses (and some additional whitespaces)
            {
                cass_cluster_set_whitelist_filtering(
                    cluster_raw.borrow_mut(),
                    c" 127.0.0.1 ,  127.0.0.2 ".as_ptr(),
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert_eq!(
                    cluster.load_balancing_config.filtering.whitelist_hosts,
                    vec![
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))
                    ]
                );
            }

            // Mixed valid and unparsable addressed.
            // Unparsable addresses should be ignored.
            {
                cass_cluster_set_whitelist_filtering(
                    cluster_raw.borrow_mut(),
                    c"foo, 127.0.0.3, bar,,baz".as_ptr(),
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert_eq!(
                    cluster.load_balancing_config.filtering.whitelist_hosts,
                    vec![
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3))
                    ]
                );
            }

            // Provide empty string - this should clear the list.
            {
                cass_cluster_set_whitelist_filtering(cluster_raw.borrow_mut(), c"".as_ptr());

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(
                    cluster
                        .load_balancing_config
                        .filtering
                        .whitelist_hosts
                        .is_empty()
                );
            }

            // Populate the list again...
            {
                cass_cluster_set_blacklist_filtering(
                    cluster_raw.borrow_mut(),
                    c"1.1.1.1,2.2.2.2,foo,,,,  ,3.3.3.3,".as_ptr(),
                );

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert_eq!(
                    cluster.load_balancing_config.filtering.blacklist_hosts,
                    vec![
                        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
                        IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)),
                        IpAddr::V4(Ipv4Addr::new(3, 3, 3, 3))
                    ]
                );
            }

            // ..and clear it with the null pointer
            {
                cass_cluster_set_blacklist_filtering(cluster_raw.borrow_mut(), std::ptr::null());

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert!(
                    cluster
                        .load_balancing_config
                        .filtering
                        .blacklist_hosts
                        .is_empty()
                );
            }

            cass_cluster_free(cluster_raw);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_register_exec_profile() {
        unsafe {
            let mut cluster_raw = cass_cluster_new();
            let mut exec_profile_raw = cass_execution_profile_new();
            {
                /* Test valid configurations */
                {
                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    assert!(cluster.execution_profile_map.is_empty());
                }
                {
                    assert_cass_error_eq!(
                        cass_cluster_set_execution_profile(
                            cluster_raw.borrow_mut(),
                            make_c_str!("profile1"),
                            exec_profile_raw.borrow_mut()
                        ),
                        CassError::CASS_OK
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    assert!(cluster.execution_profile_map.len() == 1);
                    let _profile = cluster
                        .execution_profile_map
                        .get(&"profile1".to_owned().try_into().unwrap())
                        .unwrap();

                    let (c_str, c_strlen) = str_to_c_str_n("profile1");
                    assert_cass_error_eq!(
                        cass_cluster_set_execution_profile_n(
                            cluster_raw.borrow_mut(),
                            c_str,
                            c_strlen,
                            exec_profile_raw.borrow_mut()
                        ),
                        CassError::CASS_OK
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    assert!(cluster.execution_profile_map.len() == 1);
                    let _profile = cluster
                        .execution_profile_map
                        .get(&"profile1".to_owned().try_into().unwrap())
                        .unwrap();

                    assert_cass_error_eq!(
                        cass_cluster_set_execution_profile(
                            cluster_raw.borrow_mut(),
                            make_c_str!("profile2"),
                            exec_profile_raw.borrow_mut()
                        ),
                        CassError::CASS_OK
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    assert!(cluster.execution_profile_map.len() == 2);
                    let _profile = cluster
                        .execution_profile_map
                        .get(&"profile2".to_owned().try_into().unwrap())
                        .unwrap();
                }

                /* Test invalid configurations */
                {
                    // NULL name
                    assert_cass_error_eq!(
                        cass_cluster_set_execution_profile(
                            cluster_raw.borrow_mut(),
                            std::ptr::null(),
                            exec_profile_raw.borrow_mut()
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                }
                {
                    // empty name
                    assert_cass_error_eq!(
                        cass_cluster_set_execution_profile(
                            cluster_raw.borrow_mut(),
                            make_c_str!(""),
                            exec_profile_raw.borrow_mut()
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                }
                {
                    // NULL profile
                    assert_cass_error_eq!(
                        cass_cluster_set_execution_profile(
                            cluster_raw.borrow_mut(),
                            make_c_str!("profile1"),
                            BoxFFI::null_mut(),
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                }
                // Make sure that invalid configuration did not influence the profile map

                let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                assert_eq!(
                    cluster
                        .execution_profile_map
                        .keys()
                        .cloned()
                        .collect::<HashSet<_>>(),
                    vec![
                        ExecProfileName::try_from("profile1".to_owned()).unwrap(),
                        "profile2".to_owned().try_into().unwrap()
                    ]
                    .into_iter()
                    .collect::<HashSet<ExecProfileName>>()
                );
            }

            cass_execution_profile_free(exec_profile_raw);
            cass_cluster_free(cluster_raw);
        }
    }
}
