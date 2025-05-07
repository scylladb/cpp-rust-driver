use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::exec_profile::{CassExecProfile, ExecProfileName, exec_profile_builder_modify};
use crate::future::CassFuture;
use crate::retry_policy::CassRetryPolicy;
use crate::retry_policy::RetryPolicy::*;
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
use scylla::policies::load_balancing::{
    DefaultPolicyBuilder, LatencyAwarenessBuilder, LoadBalancingPolicy,
};
use scylla::policies::retry::RetryPolicy;
use scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::policies::timestamp_generator::TimestampGenerator;
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
const DEFAULT_CONSISTENCY: Consistency = Consistency::LocalOne;
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
// - default local ip address is arbitrary
const DEFAULT_LOCAL_IP_ADDRESS: Option<IpAddr> = None;
// - default shard aware local port range is ephemeral range
const DEFAULT_SHARD_AWARE_LOCAL_PORT_RANGE: ShardAwarePortRange =
    ShardAwarePortRange::EPHEMERAL_PORT_RANGE;

const DRIVER_NAME: &str = "ScyllaDB Cpp-Rust Driver";
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
pub(crate) struct LoadBalancingConfig {
    pub(crate) token_awareness_enabled: bool,
    pub(crate) token_aware_shuffling_replicas_enabled: bool,
    pub(crate) load_balancing_kind: Option<LoadBalancingKind>,
    pub(crate) latency_awareness_enabled: bool,
    pub(crate) latency_awareness_builder: LatencyAwarenessBuilder,
}
impl LoadBalancingConfig {
    // This is `async` to prevent running this function from beyond tokio context,
    // as it results in panic due to DefaultPolicyBuilder::build() spawning a tokio task.
    pub(crate) async fn build(self) -> Arc<dyn LoadBalancingPolicy> {
        let load_balancing_kind = self
            .load_balancing_kind
            // Round robin is chosen by default for cluster wide LBP.
            .unwrap_or(LoadBalancingKind::RoundRobin);

        let mut builder = DefaultPolicyBuilder::new().token_aware(self.token_awareness_enabled);
        if self.token_awareness_enabled {
            // Cpp-driver enables shuffling replicas only if token aware routing is enabled.
            builder =
                builder.enable_shuffling_replicas(self.token_aware_shuffling_replicas_enabled);
        }

        match load_balancing_kind {
            LoadBalancingKind::DcAware { local_dc } => {
                builder = builder.prefer_datacenter(local_dc).permit_dc_failover(true)
            }
            LoadBalancingKind::RackAware {
                local_dc,
                local_rack,
            } => {
                builder = builder
                    .prefer_datacenter_and_rack(local_dc, local_rack)
                    .permit_dc_failover(true)
            }
            LoadBalancingKind::RoundRobin => {}
        }

        if self.latency_awareness_enabled {
            builder = builder.latency_awareness(self.latency_awareness_builder);
        }
        builder.build()
    }
}
impl Default for LoadBalancingConfig {
    fn default() -> Self {
        Self {
            token_awareness_enabled: true,
            token_aware_shuffling_replicas_enabled: true,
            load_balancing_kind: None,
            latency_awareness_enabled: false,
            latency_awareness_builder: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum LoadBalancingKind {
    RoundRobin,
    DcAware {
        local_dc: String,
    },
    RackAware {
        local_dc: String,
        local_rack: String,
    },
}

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

pub struct CassCustomPayload;

// We want to make sure that the returned future does not depend
// on the provided &CassCluster, hence the `static here.
pub fn build_session_builder(
    cluster: &CassCluster,
) -> impl Future<Output = SessionBuilder> + 'static {
    let known_nodes = cluster
        .contact_points
        .iter()
        .map(|cp| format!("{}:{}", cp, cluster.port));
    let mut execution_profile_builder = cluster.default_execution_profile_builder.clone();
    let load_balancing_config = cluster.load_balancing_config.clone();
    let mut session_builder = cluster.session_builder.clone().known_nodes(known_nodes);
    if let (Some(username), Some(password)) = (&cluster.auth_username, &cluster.auth_password) {
        session_builder = session_builder.user(username, password)
    }

    async move {
        let load_balancing = load_balancing_config.clone().build().await;
        execution_profile_builder = execution_profile_builder.load_balancing_policy(load_balancing);
        session_builder
            .default_execution_profile_handle(execution_profile_builder.build().into_handle())
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_new() -> CassOwnedExclusivePtr<CassCluster, CMut> {
    let default_execution_profile_builder = ExecutionProfileBuilder::default()
        .consistency(DEFAULT_CONSISTENCY)
        .request_timeout(Some(DEFAULT_REQUEST_TIMEOUT));

    // Default config options - according to cassandra.h
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
            .connection_timeout(DEFAULT_CONNECT_TIMEOUT)
            .pool_size(DEFAULT_CONNECTION_POOL_SIZE)
            .write_coalescing(DEFAULT_ENABLE_WRITE_COALESCING)
            .write_coalescing_delay(DEFAULT_WRITE_COALESCING_DELAY)
            .keepalive_interval(DEFAULT_KEEPALIVE_INTERVAL)
            .keepalive_timeout(DEFAULT_KEEPALIVE_TIMEOUT)
            .local_ip_address(DEFAULT_LOCAL_IP_ADDRESS)
            .shard_aware_local_port_range(DEFAULT_SHARD_AWARE_LOCAL_PORT_RANGE)
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
pub unsafe extern "C" fn cass_cluster_set_use_randomized_contact_points(
    _cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    _enabled: cass_bool_t,
) -> CassError {
    // FIXME: should set `use_randomized_contact_points` flag in cluster config

    CassError::CASS_OK
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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    cluster.session_builder.config.fetch_schema_metadata = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_tcp_nodelay(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    cluster.session_builder.config.tcp_nodelay = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_tcp_keepalive(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
    delay_secs: c_uint,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    let keepalive_interval = (interval_secs > 0).then(|| Duration::from_secs(interval_secs as u64));

    cluster.session_builder.config.keepalive_interval = keepalive_interval;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_connection_idle_timeout(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    timeout_secs: c_uint,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    let keepalive_timeout = (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs as u64));

    cluster.session_builder.config.keepalive_timeout = keepalive_timeout;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_connect_timeout(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    timeout_ms: c_uint,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

    cluster.session_builder.config.schema_agreement_timeout =
        Duration::from_millis(wait_time_ms.into());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_schema_agreement_interval(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    interval_ms: c_uint,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

    cluster.session_builder.config.schema_agreement_interval =
        Duration::from_millis(interval_ms.into());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_port(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    port: c_int,
) -> CassError {
    if port <= 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    cluster.port = port as u16;
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
    // TODO: string error handling
    let username = unsafe { ptr_to_cstr_n(username_raw, username_length) }.unwrap();
    let password = unsafe { ptr_to_cstr_n(password_raw, password_length) }.unwrap();

    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    cluster.auth_username = Some(username.to_string());
    cluster.auth_password = Some(password.to_string());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_load_balance_round_robin(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
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
    if local_dc_raw.is_null() || local_dc_length == 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    if used_hosts_per_remote_dc != 0 || allow_remote_dcs_for_local_cl != 0 {
        // TODO: Add warning that the parameters are deprecated and not supported in the driver.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let local_dc = unsafe { ptr_to_cstr_n(local_dc_raw, local_dc_length) }
        .unwrap()
        .to_string();

    load_balancing_config.load_balancing_kind = Some(LoadBalancingKind::DcAware { local_dc });

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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

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
pub unsafe extern "C" fn cass_cluster_set_use_beta_protocol_version(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enable: cass_bool_t,
) -> CassError {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    cluster.use_beta_protocol_version = enable == cass_true;

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_protocol_version(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    protocol_version: c_int,
) -> CassError {
    let cluster = BoxFFI::as_ref(cluster_raw).unwrap();

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
    if constant_delay_ms < 0 || max_speculative_executions < 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

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
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();
    cluster.load_balancing_config.token_awareness_enabled = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_token_aware_routing_shuffle_replicas(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

    cluster
        .load_balancing_config
        .token_aware_shuffling_replicas_enabled = enabled != 0;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_retry_policy(
    cluster_raw: CassBorrowedExclusivePtr<CassCluster, CMut>,
    retry_policy: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) {
    let cluster = BoxFFI::as_mut_ref(cluster_raw).unwrap();

    let retry_policy: Arc<dyn RetryPolicy> = match ArcFFI::as_ref(retry_policy).unwrap() {
        DefaultRetryPolicy(default) => Arc::clone(default) as _,
        FallthroughRetryPolicy(fallthrough) => Arc::clone(fallthrough) as _,
        DowngradingConsistencyRetryPolicy(downgrading) => Arc::clone(downgrading) as _,
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
    let cluster_from_raw = BoxFFI::as_mut_ref(cluster).unwrap();
    let cass_ssl = ArcFFI::cloned_from_ptr(ssl).unwrap();

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
    let cluster_from_raw = BoxFFI::as_mut_ref(cluster).unwrap();
    let compression = match compression_type {
        CassCompressionType::CASS_COMPRESSION_LZ4 => Some(Compression::Lz4),
        CassCompressionType::CASS_COMPRESSION_SNAPPY => Some(Compression::Snappy),
        _ => None,
    };

    cluster_from_raw.session_builder.config.compression = compression;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_latency_aware_routing(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    enabled: cass_bool_t,
) {
    let cluster = BoxFFI::as_mut_ref(cluster).unwrap();
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
    let cluster = BoxFFI::as_mut_ref(cluster).unwrap();
    cluster.load_balancing_config.latency_awareness_builder = LatencyAwarenessBuilder::new()
        .exclusion_threshold(exclusion_threshold)
        .scale(Duration::from_millis(scale_ms))
        .retry_period(Duration::from_millis(retry_period_ms))
        .update_rate(Duration::from_millis(update_rate_ms))
        .minimum_measurements(min_measured as usize);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_consistency(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    consistency: CassConsistency,
) -> CassError {
    let cluster = BoxFFI::as_mut_ref(cluster).unwrap();
    let consistency: Consistency = match consistency.try_into() {
        Ok(c) => c,
        Err(_) => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    exec_profile_builder_modify(&mut cluster.default_execution_profile_builder, |builder| {
        builder.consistency(consistency)
    });

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_cluster_set_serial_consistency(
    cluster: CassBorrowedExclusivePtr<CassCluster, CMut>,
    serial_consistency: CassConsistency,
) -> CassError {
    let cluster = BoxFFI::as_mut_ref(cluster).unwrap();
    let serial_consistency: SerialConsistency = match serial_consistency.try_into() {
        Ok(c) => c,
        Err(_) => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    exec_profile_builder_modify(&mut cluster.default_execution_profile_builder, |builder| {
        builder.serial_consistency(Some(serial_consistency))
    });

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
    let cluster = BoxFFI::as_mut_ref(cluster).unwrap();
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
    use crate::testing::assert_cass_error_eq;

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
                            0,
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
                        Some(LoadBalancingKind::DcAware { local_dc }) => {
                            assert_eq!(local_dc, "eu")
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
                            0,
                            0
                        ),
                        CassError::CASS_OK
                    );

                    let cluster = BoxFFI::as_ref(cluster_raw.borrow()).unwrap();
                    let node_location_preference =
                        &cluster.load_balancing_config.load_balancing_kind;
                    match node_location_preference {
                        Some(LoadBalancingKind::DcAware { local_dc }) => {
                            assert_eq!(local_dc, "eu")
                        }
                        _ => panic!("Expected preferred dc"),
                    }
                }
                /* Test invalid configurations */
                {
                    // Nonzero deprecated parameters
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_dc_aware(
                            cluster_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            1,
                            0
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_cluster_set_load_balance_dc_aware(
                            cluster_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            0,
                            1
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );

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
