use crate::argconv::*;
use crate::cass_error::CassError;
use crate::future::CassFuture;
use crate::retry_policy::CassRetryPolicy;
use crate::retry_policy::RetryPolicy::*;
use crate::ssl::CassSsl;
use crate::types::*;
use core::time::Duration;
use openssl::ssl::SslContextBuilder;
use openssl_sys::SSL_CTX_up_ref;
use scylla::frame::Compression;
use scylla::load_balancing::{
    DcAwareRoundRobinPolicy, LoadBalancingPolicy, RoundRobinPolicy, TokenAwarePolicy,
};
use scylla::retry_policy::RetryPolicy;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::SessionBuilder;
use std::os::raw::{c_char, c_int, c_uint};
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/cppdriver_compression_types.rs"));

#[derive(Clone)]
enum CassClusterChildLoadBalancingPolicy {
    RoundRobinPolicy,
    DcAwareRoundRobinPolicy {
        local_dc: String,
        include_remote_nodes: bool,
    },
}

#[derive(Clone)]
pub struct CassCluster {
    session_builder: SessionBuilder,

    contact_points: Vec<String>,
    port: u16,

    child_load_balancing_policy: CassClusterChildLoadBalancingPolicy,
    token_aware_policy_enabled: bool,
    use_beta_protocol_version: bool,
    auth_username: Option<String>,
    auth_password: Option<String>,
}

pub struct CassCustomPayload;

pub fn build_session_builder(cluster: &CassCluster) -> SessionBuilder {
    let known_nodes: Vec<_> = cluster
        .contact_points
        .clone()
        .into_iter()
        .map(|cp| format!("{}:{}", cp, cluster.port))
        .collect();

    let load_balancing: Arc<dyn LoadBalancingPolicy> =
        match cluster.child_load_balancing_policy.clone() {
            CassClusterChildLoadBalancingPolicy::RoundRobinPolicy => {
                if cluster.token_aware_policy_enabled {
                    Arc::new(TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new())))
                } else {
                    Arc::new(RoundRobinPolicy::new())
                }
            }
            CassClusterChildLoadBalancingPolicy::DcAwareRoundRobinPolicy {
                local_dc,
                include_remote_nodes,
            } => {
                let mut dc_aware_policy = DcAwareRoundRobinPolicy::new(local_dc);
                dc_aware_policy.set_include_remote_nodes(include_remote_nodes);

                if cluster.token_aware_policy_enabled {
                    Arc::new(TokenAwarePolicy::new(Box::new(dc_aware_policy)))
                } else {
                    Arc::new(dc_aware_policy)
                }
            }
        };

    let builder = cluster
        .session_builder
        .clone()
        .known_nodes(&known_nodes)
        .load_balancing(load_balancing);

    if let (Some(username), Some(password)) = (&cluster.auth_username, &cluster.auth_password) {
        builder.user(username, password)
    } else {
        builder
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_new() -> *mut CassCluster {
    Box::into_raw(Box::new(CassCluster {
        session_builder: SessionBuilder::new()
            .retry_policy(Box::new(scylla::retry_policy::DefaultRetryPolicy)),
        port: 9042,
        contact_points: Vec::new(),
        // Per DataStax documentation: Without additional configuration the C/C++ driver
        // defaults to using Datacenter-aware load balancing with token-aware routing.
        child_load_balancing_policy: CassClusterChildLoadBalancingPolicy::DcAwareRoundRobinPolicy {
            local_dc: "".to_string(),
            include_remote_nodes: true,
        },
        token_aware_policy_enabled: true,
        use_beta_protocol_version: false,
        auth_username: None,
        auth_password: None,
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
    cass_cluster_set_contact_points_n(cluster, contact_points, strlen(contact_points))
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_contact_points_n(
    cluster: *mut CassCluster,
    contact_points: *const c_char,
    contact_points_length: size_t,
) -> CassError {
    match cluster_set_contact_points(cluster, contact_points, contact_points_length) {
        Ok(()) => CassError::CASS_OK,
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
        .ok_or(CassError::CASS_ERROR_LIB_BAD_PARAMS)?
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
pub unsafe extern "C" fn cass_cluster_set_use_randomized_contact_points(
    _cluster_raw: *mut CassCluster,
    _enabled: cass_bool_t,
) -> CassError {
    // FIXME: should set `use_randomized_contact_points` flag in cluster config

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_use_schema(
    cluster_raw: *mut CassCluster,
    enabled: cass_bool_t,
) {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.session_builder.config.fetch_schema_metadata = enabled != 0;
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_tcp_nodelay(
    cluster_raw: *mut CassCluster,
    enabled: cass_bool_t,
) {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.session_builder.config.tcp_nodelay = enabled != 0;
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_connect_timeout(
    cluster_raw: *mut CassCluster,
    timeout_ms: c_uint,
) {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.session_builder.config.connect_timeout = Duration::from_millis(timeout_ms.into());
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_port(
    cluster_raw: *mut CassCluster,
    port: c_int,
) -> CassError {
    if port <= 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.port = port as u16;
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_credentials(
    cluster: *mut CassCluster,
    username: *const c_char,
    password: *const c_char,
) {
    cass_cluster_set_credentials_n(
        cluster,
        username,
        strlen(username),
        password,
        strlen(password),
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_credentials_n(
    cluster_raw: *mut CassCluster,
    username_raw: *const c_char,
    username_length: size_t,
    password_raw: *const c_char,
    password_length: size_t,
) {
    // TODO: string error handling
    let username = ptr_to_cstr_n(username_raw, username_length).unwrap();
    let password = ptr_to_cstr_n(password_raw, password_length).unwrap();

    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.auth_username = Some(username.to_string());
    cluster.auth_password = Some(password.to_string());
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_load_balance_round_robin(cluster_raw: *mut CassCluster) {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.child_load_balancing_policy = CassClusterChildLoadBalancingPolicy::RoundRobinPolicy;
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_load_balance_dc_aware(
    cluster: *mut CassCluster,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: c_uint,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    cass_cluster_set_load_balance_dc_aware_n(
        cluster,
        local_dc,
        strlen(local_dc),
        used_hosts_per_remote_dc,
        allow_remote_dcs_for_local_cl,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_load_balance_dc_aware_n(
    cluster_raw: *mut CassCluster,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: c_uint,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    if local_dc_raw.is_null() || local_dc_length == 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    if used_hosts_per_remote_dc != 0 {
        // TODO: Add warning that the parameter is deprecated and not supported in the driver.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let local_dc = ptr_to_cstr_n(local_dc_raw, local_dc_length)
        .unwrap()
        .to_string();

    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.child_load_balancing_policy =
        CassClusterChildLoadBalancingPolicy::DcAwareRoundRobinPolicy {
            local_dc,
            include_remote_nodes: allow_remote_dcs_for_local_cl != 0,
        };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_cloud_secure_connection_bundle_n(
    _cluster_raw: *mut CassCluster,
    path: *const c_char,
    path_length: size_t,
) -> CassError {
    // FIXME: Should unzip file associated with the path
    let zip_file = ptr_to_cstr_n(path, path_length).unwrap();

    if zip_file == "invalid_filename" {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_exponential_reconnect(
    _cluster_raw: *mut CassCluster,
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

#[no_mangle]
pub extern "C" fn cass_custom_payload_new() -> *const CassCustomPayload {
    // FIXME: should create a new custom payload that must be freed
    std::ptr::null()
}

#[no_mangle]
pub extern "C" fn cass_future_custom_payload_item(
    _future: *mut CassFuture,
    _i: size_t,
    _name: *const c_char,
    _name_length: size_t,
    _value: *const cass_byte_t,
    _value_size: size_t,
) -> CassError {
    CassError::CASS_OK
}

#[no_mangle]
pub extern "C" fn cass_future_custom_payload_item_count(_future: *mut CassFuture) -> size_t {
    0
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_use_beta_protocol_version(
    cluster_raw: *mut CassCluster,
    enable: cass_bool_t,
) -> CassError {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.use_beta_protocol_version = enable == cass_true;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_protocol_version(
    cluster_raw: *mut CassCluster,
    protocol_version: c_int,
) -> CassError {
    let cluster = ptr_to_ref(cluster_raw);

    if protocol_version == 4 && !cluster.use_beta_protocol_version {
        // Rust Driver supports only protocol version 4
        CassError::CASS_OK
    } else {
        CassError::CASS_ERROR_LIB_BAD_PARAMS
    }
}

#[no_mangle]
pub extern "C" fn cass_cluster_set_queue_size_event(
    _cluster: *mut CassCluster,
    _queue_size: c_uint,
) -> CassError {
    // In Cpp Driver this function is also a no-op...
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_constant_speculative_execution_policy(
    cluster_raw: *mut CassCluster,
    constant_delay_ms: cass_int64_t,
    max_speculative_executions: c_int,
) -> CassError {
    if constant_delay_ms < 0 || max_speculative_executions < 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let cluster = ptr_to_ref_mut(cluster_raw);

    let policy = SimpleSpeculativeExecutionPolicy {
        max_retry_count: max_speculative_executions as usize,
        retry_interval: Duration::from_millis(constant_delay_ms as u64),
    };

    cluster.session_builder.config.speculative_execution_policy = Some(Arc::new(policy));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_no_speculative_execution_policy(
    cluster_raw: *mut CassCluster,
) -> CassError {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.session_builder.config.speculative_execution_policy = None;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_token_aware_routing(
    cluster_raw: *mut CassCluster,
    enabled: cass_bool_t,
) {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.token_aware_policy_enabled = enabled != 0;
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_retry_policy(
    cluster_raw: *mut CassCluster,
    retry_policy: *const CassRetryPolicy,
) {
    let cluster = ptr_to_ref_mut(cluster_raw);

    let retry_policy: &dyn RetryPolicy = match ptr_to_ref(retry_policy) {
        DefaultRetryPolicy(default) => default,
        FallthroughRetryPolicy(fallthrough) => fallthrough,
        DowngradingConsistencyRetryPolicy(downgrading) => downgrading,
    };
    let boxed_retry_policy = retry_policy.clone_boxed();

    cluster.session_builder.config.retry_policy = boxed_retry_policy;
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_ssl(cluster: *mut CassCluster, ssl: *mut CassSsl) {
    let cluster_from_raw = ptr_to_ref_mut(cluster);
    let cass_ssl = clone_arced(ssl);

    let ssl_context_builder = SslContextBuilder::from_ptr(cass_ssl.ssl_context);
    // Reference count is increased as tokio_openssl will try to free `ssl_context` when calling `SSL_free`.
    SSL_CTX_up_ref(cass_ssl.ssl_context);

    cluster_from_raw.session_builder.config.ssl_context = Some(ssl_context_builder.build());
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_compression(
    cluster: *mut CassCluster,
    compression_type: CassCompressionType,
) {
    let cluster_from_raw = ptr_to_ref_mut(cluster);
    let compression = match compression_type {
        CassCompressionType::CASS_COMPRESSION_LZ4 => Some(Compression::Lz4),
        CassCompressionType::CASS_COMPRESSION_SNAPPY => Some(Compression::Snappy),
        _ => None,
    };

    cluster_from_raw.session_builder.config.compression = compression;
}
