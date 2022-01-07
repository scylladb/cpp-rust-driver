use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use core::time::Duration;
use scylla::load_balancing::{
    DcAwareRoundRobinPolicy, LoadBalancingPolicy, RoundRobinPolicy, TokenAwarePolicy,
};
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::SessionBuilder;
use std::os::raw::{c_char, c_int, c_uint};
use std::sync::Arc;

#[derive(Clone)]
enum CassClusterChildLoadBalancingPolicy {
    RoundRobinPolicy(RoundRobinPolicy),
    DcAwareRoundRobinPolicy(DcAwareRoundRobinPolicy),
}

#[derive(Clone)]
pub struct CassCluster {
    session_builder: SessionBuilder,

    contact_points: Vec<String>,
    port: u16,

    child_load_balancing_policy: CassClusterChildLoadBalancingPolicy,
    token_aware_policy_enabled: bool,
}

pub fn build_session_builder(cluster: &CassCluster) -> SessionBuilder {
    let known_nodes: Vec<_> = cluster
        .contact_points
        .clone()
        .into_iter()
        .map(|cp| format!("{}:{}", cp, cluster.port))
        .collect();

    let load_balancing: Arc<dyn LoadBalancingPolicy> = match &cluster.child_load_balancing_policy {
        CassClusterChildLoadBalancingPolicy::RoundRobinPolicy(policy) => {
            if cluster.token_aware_policy_enabled {
                Arc::new(TokenAwarePolicy::new(Box::new(policy.clone())))
            } else {
                Arc::new(policy.clone())
            }
        }
        CassClusterChildLoadBalancingPolicy::DcAwareRoundRobinPolicy(policy) => {
            if cluster.token_aware_policy_enabled {
                Arc::new(TokenAwarePolicy::new(Box::new(policy.clone())))
            } else {
                Arc::new(policy.clone())
            }
        }
    };

    cluster
        .session_builder
        .clone()
        .known_nodes(&known_nodes)
        .load_balancing(load_balancing)
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_new() -> *mut CassCluster {
    Box::into_raw(Box::new(CassCluster {
        session_builder: SessionBuilder::new(),
        port: 9042,
        contact_points: Vec::new(),
        // Per DataStax documentation: Without additional configuration the C/C++ driver
        // defaults to using Datacenter-aware load balancing with token-aware routing.
        child_load_balancing_policy: CassClusterChildLoadBalancingPolicy::DcAwareRoundRobinPolicy(
            DcAwareRoundRobinPolicy::new("".to_string()),
        ),
        token_aware_policy_enabled: true,
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
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.port = port as u16; // FIXME: validate port number
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
    cluster.session_builder.config.auth_username = Some(username.to_string());
    cluster.session_builder.config.auth_password = Some(password.to_string());
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_load_balance_round_robin(cluster_raw: *mut CassCluster) {
    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.child_load_balancing_policy =
        CassClusterChildLoadBalancingPolicy::RoundRobinPolicy(RoundRobinPolicy::new());
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
    _used_hosts_per_remote_dc: c_uint,
    _allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    // FIMXE: validation
    // FIXME: used_hosts_per_remote_dc, allow_remote_dcs_for_local_cl ignored
    // as there is no equivalent configuration in Rust Driver.
    // TODO: string error handling
    let local_dc = ptr_to_cstr_n(local_dc_raw, local_dc_length).unwrap();

    let cluster = ptr_to_ref_mut(cluster_raw);
    cluster.child_load_balancing_policy =
        CassClusterChildLoadBalancingPolicy::DcAwareRoundRobinPolicy(DcAwareRoundRobinPolicy::new(
            local_dc.to_string(),
        ));

    CassError::CASS_OK
}

#[no_mangle]
pub extern "C" fn cass_cluster_set_protocol_version(
    _cluster: *mut CassCluster,
    protocol_version: c_int,
) -> CassError {
    if protocol_version == 4 {
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
