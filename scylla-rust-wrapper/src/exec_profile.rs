use std::convert::{TryFrom, TryInto};
use std::ffi::c_char;
use std::future::Future;
use std::net::IpAddr;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use scylla::client::execution_profile::{
    ExecutionProfile, ExecutionProfileBuilder, ExecutionProfileHandle,
};
use scylla::policies::load_balancing::LatencyAwarenessBuilder;
use scylla::policies::retry::RetryPolicy;
use scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::statement::{Consistency, SerialConsistency};

use crate::argconv::{
    ArcFFI, BoxFFI, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr, CassOwnedExclusivePtr,
    FFI, FromBox, ptr_to_cstr_n, strlen,
};
use crate::batch::CassBatch;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cluster::{
    set_load_balance_dc_aware_n, set_load_balance_rack_aware_n, update_comma_delimited_list,
};
use crate::config_value::MaybeUnsetConfig;
use crate::load_balancing::{LoadBalancingConfig, LoadBalancingKind};
use crate::retry_policy::CassRetryPolicy;
use crate::session::CassSessionInner;
use crate::statement::CassStatement;
use crate::types::{
    cass_bool_t, cass_double_t, cass_int32_t, cass_int64_t, cass_uint32_t, cass_uint64_t, size_t,
};

#[derive(Clone, Debug)]
pub struct CassExecProfile {
    inner: ExecutionProfileBuilder,
    pub(crate) load_balancing_config: LoadBalancingConfig,
}

impl FFI for CassExecProfile {
    type Origin = FromBox;
}

impl CassExecProfile {
    fn new() -> Self {
        Self {
            inner: ExecutionProfile::builder(),
            load_balancing_config: Default::default(),
        }
    }

    pub(crate) async fn build(
        self,
        cluster_default_profile: &ExecutionProfile,
    ) -> ExecutionProfile {
        let load_balacing = if self.load_balancing_config.load_balancing_kind.is_some() {
            self.load_balancing_config.build().await
        } else {
            // If load balancing config does not have LB kind defined,
            // we make use of cluster's LBP.
            cluster_default_profile.get_load_balancing_policy().clone()
        };

        self.inner.load_balancing_policy(load_balacing).build()
    }
}

/// Represents a non-empty execution profile name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ExecProfileName(String);

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct EmptyProfileName;

impl TryFrom<String> for ExecProfileName {
    type Error = EmptyProfileName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.is_empty() {
            Err(EmptyProfileName)
        } else {
            Ok(ExecProfileName(name))
        }
    }
}

impl Deref for ExecProfileName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// The resolved or not yet resolved execution profile that is specific
// for a particular statement or batch.
#[derive(Debug, Clone)]
pub(crate) struct PerStatementExecProfile(Arc<RwLock<PerStatementExecProfileInner>>);

impl PerStatementExecProfile {
    pub(crate) fn new_unresolved(name: ExecProfileName) -> Self {
        Self(Arc::new(RwLock::new(
            PerStatementExecProfileInner::Unresolved(name),
        )))
    }

    // Clippy claims it is possible to make this `async fn`, but it's terribly wrong,
    // because async fn can't have its future bound to a specific lifetime, which is
    // required in this case.
    #[allow(clippy::manual_async_fn)]
    pub(crate) fn get_or_resolve_profile_handle<'a>(
        &'a self,
        cass_session_inner: &'a CassSessionInner,
    ) -> impl Future<Output = Result<ExecutionProfileHandle, (CassError, String)>> + 'a {
        async move {
            let already_resolved = {
                let read_guard = self.0.read().unwrap();
                match read_guard.deref() {
                    PerStatementExecProfileInner::Unresolved(_) => None,
                    PerStatementExecProfileInner::Resolved(handle) => Some(handle.clone()),
                }
            };

            let handle = if let Some(handle) = already_resolved {
                handle
            } else {
                let inner = &mut *self.0.write().unwrap();
                match &*inner {
                    PerStatementExecProfileInner::Unresolved(name) => {
                        let handle = cass_session_inner.resolve_exec_profile(name)?;
                        *inner = PerStatementExecProfileInner::Resolved(handle.clone());
                        handle
                    }
                    PerStatementExecProfileInner::Resolved(handle) => handle,
                }
                .clone()
            };

            Ok(handle)
        }
    }
}

// The resolved or not yet resolved execution profile that is specific
// for a particular statement or batch.
#[derive(Debug)]
pub(crate) enum PerStatementExecProfileInner {
    // This is set eagerly on `cass_{statement,batch}_set_execution_profile()` call. CPP driver resolves
    // the execution profile's name in the global hashmap on each query. As we want to avoid it,
    // we resolve it only once, lazily, on the first query after the name it set. The reason why
    // it can't be set eagerly is simple: the mentioned function does not provide any access
    // to the Session.
    Unresolved(ExecProfileName),

    // This is the handle to be cloned as per-statement handle upon each query.
    // We have to do that due to limitations of CPP driver API:
    // `cass_{statement,batch}_set_execution_profile()` does not have access to exec profiles map,
    // whereas `cass_session_execute[_batch]()` can't mutate the shared statement
    // (but only its own copy of the statement).
    //
    // The purpose of this is to make it possible to resolve the profile only once after its alterations
    // (e.g. re-setting an exec profile name) take place, and then (on further executions) used straight
    // from here without resolution.
    // In order to achieve that, we want the resolution's result to be saved into the Statement
    // struct that is shared between calls to cass_session_execute().
    // Note that we can't use references to that Statement inside of a CassFuture returned from
    // `cass_session_execute()` because of lifetime issues (possible use-after-free).
    // Therefore, we have to clone Statement's contents to inside the future and perform resolution
    // there. If this struct weren't shared under Arc and we cloned it into the future,
    // then the resolution inside the future would never propagate into the shared Statement struct.
    // The same is true for Arc'ed `PerStatementExecProfileInner` in Batch.
    Resolved(ExecutionProfileHandle),
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_new() -> CassOwnedExclusivePtr<CassExecProfile, CMut>
{
    BoxFFI::into_ptr(Box::new(CassExecProfile::new()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_free(
    profile: CassOwnedExclusivePtr<CassExecProfile, CMut>,
) {
    BoxFFI::free(profile);
}

/* Exec profiles scope setters */

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_execution_profile(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    name: *const c_char,
) -> CassError {
    unsafe { cass_statement_set_execution_profile_n(statement, name, strlen(name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_execution_profile_n(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_execution_profile!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let name: Option<ExecProfileName> = unsafe { ptr_to_cstr_n(name, name_length) }
        .and_then(|name| name.to_owned().try_into().ok());
    statement.exec_profile = name.map(PerStatementExecProfile::new_unresolved);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_execution_profile(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    name: *const c_char,
) -> CassError {
    unsafe { cass_batch_set_execution_profile_n(batch, name, strlen(name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_execution_profile_n(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_execution_profile!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let name: Option<ExecProfileName> = unsafe { ptr_to_cstr_n(name, name_length) }
        .and_then(|name| name.to_owned().try_into().ok());
    batch.exec_profile = name.map(PerStatementExecProfile::new_unresolved);

    CassError::CASS_OK
}

/* Config options setters */

pub(crate) fn exec_profile_builder_modify(
    builder: &mut ExecutionProfileBuilder,
    builder_modifier: impl FnOnce(ExecutionProfileBuilder) -> ExecutionProfileBuilder,
) {
    let taken_builder = std::mem::take(builder);
    let new_builder = builder_modifier(taken_builder);
    *builder = new_builder;
}

impl CassExecProfile {
    fn modify_in_place(
        &mut self,
        builder_modifier: impl FnOnce(ExecutionProfileBuilder) -> ExecutionProfileBuilder,
    ) {
        exec_profile_builder_modify(&mut self.inner, builder_modifier)
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_consistency(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    consistency: CassConsistency,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!("Provided null profile pointer to cass_execution_profile_set_consistency!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Ok(maybe_set_consistency) = MaybeUnsetConfig::<Consistency>::from_c_value(consistency)
    else {
        // Invalid consistency value provided.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match maybe_set_consistency {
        MaybeUnsetConfig::Unset => {
            // CASS_CONSISTENCY_UNKNOWN
            // TODO: implement semantics of this.
            // A workaround is needed, because Rust Driver's ExecutionProfileBuilder
            // does not expose API to unset consistency (enabling semantics:
            // "ignore me and use the default profile's setting").
            tracing::warn!(
                "Passed `CASS_CONSISTENCY_UNKNOWN` to `cass_exec_profile_set_consistency`. \
                This is not supported by the CPP Rust Driver yet: once you set some consistency \
                on an execution profile, you cannot unset it. This limitation will be fixed in the future. \
                As a workaround, you can refrain from setting consistency on an execution profile, \
                which will make the driver use the consistency set on cluster level."
            );
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        MaybeUnsetConfig::Set(consistency) => {
            profile_builder.modify_in_place(|builder| builder.consistency(consistency));
        }
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_no_speculative_execution_policy(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_no_speculative_execution_policy!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    profile_builder.modify_in_place(|builder| builder.speculative_execution_policy(None));

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_constant_speculative_execution_policy(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    constant_delay_ms: cass_int64_t,
    max_speculative_executions: cass_int32_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_constant_speculative_execution_policy!"
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

    profile_builder
        .modify_in_place(|builder| builder.speculative_execution_policy(Some(Arc::new(policy))));

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    enabled: cass_bool_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_latency_aware_routing!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    profile_builder
        .load_balancing_config
        .latency_awareness_enabled = enabled != 0;

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing_settings(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    exclusion_threshold: cass_double_t,
    _scale_ms: cass_uint64_t, // Currently ignored, TODO: add this parameter to Rust driver
    retry_period_ms: cass_uint64_t,
    update_rate_ms: cass_uint64_t,
    min_measured: cass_uint64_t,
) {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_latency_aware_routing_settings!"
        );
        return;
    };

    profile_builder
        .load_balancing_config
        .latency_awareness_builder = LatencyAwarenessBuilder::new()
        .exclusion_threshold(exclusion_threshold)
        .retry_period(Duration::from_millis(retry_period_ms))
        .update_rate(Duration::from_millis(update_rate_ms))
        .minimum_measurements(min_measured as usize);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    unsafe {
        cass_execution_profile_set_load_balance_dc_aware_n(
            profile,
            local_dc,
            strlen(local_dc),
            used_hosts_per_remote_dc,
            allow_remote_dcs_for_local_cl,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware_n(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    local_dc: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_load_balance_dc_aware!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    unsafe {
        set_load_balance_dc_aware_n(
            &mut profile_builder.load_balancing_config,
            local_dc,
            local_dc_length,
            used_hosts_per_remote_dc,
            allow_remote_dcs_for_local_cl,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_rack_aware(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    local_dc_raw: *const c_char,
    local_rack_raw: *const c_char,
) -> CassError {
    unsafe {
        cass_execution_profile_set_load_balance_rack_aware_n(
            profile,
            local_dc_raw,
            strlen(local_dc_raw),
            local_rack_raw,
            strlen(local_rack_raw),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_rack_aware_n(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    local_rack_raw: *const c_char,
    local_rack_length: size_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_load_balance_rack_aware!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    unsafe {
        set_load_balance_rack_aware_n(
            &mut profile_builder.load_balancing_config,
            local_dc_raw,
            local_dc_length,
            local_rack_raw,
            local_rack_length,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_round_robin(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_load_balance_round_robin!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    profile_builder.load_balancing_config.load_balancing_kind = Some(LoadBalancingKind::RoundRobin);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_whitelist_filtering(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    hosts: *const c_char,
) -> CassError {
    unsafe { cass_execution_profile_set_whitelist_filtering_n(profile_raw, hosts, strlen(hosts)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_whitelist_filtering_n(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    hosts: *const c_char,
    hosts_size: size_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile_raw) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_whitelist_filtering_n!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let result = unsafe {
        update_comma_delimited_list(
            &mut profile_builder
                .load_balancing_config
                .filtering
                .whitelist_hosts,
            hosts,
            hosts_size,
            |s| match IpAddr::from_str(s) {
                Ok(ip) => Some(ip),
                Err(err) => {
                    tracing::error!("Failed to parse ip address <{}>: {}", s, err);
                    None
                }
            },
        )
    };

    match result {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_blacklist_filtering(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    hosts: *const c_char,
) -> CassError {
    unsafe { cass_execution_profile_set_blacklist_filtering_n(profile_raw, hosts, strlen(hosts)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_blacklist_filtering_n(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    hosts: *const c_char,
    hosts_size: size_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile_raw) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_blacklist_filtering_n!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let result = unsafe {
        update_comma_delimited_list(
            &mut profile_builder
                .load_balancing_config
                .filtering
                .blacklist_hosts,
            hosts,
            hosts_size,
            |s| match IpAddr::from_str(s) {
                Ok(ip) => Some(ip),
                Err(err) => {
                    tracing::error!("Failed to parse ip address <{}>: {}", s, err);
                    None
                }
            },
        )
    };

    match result {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_whitelist_dc_filtering(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    dcs: *const c_char,
) -> CassError {
    unsafe { cass_execution_profile_set_whitelist_dc_filtering_n(profile_raw, dcs, strlen(dcs)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_whitelist_dc_filtering_n(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    dcs: *const c_char,
    dcs_size: size_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile_raw) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_whitelist_dc_filtering_n!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let result = unsafe {
        update_comma_delimited_list(
            &mut profile_builder.load_balancing_config.filtering.whitelist_dc,
            dcs,
            dcs_size,
            // Filter out empty dcs.
            |s| (!s.is_empty()).then(|| s.to_owned()),
        )
    };

    match result {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_blacklist_dc_filtering(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    dcs: *const c_char,
) -> CassError {
    unsafe { cass_execution_profile_set_blacklist_dc_filtering_n(profile_raw, dcs, strlen(dcs)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_blacklist_dc_filtering_n(
    profile_raw: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    dcs: *const c_char,
    dcs_size: size_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile_raw) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_blacklist_dc_filtering_n!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let result = unsafe {
        update_comma_delimited_list(
            &mut profile_builder.load_balancing_config.filtering.blacklist_dc,
            dcs,
            dcs_size,
            // Filter out empty dcs.
            |s| (!s.is_empty()).then(|| s.to_owned()),
        )
    };

    match result {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_request_timeout(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_request_timeout!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    profile_builder.modify_in_place(|builder| {
        builder.request_timeout(Some(std::time::Duration::from_millis(timeout_ms)))
    });

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_retry_policy(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    retry_policy: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_retry_policy!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let retry_policy: Arc<dyn RetryPolicy> = match ArcFFI::as_ref(retry_policy) {
        Some(CassRetryPolicy::Default(default)) => Arc::clone(default) as _,
        Some(CassRetryPolicy::Fallthrough(fallthrough)) => Arc::clone(fallthrough) as _,
        Some(CassRetryPolicy::DowngradingConsistency(downgrading)) => Arc::clone(downgrading) as _,
        Some(CassRetryPolicy::Logging(logging)) => Arc::clone(logging) as _,
        #[cfg(cpp_integration_testing)]
        Some(CassRetryPolicy::Ignoring(ignoring)) => Arc::clone(ignoring) as _,
        None => {
            tracing::error!(
                "Provided null retry policy pointer to cass_execution_profile_set_retry_policy!"
            );
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
    };

    profile_builder.modify_in_place(|builder| builder.retry_policy(retry_policy));

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_serial_consistency(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    serial_consistency: CassConsistency,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_serial_consistency!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Ok(maybe_set_serial_consistency) =
        MaybeUnsetConfig::<Option<SerialConsistency>>::from_c_value(serial_consistency)
    else {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match maybe_set_serial_consistency {
        MaybeUnsetConfig::Unset => {
            // CASS_CONSISTENCY_UNKNOWN
            // TODO: implement semantics of this.
            // A workaround is needed, because Rust Driver's ExecutionProfileBuilder
            // does not expose API to unset serial consistency (enabling semantics:
            // "ignore me and use the default profile's setting").
            tracing::warn!(
                "Passed `CASS_CONSISTENCY_UNKNOWN` to `cass_exec_profile_set_serial_consistency`. \
                This is not supported by the CPP Rust Driver yet: once you set some consistency \
                on an execution profile, you cannot unset it. This limitation will be fixed in the future. \
                As a workaround, you can refrain from setting serial consistency on an execution profile, \
                which will make the driver use the consistency set on cluster level."
            );
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        MaybeUnsetConfig::Set(maybe_serial_consistency) => {
            // CASS_CONSISTENCY_ANY -> None
            // other consistency -> Some()
            profile_builder
                .modify_in_place(|builder| builder.serial_consistency(maybe_serial_consistency));
        }
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_token_aware_routing(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    enabled: cass_bool_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_token_aware_routing!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    profile_builder
        .load_balancing_config
        .token_awareness_enabled = enabled != 0;

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_execution_profile_set_token_aware_routing_shuffle_replicas(
    profile: CassBorrowedExclusivePtr<CassExecProfile, CMut>,
    enabled: cass_bool_t,
) -> CassError {
    let Some(profile_builder) = BoxFFI::as_mut_ref(profile) else {
        tracing::error!(
            "Provided null profile pointer to cass_execution_profile_set_token_aware_routing_shuffle_replicas!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    profile_builder
        .load_balancing_config
        .token_aware_shuffling_replicas_enabled = enabled != 0;

    CassError::CASS_OK
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    use crate::testing::{assert_cass_error_eq, setup_tracing};
    use crate::{
        argconv::{make_c_str, str_to_c_str_n},
        batch::{cass_batch_add_statement, cass_batch_free, cass_batch_new},
        cass_types::CassBatchType,
        statement::{cass_statement_free, cass_statement_new},
    };

    use assert_matches::assert_matches;

    #[test]
    fn test_exec_profile_whitelist_blacklist_filtering_config() {
        setup_tracing();

        unsafe {
            let mut profile_raw = cass_execution_profile_new();

            // Check the defaults
            {
                let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                assert!(
                    profile
                        .load_balancing_config
                        .filtering
                        .whitelist_hosts
                        .is_empty()
                );
                assert!(
                    profile
                        .load_balancing_config
                        .filtering
                        .blacklist_hosts
                        .is_empty()
                );
                assert!(
                    profile
                        .load_balancing_config
                        .filtering
                        .whitelist_dc
                        .is_empty()
                );
                assert!(
                    profile
                        .load_balancing_config
                        .filtering
                        .blacklist_dc
                        .is_empty()
                );
            }

            // add some addresses (and some additional whitespaces)
            {
                cass_execution_profile_set_blacklist_filtering(
                    profile_raw.borrow_mut(),
                    c" 127.0.0.1 ,  127.0.0.2 ".as_ptr(),
                );

                let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                assert_eq!(
                    profile.load_balancing_config.filtering.blacklist_hosts,
                    vec![
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))
                    ]
                );
            }

            // Mixed valid and unparsable addressed.
            // Unparsable addresses should be ignored.
            {
                cass_execution_profile_set_blacklist_filtering(
                    profile_raw.borrow_mut(),
                    c"foo, 127.0.0.3, bar,,baz".as_ptr(),
                );

                let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                assert_eq!(
                    profile.load_balancing_config.filtering.blacklist_hosts,
                    vec![
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3))
                    ]
                );
            }

            // Provide empty string - this should clear the list.
            {
                cass_execution_profile_set_blacklist_filtering(
                    profile_raw.borrow_mut(),
                    c"".as_ptr(),
                );

                let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                assert!(
                    profile
                        .load_balancing_config
                        .filtering
                        .blacklist_hosts
                        .is_empty()
                );
            }

            // Populate the list again...
            {
                cass_execution_profile_set_blacklist_filtering(
                    profile_raw.borrow_mut(),
                    c"1.1.1.1,2.2.2.2,foo,,,,  ,3.3.3.3,".as_ptr(),
                );

                let cluster = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
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
                cass_execution_profile_set_blacklist_filtering(
                    profile_raw.borrow_mut(),
                    std::ptr::null(),
                );

                let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                assert!(
                    profile
                        .load_balancing_config
                        .filtering
                        .blacklist_hosts
                        .is_empty()
                );
            }

            cass_execution_profile_free(profile_raw);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_exec_profile_name() {
        use std::convert::TryInto;
        let empty = "".to_owned();
        let nonempty = "a".to_owned();
        assert_eq!(
            empty.try_into() as Result<ExecProfileName, _>,
            Err(EmptyProfileName)
        );
        assert_eq!(
            nonempty.clone().try_into() as Result<ExecProfileName, _>,
            Ok(ExecProfileName(nonempty))
        );
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_load_balancing_config() {
        unsafe {
            let mut profile_raw = cass_execution_profile_new();
            {
                /* Test valid configurations */
                {
                    let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                    assert_matches!(profile.load_balancing_config.load_balancing_kind, None);
                    assert!(profile.load_balancing_config.token_awareness_enabled);
                    assert!(!profile.load_balancing_config.latency_awareness_enabled);
                }
                {
                    cass_execution_profile_set_token_aware_routing(profile_raw.borrow_mut(), 0);
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            0, // forbid DC failover
                            0
                        ),
                        CassError::CASS_OK
                    );
                    cass_execution_profile_set_latency_aware_routing(profile_raw.borrow_mut(), 1);
                    // These values cannot currently be tested to be set properly in the latency awareness builder,
                    // but at least we test that the function completed successfully.
                    cass_execution_profile_set_latency_aware_routing_settings(
                        profile_raw.borrow_mut(),
                        2.,
                        1,
                        2000,
                        100,
                        40,
                    );

                    let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                    let load_balancing_kind = &profile.load_balancing_config.load_balancing_kind;
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
                    assert!(!profile.load_balancing_config.token_awareness_enabled);
                    assert!(profile.load_balancing_config.latency_awareness_enabled);
                }
            }

            cass_execution_profile_free(profile_raw);
        }
    }

    impl PerStatementExecProfile {
        pub(crate) fn inner(&self) -> &Arc<RwLock<PerStatementExecProfileInner>> {
            &self.0
        }
    }

    impl PerStatementExecProfileInner {
        pub(crate) fn as_name(&self) -> Option<&ExecProfileName> {
            if let PerStatementExecProfileInner::Unresolved(name) = self {
                Some(name)
            } else {
                None
            }
        }

        pub(crate) fn as_handle(&self) -> Option<&ExecutionProfileHandle> {
            if let PerStatementExecProfileInner::Resolved(profile) = self {
                Some(profile)
            } else {
                None
            }
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_statement_and_batch_set_exec_profile() {
        unsafe {
            let empty_query = make_c_str!("");
            let mut statement_raw = cass_statement_new(empty_query, 0);
            let mut batch_raw = cass_batch_new(CassBatchType::CASS_BATCH_TYPE_LOGGED);
            assert_cass_error_eq!(
                cass_batch_add_statement(batch_raw.borrow_mut(), statement_raw.borrow()),
                CassError::CASS_OK
            );

            {
                /* Test valid configurations */
                {
                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());
                }
                {
                    let valid_name = "profile";
                    let valid_name_c_str = make_c_str!("profile");
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.borrow_mut(),
                            valid_name_c_str,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.borrow_mut(), valid_name_c_str,),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                    assert_eq!(
                        batch
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                }
                {
                    // NULL name sets exec profile to None
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.borrow_mut(),
                            std::ptr::null::<i8>()
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(
                            batch_raw.borrow_mut(),
                            std::ptr::null::<i8>()
                        ),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());
                }
                {
                    // valid name again, this time using `..._n` setter
                    let valid_name = "profile1";
                    let (valid_name_c_str, valid_name_len) = str_to_c_str_n(valid_name);
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile_n(
                            statement_raw.borrow_mut(),
                            valid_name_c_str,
                            valid_name_len,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile_n(
                            batch_raw.borrow_mut(),
                            valid_name_c_str,
                            valid_name_len,
                        ),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                    assert_eq!(
                        batch
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                }
                {
                    // empty name sets exec profile to None
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.borrow_mut(),
                            make_c_str!("")
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.borrow_mut(), make_c_str!("")),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());
                }
            }

            cass_statement_free(statement_raw);
            cass_batch_free(batch_raw);
        }
    }
}
