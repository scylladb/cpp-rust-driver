use std::convert::{TryFrom, TryInto};
use std::ffi::c_char;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use scylla::execution_profile::{
    ExecutionProfile, ExecutionProfileBuilder, ExecutionProfileHandle,
};
use scylla::load_balancing::LatencyAwarenessBuilder;
use scylla::retry_policy::RetryPolicy;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::statement::Consistency;

use crate::argconv::{free_boxed, ptr_to_ref, ptr_to_ref_mut, strlen};
use crate::batch::CassBatch;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cluster::{set_load_balance_dc_aware_n, LoadBalancingConfig};
use crate::retry_policy::CassRetryPolicy;
use crate::retry_policy::RetryPolicy::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use crate::statement::CassStatement;
use crate::types::{
    cass_bool_t, cass_double_t, cass_int32_t, cass_int64_t, cass_uint32_t, cass_uint64_t, size_t,
};

#[derive(Clone, Debug)]
pub struct CassExecProfile {
    inner: ExecutionProfileBuilder,
    load_balancing_config: LoadBalancingConfig,
}

impl CassExecProfile {
    fn new() -> Self {
        Self {
            inner: ExecutionProfile::builder(),
            load_balancing_config: Default::default(),
        }
    }

    pub(crate) async fn build(self) -> ExecutionProfile {
        self.inner
            .load_balancing_policy(self.load_balancing_config.build().await)
            .build()
    }
}

/// Represents a non-empty execution profile name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecProfileName(String);

#[derive(Debug, PartialEq, Eq)]
pub struct EmptyProfileName;

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

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_new() -> *mut CassExecProfile {
    Box::into_raw(Box::new(CassExecProfile::new()))
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_free(profile: *mut CassExecProfile) {
    free_boxed(profile);
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

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_consistency(
    profile: *mut CassExecProfile,
    consistency: CassConsistency,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);
    let consistency: Consistency = match consistency.try_into() {
        Ok(c) => c,
        Err(_) => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    profile_builder.modify_in_place(|builder| builder.consistency(consistency));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_constant_speculative_execution_policy(
    profile: *mut CassExecProfile,
    constant_delay_ms: cass_int64_t,
    max_speculative_executions: cass_int32_t,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);
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

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing(
    profile: *mut CassExecProfile,
    enabled: cass_bool_t,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);
    profile_builder
        .load_balancing_config
        .latency_awareness_enabled = enabled != 0;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing_settings(
    profile: *mut CassExecProfile,
    exclusion_threshold: cass_double_t,
    _scale_ms: cass_uint64_t, // Currently ignored, TODO: add this parameter to Rust driver
    retry_period_ms: cass_uint64_t,
    update_rate_ms: cass_uint64_t,
    min_measured: cass_uint64_t,
) {
    let profile_builder = ptr_to_ref_mut(profile);
    profile_builder
        .load_balancing_config
        .latency_awareness_builder = LatencyAwarenessBuilder::new()
        .exclusion_threshold(exclusion_threshold)
        .retry_period(Duration::from_millis(retry_period_ms))
        .update_rate(Duration::from_millis(update_rate_ms))
        .minimum_measurements(min_measured as usize);
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware(
    profile: *mut CassExecProfile,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    cass_execution_profile_set_load_balance_dc_aware_n(
        profile,
        local_dc,
        strlen(local_dc),
        used_hosts_per_remote_dc,
        allow_remote_dcs_for_local_cl,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware_n(
    profile: *mut CassExecProfile,
    local_dc: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);

    set_load_balance_dc_aware_n(
        &mut profile_builder.load_balancing_config,
        local_dc,
        local_dc_length,
        used_hosts_per_remote_dc,
        allow_remote_dcs_for_local_cl,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_round_robin(
    profile: *mut CassExecProfile,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);
    profile_builder.load_balancing_config.dc_awareness = None;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_request_timeout(
    profile: *mut CassExecProfile,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);
    profile_builder.modify_in_place(|builder| {
        builder.request_timeout(Some(std::time::Duration::from_millis(timeout_ms)))
    });

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_retry_policy(
    profile: *mut CassExecProfile,
    retry_policy: *const CassRetryPolicy,
) -> CassError {
    let retry_policy: &dyn RetryPolicy = match ptr_to_ref(retry_policy) {
        DefaultRetryPolicy(default) => default.as_ref(),
        FallthroughRetryPolicy(fallthrough) => fallthrough.as_ref(),
        DowngradingConsistencyRetryPolicy(downgrading) => downgrading.as_ref(),
    };
    let profile_builder = ptr_to_ref_mut(profile);
    profile_builder.modify_in_place(|builder| builder.retry_policy(retry_policy.clone_boxed()));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_serial_consistency(
    profile: *mut CassExecProfile,
    serial_consistency: CassConsistency,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);

    let maybe_serial_consistency =
        if serial_consistency == CassConsistency::CASS_CONSISTENCY_UNKNOWN {
            None
        } else {
            match serial_consistency.try_into() {
                Ok(c) => Some(c),
                Err(_) => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
            }
        };
    profile_builder.modify_in_place(|builder| builder.serial_consistency(maybe_serial_consistency));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_token_aware_routing(
    profile: *mut CassExecProfile,
    enabled: cass_bool_t,
) -> CassError {
    let profile_builder = ptr_to_ref_mut(profile);
    profile_builder
        .load_balancing_config
        .token_awareness_enabled = enabled != 0;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_execution_profile(
    statement: *mut CassStatement,
    name: *const c_char,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_execution_profile_n(
    statement: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile(
    batch: *mut CassBatch,
    name: *const c_char,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile_n(
    batch: *mut CassBatch,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::assert_cass_error_eq;

    use assert_matches::assert_matches;

    #[test]
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
    fn test_load_balancing_config() {
        unsafe {
            let profile_raw = cass_execution_profile_new();
            {
                /* Test valid configurations */
                let profile = ptr_to_ref(profile_raw);
                {
                    assert_matches!(profile.load_balancing_config.dc_awareness, None);
                    assert!(profile.load_balancing_config.token_awareness_enabled);
                    assert!(!profile.load_balancing_config.latency_awareness_enabled);
                }
                {
                    cass_execution_profile_set_token_aware_routing(profile_raw, 0);
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw,
                            "eu\0".as_ptr() as *const i8,
                            0,
                            0
                        ),
                        CassError::CASS_OK
                    );
                    cass_execution_profile_set_latency_aware_routing(profile_raw, 1);
                    // These values cannot currently be tested to be set properly in the latency awareness builder,
                    // but at least we test that the function completed successfully.
                    cass_execution_profile_set_latency_aware_routing_settings(
                        profile_raw,
                        2.,
                        1,
                        2000,
                        100,
                        40,
                    );

                    let dc_awareness = profile.load_balancing_config.dc_awareness.as_ref().unwrap();
                    assert_eq!(dc_awareness.local_dc, "eu");
                    assert!(!profile.load_balancing_config.token_awareness_enabled);
                    assert!(profile.load_balancing_config.latency_awareness_enabled);
                }
                /* Test invalid configurations */
                {
                    // Nonzero deprecated parameters
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw,
                            "eu\0".as_ptr() as *const i8,
                            1,
                            0
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw,
                            "eu\0".as_ptr() as *const i8,
                            0,
                            1
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                }
            }

            cass_execution_profile_free(profile_raw);
        }
    }
}
