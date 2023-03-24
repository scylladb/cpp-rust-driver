use std::convert::{TryFrom, TryInto};
use std::ffi::c_char;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::retry_policy::RetryPolicy;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::statement::Consistency;
use scylla::ExecutionProfile;

use crate::argconv::{free_boxed, ptr_to_ref, ptr_to_ref_mut};
use crate::batch::CassBatch;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cluster::CassCluster;
use crate::retry_policy::CassRetryPolicy;
use crate::retry_policy::RetryPolicy::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use crate::statement::CassStatement;
use crate::types::{cass_bool_t, cass_int32_t, cass_int64_t, cass_uint32_t, cass_uint64_t, size_t};

#[derive(Clone, Debug)]
pub struct CassExecProfile {
    inner: ExecutionProfileBuilder,
}

impl CassExecProfile {
    fn new() -> Self {
        Self {
            inner: ExecutionProfile::builder(),
        }
    }

    pub(crate) fn build(self) -> ExecutionProfile {
        self.inner.build()
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
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware(
    profile: *mut CassExecProfile,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware_n(
    profile: *mut CassExecProfile,
    local_dc: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_round_robin(
    profile: *mut CassExecProfile,
) -> CassError {
    unimplemented!()
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
    unimplemented!()
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

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_execution_profile(
    cluster: *mut CassCluster,
    name: *const c_char,
    profile: *const CassExecProfile,
) -> CassError {
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_cluster_set_execution_profile_n(
    cluster: *mut CassCluster,
    name: *const c_char,
    name_length: size_t,
    profile: *const CassExecProfile,
) -> CassError {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
