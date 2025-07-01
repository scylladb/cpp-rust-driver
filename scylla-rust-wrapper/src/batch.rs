use crate::argconv::{
    ArcFFI, BoxFFI, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr, CassOwnedExclusivePtr,
    FFI, FromBox,
};
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cass_types::{CassBatchType, make_batch_type};
use crate::config_value::MaybeUnsetConfig;
use crate::exec_profile::PerStatementExecProfile;
use crate::retry_policy::CassRetryPolicy;
use crate::statement::{BoundStatement, CassStatement};
use crate::types::*;
use crate::value::CassCqlValue;
use scylla::statement::Consistency;
use scylla::statement::batch::Batch;
use scylla::value::MaybeUnset;
use std::convert::TryInto;
use std::sync::Arc;

pub struct CassBatch {
    pub(crate) state: Arc<CassBatchState>,
    pub(crate) batch_request_timeout_ms: Option<cass_uint64_t>,

    pub(crate) exec_profile: Option<PerStatementExecProfile>,
}

impl FFI for CassBatch {
    type Origin = FromBox;
}

#[derive(Clone)]
pub(crate) struct CassBatchState {
    pub(crate) batch: Batch,
    pub(crate) bound_values: Vec<Vec<MaybeUnset<Option<CassCqlValue>>>>,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_new(
    type_: CassBatchType,
) -> CassOwnedExclusivePtr<CassBatch, CMut> {
    if let Some(batch_type) = make_batch_type(type_) {
        BoxFFI::into_ptr(Box::new(CassBatch {
            state: Arc::new(CassBatchState {
                batch: Batch::new(batch_type),
                bound_values: Vec::new(),
            }),
            batch_request_timeout_ms: None,
            exec_profile: None,
        }))
    } else {
        BoxFFI::null_mut()
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_free(batch: CassOwnedExclusivePtr<CassBatch, CMut>) {
    BoxFFI::free(batch);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_consistency(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    consistency: CassConsistency,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_consistency!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Ok(maybe_set_consistency) = MaybeUnsetConfig::<Consistency>::from_c_value(consistency)
    else {
        // Invalid consistency value provided.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match maybe_set_consistency {
        MaybeUnsetConfig::Unset => {
            // The correct semantics for `CASS_CONSISTENCY_UNKNOWN` is to
            // make batch not have any opinion at all about consistency.
            // Then, the default from the cluster/execution profile should be used.
            // Unfortunately, the Rust Driver does not support
            // "unsetting" consistency from a batch at the moment.
            //
            // FIXME: Implement unsetting consistency in the Rust Driver.
            // Then, fix this code.
            //
            // For now, we will throw an error in order to warn the user
            // about this limitation.
            tracing::warn!(
                "Passed `CASS_CONSISTENCY_UNKNOWN` to `cass_batch_set_consistency`. \
                This is not supported by the CPP Rust Driver yet: once you set some consistency \
                on a batch, you cannot unset it. This limitation will be fixed in the future. \
                As a workaround, you can refrain from setting consistency on a batch, which \
                will make the driver use the consistency set on execution profile or cluster level."
            );

            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        MaybeUnsetConfig::Set(consistency) => {
            Arc::make_mut(&mut batch.state)
                .batch
                .set_consistency(consistency);
        }
    };
    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_serial_consistency(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    serial_consistency: CassConsistency,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_serial_consistency!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let serial_consistency = match serial_consistency.try_into().ok() {
        Some(c) => c,
        None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };
    Arc::make_mut(&mut batch.state)
        .batch
        .set_serial_consistency(Some(serial_consistency));

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_retry_policy(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    retry_policy: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_retry_policy!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let maybe_arced_retry_policy: Option<Arc<dyn scylla::policies::retry::RetryPolicy>> =
        ArcFFI::as_ref(retry_policy).map(|policy| match policy {
            CassRetryPolicy::Default(default) => {
                default.clone() as Arc<dyn scylla::policies::retry::RetryPolicy>
            }
            CassRetryPolicy::Fallthrough(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistency(downgrading) => downgrading.clone(),
            CassRetryPolicy::Logging(logging) => Arc::clone(logging) as _,
            #[cfg(cpp_integration_testing)]
            CassRetryPolicy::Ignoring(ignoring) => Arc::clone(ignoring) as _,
        });

    Arc::make_mut(&mut batch.state)
        .batch
        .set_retry_policy(maybe_arced_retry_policy);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_timestamp(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    timestamp: cass_int64_t,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_timestamp!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    Arc::make_mut(&mut batch.state)
        .batch
        .set_timestamp(Some(timestamp));

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_request_timeout(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_request_timeout!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    batch.batch_request_timeout_ms = Some(timeout_ms);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_is_idempotent(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    is_idempotent: cass_bool_t,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_is_idempotent!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    Arc::make_mut(&mut batch.state)
        .batch
        .set_is_idempotent(is_idempotent != 0);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_tracing(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    enabled: cass_bool_t,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_tracing!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    Arc::make_mut(&mut batch.state)
        .batch
        .set_tracing(enabled != 0);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_add_statement(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    statement: CassBorrowedSharedPtr<CassStatement, CMut>,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_add_statement!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let Some(statement) = BoxFFI::as_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_batch_add_statement!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let state = Arc::make_mut(&mut batch.state);

    match &statement.statement {
        BoundStatement::Simple(q) => {
            state.batch.append_statement(q.query.clone());
            state.bound_values.push(q.bound_values.clone());
        }
        BoundStatement::Prepared(p) => {
            state.batch.append_statement(p.statement.statement.clone());
            state.bound_values.push(p.bound_values.clone());
        }
    };

    CassError::CASS_OK
}
