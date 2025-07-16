use crate::argconv::{
    ArcFFI, BoxFFI, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr, CassOwnedExclusivePtr,
    FFI, FromBox,
};
use crate::cass_error::CassError;
pub use crate::cass_types::CassBatchType;
use crate::cass_types::{CassConsistency, make_batch_type};
use crate::config_value::{MaybeUnsetConfig, RequestTimeout};
use crate::exec_profile::PerStatementExecProfile;
use crate::retry_policy::CassRetryPolicy;
use crate::statement::{BoundStatement, CassStatement};
use crate::types::*;
use crate::value::CassCqlValue;
use scylla::statement::batch::Batch;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::value::MaybeUnset;
use std::sync::Arc;

pub struct CassBatch {
    pub(crate) state: Arc<CassBatchState>,
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
    typ: CassBatchType,
) -> CassOwnedExclusivePtr<CassBatch, CMut> {
    let Ok(batch_type) = make_batch_type(typ) else {
        tracing::error!("Provided invalid batch type to cass_batch_new: {typ:?}");
        return BoxFFI::null_mut();
    };

    BoxFFI::into_ptr(Box::new(CassBatch {
        state: Arc::new(CassBatchState {
            batch: Batch::new(batch_type),
            bound_values: Vec::new(),
        }),
        exec_profile: None,
    }))
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

    let Ok(maybe_set_consistency) = MaybeUnsetConfig::<_, Consistency>::from_c_value(consistency)
    else {
        // Invalid consistency value provided.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let batch = &mut Arc::make_mut(&mut batch.state).batch;
    match maybe_set_consistency {
        MaybeUnsetConfig::Unset(_) => {
            // The correct semantics for `CASS_CONSISTENCY_UNKNOWN` is to
            // make batch not have any opinion at all about consistency.
            // Then, the default from the cluster/execution profile should be used.
            batch.unset_consistency();
        }
        MaybeUnsetConfig::Set(consistency) => {
            batch.set_consistency(consistency);
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

    // cpp-driver doesn't validate passed value in any way.
    // If it is an incorrect serial-consistency value then it will be set
    // and sent as-is.
    // Before adapting the driver to Rust Driver 0.12 this code
    // set serial consistency if a user passed correct value and set it to
    // None otherwise.
    // I think that failing explicitly is a better idea, so I decided to return
    // an error.
    let Ok(maybe_set_serial_consistency) =
        MaybeUnsetConfig::<_, Option<SerialConsistency>>::from_c_value(serial_consistency)
    else {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let batch = &mut Arc::make_mut(&mut batch.state).batch;
    match maybe_set_serial_consistency {
        MaybeUnsetConfig::Unset(_) => {
            // The correct semantics for `CASS_CONSISTENCY_UNKNOWN` is to
            // make batch not have any opinion at all about serial consistency.
            // Then, the default from the cluster/execution profile should be used.
            batch.unset_serial_consistency();
        }
        MaybeUnsetConfig::Set(serial_consistency) => {
            batch.set_serial_consistency(serial_consistency);
        }
    };

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_batch_set_retry_policy(
    batch: CassBorrowedExclusivePtr<CassBatch, CMut>,
    cass_retry_policy: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) -> CassError {
    let Some(batch) = BoxFFI::as_mut_ref(batch) else {
        tracing::error!("Provided null batch pointer to cass_batch_set_retry_policy!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let maybe_unset_cass_retry_policy = ArcFFI::as_ref(cass_retry_policy);
    let retry_policy_opt =
        match MaybeUnsetConfig::from_c_value_infallible(maybe_unset_cass_retry_policy) {
            MaybeUnsetConfig::Set(retry_policy) => Some(retry_policy),
            MaybeUnsetConfig::Unset(_) => None,
        };

    Arc::make_mut(&mut batch.state)
        .batch
        .set_retry_policy(retry_policy_opt);

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
    let maybe_unset_timeout =
        MaybeUnsetConfig::<_, RequestTimeout>::from_c_value_infallible(timeout_ms);

    // `Batch::set_request_timeout` expects an Option<Duration> with unusual semantics:
    // - `None` means "ignore me and use the default timeout from the cluster/execution profile" - this is
    //   different than other configuration parameters such as retry policy or serial consistency,
    //   where `None` means "unset this parameter";
    // - `Some(timeout)` means "use timeout of given value".
    // Therefore, to acquire "no timeout" semantics, we need to emulate it with an extremely long timeout.
    let timeout = match maybe_unset_timeout {
        MaybeUnsetConfig::Unset(_) => None,
        MaybeUnsetConfig::Set(RequestTimeout(timeout)) => {
            Some(timeout.unwrap_or(RequestTimeout::INFINITE))
        }
    };

    Arc::make_mut(&mut batch.state)
        .batch
        .set_request_timeout(timeout);

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
