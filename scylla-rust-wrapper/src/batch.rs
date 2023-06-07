use crate::argconv::{free_boxed, ptr_to_ref, ptr_to_ref_mut};
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cass_types::{make_batch_type, CassBatchType};
use crate::exec_profile::PerStatementExecProfile;
use crate::retry_policy::CassRetryPolicy;
use crate::statement::{CassStatement, Statement};
use crate::types::*;
use scylla::batch::Batch;
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::MaybeUnset;
use std::convert::TryInto;
use std::sync::Arc;

pub struct CassBatch {
    pub state: Arc<CassBatchState>,
    pub batch_request_timeout_ms: Option<cass_uint64_t>,

    pub(crate) exec_profile: Option<PerStatementExecProfile>,
}

#[derive(Clone)]
pub struct CassBatchState {
    pub batch: Batch,
    pub bound_values: Vec<Vec<MaybeUnset<Option<CqlValue>>>>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_new(type_: CassBatchType) -> *mut CassBatch {
    if let Some(batch_type) = make_batch_type(type_) {
        Box::into_raw(Box::new(CassBatch {
            state: Arc::new(CassBatchState {
                batch: Batch::new(batch_type),
                bound_values: Vec::new(),
            }),
            batch_request_timeout_ms: None,
            exec_profile: None,
        }))
    } else {
        std::ptr::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_free(batch: *mut CassBatch) {
    free_boxed(batch)
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_consistency(
    batch: *mut CassBatch,
    consistency: CassConsistency,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    let consistency = match consistency.try_into().ok() {
        Some(c) => c,
        None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };
    Arc::make_mut(&mut batch.state)
        .batch
        .set_consistency(consistency);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_serial_consistency(
    batch: *mut CassBatch,
    serial_consistency: CassConsistency,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    let serial_consistency = match serial_consistency.try_into().ok() {
        Some(c) => c,
        None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };
    Arc::make_mut(&mut batch.state)
        .batch
        .set_serial_consistency(Some(serial_consistency));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_retry_policy(
    batch: *mut CassBatch,
    retry_policy: *const CassRetryPolicy,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);

    let maybe_arced_retry_policy: Option<Arc<dyn scylla::retry_policy::RetryPolicy>> =
        retry_policy.as_ref().map(|policy| match policy {
            CassRetryPolicy::DefaultRetryPolicy(default) => {
                default.clone() as Arc<dyn scylla::retry_policy::RetryPolicy>
            }
            CassRetryPolicy::FallthroughRetryPolicy(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistencyRetryPolicy(downgrading) => downgrading.clone(),
        });

    Arc::make_mut(&mut batch.state)
        .batch
        .set_retry_policy(maybe_arced_retry_policy);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_timestamp(
    batch: *mut CassBatch,
    timestamp: cass_int64_t,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);

    Arc::make_mut(&mut batch.state)
        .batch
        .set_timestamp(Some(timestamp));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_request_timeout(
    batch: *mut CassBatch,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    batch.batch_request_timeout_ms = Some(timeout_ms);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_is_idempotent(
    batch: *mut CassBatch,
    is_idempotent: cass_bool_t,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    Arc::make_mut(&mut batch.state)
        .batch
        .set_is_idempotent(is_idempotent != 0);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_tracing(
    batch: *mut CassBatch,
    enabled: cass_bool_t,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    Arc::make_mut(&mut batch.state)
        .batch
        .set_tracing(enabled != 0);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_add_statement(
    batch: *mut CassBatch,
    statement: *const CassStatement,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    let state = Arc::make_mut(&mut batch.state);
    let statement = ptr_to_ref(statement);

    match &statement.statement {
        Statement::Simple(q) => state.batch.append_statement(q.query.clone()),
        Statement::Prepared(p) => state.batch.append_statement((**p).clone()),
    };

    state.bound_values.push(statement.bound_values.clone());

    CassError::CASS_OK
}
