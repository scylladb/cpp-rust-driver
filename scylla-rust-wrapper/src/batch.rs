use crate::argconv::*;
use crate::cass_error::CassError;
use crate::statement::{CassStatement, Statement};
use crate::types::{self, *};
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::MaybeUnset;
use std::convert::TryInto;
use std::os::raw::c_char;
use std::sync::Arc;
// use std::time::Duration;

use scylla::batch::Batch;

pub struct CassBatch {
    pub state: Arc<CassBatchState>,
    // timeout: Duration, // TODO: Implement timeouts!
}

#[derive(Clone)]
pub struct CassBatchState {
    pub batch: Batch,
    pub bound_values: Vec<Vec<MaybeUnset<Option<CqlValue>>>>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_new(type_: CassBatchType) -> *mut CassBatch {
    if let Some(batch_type) = types::make_batch_type(type_) {
        Box::into_raw(Box::new(CassBatch {
            state: Arc::new(CassBatchState {
                batch: Batch::new(batch_type),
                bound_values: Vec::new(),
            }),
            // timeout: Duration::from_secs(10), // TODO
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
pub unsafe extern "C" fn cass_batch_set_keyspace(
    _batch: *mut CassBatch,
    _keyspace: *const c_char,
) -> CassError {
    // XXX: Not supported by v4 of protocol
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_keyspace_n(
    _batch: *mut CassBatch,
    _keyspace: *const c_char,
    _keyspace_length: size_t,
) -> CassError {
    // XXX: Not supported by v4 of protocol
    CassError::CASS_OK
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

// #[no_mangle]
// pub unsafe extern "C" fn cass_batch_set_timestamp(
//     batch: *mut CassBatch,
//     timestamp: cass_int64_t,
// ) -> CassError {
//     todo!()
// }

// #[no_mangle]
// pub unsafe extern "C" fn cass_batch_set_request_timeout(
//     batch: *mut CassBatch,
//     timeout_ms: cass_uint64_t,
// ) -> CassError {
//     let batch = ptr_to_ref_mut(batch);
//     batch.timeout = Duration::from_millis(timeout_ms);
//     CassError::CASS_OK
// }

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_is_idempotent(
    batch: *mut CassBatch,
    is_idempotent: cass_bool_t,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    let state = Arc::make_mut(&mut batch.state);
    state.batch.set_is_idempotent(is_idempotent != 0);
    CassError::CASS_OK
}

// #[no_mangle]
// pub unsafe extern "C" fn cass_batch_set_retry_policy(
//     batch: *mut CassBatch,
//     retry_policy: *mut CassRetryPolicy,
// ) -> CassError {
//     todo!()
// }

// #[no_mangle]
// pub unsafe extern "C" fn cass_batch_set_custom_payload(
//     batch: *mut CassBatch,
//     payload: *const CassCustomPayload,
// ) -> CassError {
//     // Not supported in the rust driver
//     unimplemented!()
// }

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_tracing(
    batch: *mut CassBatch,
    enabled: cass_bool_t,
) -> CassError {
    let batch = ptr_to_ref_mut(batch);
    let state = Arc::make_mut(&mut batch.state);
    state.batch.set_tracing(enabled != 0);
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
        Statement::Simple(q) => state.batch.append_statement(q.clone()),
        Statement::Prepared(p) => state.batch.append_statement((**p).clone()),
    };

    state.bound_values.push(statement.bound_values.clone());

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile(
    _batch: *mut CassBatch,
    _name: *const c_char,
) -> CassError {
    // We don't support execution profiles
    unimplemented!()
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile_n(
    _batch: *mut CassBatch,
    _name: *const c_char,
    _name_length: size_t,
) -> CassError {
    // We don't support execution profiles
    unimplemented!()
}
