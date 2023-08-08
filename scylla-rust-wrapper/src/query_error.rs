use crate::argconv::*;
use crate::cass_error::*;
use crate::types::*;
use scylla::frame::types::{LegacyConsistency, SerialConsistency};
use scylla::statement::Consistency;
use scylla::transport::errors::*;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_query_error.rs"));

pub type CassErrorResult = QueryError;

impl From<Consistency> for CassConsistency {
    fn from(c: Consistency) -> CassConsistency {
        match c {
            Consistency::Any => CassConsistency::CASS_CONSISTENCY_ANY,
            Consistency::One => CassConsistency::CASS_CONSISTENCY_ONE,
            Consistency::Two => CassConsistency::CASS_CONSISTENCY_TWO,
            Consistency::Three => CassConsistency::CASS_CONSISTENCY_THREE,
            Consistency::Quorum => CassConsistency::CASS_CONSISTENCY_QUORUM,
            Consistency::All => CassConsistency::CASS_CONSISTENCY_ALL,
            Consistency::LocalQuorum => CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM,
            Consistency::EachQuorum => CassConsistency::CASS_CONSISTENCY_EACH_QUORUM,
            Consistency::LocalOne => CassConsistency::CASS_CONSISTENCY_LOCAL_ONE,
        }
    }
}

impl From<&WriteType> for CassWriteType {
    fn from(c: &WriteType) -> CassWriteType {
        match c {
            WriteType::Simple => CassWriteType::CASS_WRITE_TYPE_SIMPLE,
            WriteType::Batch => CassWriteType::CASS_WRITE_TYPE_BATCH,
            WriteType::UnloggedBatch => CassWriteType::CASS_WRITE_TYPE_UNLOGGED_BATCH,
            WriteType::Counter => CassWriteType::CASS_WRITE_TYPE_COUNTER,
            WriteType::BatchLog => CassWriteType::CASS_WRITE_TYPE_BATCH_LOG,
            WriteType::Cas => CassWriteType::CASS_WRITE_TYPE_CAS,
            WriteType::View => CassWriteType::CASS_WRITE_TYPE_VIEW,
            WriteType::Cdc => CassWriteType::CASS_WRITE_TYPE_CDC,
            WriteType::Other(_) => CassWriteType::CASS_WRITE_TYPE_UNKNOWN,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_free(error_result: *const CassErrorResult) {
    free_arced(error_result);
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_code(error_result: *const CassErrorResult) -> CassError {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    CassError::from(error_result)
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_consistency(
    error_result: *const CassErrorResult,
) -> CassConsistency {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::Unavailable { consistency, .. }, _) => {
            get_cass_consistency_from_legacy(consistency)
        }
        QueryError::DbError(DbError::ReadTimeout { consistency, .. }, _) => {
            get_cass_consistency_from_legacy(consistency)
        }
        QueryError::DbError(DbError::WriteTimeout { consistency, .. }, _) => {
            get_cass_consistency_from_legacy(consistency)
        }
        QueryError::DbError(DbError::ReadFailure { consistency, .. }, _) => {
            get_cass_consistency_from_legacy(consistency)
        }
        QueryError::DbError(DbError::WriteFailure { consistency, .. }, _) => {
            get_cass_consistency_from_legacy(consistency)
        }
        _ => CassConsistency::CASS_CONSISTENCY_UNKNOWN,
    }
}

fn get_cass_consistency_from_legacy(consistency: &LegacyConsistency) -> CassConsistency {
    match consistency {
        LegacyConsistency::Regular(regular) => CassConsistency::from(*regular),
        LegacyConsistency::Serial(serial) => match serial {
            SerialConsistency::Serial => CassConsistency::CASS_CONSISTENCY_SERIAL,
            SerialConsistency::LocalSerial => CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL,
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_responses_received(
    error_result: *const CassErrorResult,
) -> cass_int32_t {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::Unavailable { alive, .. }, _) => *alive,
        QueryError::DbError(DbError::ReadTimeout { received, .. }, _) => *received,
        QueryError::DbError(DbError::WriteTimeout { received, .. }, _) => *received,
        QueryError::DbError(DbError::ReadFailure { received, .. }, _) => *received,
        QueryError::DbError(DbError::WriteFailure { received, .. }, _) => *received,
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_responses_required(
    error_result: *const CassErrorResult,
) -> cass_int32_t {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::Unavailable { required, .. }, _) => *required,
        QueryError::DbError(DbError::ReadTimeout { required, .. }, _) => *required,
        QueryError::DbError(DbError::WriteTimeout { required, .. }, _) => *required,
        QueryError::DbError(DbError::ReadFailure { required, .. }, _) => *required,
        QueryError::DbError(DbError::WriteFailure { required, .. }, _) => *required,
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_num_failures(
    error_result: *const CassErrorResult,
) -> cass_int32_t {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::ReadFailure { numfailures, .. }, _) => *numfailures,
        QueryError::DbError(DbError::WriteFailure { numfailures, .. }, _) => *numfailures,
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_data_present(
    error_result: *const CassErrorResult,
) -> cass_bool_t {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::ReadTimeout { data_present, .. }, _) => {
            if *data_present {
                cass_true
            } else {
                cass_false
            }
        }
        QueryError::DbError(DbError::ReadFailure { data_present, .. }, _) => {
            if *data_present {
                cass_true
            } else {
                cass_false
            }
        }
        _ => cass_false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_write_type(
    error_result: *const CassErrorResult,
) -> CassWriteType {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::WriteTimeout { write_type, .. }, _) => {
            CassWriteType::from(write_type)
        }
        QueryError::DbError(DbError::WriteFailure { write_type, .. }, _) => {
            CassWriteType::from(write_type)
        }
        _ => CassWriteType::CASS_WRITE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_keyspace(
    error_result: *const CassErrorResult,
    c_keyspace: *mut *const ::std::os::raw::c_char,
    c_keyspace_len: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::AlreadyExists { keyspace, .. }, _) => {
            write_str_to_c(keyspace.as_str(), c_keyspace, c_keyspace_len);
            CassError::CASS_OK
        }
        QueryError::DbError(DbError::FunctionFailure { keyspace, .. }, _) => {
            write_str_to_c(keyspace.as_str(), c_keyspace, c_keyspace_len);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_table(
    error_result: *const CassErrorResult,
    c_table: *mut *const ::std::os::raw::c_char,
    c_table_len: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::AlreadyExists { table, .. }, _) => {
            write_str_to_c(table.as_str(), c_table, c_table_len);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_function(
    error_result: *const CassErrorResult,
    c_function: *mut *const ::std::os::raw::c_char,
    c_function_len: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::FunctionFailure { function, .. }, _) => {
            write_str_to_c(function.as_str(), c_function, c_function_len);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_num_arg_types(error_result: *const CassErrorResult) -> size_t {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::FunctionFailure { arg_types, .. }, _) => {
            arg_types.len() as size_t
        }
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_arg_type(
    error_result: *const CassErrorResult,
    index: size_t,
    arg_type: *mut *const ::std::os::raw::c_char,
    arg_type_length: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ptr_to_ref(error_result);
    match error_result {
        QueryError::DbError(DbError::FunctionFailure { arg_types, .. }, _) => {
            if index >= arg_types.len() as size_t {
                return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
            }
            write_str_to_c(
                arg_types[index as usize].as_str(),
                arg_type,
                arg_type_length,
            );
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}
