use crate::argconv::*;
use crate::cass_types::CassConsistency;
use crate::misc::CassWriteType;
use crate::types::*;
use libc::c_char;
use scylla::deserialize::DeserializationError;
use scylla::errors::{
    BadKeyspaceName, BadQuery, ConnectionPoolError, DbError, ExecutionError, MetadataError,
    MetadataFetchErrorKind, NewSessionError, PrepareError, RequestAttemptError, SerializationError,
    WriteType,
};
use scylla::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use scylla::statement::Consistency;
use thiserror::Error;

// Re-export error types.
pub use crate::cass_error_types::{CassError, CassErrorSource};
use crate::statement::UnknownNamedParameterError;

pub(crate) trait ToCassError {
    fn to_cass_error(&self) -> CassError;
}

impl ToCassError for CassErrorResult {
    fn to_cass_error(&self) -> CassError {
        match self {
            CassErrorResult::Execution(execution_error) => execution_error.to_cass_error(),

            // TODO:
            // For now let's leave these as LIB_INVALID_DATA.
            // I don't see any variants that would make more sense.
            // TBH, I'm almost sure that we should introduce additional enum variants
            // of CassError in the future ~ muzarski.
            CassErrorResult::ResultMetadataLazyDeserialization(_) => {
                CassError::CASS_ERROR_LIB_INVALID_DATA
            }
            CassErrorResult::Deserialization(_) => CassError::CASS_ERROR_LIB_INVALID_DATA,
        }
    }
}

impl ToCassError for ExecutionError {
    fn to_cass_error(&self) -> CassError {
        match self {
            ExecutionError::BadQuery(bad_query) => bad_query.to_cass_error(),
            ExecutionError::RequestTimeout(_) => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT,
            ExecutionError::EmptyPlan => CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
            ExecutionError::MetadataError(e) => e.to_cass_error(),
            ExecutionError::ConnectionPoolError(e) => e.to_cass_error(),
            ExecutionError::PrepareError(e) => e.to_cass_error(),
            ExecutionError::LastAttemptError(e) => e.to_cass_error(),
            ExecutionError::UseKeyspaceError(_) => CassError::CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
            ExecutionError::SchemaAgreementError(_) => CassError::CASS_ERROR_LIB_INVALID_STATE,
            // ExecutionError is non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for ConnectionPoolError {
    fn to_cass_error(&self) -> CassError {
        CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
    }
}

impl ToCassError for PrepareError {
    fn to_cass_error(&self) -> CassError {
        match self {
            PrepareError::ConnectionPoolError(e) => e.to_cass_error(),
            PrepareError::AllAttemptsFailed { first_attempt } => first_attempt.to_cass_error(),
            PrepareError::PreparedStatementIdsMismatch => {
                CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR
            }

            // PrepareError is non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for RequestAttemptError {
    fn to_cass_error(&self) -> CassError {
        match self {
            RequestAttemptError::SerializationError(e) => e.to_cass_error(),
            RequestAttemptError::CqlRequestSerialization(_) => {
                CassError::CASS_ERROR_LIB_MESSAGE_ENCODE
            }
            RequestAttemptError::UnableToAllocStreamId => CassError::CASS_ERROR_LIB_NO_STREAMS,
            RequestAttemptError::BrokenConnectionError(_) => {
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
            }
            RequestAttemptError::BodyExtensionsParseError(_) => {
                CassError::CASS_ERROR_LIB_INVALID_DATA
            }
            RequestAttemptError::CqlResultParseError(_) => CassError::CASS_ERROR_LIB_INVALID_DATA,
            RequestAttemptError::CqlErrorParseError(_) => CassError::CASS_ERROR_LIB_INVALID_DATA,
            RequestAttemptError::DbError(db_error, _) => db_error.to_cass_error(),
            RequestAttemptError::UnexpectedResponse(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            RequestAttemptError::RepreparedIdChanged { .. } => {
                CassError::CASS_ERROR_LIB_INVALID_STATE
            }
            RequestAttemptError::RepreparedIdMissingInBatch => {
                CassError::CASS_ERROR_LIB_INVALID_STATE
            }
            RequestAttemptError::NonfinishedPagingState => CassError::CASS_ERROR_LIB_INVALID_STATE,

            // RequestAttemptError is non_exhaustive.
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for DbError {
    fn to_cass_error(&self) -> CassError {
        match self {
            DbError::ServerError => CassError::CASS_ERROR_SERVER_SERVER_ERROR,
            DbError::ProtocolError => CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR,
            DbError::AuthenticationError => CassError::CASS_ERROR_SERVER_BAD_CREDENTIALS,
            DbError::Unavailable { .. } => CassError::CASS_ERROR_SERVER_UNAVAILABLE,
            DbError::Overloaded => CassError::CASS_ERROR_SERVER_OVERLOADED,
            DbError::IsBootstrapping => CassError::CASS_ERROR_SERVER_IS_BOOTSTRAPPING,
            DbError::TruncateError => CassError::CASS_ERROR_SERVER_TRUNCATE_ERROR,
            DbError::WriteTimeout { .. } => CassError::CASS_ERROR_SERVER_WRITE_TIMEOUT,
            DbError::ReadTimeout { .. } => CassError::CASS_ERROR_SERVER_READ_TIMEOUT,
            DbError::ReadFailure { .. } => CassError::CASS_ERROR_SERVER_READ_FAILURE,
            DbError::FunctionFailure { .. } => CassError::CASS_ERROR_SERVER_FUNCTION_FAILURE,
            DbError::WriteFailure { .. } => CassError::CASS_ERROR_SERVER_WRITE_FAILURE,
            DbError::SyntaxError => CassError::CASS_ERROR_SERVER_SYNTAX_ERROR,
            DbError::Unauthorized => CassError::CASS_ERROR_SERVER_UNAUTHORIZED,
            DbError::Invalid => CassError::CASS_ERROR_SERVER_INVALID_QUERY,
            DbError::ConfigError => CassError::CASS_ERROR_SERVER_CONFIG_ERROR,
            DbError::AlreadyExists { .. } => CassError::CASS_ERROR_SERVER_ALREADY_EXISTS,
            DbError::Unprepared { .. } => CassError::CASS_ERROR_SERVER_UNPREPARED,
            DbError::Other(num) => {
                CassError((CassErrorSource::CASS_ERROR_SOURCE_SERVER.0 << 24) | *num as u32)
            }
            // TODO: add appropriate error if rate limit reached
            DbError::RateLimitReached { .. } => CassError::CASS_ERROR_SERVER_UNAVAILABLE,
            // DbError is non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for BadQuery {
    fn to_cass_error(&self) -> CassError {
        match self {
            BadQuery::ValuesTooLongForKey(_usize, _usize2) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::PartitionKeyExtraction => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::SerializationError(e) => e.to_cass_error(),
            BadQuery::TooManyQueriesInBatchStatement(_) => CassError::CASS_ERROR_LAST_ENTRY,
            // BadQuery is non_exhaustive
            // For now, since all other variants return LAST_ENTRY,
            // let's do it here as well.
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for NewSessionError {
    fn to_cass_error(&self) -> CassError {
        match self {
            NewSessionError::FailedToResolveAnyHostname(_hostnames) => {
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
            }
            NewSessionError::EmptyKnownNodesList => CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
            NewSessionError::MetadataError(e) => e.to_cass_error(),
            NewSessionError::UseKeyspaceError(_) => {
                CassError::CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE
            }
            // NS error is non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for MetadataError {
    fn to_cass_error(&self) -> CassError {
        match self {
            MetadataError::ConnectionPoolError(e) => e.to_cass_error(),
            MetadataError::FetchError(e) => match &e.error {
                // Server bug - invalid CQL type in system table.
                MetadataFetchErrorKind::InvalidColumnType(_) => {
                    CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
                }
                MetadataFetchErrorKind::PrepareError(e) => e.to_cass_error(),
                MetadataFetchErrorKind::SerializationError(e) => e.to_cass_error(),
                MetadataFetchErrorKind::NextRowError(_) => {
                    CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
                }

                // non_exhaustive
                _ => CassError::CASS_ERROR_LAST_ENTRY,
            },
            // Remaining errors indicate a serious server bug - e.g. all peers have empty token lists.
            MetadataError::Keyspaces(_)
            | MetadataError::Peers(_)
            | MetadataError::Tables(_)
            | MetadataError::Udts(_) => CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE,

            // non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for BadKeyspaceName {
    fn to_cass_error(&self) -> CassError {
        match self {
            BadKeyspaceName::Empty => CassError::CASS_ERROR_LAST_ENTRY,
            BadKeyspaceName::TooLong(_string, _usize) => CassError::CASS_ERROR_LAST_ENTRY,
            BadKeyspaceName::IllegalCharacter(_string, _char) => CassError::CASS_ERROR_LAST_ENTRY,
            // non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for SerializationError {
    fn to_cass_error(&self) -> CassError {
        if self.downcast_ref::<UnknownNamedParameterError>().is_some() {
            // It means that our custom `UnknownNamedParameterError` was returned.
            CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
        } else {
            CassError::CASS_ERROR_LAST_ENTRY
        }
    }
}

pub(crate) trait CassErrorMessage {
    fn msg(&self) -> String;
}

impl CassErrorMessage for CassErrorResult {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for ExecutionError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for PrepareError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for DbError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for BadQuery {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for NewSessionError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for BadKeyspaceName {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for str {
    fn msg(&self) -> String {
        self.to_string()
    }
}

#[derive(Error, Debug)]
pub enum CassErrorResult {
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    ResultMetadataLazyDeserialization(#[from] ResultMetadataAndRowsCountParseError),
    #[error("Failed to deserialize first row: {0}")]
    Deserialization(#[from] DeserializationError),
}

impl FFI for CassErrorResult {
    type Origin = FromArc;
}

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
            Consistency::Serial => CassConsistency::CASS_CONSISTENCY_SERIAL,
            Consistency::LocalSerial => CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL,
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_free(
    error_result: CassOwnedSharedPtr<CassErrorResult, CConst>,
) {
    ArcFFI::free(error_result);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_code(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> CassError {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_code!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    error_result.to_cass_error()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_consistency(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> CassConsistency {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_consistency!");
        return CassConsistency::CASS_CONSISTENCY_UNKNOWN;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::Unavailable { consistency, .. }, _)
            | RequestAttemptError::DbError(DbError::ReadTimeout { consistency, .. }, _)
            | RequestAttemptError::DbError(DbError::WriteTimeout { consistency, .. }, _)
            | RequestAttemptError::DbError(DbError::ReadFailure { consistency, .. }, _)
            | RequestAttemptError::DbError(DbError::WriteFailure { consistency, .. }, _),
        )) => CassConsistency::from(*consistency),
        _ => CassConsistency::CASS_CONSISTENCY_UNKNOWN,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_responses_received(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> cass_int32_t {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!(
            "Provided null error result pointer to cass_error_result_responses_received!"
        );
        return -1;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(attempt_error)) => {
            match attempt_error {
                RequestAttemptError::DbError(DbError::Unavailable { alive, .. }, _) => *alive,
                RequestAttemptError::DbError(DbError::ReadTimeout { received, .. }, _) => *received,
                RequestAttemptError::DbError(DbError::WriteTimeout { received, .. }, _) => {
                    *received
                }
                RequestAttemptError::DbError(DbError::ReadFailure { received, .. }, _) => *received,
                RequestAttemptError::DbError(DbError::WriteFailure { received, .. }, _) => {
                    *received
                }
                _ => -1,
            }
        }
        _ => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_responses_required(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> cass_int32_t {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!(
            "Provided null error result pointer to cass_error_result_responses_required!"
        );
        return -1;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(attempt_error)) => {
            match attempt_error {
                RequestAttemptError::DbError(DbError::Unavailable { required, .. }, _) => *required,
                RequestAttemptError::DbError(DbError::ReadTimeout { required, .. }, _) => *required,
                RequestAttemptError::DbError(DbError::WriteTimeout { required, .. }, _) => {
                    *required
                }
                RequestAttemptError::DbError(DbError::ReadFailure { required, .. }, _) => *required,
                RequestAttemptError::DbError(DbError::WriteFailure { required, .. }, _) => {
                    *required
                }
                _ => -1,
            }
        }
        _ => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_num_failures(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> cass_int32_t {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_num_failures!");
        return -1;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::ReadFailure { numfailures, .. }, _),
        )) => *numfailures,
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::WriteFailure { numfailures, .. }, _),
        )) => *numfailures,
        _ => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_data_present(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> cass_bool_t {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_data_present!");
        return cass_false;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::ReadTimeout { data_present, .. }, _),
        )) => {
            if *data_present {
                cass_true
            } else {
                cass_false
            }
        }
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::ReadFailure { data_present, .. }, _),
        )) => {
            if *data_present {
                cass_true
            } else {
                cass_false
            }
        }
        _ => cass_false,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_write_type(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> CassWriteType {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_write_type!");
        return CassWriteType::CASS_WRITE_TYPE_UNKNOWN;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::WriteTimeout { write_type, .. }, _),
        )) => CassWriteType::from(write_type),
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::WriteFailure { write_type, .. }, _),
        )) => CassWriteType::from(write_type),
        _ => CassWriteType::CASS_WRITE_TYPE_UNKNOWN,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_keyspace(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
    c_keyspace: *mut *const ::std::os::raw::c_char,
    c_keyspace_len: *mut size_t,
) -> CassError {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_keyspace!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::AlreadyExists { keyspace, .. }, _),
        )) => {
            unsafe { write_str_to_c(keyspace.as_str(), c_keyspace, c_keyspace_len) };
            CassError::CASS_OK
        }
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::FunctionFailure { keyspace, .. }, _),
        )) => {
            unsafe { write_str_to_c(keyspace.as_str(), c_keyspace, c_keyspace_len) };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_table(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
    c_table: *mut *const ::std::os::raw::c_char,
    c_table_len: *mut size_t,
) -> CassError {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_table!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::AlreadyExists { table, .. }, _),
        )) => {
            unsafe { write_str_to_c(table.as_str(), c_table, c_table_len) };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_function(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
    c_function: *mut *const ::std::os::raw::c_char,
    c_function_len: *mut size_t,
) -> CassError {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_function!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::FunctionFailure { function, .. }, _),
        )) => {
            unsafe { write_str_to_c(function.as_str(), c_function, c_function_len) };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_num_arg_types(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
) -> size_t {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_num_arg_types!");
        return 0;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::FunctionFailure { arg_types, .. }, _),
        )) => arg_types.len() as size_t,
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_result_arg_type(
    error_result: CassBorrowedSharedPtr<CassErrorResult, CConst>,
    index: size_t,
    arg_type: *mut *const ::std::os::raw::c_char,
    arg_type_length: *mut size_t,
) -> CassError {
    let Some(error_result) = ArcFFI::as_ref(error_result) else {
        tracing::error!("Provided null error result pointer to cass_error_result_arg_type!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match error_result {
        CassErrorResult::Execution(ExecutionError::LastAttemptError(
            RequestAttemptError::DbError(DbError::FunctionFailure { arg_types, .. }, _),
        )) => {
            if index >= arg_types.len() as size_t {
                return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
            }
            unsafe {
                write_str_to_c(
                    arg_types[index as usize].as_str(),
                    arg_type,
                    arg_type_length,
                )
            };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_error_desc(error: CassError) -> *const c_char {
    let desc = match error {
        CassError::CASS_ERROR_LIB_BAD_PARAMS => c"Bad parameters",
        CassError::CASS_ERROR_LIB_NO_STREAMS => c"No streams available",
        CassError::CASS_ERROR_LIB_UNABLE_TO_INIT => c"Unable to initialize",
        CassError::CASS_ERROR_LIB_MESSAGE_ENCODE => c"Unable to encode message",
        CassError::CASS_ERROR_LIB_HOST_RESOLUTION => c"Unable to resolve host",
        CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE => c"Unexpected response from server",
        CassError::CASS_ERROR_LIB_REQUEST_QUEUE_FULL => c"The request queue is full",
        CassError::CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD => c"No available IO threads",
        CassError::CASS_ERROR_LIB_WRITE_ERROR => c"Write error",
        CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE => c"No hosts available",
        CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS => c"Index out of bounds",
        CassError::CASS_ERROR_LIB_INVALID_ITEM_COUNT => c"Invalid item count",
        CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE => c"Invalid value type",
        CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT => c"Request timed out",
        CassError::CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE => c"Unable to set keyspace",
        CassError::CASS_ERROR_LIB_CALLBACK_ALREADY_SET => c"Callback already set",
        CassError::CASS_ERROR_LIB_INVALID_STATEMENT_TYPE => c"Invalid statement type",
        CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST => c"No value or column for name",
        CassError::CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL => {
            c"Unable to find supported protocol version"
        }
        CassError::CASS_ERROR_LIB_NULL_VALUE => c"NULL value specified",
        CassError::CASS_ERROR_LIB_NOT_IMPLEMENTED => c"Not implemented",
        CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT => c"Unable to connect",
        CassError::CASS_ERROR_LIB_UNABLE_TO_CLOSE => c"Unable to close",
        CassError::CASS_ERROR_LIB_NO_PAGING_STATE => c"No paging state",
        CassError::CASS_ERROR_LIB_PARAMETER_UNSET => c"Parameter unset",
        CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE => c"Invalid error result type",
        CassError::CASS_ERROR_LIB_INVALID_FUTURE_TYPE => c"Invalid future type",
        CassError::CASS_ERROR_LIB_INTERNAL_ERROR => c"Internal error",
        CassError::CASS_ERROR_LIB_INVALID_CUSTOM_TYPE => c"Invalid custom type",
        CassError::CASS_ERROR_LIB_INVALID_DATA => c"Invalid data",
        CassError::CASS_ERROR_LIB_NOT_ENOUGH_DATA => c"Not enough data",
        CassError::CASS_ERROR_LIB_INVALID_STATE => c"Invalid state",
        CassError::CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD => c"No custom payload",
        CassError::CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID => {
            c"Invalid execution profile specified"
        }
        CassError::CASS_ERROR_LIB_NO_TRACING_ID => c"No tracing ID",
        CassError::CASS_ERROR_SERVER_SERVER_ERROR => c"Server error",
        CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR => c"Protocol error",
        CassError::CASS_ERROR_SERVER_BAD_CREDENTIALS => c"Bad credentials",
        CassError::CASS_ERROR_SERVER_UNAVAILABLE => c"Unavailable",
        CassError::CASS_ERROR_SERVER_OVERLOADED => c"Overloaded",
        CassError::CASS_ERROR_SERVER_IS_BOOTSTRAPPING => c"Is bootstrapping",
        CassError::CASS_ERROR_SERVER_TRUNCATE_ERROR => c"Truncate error",
        CassError::CASS_ERROR_SERVER_WRITE_TIMEOUT => c"Write timeout",
        CassError::CASS_ERROR_SERVER_READ_TIMEOUT => c"Read timeout",
        CassError::CASS_ERROR_SERVER_READ_FAILURE => c"Read failure",
        CassError::CASS_ERROR_SERVER_FUNCTION_FAILURE => c"Function failure",
        CassError::CASS_ERROR_SERVER_WRITE_FAILURE => c"Write failure",
        CassError::CASS_ERROR_SERVER_SYNTAX_ERROR => c"Syntax error",
        CassError::CASS_ERROR_SERVER_UNAUTHORIZED => c"Unauthorized",
        CassError::CASS_ERROR_SERVER_INVALID_QUERY => c"Invalid query",
        CassError::CASS_ERROR_SERVER_CONFIG_ERROR => c"Configuration error",
        CassError::CASS_ERROR_SERVER_ALREADY_EXISTS => c"Already exists",
        CassError::CASS_ERROR_SERVER_UNPREPARED => c"Unprepared",
        CassError::CASS_ERROR_SSL_INVALID_CERT => c"Unable to load certificate",
        CassError::CASS_ERROR_SSL_INVALID_PRIVATE_KEY => c"Unable to load private key",
        CassError::CASS_ERROR_SSL_NO_PEER_CERT => c"No peer certificate",
        CassError::CASS_ERROR_SSL_INVALID_PEER_CERT => c"Invalid peer certificate",
        CassError::CASS_ERROR_SSL_IDENTITY_MISMATCH => {
            c"Certificate does not match host or IP address"
        }
        CassError::CASS_ERROR_SSL_PROTOCOL_ERROR => c"Protocol error",
        CassError::CASS_ERROR_SSL_CLOSED => c"Connection closed",
        _ => c"",
    };

    desc.as_ptr()
}
