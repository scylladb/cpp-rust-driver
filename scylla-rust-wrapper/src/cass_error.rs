use scylla::errors::*;

// Re-export error types.
pub(crate) use crate::cass_error_types::{CassError, CassErrorSource};
use crate::execution_error::CassErrorResult;
use crate::statement::UnknownNamedParameterError;

pub trait ToCassError {
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
                CassError::CASS_ERROR_LIB_MESSAGE_ENCODE
            }
            RequestAttemptError::CqlResultParseError(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            RequestAttemptError::CqlErrorParseError(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            RequestAttemptError::DbError(db_error, _) => db_error.to_cass_error(),
            RequestAttemptError::UnexpectedResponse(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            RequestAttemptError::RepreparedIdChanged { .. } => {
                CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR
            }
            RequestAttemptError::RepreparedIdMissingInBatch => {
                CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR
            }
            RequestAttemptError::NonfinishedPagingState => {
                CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR
            }

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

pub trait CassErrorMessage {
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
