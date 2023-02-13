use scylla::transport::errors::*;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_errors.rs"));

impl From<&QueryError> for CassError {
    fn from(error: &QueryError) -> Self {
        match error {
            QueryError::DbError(db_error, _string) => CassError::from(db_error),
            QueryError::BadQuery(bad_query) => CassError::from(bad_query),
            QueryError::IoError(_io_error) => CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
            QueryError::ProtocolError(_str) => CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR,
            QueryError::InvalidMessage(_string) => CassError::CASS_ERROR_SERVER_INVALID_QUERY,
            QueryError::TimeoutError => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT, // This may be either read or write timeout error
            QueryError::TooManyOrphanedStreamIds(_) => CassError::CASS_ERROR_LIB_INVALID_STATE,
            QueryError::UnableToAllocStreamId => CassError::CASS_ERROR_LIB_NO_STREAMS,
            QueryError::RequestTimeout(_) => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT,
        }
    }
}

impl From<&DbError> for CassError {
    fn from(error: &DbError) -> Self {
        match error {
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
        }
    }
}

impl From<&BadQuery> for CassError {
    fn from(error: &BadQuery) -> Self {
        match error {
            BadQuery::SerializeValuesError(_serialize_values_error) => {
                CassError::CASS_ERROR_LAST_ENTRY
            }
            BadQuery::ValuesTooLongForKey(_usize, _usize2) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::BadKeyspaceName(_bad_keyspace_name) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::Other(_other_query) => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl From<&NewSessionError> for CassError {
    fn from(error: &NewSessionError) -> Self {
        match error {
            NewSessionError::FailedToResolveAddress(_string) => {
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
            }
            NewSessionError::EmptyKnownNodesList => CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
            NewSessionError::DbError(_db_error, _string) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::BadQuery(_bad_query) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::IoError(_io_error) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::ProtocolError(_str) => {
                CassError::CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL
            }
            NewSessionError::InvalidMessage(_string) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::TimeoutError => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::TooManyOrphanedStreamIds(_) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::UnableToAllocStreamId => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::RequestTimeout(_) => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT,
        }
    }
}

impl From<&BadKeyspaceName> for CassError {
    fn from(error: &BadKeyspaceName) -> Self {
        match error {
            BadKeyspaceName::Empty => CassError::CASS_ERROR_LAST_ENTRY,
            BadKeyspaceName::TooLong(_string, _usize) => CassError::CASS_ERROR_LAST_ENTRY,
            BadKeyspaceName::IllegalCharacter(_string, _char) => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

pub trait CassErrorMessage {
    fn msg(&self) -> String;
}

impl CassErrorMessage for QueryError {
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
