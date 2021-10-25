use scylla::transport::errors::*;

// TODO: import from bindgen instead of copying...
impl CassErrorSource_ {
    pub const CASS_ERROR_SOURCE_NONE: CassErrorSource_ = CassErrorSource_(0);
}
impl CassErrorSource_ {
    pub const CASS_ERROR_SOURCE_LIB: CassErrorSource_ = CassErrorSource_(1);
}
impl CassErrorSource_ {
    pub const CASS_ERROR_SOURCE_SERVER: CassErrorSource_ = CassErrorSource_(2);
}
impl CassErrorSource_ {
    pub const CASS_ERROR_SOURCE_SSL: CassErrorSource_ = CassErrorSource_(3);
}
impl CassErrorSource_ {
    pub const CASS_ERROR_SOURCE_COMPRESSION: CassErrorSource_ = CassErrorSource_(4);
}
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct CassErrorSource_(pub ::std::os::raw::c_uint);
pub use self::CassErrorSource_ as CassErrorSource;
impl CassError_ {
    pub const CASS_OK: CassError_ = CassError_(0);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_BAD_PARAMS: CassError_ = CassError_(16777217);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NO_STREAMS: CassError_ = CassError_(16777218);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_UNABLE_TO_INIT: CassError_ = CassError_(16777219);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_MESSAGE_ENCODE: CassError_ = CassError_(16777220);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_HOST_RESOLUTION: CassError_ = CassError_(16777221);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_UNEXPECTED_RESPONSE: CassError_ = CassError_(16777222);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_REQUEST_QUEUE_FULL: CassError_ = CassError_(16777223);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD: CassError_ = CassError_(16777224);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_WRITE_ERROR: CassError_ = CassError_(16777225);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NO_HOSTS_AVAILABLE: CassError_ = CassError_(16777226);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS: CassError_ = CassError_(16777227);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_ITEM_COUNT: CassError_ = CassError_(16777228);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_VALUE_TYPE: CassError_ = CassError_(16777229);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_REQUEST_TIMED_OUT: CassError_ = CassError_(16777230);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE: CassError_ = CassError_(16777231);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_CALLBACK_ALREADY_SET: CassError_ = CassError_(16777232);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_STATEMENT_TYPE: CassError_ = CassError_(16777233);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NAME_DOES_NOT_EXIST: CassError_ = CassError_(16777234);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL: CassError_ = CassError_(16777235);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NULL_VALUE: CassError_ = CassError_(16777236);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NOT_IMPLEMENTED: CassError_ = CassError_(16777237);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_UNABLE_TO_CONNECT: CassError_ = CassError_(16777238);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_UNABLE_TO_CLOSE: CassError_ = CassError_(16777239);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NO_PAGING_STATE: CassError_ = CassError_(16777240);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_PARAMETER_UNSET: CassError_ = CassError_(16777241);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE: CassError_ = CassError_(16777242);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_FUTURE_TYPE: CassError_ = CassError_(16777243);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INTERNAL_ERROR: CassError_ = CassError_(16777244);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_CUSTOM_TYPE: CassError_ = CassError_(16777245);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_DATA: CassError_ = CassError_(16777246);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NOT_ENOUGH_DATA: CassError_ = CassError_(16777247);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_INVALID_STATE: CassError_ = CassError_(16777248);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD: CassError_ = CassError_(16777249);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID: CassError_ = CassError_(16777250);
}
impl CassError_ {
    pub const CASS_ERROR_LIB_NO_TRACING_ID: CassError_ = CassError_(16777251);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_SERVER_ERROR: CassError_ = CassError_(33554432);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_PROTOCOL_ERROR: CassError_ = CassError_(33554442);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_BAD_CREDENTIALS: CassError_ = CassError_(33554688);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_UNAVAILABLE: CassError_ = CassError_(33558528);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_OVERLOADED: CassError_ = CassError_(33558529);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_IS_BOOTSTRAPPING: CassError_ = CassError_(33558530);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_TRUNCATE_ERROR: CassError_ = CassError_(33558531);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_WRITE_TIMEOUT: CassError_ = CassError_(33558784);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_READ_TIMEOUT: CassError_ = CassError_(33559040);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_READ_FAILURE: CassError_ = CassError_(33559296);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_FUNCTION_FAILURE: CassError_ = CassError_(33559552);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_WRITE_FAILURE: CassError_ = CassError_(33559808);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_SYNTAX_ERROR: CassError_ = CassError_(33562624);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_UNAUTHORIZED: CassError_ = CassError_(33562880);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_INVALID_QUERY: CassError_ = CassError_(33563136);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_CONFIG_ERROR: CassError_ = CassError_(33563392);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_ALREADY_EXISTS: CassError_ = CassError_(33563648);
}
impl CassError_ {
    pub const CASS_ERROR_SERVER_UNPREPARED: CassError_ = CassError_(33563904);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_INVALID_CERT: CassError_ = CassError_(50331649);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_INVALID_PRIVATE_KEY: CassError_ = CassError_(50331650);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_NO_PEER_CERT: CassError_ = CassError_(50331651);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_INVALID_PEER_CERT: CassError_ = CassError_(50331652);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_IDENTITY_MISMATCH: CassError_ = CassError_(50331653);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_PROTOCOL_ERROR: CassError_ = CassError_(50331654);
}
impl CassError_ {
    pub const CASS_ERROR_SSL_CLOSED: CassError_ = CassError_(50331655);
}
impl CassError_ {
    pub const CASS_ERROR_LAST_ENTRY: CassError_ = CassError_(50331656);
}
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct CassError_(pub ::std::os::raw::c_uint);
pub use self::CassError_ as CassError;

impl From<&QueryError> for CassError {
    fn from(error: &QueryError) -> Self {
        match error {
            QueryError::DbError(db_error, _string) => CassError::from(db_error),
            QueryError::BadQuery(_bad_query) => CassError::CASS_ERROR_LAST_ENTRY,
            QueryError::IoError(_io_error) => CassError::CASS_ERROR_LAST_ENTRY,
            QueryError::ProtocolError(_str) => CassError::CASS_ERROR_LAST_ENTRY,
            QueryError::InvalidMessage(_string) => CassError::CASS_ERROR_LAST_ENTRY,
            QueryError::TimeoutError => CassError::CASS_ERROR_LAST_ENTRY,
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
            DbError::Unprepared => CassError::CASS_ERROR_SERVER_UNPREPARED,
            DbError::Other(num) => {
                CassError((CassErrorSource::CASS_ERROR_SOURCE_SERVER.0 << 24) | *num as u32)
            }
        }
    }
}

impl From<&BadQuery> for CassError {
    fn from(error: &BadQuery) -> Self {
        match error {
            BadQuery::SerializeValuesError(_serialize_values_error) => {
                CassError::CASS_ERROR_LAST_ENTRY
            }
            BadQuery::ValueLenMismatch(_usize, _usize2) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::ValuesTooLongForKey(_usize, _usize2) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::BadKeyspaceName(_bad_keyspace_name) => CassError::CASS_ERROR_LAST_ENTRY,
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
