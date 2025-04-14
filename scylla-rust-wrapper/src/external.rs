use crate::cass_error::*;
use std::os::raw::c_char;

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
