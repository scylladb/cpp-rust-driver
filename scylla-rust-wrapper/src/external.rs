use crate::argconv::ptr_to_ref;
use crate::cass_error::*;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn cass_error_desc(error: *const CassError) -> *const c_char {
    let error: &CassError = ptr_to_ref(error);
    let desc = match *error {
        CassError::CASS_ERROR_LIB_BAD_PARAMS => "Bad parameters",
        CassError::CASS_ERROR_LIB_NO_STREAMS => "No streams available",
        CassError::CASS_ERROR_LIB_UNABLE_TO_INIT => "Unable to initialize",
        CassError::CASS_ERROR_LIB_MESSAGE_ENCODE => "Unable to encode message",
        CassError::CASS_ERROR_LIB_HOST_RESOLUTION => "Unable to resolve host",
        CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE => "Unexpected response from server",
        CassError::CASS_ERROR_LIB_REQUEST_QUEUE_FULL => "The request queue is full",
        CassError::CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD => "No available IO threads",
        CassError::CASS_ERROR_LIB_WRITE_ERROR => "Write error",
        CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE => "No hosts available",
        CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS => "Index out of bounds",
        CassError::CASS_ERROR_LIB_INVALID_ITEM_COUNT => "Invalid item count",
        CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE => "Invalid value type",
        CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT => "Request timed out",
        CassError::CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE => "Unable to set keyspace",
        CassError::CASS_ERROR_LIB_CALLBACK_ALREADY_SET => "Callback already set",
        CassError::CASS_ERROR_LIB_INVALID_STATEMENT_TYPE => "Invalid statement type",
        CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST => "No value or column for name",
        CassError::CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL => {
            "Unable to find supported protocol version"
        }
        CassError::CASS_ERROR_LIB_NULL_VALUE => "NULL value specified",
        CassError::CASS_ERROR_LIB_NOT_IMPLEMENTED => "Not implemented",
        CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT => "Unable to connect",
        CassError::CASS_ERROR_LIB_UNABLE_TO_CLOSE => "Unable to close",
        CassError::CASS_ERROR_LIB_NO_PAGING_STATE => "No paging state",
        CassError::CASS_ERROR_LIB_PARAMETER_UNSET => "Parameter unset",
        CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE => "Invalid error result type",
        CassError::CASS_ERROR_LIB_INVALID_FUTURE_TYPE => "Invalid future type",
        CassError::CASS_ERROR_LIB_INTERNAL_ERROR => "Internal error",
        CassError::CASS_ERROR_LIB_INVALID_CUSTOM_TYPE => "Invalid custom type",
        CassError::CASS_ERROR_LIB_INVALID_DATA => "Invalid data",
        CassError::CASS_ERROR_LIB_NOT_ENOUGH_DATA => "Not enough data",
        CassError::CASS_ERROR_LIB_INVALID_STATE => "Invalid state",
        CassError::CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD => "No custom payload",
        CassError::CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID => {
            "Invalid execution profile specified"
        }
        CassError::CASS_ERROR_LIB_NO_TRACING_ID => "No tracing ID",
        CassError::CASS_ERROR_SERVER_SERVER_ERROR => "Server error",
        CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR => "Protocol error",
        CassError::CASS_ERROR_SERVER_BAD_CREDENTIALS => "Bad credentials",
        CassError::CASS_ERROR_SERVER_UNAVAILABLE => "Unavailable",
        CassError::CASS_ERROR_SERVER_OVERLOADED => "Overloaded",
        CassError::CASS_ERROR_SERVER_IS_BOOTSTRAPPING => "Is bootstrapping",
        CassError::CASS_ERROR_SERVER_TRUNCATE_ERROR => "Truncate error",
        CassError::CASS_ERROR_SERVER_WRITE_TIMEOUT => "Write timeout",
        CassError::CASS_ERROR_SERVER_READ_TIMEOUT => "Read timeout",
        CassError::CASS_ERROR_SERVER_READ_FAILURE => "Read failure",
        CassError::CASS_ERROR_SERVER_FUNCTION_FAILURE => "Function failure",
        CassError::CASS_ERROR_SERVER_WRITE_FAILURE => "Write failure",
        CassError::CASS_ERROR_SERVER_SYNTAX_ERROR => "Syntax error",
        CassError::CASS_ERROR_SERVER_UNAUTHORIZED => "Unauthorized",
        CassError::CASS_ERROR_SERVER_INVALID_QUERY => "Invalid query",
        CassError::CASS_ERROR_SERVER_CONFIG_ERROR => "Configuration error",
        CassError::CASS_ERROR_SERVER_ALREADY_EXISTS => "Already exists",
        CassError::CASS_ERROR_SERVER_UNPREPARED => "Unprepared",
        CassError::CASS_ERROR_SSL_INVALID_CERT => "Unable to load certificate",
        CassError::CASS_ERROR_SSL_INVALID_PRIVATE_KEY => "Unable to load private key",
        CassError::CASS_ERROR_SSL_NO_PEER_CERT => "No peer certificate",
        CassError::CASS_ERROR_SSL_INVALID_PEER_CERT => "Invalid peer certificate",
        CassError::CASS_ERROR_SSL_IDENTITY_MISMATCH => {
            "Certificate does not match host or IP address"
        }
        CassError::CASS_ERROR_SSL_PROTOCOL_ERROR => "Protocol error",
        CassError::CASS_ERROR_SSL_CLOSED => "Connection closed",
        _ => "",
    };

    desc.as_ptr() as *const c_char
}
