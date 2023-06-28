use crate::cass_error::*;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn cass_error_desc(error: CassError) -> *const c_char {
    let desc = match error {
        CassError::CASS_ERROR_LIB_BAD_PARAMS => "Bad parameters\0",
        CassError::CASS_ERROR_LIB_NO_STREAMS => "No streams available\0",
        CassError::CASS_ERROR_LIB_UNABLE_TO_INIT => "Unable to initialize\0",
        CassError::CASS_ERROR_LIB_MESSAGE_ENCODE => "Unable to encode message\0",
        CassError::CASS_ERROR_LIB_HOST_RESOLUTION => "Unable to resolve host\0",
        CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE => "Unexpected response from server\0",
        CassError::CASS_ERROR_LIB_REQUEST_QUEUE_FULL => "The request queue is full\0",
        CassError::CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD => "No available IO threads\0",
        CassError::CASS_ERROR_LIB_WRITE_ERROR => "Write error\0",
        CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE => "No hosts available\0",
        CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS => "Index out of bounds\0",
        CassError::CASS_ERROR_LIB_INVALID_ITEM_COUNT => "Invalid item count\0",
        CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE => "Invalid value type\0",
        CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT => "Request timed out\0",
        CassError::CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE => "Unable to set keyspace\0",
        CassError::CASS_ERROR_LIB_CALLBACK_ALREADY_SET => "Callback already set\0",
        CassError::CASS_ERROR_LIB_INVALID_STATEMENT_TYPE => "Invalid statement type\0",
        CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST => "No value or column for name\0",
        CassError::CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL => {
            "Unable to find supported protocol version\0"
        }
        CassError::CASS_ERROR_LIB_NULL_VALUE => "NULL value specified\0",
        CassError::CASS_ERROR_LIB_NOT_IMPLEMENTED => "Not implemented\0",
        CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT => "Unable to connect\0",
        CassError::CASS_ERROR_LIB_UNABLE_TO_CLOSE => "Unable to close\0",
        CassError::CASS_ERROR_LIB_NO_PAGING_STATE => "No paging state\0",
        CassError::CASS_ERROR_LIB_PARAMETER_UNSET => "Parameter unset\0",
        CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE => "Invalid error result type\0",
        CassError::CASS_ERROR_LIB_INVALID_FUTURE_TYPE => "Invalid future type\0",
        CassError::CASS_ERROR_LIB_INTERNAL_ERROR => "Internal error\0",
        CassError::CASS_ERROR_LIB_INVALID_CUSTOM_TYPE => "Invalid custom type\0",
        CassError::CASS_ERROR_LIB_INVALID_DATA => "Invalid data\0",
        CassError::CASS_ERROR_LIB_NOT_ENOUGH_DATA => "Not enough data\0",
        CassError::CASS_ERROR_LIB_INVALID_STATE => "Invalid state\0",
        CassError::CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD => "No custom payload\0",
        CassError::CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID => {
            "Invalid execution profile specified\0"
        }
        CassError::CASS_ERROR_LIB_NO_TRACING_ID => "No tracing ID\0",
        CassError::CASS_ERROR_SERVER_SERVER_ERROR => "Server error\0",
        CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR => "Protocol error\0",
        CassError::CASS_ERROR_SERVER_BAD_CREDENTIALS => "Bad credentials\0",
        CassError::CASS_ERROR_SERVER_UNAVAILABLE => "Unavailable\0",
        CassError::CASS_ERROR_SERVER_OVERLOADED => "Overloaded\0",
        CassError::CASS_ERROR_SERVER_IS_BOOTSTRAPPING => "Is bootstrapping\0",
        CassError::CASS_ERROR_SERVER_TRUNCATE_ERROR => "Truncate error\0",
        CassError::CASS_ERROR_SERVER_WRITE_TIMEOUT => "Write timeout\0",
        CassError::CASS_ERROR_SERVER_READ_TIMEOUT => "Read timeout\0",
        CassError::CASS_ERROR_SERVER_READ_FAILURE => "Read failure\0",
        CassError::CASS_ERROR_SERVER_FUNCTION_FAILURE => "Function failure\0",
        CassError::CASS_ERROR_SERVER_WRITE_FAILURE => "Write failure\0",
        CassError::CASS_ERROR_SERVER_SYNTAX_ERROR => "Syntax error\0",
        CassError::CASS_ERROR_SERVER_UNAUTHORIZED => "Unauthorized\0",
        CassError::CASS_ERROR_SERVER_INVALID_QUERY => "Invalid query\0",
        CassError::CASS_ERROR_SERVER_CONFIG_ERROR => "Configuration error\0",
        CassError::CASS_ERROR_SERVER_ALREADY_EXISTS => "Already exists\0",
        CassError::CASS_ERROR_SERVER_UNPREPARED => "Unprepared\0",
        CassError::CASS_ERROR_SSL_INVALID_CERT => "Unable to load certificate\0",
        CassError::CASS_ERROR_SSL_INVALID_PRIVATE_KEY => "Unable to load private key\0",
        CassError::CASS_ERROR_SSL_NO_PEER_CERT => "No peer certificate\0",
        CassError::CASS_ERROR_SSL_INVALID_PEER_CERT => "Invalid peer certificate\0",
        CassError::CASS_ERROR_SSL_IDENTITY_MISMATCH => {
            "Certificate does not match host or IP address\0"
        }
        CassError::CASS_ERROR_SSL_PROTOCOL_ERROR => "Protocol error\0",
        CassError::CASS_ERROR_SSL_CLOSED => "Connection closed\0",
        CassError::CASS_ERROR_CLOUD_CONFIG_OPEN_ERROR => "Unable to open cloud config file\0",
        CassError::CASS_ERROR_CLOUD_CONFIG_PARSE_ERROR => "Unable to parse cloud config file\0",
        CassError::CASS_ERROR_CLOUD_CONFIG_DECODE_ERROR => {
            "Unable to decode cloud base64 key/cert\0"
        }
        CassError::CASS_ERROR_CLOUD_CONFIG_VALIDATION_ERROR => "Invalid cloud config\0",
        CassError::CASS_ERROR_CLOUD_CONFIG_BAD_SSL_PARAMS => "Unable to parse cloud SSL key/cert\0",
        CassError::CASS_ERROR_CLOUD_CONFIG_UNKNOWN_ERROR => "Unknown cloud error\0",
        _ => "\0",
    };

    desc.as_ptr() as *const c_char
}
