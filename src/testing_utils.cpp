#include "testing_utils.h"
#include <stdexcept>

CASS_EXPORT CassConsistency testing_get_consistency(const CassStatement* statement) {
  throw std::runtime_error("UNIMPLEMENTED testing_get_consistency\n");
}

CASS_EXPORT CassConsistency testing_get_serial_consistency(const CassStatement* statement) {
  throw std::runtime_error("UNIMPLEMENTED testing_get_serial_consistency\n");
}

CASS_EXPORT cass_uint64_t testing_get_request_timeout_ms(const CassStatement* statement) {
  throw std::runtime_error("UNIMPLEMENTED testing_get_request_timeout_ms\n");
}

CASS_EXPORT const CassRetryPolicy* testing_get_retry_policy(const CassStatement* statement) {
  throw std::runtime_error("UNIMPLEMENTED testing_get_retry_policy\n");
}

CASS_EXPORT const char* testing_get_server_name(CassFuture* future) {
  throw std::runtime_error("UNIMPLEMENTED testing_get_server_name\n");
}

CASS_EXPORT void testing_set_record_attempted_hosts(CassStatement* statement, cass_bool_t enable) {
  throw std::runtime_error("UNIMPLEMENTED testing_set_record_attempted_hosts\n");
}
