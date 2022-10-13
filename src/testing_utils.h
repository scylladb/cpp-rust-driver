#ifndef CPPDRIVERV2_TESTING_UTILS_H
#define CPPDRIVERV2_TESTING_UTILS_H

#include "cassandra.h"

extern "C" {
  CASS_EXPORT cass_uint16_t testing_get_connect_timeout_from_cluster(CassCluster* cluster);

  CASS_EXPORT cass_int32_t testing_get_port_from_cluster(CassCluster* cluster);

  CASS_EXPORT void
  testing_get_contact_points_from_cluster(
      CassCluster* cluster,
      const char** contact_points,
      size_t* contact_points_length,
      void** contact_points_boxed
  );

  CASS_EXPORT void testing_free_contact_points_string(void* box);

  CASS_EXPORT CassConsistency testing_get_consistency(const CassStatement* statement);

  CASS_EXPORT CassConsistency testing_get_serial_consistency(const CassStatement* statement);

  CASS_EXPORT cass_uint64_t testing_get_request_timeout_ms(const CassStatement* statement);

  CASS_EXPORT const CassRetryPolicy* testing_get_retry_policy(const CassStatement* statement);

  CASS_EXPORT const char* testing_get_server_name(CassFuture* future);

  CASS_EXPORT void testing_set_record_attempted_hosts(CassStatement* statement, cass_bool_t enable);
}

#endif //CPPDRIVERV2_TESTING_UTILS_H
