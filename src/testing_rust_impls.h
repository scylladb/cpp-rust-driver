#ifndef CPP_RUST_DRIVER_TESTING_RUST_IMPLS_HPP
#define CPP_RUST_DRIVER_TESTING_RUST_IMPLS_HPP

#include "cassandra.h"

extern "C" {
// Retrieves a connect timeout from cluster config.
CASS_EXPORT cass_uint16_t testing_cluster_get_connect_timeout(CassCluster* cluster);

// Retrieves a CQL connection port from cluster config.
CASS_EXPORT cass_int32_t testing_cluster_get_port(CassCluster* cluster);

// Retrieves a contact points string. The contact points are delimited with ','.
//
// This function can fail, if any of the contact points contains a nul byte.
// Then, the resulting pointer is set to null.
//
// On success, this function allocates a contact points string, which needs to be then
// freed with `testing_free_contact_points`.
CASS_EXPORT void testing_cluster_get_contact_points(CassCluster* cluster, char** contact_points,
                                                    size_t* contact_points_length);

CASS_EXPORT void testing_free_contact_points(char* contact_points);

// Sets a sleeping history listener on the statement.
// This can be used to enforce a sleep time during statement execution, which increases the latency.
CASS_EXPORT void testing_statement_set_sleeping_history_listener(CassStatement *statement,
                                                                 cass_uint64_t sleep_time_ms);
}

#endif
