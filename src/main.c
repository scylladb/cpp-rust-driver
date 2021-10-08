#include <stddef.h>
#include <assert.h>
#include <stdio.h>

#include "cassandra.h"

int main() {
    CassFuture* connect_future = NULL;
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    cass_cluster_set_contact_points(cluster, "127.0.1.1");
    connect_future = cass_session_connect(session, cluster);
    printf("code: %d\n", cass_future_error_code(connect_future));
    cass_future_free(connect_future);

    cass_cluster_free(cluster);
    cass_session_free(session);
}