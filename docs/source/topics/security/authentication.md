# Authentication

## Plain text

Credentials are provided using the [`cass_cluster_set_credentials()`] function.

```c
CassCluster* cluster = cass_cluster_new();

const char* username = "cassandra";
const char* password = "cassandra";

cass_cluster_set_credentials(cluster, username, password);

/* Connect session object */

cass_cluster_free(cluster);
```

**Important**: The credentials are sent in plain text to the server. For this
reason, it is highly recommended that this be used in conjunction with
client-to-node encryption (TLS), or in a trusted network environment.

[`cass_cluster_set_credentials()`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassCluster#function-cass_cluster_set_credentials_n
