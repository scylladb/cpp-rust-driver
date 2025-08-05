# Security

The driver currently supports authentication (using plain text or a custom
authenticator) and TLS (via OpenSSL).

## Authentication

### Plain text

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

[`cass_cluster_set_credentials()`]: http://datastax.github.io/cpp-driver/api/struct.CassCluster#function-cass_cluster_set_credentials_n

```{eval-rst}
.. toctree::
  :hidden:
  :glob:

  ssl
```
