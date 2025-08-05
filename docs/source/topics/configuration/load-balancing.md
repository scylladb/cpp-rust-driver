# Load balancing

Load balancing controls how queries are distributed to nodes in a ScyllaDB/Cassandra
cluster.

Without additional configuration the C/C++ driver defaults to using Datacenter-aware
load balancing with token-aware routing. This means that driver will only send
queries to nodes in the local datacenter (for local consistency levels) and
it will use the primary key of queries to route them directly to the nodes where the
corresponding data is located.

## Round-robin Load Balancing

This load balancing policy equally distributes queries across cluster without
consideration of datacenter locality. This should only be used with
ScyllaDB/Cassandra clusters where all nodes are located in the same datacenter.

## Datacenter-aware Load Balancing

This load balancing policy equally distributes queries to nodes in the local
datacenter. Nodes in remote datacenters are only used when all local nodes are
unavailable. Additionally, remote nodes are only considered when non-local
consistency levels are used or if the driver is configured to use remote nodes
with the [`allow_remote_dcs_for_local_cl`] setting.

```c
CassCluster* cluster = cass_cluster_new();

const char* local_dc = "dc1"; /* Local datacenter name */

/*
 * Use up to 2 remote datacenter nodes for remote consistency levels
 * or when `allow_remote_dcs_for_local_cl` is enabled.
 */
unsigned used_hosts_per_remote_dc = 2;

/* Don't use remote datacenter nodes for local consistency levels */
cass_bool_t allow_remote_dcs_for_local_cl = cass_false;

cass_cluster_set_load_balance_dc_aware(cluster,
                                       local_dc,
                                       used_hosts_per_remote_dc,
                                       allow_remote_dcs_for_local_cl);

/* ... */

cass_cluster_free(cluster);
```

## Token-aware Routing

Token-aware routing uses the primary key of queries to route requests directly to
the ScyllaDB/Cassandra nodes where the data is located. Using this policy avoids having
to route requests through an extra coordinator node in the ScyllaDB/Cassandra cluster. This
can improve query latency and reduce load on the ScyllaDB/Cassandra nodes. It can be used
in conjunction with other load balancing and routing policies.

```c
CassCluster* cluster = cass_cluster_new();

/* Enable token-aware routing (this is the default setting) */
cass_cluster_set_token_aware_routing(cluster, cass_true);

/* Disable token-aware routing */
cass_cluster_set_token_aware_routing(cluster, cass_false);

/* ... */

cass_cluster_free(cluster);
```

## Latency-aware Routing

Latency-aware routing tracks the latency of queries to avoid sending new queries
to poorly performing ScyllaDB/Cassandra nodes. It can be used in conjunction with other
load balancing and routing policies.

```c
CassCluster* cluster = cass_cluster_new();

/* Disable latency-aware routing (this is the default setting) */
cass_cluster_set_latency_aware_routing(cluster, cass_false);

/* Enable latency-aware routing */
cass_cluster_set_latency_aware_routing(cluster, cass_true);

/*
 * Configure latency-aware routing settings
 */

/* Up to 2 times the best performing latency is okay */
cass_double_t exclusion_threshold = 2.0;

 /* Use the default scale */
cass_uint64_t scale_ms = 100;

/* Retry a node after 10 seconds even if it was performing poorly before */
cass_uint64_t retry_period_ms = 10000;

/* Find the best performing latency every 100 milliseconds */
cass_uint64_t update_rate_ms = 100;

/* Only consider the average latency of a node after it's been queried 50 times */
cass_uint64_t min_measured = 50;

cass_cluster_set_latency_aware_routing_settings(cluster,
                                                exclusion_threshold,
                                                scale_ms,
                                                retry_period_ms,
                                                update_rate_ms,
                                                min_measured);

/* ... */

cass_cluster_free(cluster);
```

## Filtering policies

### Whitelist

This policy ensures that only hosts from the provided whitelist filter will
ever be used. Any host that is not contained within the whitelist will be
considered ignored and a connection will not be established.  It can be used in
conjunction with other load balancing and routing policies.

NOTE: Using this policy to limit the connections of the driver to a predefined
      set of hosts will defeat the auto-detection features of the driver. If
      the goal is to limit connections to hosts in a local datacenter use
      DC aware in conjunction with the round robin load balancing policy.

```c
CassCluster* cluster = cass_cluster_new();

/* Set the list of predefined hosts the driver is allowed to connect to */
cass_cluster_set_whitelist_filtering(cluster,
                                     "127.0.0.1, 127.0.0.3, 127.0.0.5");

/* The whitelist can be cleared (and disabled) by using an empty string */
cass_cluster_set_whitelist_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

### Blacklist

This policy is the inverse of the whitelist policy where hosts provided in the
blacklist filter will be ignored and a connection will not be established.

```c
CassCluster* cluster = cass_cluster_new();

/* Set the list of predefined hosts the driver is NOT allowed to connect to */
cass_cluster_set_blacklist_filtering(cluster,
                                     "127.0.0.1, 127.0.0.3, 127.0.0.5");

/* The blacklist can be cleared (and disabled) by using an empty string */
cass_cluster_set_blacklist_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

### Datacenter

Filtering can also be performed on all hosts in a datacenter or multiple
datacenters when using the whitelist/blacklist datacenter filtering polices.

```c
CassCluster* cluster = cass_cluster_new();

/* Set the list of predefined datacenters the driver is allowed to connect to */
cass_cluster_set_whitelist_dc_filtering(cluster, "dc2, dc4");

/* The datacenter whitelist can be cleared/disabled by using an empty string */
cass_cluster_set_whitelist_dc_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

```c
CassCluster* cluster = cass_cluster_new();


/* Set the list of predefined datacenters the driver is NOT allowed to connect to */
cass_cluster_set_blacklist_dc_filtering(cluster, "dc2, dc4");

/* The datacenter blacklist can be cleared/disabled by using an empty string */
cass_cluster_set_blacklist_dc_filtering(cluster, "");

/* ... */

cass_cluster_free(cluster);
```

[`allow_remote_dcs_for_local_cl`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassCluster#1a46b9816129aaa5ab61a1363489dccfd0
