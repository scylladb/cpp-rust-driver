# Load balancing

Load balancing controls how queries are distributed to nodes in a ScyllaDB/Cassandra
cluster.

Without additional configuration the C/C++ driver defaults to using Datacenter-unaware
load balancing with token-aware routing. This means that the driver will use the primary key
of queries to route them directly to the nodes where the corresponding data is located.
It will not consider datacenter locality when distributing queries, because it does not
know if there exists a datacenter that should be preferred over others, and, if so,
which one.

## Round-robin Load Balancing

This load balancing policy equally distributes queries across cluster without
consideration of datacenter locality. This policy normally should always be
accompanied by token-aware routing to ensure that queries are sent directly
to the nodes where the data is located.

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
The way latency-aware interacts with other policies is tricky and may as well worsen
the performance of your application. It is recommended to use it only after
comparing the performance of your application with and without it.

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

## Shard-Awareness

ScyllaDB is built around the concept o a *sharded architecture*. What it means for
clients is that each piece of data is bound to specific CPU(s) on specific
node(s). The ability of the driver to query specific shard (CPU) is called
"shard-awareness".

One can think of shard-awareness as token-awareness brought to a higher level.
Token-aware drivers execute the queries on specific node(s) - where the data
of interest "belongs". This eliminates the network traffic between the
coordinator node and the "data owning node" and thus leads to performance
improvements. That idea can be taken further: the driver can open a separate
connection to every CPU on the target node and use the *right connection* to
query the *right CPU on the right node*. This eliminates the cross-CPU traffic
on that node and results in even greater speedups.

**NOTE:** Only prepared statements benefit from shard-awareness.

### "Basic" shard-awareness

Through extensions to the CQL protocol, ScyllaDB node informs the incoming CQL
connection about:

1. the total number of shards within the node;
2. the ID of the specific shard that handles this connection.

Driver opens new connections until it reaches or exceeds the number specified
by `cass_cluster_set_core_connections_per_shard()`. No particular action is needed to
achieve shard-awareness this way, as this is the default behavior
of the driver.

### "Advanced" shard-awareness

Since ScyllaDB 4.3 however, drivers can use a new, more powerful method of
establishing per-shard connection pools. This is the recommended usage pattern,
commonly referred to as "advanced" shard-awareness. The idea behind it is that
ScyllaDB listens for CQL connections on an additional port, by default 19042.
Connections incoming to that port, are being routed to the shard determined by
*client-side (ephemeral) port number*. Precisely, if a client socket has local
port number `P` then that connection lands on shard `P % shard_count`. The
function of the usual port 9042 (`native_transport_port`) is unchanged and
non-shard-aware drivers should continue using it.

Advanced shard-awareness is the preferred mode because it reduces load on
the cluster while building connection pools. The reason is that with basic
shard-awareness, driver keeps opening CQL connections until it randomly reaches
each shard, often ending up with some excess connections being established and
discarded soon after. In advanced mode, driver opens only as many connections
as needed.

**NOTE:** It's important to unblock `native_shard_aware_transport_port` and/or
`native_shard_aware_transport_port_ssl` in the firewall rules, if applicable.

**NOTE:** If the client app runs behind a NAT (e.g. on a desktop in the office
network) while the ScyllaDB cluster is hosted somewhere else (e.g. on Azure or
AWS) then, most likely, the router at the office alters the client-side port
numbers. In this case port-based ("advanced") shard selection will not work and
will fall back to the "basic" mode.

The advanced mode is also supported by the driver by default.


[`allow_remote_dcs_for_local_cl`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassCluster#1a46b9816129aaa5ab61a1363489dccfd0
