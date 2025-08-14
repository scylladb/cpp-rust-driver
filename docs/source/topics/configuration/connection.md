# Connection

## Heartbeats

To prevent intermediate network devices (routers, switches, etc.) from
disconnecting pooled connections the driver sends a lightweight heartbeat
request (using an [`OPTIONS`] protocol request) periodically. By default the
driver sends a heartbeat every 30 seconds. This can be changed or disabled (0
second interval) using the following:

```c
CassCluster* cluster = cass_cluster_new();

/* Change the heartbeat interval to 1 minute */
cass_cluster_set_connection_heartbeat_interval(cluster, 60);

/* Disable heartbeat requests */
cass_cluster_set_connection_heartbeat_interval(cluster, 0);

/* ... */

cass_cluster_free(cluster);
```
Heartbeats are also used to detect unresponsive connections. An idle timeout
setting controls the amount of time a connection is allowed to be without a
successful heartbeat before being terminated and scheduled for reconnection. This
interval can be changed from the default of 60 seconds:

```c
CassCluster* cluster = cass_cluster_new();

/* Change the idle timeout to 2 minute */
cass_cluster_set_connection_idle_timeout(cluster, 120);

/* ... */

cass_cluster_free(cluster);
```

It can be disabled by setting the value to a very long timeout or by disabling
heartbeats.

[`OPTIONS`]: https://github.com/apache/cassandra/blob/a39f3b066f010d465a1be1038d5e06f1e31b0391/doc/native_protocol_v4.spec#L330
