# Configuration

### Connection Heartbeats

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

### Host State Changes

The status and membership of a node can change within the life-cycle of the
cluster. A host listener callback can be used to detect these changes.

**Important**: The driver runs the host listener callback on a thread that is
               different from the application. Any data accessed in the
               callback must be immutable or synchronized with a mutex,
               semaphore, etc.

```c
void on_host_listener(CassHostListenerEvent event, CassInet inet, void* data) {
  /* Get the string representation of the inet address */
  char address[CASS_INET_STRING_LENGTH];
  cass_inet_string(inet, address);

  /* Perform application logic for host listener event */
  if (event == CASS_HOST_LISTENER_EVENT_ADD) {
    printf("Host %s has been ADDED\n", address);
   } else if (event == CASS_HOST_LISTENER_EVENT_REMOVE) {
    printf("Host %s has been REMOVED\n", address);
   } else if (event == CASS_HOST_LISTENER_EVENT_UP) {
    printf("Host %s is UP\n", address);
   } else if (event == CASS_HOST_LISTENER_EVENT_DOWN) {
    printf("Host %s is DOWN\n", address);
   }
}

int main() {
  CassCluster* cluster = cass_cluster_new();

  /* Register the host listener callback */
  cass_cluster_set_host_listener_callback(cluster, on_host_listener, NULL);

  /* ... */

  cass_cluster_free(cluster);
}
```

**Note**: Expensive (e.g. slow) operations should not be performed in host
          listener callbacks. Performing expensive operations in a callback
          will block or slow the driver's normal operation.

### Reconnection Policy

The reconnection policy controls the interval between each attempt for a given
connection.

#### Exponential Reconnection Policy

The exponential reconnection policy is the default reconnection policy. It
starts by using a base delay in milliseconds which is then exponentially
increased (doubled) during each reconnection attempt; up to the defined maximum
delay.

**Note**: Once the connection is re-established, this policy will restart using
          base delay if a reconnection occurs.

#### Constant Reconnection Policy

The constant reconnection policy is a fixed delay for each reconnection
attempt.

### Performance Tips

#### Use a single persistent session

Sessions are expensive objects to create in both time and resources because they
maintain a pool of connections to your Cassandra cluster. An application should
create a minimal number of sessions and maintain them for the lifetime of an
application.

#### Use token-aware and latency-aware policies

The token-aware load balancing can reduce the latency of requests by avoiding an
extra network hop through a coordinator node. When using the token-aware policy
requests are sent to one of the nodes which will retrieved or stored instead of
routing the request through a proxy node (coordinator node).

The latency-aware load balancing policy can also reduce the latency of requests
by routing requests to nodes that historical performing with the lowest latency.
This can prevent requests from being sent to nodes that are underperforming.

Both [latency-aware] and [token-aware] can be use together to obtain the benefits of
both.

#### Use [paging] when retrieving large result sets

Using a large page size or a very high `LIMIT` clause can cause your application
to delay for each individual request. The driver's paging mechanism can be used
to decrease the latency of individual requests.

#### Choose a lower consistency level

Ultimately, choosing a consistency level is a trade-off between consistency and
availability. Performance should not be a large deciding factor when choosing a
consistency level. However, it can affect high-percentile latency numbers
because requests with consistency levels greater than `ONE` can cause requests
to wait for one or more nodes to respond back to the coordinator node before a
request can complete. In multi-datacenter configurations, consistency levels such as
`EACH_QUORUM` can cause a request to wait for replication across a slower cross
datacenter network link.  More information about setting the consistency level
can be found [here](http://datastax.github.io/cpp-driver/topics/basics/consistency/).

### Driver Tuning

Beyond the performance tips and best practices considered in the previous
section your application might consider tuning the more fine-grain driver
settings in this section to achieve optimal performance for your application's
specific workload.

#### Increasing core connections

In some workloads, throughput can be increased by increasing the number of core
connections. By default, the driver uses a single core connection per host. It's
recommended that you try increasing the core connections to two and slowly
increase this number while doing performance testing. Two core connections is
often a good setting and increasing the core connections too high will decrease
performance because having multiple connections to a single host inhibits the
driver's ability to coalesce multiple requests into a fewer number of system
calls.

#### Coalesce delay

The coalesce delay is an optimization to reduce the number of system calls
required to process requests. This setting controls how long the driver's I/O
threads wait for requests to accumulate before flushing them on to the wire.
Larger values for coalesce delay are preferred for throughput-based workloads as
it can significantly reduce the number of system calls required to process
requests.

In general, the coalesce delay should be increased for throughput-based
workloads and can be decreased for latency-based workloads. Most importantly,
the delay should consider the responsiveness guarantees of your application.

Note: Single, sporadic requests are not generally affected by this delay and
are processed immediately.

#### New request ratio

The new request ratio controls how much time an I/O thread spends processing new
requests versus handling outstanding requests. This value is a percentage (with
a value from 1 to 100), where larger values will dedicate more time to
processing new requests and less time on outstanding requests. The goal of this
setting is to balance the time spent processing new/outstanding requests and
prevent either from fully monopolizing the I/O thread's processing time. It's
recommended that your application decrease this value if computationally
expensive or long-running future callbacks are used (via
`cass_future_set_callback()`), otherwise this can be left unchanged.

[`allow_remote_dcs_for_local_cl`]: http://datastax.github.io/cpp-driver/api/struct.CassCluster#1a46b9816129aaa5ab61a1363489dccfd0
[`OPTIONS`]: https://github.com/apache/cassandra/blob/cassandra-3.0/doc/native_protocol_v3.spec
[token-aware]: http://datastax.github.io/cpp-driver/topics/configuration#latency-aware-routing
[latency-aware]: http://datastax.github.io/cpp-driver/topics/configuration#token-aware-routing
[paging]: http://datastax.github.io/cpp-driver/topics/basics/handling_results#paging

```{eval-rst}
.. toctree::
  :hidden:
  :glob:

  load_balancing
  retry_policies/*
  speculative_execution
```
