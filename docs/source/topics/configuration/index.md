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

[`OPTIONS`]: https://github.com/apache/cassandra/blob/a39f3b066f010d465a1be1038d5e06f1e31b0391/doc/native_protocol_v4.spec#L330

```{eval-rst}
.. toctree::
  :hidden:
  :glob:

  load-balancing
  retry-policies
  speculative-execution
  performance-tips
```
