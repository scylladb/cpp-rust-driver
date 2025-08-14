<!--
This file is purposely not part of the documentation tree,
as we do not want to advertise these features.
It is only left here to document the features that are not supported,
as this can be helpful if we want to implement them in the future:
- implementors can refer to this file to see what is not supported
  and how it is supposed to work;
- this can be moved to the documentation tree in the future
  if we decide to implement these features.
-->

```{eval-rst}
:orphan:
```

# Nonsupported configuration features

## Not implemented in the CPP-Rust Driver

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
