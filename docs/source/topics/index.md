# Overview

## Architecture

### Cluster

The [`CassCluster`] object describes a ScyllaDB/Cassandra cluster’s configuration.
The default cluster object is good for most clusters and only requires a single
or multiple lists of contact points in order to establish a session connection.
Once a session is connected using a cluster object, its configuration is
constant. Modifying the cluster's object configuration after a session is
established does not alter the session's configuration.

### Session

The [`CassSession`] object is used for query execution. Internally, a session
object also manages a pool of client connections to ScyllaDB/Cassandra and uses
a load balancing policy to distribute requests across those connections. An
application should create a single session object per keyspace. A session
object is designed to be created once, reused, and shared by multiple threads
within the application. The throughput of a session can be scaled by
increasing the number of I/O threads. An I/O thread is used to drive the inner
driver machinery, which among others sends requests to Cassandra/Scylla and handle
responses. The number of I/O threads defaults to one per CPU core, but it can be
configured using [`cass_cluster_set_num_threads_io()`]. It’s generally much better
to create a single session with more I/O threads than multiple sessions with
a smaller number of I/O threads, especially that a session is a heavyweight
object - it keeps the connection pool and up-to-date cluster metadata.

### Asynchronous I/O

Each session maintains a number of connections for each node in the cluster.
This number can be controlled by `cass_cluster_set_core_connections_per_host()`.
In case of ScyllaDB, it is possible to specify a number of connection per **shard**
instead of a node by calling `cass_cluster_set_core_connections_per_shard()`,
which is the recommended way to configure the driver for ScyllaDB.

Each of those connections can handle several simultaneous requests using
pipelining. Asynchronous I/O and pipelining together allow each connection
to handle several (up to 32k) in-flight requests concurrently.
This significantly reduces the number of connections required to be open to
ScyllaDB/Cassandra and allows the driver to batch requests destined for the
same node.

### Thread safety

A [`CassSession`] is designed to be used concurrently from multiple threads.
[`CassFuture`] is also thread safe. Other than these exclusions, in general,
functions that might modify an object's state are **NOT** thread safe. Objects
that are immutable (marked 'const') can be read safely by multiple threads.

**NOTE:** The object/resource free-ing functions (e.g. `cass_cluster_free`,
`cass_session_free`, ... `cass_*_free`) cannot be called concurrently on the
same instance of an object.

### Memory handling

Values such as strings (`const char*`), bytes and decimals
(`const cass_bytes_t*`) point to memory held by the result object. The
lifetimes of these values are valid as long as the result object isn’t freed.
These values **must** be copied into application memory if they need to live
longer than the result object’s lifetime. Primitive types such as
[`cass_int32_t`] are copied by the driver because it can be done cheaply
without incurring extra allocations.

**NOTE:** Advancing an iterator invalidates the value it previously returned.

[`cass_int32_t`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/cassandra.h#cass-int32-t
[`cass_cluster_set_num_threads_io()`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassCluster#function-cass_cluster_set_num_threads_io
[`CassCluster`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassCluster
[`CassFuture`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassFuture
[`CassSession`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassSession
