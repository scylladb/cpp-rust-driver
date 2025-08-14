# Metrics

Performance metrics and diagnostic information can be obtained from the C/C++
driver using [`cass_session_get_metrics()`]. The resulting [`CassMetrics`] object
contains several useful metrics for accessing request performance and/or
debugging issues.

```c
CassSession* session = cass_session_new();

/* Connect session */

CassMetrics metrics;

/* Get a snapshot of the driver's metrics */
cass_session_get_metrics(session, &metrics);

/* Run queries */

cass_session_free(session);
```

## Request metrics

The [`requests`] field  contains information about request latency and
throughput. All latency times are in microseconds and throughput
numbers are in requests per seconds.

## Statistics

The [`stats`] field contains information about the total number of connections.

## Errors

The [`errors`] field contains information about the
occurrence of requests and connection timeouts. Request timeouts occur when
a request fails to get a timely response.
Connection timeouts occur when the process of establishing new connections is
unresponsive.

[`cass_session_get_metrics()`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassSession#1ab3773670c98c00290bad48a6df0f9eae
[`CassMetrics`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassMetrics
[`requests`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassMetrics#attribute-requests
[`stats`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassMetrics#attribute-stats
[`errors`]: https://cpp-rust-driver.docs.scylladb.com/stable/api/struct.CassMetrics#attribute-errors
