# Speculative Execution

For certain applications it is of the utmost importance to minimize latency.
Speculative execution is a way to minimize latency by preemptively executing
several instances of the same query against different nodes. The fastest
response is then returned to the client application and the other requests are
cancelled. Speculative execution is <b>disabled</b> by default.

## Query Idempotence

Speculative execution will result in executing the same query several times.
Therefore, it is important that queries are idempotent i.e. a query can be
applied multiple times without changing the result beyond the initial
application. <b>Queries that are not explicitly marked as idempotent will not be
scheduled for speculative executions.</b>

The following types of queries are <b>not</b> idempotent:

* Mutation of `counter` columns
* Prepending or appending to a `list` column
* Use of non-idempotent CQL function e.g. `now()` or `uuid()`

The driver is unable to determine if a query is idempotent therefore it is up to
an application to explicitly mark a statement as being idempotent.

```c
CassStatement* statement = cass_statement_new( "SELECT * FROM table1", 0);

/* Make the statement idempotent */
cass_statement_set_is_idempotent(statement, cass_true);

cass_statement_free(statement);
```

## Enabling speculative execution

Speculative execution is enabled by connecting a `CassSession` with a
`CassCluster` that has a speculative execution policy enabled. The driver
currently only supports a constant policy, but may support more in the future.

### Constant speculative execution policy

The following will start up to 2 more executions after the initial execution
with the subsequent executions being created 500 milliseconds apart.

```c
CassCluster* cluster = cass_cluster_new();

cass_int64_t constant_delay_ms = 500; /* Delay before a new execution is created */
int max_speculative_executions = 2;   /* Number of executions */

cass_cluster_set_constant_speculative_execution_policy(cluster,
                                                       constant_delay_ms,
                                                       max_speculative_executions);

/* ... */

cass_cluster_free(cluster);
```
