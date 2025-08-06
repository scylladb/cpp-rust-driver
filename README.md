# ScyllaDB Cpp-Rust Driver
___
Wrapper around ScyllaDB's rust-driver compatible with Datastax cpp-driver.

#### Note: It is work in progress, bug reports and pull requests are welcome!

# CMake options

In this section we will go over most important CMake options and what each of them controls.
- `CASS_BUILD_SHARED` - Build shared driver library (.so). `ON` by default.
- `CASS_BUILD_STATIC` - Build static driver library (.a). `ON` by default.
- `CASS_BUILD_INTEGRATION_TESTS` - Build integration tests (see `Testing` section below) `OFF` by default.
- `CASS_BUILD_EXAMPLES` - Build examples (see `Examples` section below). `OFF` by default.
- `CASS_USE_STATIC_LIBS` - Link against static libraries when building tests/examples. `OFF` by default.
- `CMAKE_BUILD_TYPE` - Controls the cargo profile library is built with. Possible values are: `Debug`, `RelWithDebInfo` and `Release`. Default value is `Debug`. For more information, see the profiles definitions in `scylla-rust-wrapper/Cargo.toml`.

For example, to build a shared driver library (no static library) in release mode and integration tests you can do:
```shell
mkdir build
cd build
cmake -DCASS_BUILD_STATIC=OFF -DCASS_BUILD_INTEGRATION_TESTS=ON -DCMAKE_BUILD_TYPE=Release ..
make
```

# Examples
___

There are some examples in the [examples](https://github.com/scylladb/cpp-rust-driver/tree/master/examples) directory.
To run a single example:
```shell
cd examples/cloud
gcc cloud.c PATH_TO_CPP_RUST/scylla-rust-wrapper/target/debug/libscylla_cpp_driver.so -Wl,-rpath,PATH_TO_CPP_RUST/build  -I PATH_TO_CPP_RUST/include -o cloud
./cloud path_to_connection_bundle username password
```

```c++
#include <cassandra.h>
#include <stdio.h>

int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
    const char* query = "SELECT release_version FROM system.local WHERE key='local'";
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);

      if (row) {
        const CassValue* value = cass_row_get_column_by_name(row, "release_version");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length, release_version);
      }

      cass_result_free(result);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
```

# Logging
___

The logging API and implementation are compatible with the C++ driver, for more details please refer to the [logging documentation](https://cpp-driver.docs.scylladb.com/master/topics/logging/index.html).
As the `tracing` framework is used under the hood to instrument the collection of logs from the Rust driver and the Cpp-Rust wrapper,
the logging level and callback are passed through a custom event subscriber which is globally set as default when `cass_log_set_level` is called.
So, `cass_log_set_level` *must* be called only once as subsequent attempts trying to modify the globally set event subscriber will be ignored.
Also, Rust programs using Cpp-Rust driver under the hood must avoid calling `tracing::subscriber::set_global_default` as this will cause conflicts.

##### Note: The logging configuration must be done before any other driver function is called, otherwise, the default logging callback will be used, and logs will appear on stderr.

```c++
void on_log(const CassLogMessage* message, void* data) {
  /* Handle logging */
}

int main() {
  void* log_data = NULL /* Custom log resource */;
  cass_log_set_callback(on_log, log_data);
  cass_log_set_level(CASS_LOG_INFO);

  /* Create cluster and connect session */
}
```

# Features
___

The driver inherits almost all the features of C/C++ and Rust drivers, such as:
 * [Asynchronous API](http://datastax.github.io/cpp-driver/topics/#futures)
 * Shard-aware routing
 * Simple, Prepared and Batch statements
 * Query paging
 * CQL binary protocol version 4
 * Load balancing policies
 * Retry policies
 * SSL
 * Authentication
 * [Tuples](http://datastax.github.io/cpp-driver/topics/basics/data-types/tuples/) and [UDTs](http://datastax.github.io/cpp-driver/topics/basics/data-types/user-defined-types/)
 * [Nested collections](http://datastax.github.io/cpp-driver/topics/basics/binding-parameters/#nested-collections)
 * [Data types](http://datastax.github.io/cpp-driver/topics/basics/data-types/)
 * Schema metadata (keyspace metadata, materialized views, etc.)

# Limitations

##### Note: This section may be incomplete, so not everything that is not fully supported is mentioned here.

<table>
    <thead>
        <tr>
            <th>Function</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td colspan=2 align="center" style="font-weight:bold">Statement</td>
        </tr>
        <tr>
            <td>cass_statement_bind_custom[by_name]</td>
            <td>Binding is not implemented for custom types in the Rust driver.</td>
        </tr>
        <tr>
            <td colspan=2 align="center" style="font-weight:bold">Collection</td>
        </tr>
        <tr>
            <td>cass_collection_append_custom[_n]</td>
            <td>Unimplemented because of the same reasons as binding for statements.<br> <b>Note</b>: The driver does not check whether the type of the appended value is compatible with the type of the collection items.</td>
        </tr>
        <tr>
            <td colspan=2 align="center" style="font-weight:bold">User Defined Type</td>
        </tr>
        <tr>
            <td>cass_user_type_set_custom[by_name]</td>
            <td>Unimplemented because of the same reasons as binding for statements.<br> <b>Note</b>: The driver does not check whether the type of the value being set for a field of the UDT is compatible with the field's actual type.</td>
        </tr>
        <tr>
            <td colspan=2 align="center" style="font-weight:bold">Metadata</td>
        </tr>
        <tr>
            <td>cass_keyspace_meta_is_virtual</td>
            <td rowspan="7"> UDF, Aggregate and Index are not supported in the Rust driver, yet. </td>
        </tr>
        <tr>
            <td>cass_table_meta_is_virtual</td>
        </tr>
        <tr>
            <td>cass_table_meta_clustering_key_order</td>
        </tr>
        <tr>
            <td>cass_materialized_view_meta_clustering_key_order</td>
        </tr>
        <tr>
            <td>cass_function_*</td>
        </tr>
        <tr>
            <td>cass_aggregate_*</td>
        </tr>
        <tr>
            <td>cass_index_*</td>
        </tr>
    </tbody>
</table>

## Unimplemented functions from `cassandra.h`

#### CassAggregate:
- cass_aggregate_meta_argument_count
- cass_aggregate_meta_argument_type
- cass_aggregate_meta_field_by_name
- cass_aggregate_meta_field_by_name_n
- cass_aggregate_meta_final_func
- cass_aggregate_meta_full_name
- cass_aggregate_meta_init_cond
- cass_aggregate_meta_name
- cass_aggregate_meta_return_type
- cass_aggregate_meta_state_func
- cass_aggregate_meta_state_type

#### CassAlloc:
- cass_alloc_set_functions

#### CassAuthenticator:
- cass_authenticator_address
- cass_authenticator_class_name
- cass_authenticator_exchange_data
- cass_authenticator_hostname
- cass_authenticator_response
- cass_authenticator_set_error
- cass_authenticator_set_error_n
- cass_authenticator_set_exchange_data
- cass_authenticator_set_response

#### CassBatch:
- cass_batch_set_custom_payload
- cass_batch_set_keyspace
- cass_batch_set_keyspace_n

#### CassCluster:
- cass_cluster_set_authenticator_callbacks
- cass_cluster_set_cloud_secure_connection_bundle
- cass_cluster_set_cloud_secure_connection_bundle_n
- cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init
- cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init_n
- cass_cluster_set_constant_reconnect
- cass_cluster_set_exponential_reconnect
- cass_cluster_set_host_listener_callback
- cass_cluster_set_max_concurrent_creation
- cass_cluster_set_max_concurrent_requests_threshold
- cass_cluster_set_max_connections_per_host
- cass_cluster_set_max_requests_per_flush
- cass_cluster_set_max_reusable_write_objects
- cass_cluster_set_monitor_reporting_interval
- cass_cluster_set_new_request_ratio
- cass_cluster_set_no_compact
- cass_cluster_set_pending_requests_high_water_mark
- cass_cluster_set_pending_requests_low_water_mark
- cass_cluster_set_prepare_on_all_hosts
- cass_cluster_set_prepare_on_up_or_add_host
- cass_cluster_set_queue_size_event
- cass_cluster_set_queue_size_io
- cass_cluster_set_reconnect_wait_time
- cass_cluster_set_resolve_timeout
- cass_cluster_set_tracing_consistency
- cass_cluster_set_tracing_max_wait_time
- cass_cluster_set_tracing_retry_wait_time
- cass_cluster_set_use_hostname_resolution
- cass_cluster_set_use_randomized_contact_points
- cass_cluster_set_write_bytes_high_water_mark
- cass_cluster_set_write_bytes_low_water_mark

#### CassCollection:
- cass_collection_append_custom
- cass_collection_append_custom_n

#### CassColumnMeta:
- cass_column_meta_field_by_name
- cass_column_meta_field_by_name_n

#### CassCustomPayload:
- cass_custom_payload_free
- cass_custom_payload_new
- cass_custom_payload_remove
- cass_custom_payload_remove_n
- cass_custom_payload_set
- cass_custom_payload_set_n

#### CassFunctionMeta:
- cass_function_meta_argument
- cass_function_meta_argument_count
- cass_function_meta_argument_type_by_name
- cass_function_meta_argument_type_by_name_n
- cass_function_meta_body
- cass_function_meta_body
- cass_function_meta_called_on_null_input
- cass_function_meta_field_by_name
- cass_function_meta_field_by_name_n
- cass_function_meta_full_name
- cass_function_meta_language
- cass_function_meta_name
- cass_function_meta_return_type

#### CassFuture:
- cass_future_custom_payload_item
- cass_future_custom_payload_item_count

#### CassIndexMeta:
- cass_index_meta_field_by_name
- cass_index_meta_field_by_name_n
- cass_index_meta_name
- cass_index_meta_options
- cass_index_meta_target
- cass_index_meta_type

#### CassIterator:
- cass_iterator_aggregates_from_keyspace_meta
- cass_iterator_fields_from_aggregate_meta
- cass_iterator_fields_from_column_meta
- cass_iterator_fields_from_function_meta
- cass_iterator_fields_from_index_meta
- cass_iterator_fields_from_keyspace_meta
- cass_iterator_fields_from_materialized_view_meta
- cass_iterator_fields_from_table_meta
- cass_iterator_functions_from_keyspace_meta
- cass_iterator_get_aggregate_meta
- cass_iterator_get_function_meta
- cass_iterator_get_index_meta
- cass_iterator_get_meta_field_name
- cass_iterator_get_meta_field_value
- cass_iterator_indexes_from_table_meta

#### CassKeyspaceMeta:
- cass_keyspace_meta_aggregate_by_name
- cass_keyspace_meta_aggregate_by_name_n
- cass_keyspace_meta_field_by_name
- cass_keyspace_meta_field_by_name_n
- cass_keyspace_meta_function_by_name
- cass_keyspace_meta_function_by_name_n
- cass_keyspace_meta_is_virtual

#### CassMaterializedViewMeta:
- cass_materialized_view_meta_clustering_key_order
- cass_materialized_view_meta_field_by_name
- cass_materialized_view_meta_field_by_name_n

#### CassSchemaMeta:
- cass_schema_meta_snapshot_version
- cass_schema_meta_version

#### CassSession:
- cass_session_get_speculative_execution_metrics

#### CassStatement:
- cass_statement_add_key_index
- cass_statement_bind_custom
- cass_statement_bind_custom_by_name
- cass_statement_bind_custom_by_name_n
- cass_statement_bind_custom_n
- cass_statement_set_custom_payload
- cass_statement_set_keyspace
- cass_statement_set_keyspace_n

#### CassTableMeta:
- cass_table_meta_clustering_key_order
- cass_table_meta_field_by_name
- cass_table_meta_field_by_name_n
- cass_table_meta_index
- cass_table_meta_index_by_name
- cass_table_meta_index_by_name_n
- cass_table_meta_index_count
- cass_table_meta_is_virtual

#### CassTuple:
- cass_tuple_set_custom
- cass_tuple_set_custom_n

#### CassValue:
- cass_value_get_custom

# Testing
___

Integration tests from the original `cpp-driver` are compilable but not all tests pass yet.
Some tests are added to GitHub Actions workflows and are used to test every pull request on both ScyllaDB and Cassandra clusters.

To build and run the integration tests several requirements need to be met:

* Install `libuv` and `openssl` on your system:

```shell
# On Ubuntu
sudo apt-get install libuv1-dev
sudo apt-get install libssl1.0.0
sudo apt-get install libkrb5-dev

# On Fedora
sudo dnf install libuv-devel
sudo dnf install openssl-devel
sudo dnf install krb5-devel
```

* Clone and install [scylla-ccm](https://github.com/scylladb/scylla-ccm) system-wide
* Clone and build [scylla-jmx](https://github.com/scylladb/scylla-jmx) alongside `scylla-ccm`
* Clone, build and symlink [scylla-tools-java](https://github.com/scylladb/scylla-tools-java) from `[SCYLLA_ROOT]/resources/cassandra`
  * Assuming `scylla` is installed and built in the release mode <br> ``` ln -s [PATH_TO_TOOLS]/scylla-tools-java [PATH_TO_SCYLLA]/resources/cassandra```

Finally, to build the integration tests:

```
mkdir build && cd build
cmake -DCASS_BUILD_INTEGRATION_TESTS=ON  .. && make
```

Now, use `--gtest_filter` to run certain integration tests:

```shell
./cassandra-integration-tests --scylla --install-dir=[PATH_TO_SCYLLA] --version=3.0.8 --category=CASSANDRA --verbose=ccm --gtest_filter="ClusterTests.*"
```

##### Note: Tests that pass with ScyllaDB and Cassandra clusters can be found in Makefile: `SCYLLA_TEST_FILTER` and `CASSANDRA_TEST_FILTER` env variables.

# Build rpm package
___

To build rpm package, run the following command:
```shell
./dist/redhat/build_rpm.sh --target rocky-8-x86_64
```
It will construct chrooted build environment of target distribution using mock,
and build rpm in the environment.
Target parameter should be mock .cfg file name.
Currently tested on rocky-8-x86_64, rocky-9-x86_64, fedora-38-x86_64, fedora-39-x86_64, fedora-40-x86_64, fedora-rawhide-x86_64.
Build environment should be Fedora or RHEL variants + EPEL, since
other distribution does not provide mock package.
Built result will placed under build/redhat/{rpms,srpms}.

# Build deb package
___

To build deb package, run the following command:
```shell
./dist/debian/build_deb.sh --target mantic
```
It will construct chrooted build environment of target distribution using
pbuilder, and build deb in the environment.
Target parameter should be debian/ubuntu codename.
On Ubuntu targets, currently tested on bionic (18.04), focal (20.04), jammy (22.04), mantic (23.10), noble (24.04).
On Debian targets, currently tested on buster (10), bullseye (11), bookworm (12), trixie (13), sid (unstable).
Build environment should be Fedora, Ubuntu or Debian, since these distribution
provides pbuilder package.
Built result will placed under build/debian/debs.

# Build & install HomeBrew package (macOS)

---

To build HomeBrew pacakge, run the following command:
```shell
cd dist/homebrew
brew install --HEAD ./scylla-cpp-rust-driver.rb
```
It will run build & install the driver in HomeBrew environment.
Tested on macOS 14.5.

# Getting Help
___

* Slack: http://slack.scylladb.com/
* `Issues` section of GitHub repository

# Reference Documentation
___

* [CQL binary protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec) version 4

# Other Drivers
___

* [ScyllaDB Rust driver](https://github.com/scylladb/scylla-rust-driver)
* [C/C++ Driver for ScyllaDB](https://github.com/scylladb/cpp-driver)

# License
___

This project is licensed under the GNU Lesser General Public License v2.1
