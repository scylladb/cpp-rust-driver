//! This module re-exports the driver's C API functions.
//! This has multiple goals:
//! 1. Provide a single point of access to all C API functions.
//!    Useful for in-Rust integration testing and intra-crate usage.
//! 2. Have a complete list of the API, together with clearly marked
//!    unimplemented functions.
//! 3. Ensure that the API is `pub`, which happened to not be the case
//!    because of accidental omission of `pub` in some cases.
//!
//! The submodules are ordered by the `cassandra.h` order.
//! However, the order of the functions within each submodule is alphabetical,
//! as opposed to the order in `cassandra.h`.

pub mod execution_profile {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::exec_profile::{
        CassExecProfile,
        cass_execution_profile_free,
        cass_execution_profile_new,
        cass_execution_profile_set_blacklist_dc_filtering,
        cass_execution_profile_set_blacklist_dc_filtering_n,
        cass_execution_profile_set_blacklist_filtering,
        cass_execution_profile_set_blacklist_filtering_n,
        cass_execution_profile_set_consistency,
        cass_execution_profile_set_constant_speculative_execution_policy,
        cass_execution_profile_set_latency_aware_routing,
        cass_execution_profile_set_latency_aware_routing_settings,
        cass_execution_profile_set_load_balance_dc_aware,
        cass_execution_profile_set_load_balance_dc_aware_n,
        cass_execution_profile_set_load_balance_rack_aware,
        cass_execution_profile_set_load_balance_rack_aware_n,
        cass_execution_profile_set_load_balance_round_robin,
        cass_execution_profile_set_no_speculative_execution_policy,
        cass_execution_profile_set_request_timeout,
        cass_execution_profile_set_retry_policy,
        cass_execution_profile_set_serial_consistency,
        cass_execution_profile_set_token_aware_routing,
        cass_execution_profile_set_token_aware_routing_shuffle_replicas,
        cass_execution_profile_set_whitelist_dc_filtering,
        cass_execution_profile_set_whitelist_dc_filtering_n,
        cass_execution_profile_set_whitelist_filtering,
        cass_execution_profile_set_whitelist_filtering_n,
    };
}

pub mod cluster {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::cluster::{
        CassCluster,
        cass_cluster_free,
        cass_cluster_new,
        cass_cluster_set_application_name,
        cass_cluster_set_application_name_n,
        cass_cluster_set_application_version,
        cass_cluster_set_application_version_n,
        // cass_cluster_set_authenticator_callbacks, UNIMPLEMENTED
        cass_cluster_set_blacklist_dc_filtering,
        cass_cluster_set_blacklist_dc_filtering_n,
        cass_cluster_set_blacklist_filtering,
        cass_cluster_set_blacklist_filtering_n,
        cass_cluster_set_client_id,
        // cass_cluster_set_cloud_secure_connection_bundle, UNIMPLEMENTED
        // cass_cluster_set_cloud_secure_connection_bundle_n, UNIMPLEMENTED, stub present
        // cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init, UNIMPLEMENTED
        // cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init_n, UNIMPLEMENTED
        cass_cluster_set_coalesce_delay,
        cass_cluster_set_compression,
        cass_cluster_set_connect_timeout,
        cass_cluster_set_connection_heartbeat_interval,
        cass_cluster_set_connection_idle_timeout,
        cass_cluster_set_consistency,
        // cass_cluster_set_constant_reconnect, UNIMPLEMENTED
        cass_cluster_set_contact_points,
        cass_cluster_set_contact_points_n,
        cass_cluster_set_constant_speculative_execution_policy,
        cass_cluster_set_core_connections_per_host,
        cass_cluster_set_core_connections_per_shard,
        cass_cluster_set_credentials,
        cass_cluster_set_credentials_n,
        cass_cluster_set_execution_profile,
        cass_cluster_set_execution_profile_n,
        // cass_cluster_set_exponential_reconnect, UNIMPLEMENTED, stub present
        // cass_cluster_set_host_listener_callback, UNIMPLEMENTED
        cass_cluster_set_latency_aware_routing,
        cass_cluster_set_latency_aware_routing_settings,
        cass_cluster_set_load_balance_round_robin,
        cass_cluster_set_load_balance_dc_aware,
        cass_cluster_set_load_balance_dc_aware_n,
        cass_cluster_set_local_address,
        cass_cluster_set_local_address_n,
        cass_cluster_set_local_port_range,
        cass_cluster_set_load_balance_rack_aware,
        cass_cluster_set_load_balance_rack_aware_n,
        // cass_cluster_set_max_concurrent_creation, UNIMPLEMENTED
        // cass_cluster_set_max_concurrent_requests_threshold, UNIMPLEMENTED
        // cass_cluster_set_max_connections_per_host, UNIMPLEMENTED
        // cass_cluster_set_max_requests_per_flush, UNIMPLEMENTED
        // cass_cluster_set_max_reusable_write_objects, UNIMPLEMENTED
        // cass_cluster_set_monitor_reporting_interval, UNIMPLEMENTED
        cass_cluster_set_max_schema_wait_time,
        // cass_cluster_set_new_request_ratio, UNIMPLEMENTED
        // cass_cluster_set_no_compact, UNIMPLEMENTED
        cass_cluster_set_no_speculative_execution_policy,
        // cass_cluster_set_num_threads_io, UNIMPLEMENTED
        cass_cluster_set_port,
        // cass_cluster_set_prepare_on_all_hosts, UNIMPLEMENTED
        // cass_cluster_set_prepare_on_up_or_add_host, UNIMPLEMENTED
        cass_cluster_set_protocol_version,
        cass_cluster_set_queue_size_event,
        // cass_cluster_set_queue_size_io, UNIMPLEMENTED
        // cass_cluster_set_reconnect_wait_time, UNIMPLEMENTED
        cass_cluster_set_request_timeout,
        // cass_cluster_set_resolve_timeout, UNIMPLEMENTED
        cass_cluster_set_retry_policy,
        cass_cluster_set_schema_agreement_interval,
        cass_cluster_set_serial_consistency,
        cass_cluster_set_ssl,
        cass_cluster_set_tcp_keepalive,
        cass_cluster_set_tcp_nodelay,
        cass_cluster_set_timestamp_gen,
        cass_cluster_set_token_aware_routing,
        cass_cluster_set_token_aware_routing_shuffle_replicas,
        // cass_cluster_set_tracing_max_wait_time, UNIMPLEMENTED
        // cass_cluster_set_tracing_retry_wait_time, UNIMPLEMENTED
        // cass_cluster_set_tracing_consistency, UNIMPLEMENTED
        cass_cluster_set_use_beta_protocol_version,
        // cass_cluster_set_use_hostname_resolution, UNIMPLEMENTED
        // cass_cluster_set_use_randomized_contact_points, UNIMPLEMENTED, stub present
        cass_cluster_set_use_schema,
        cass_cluster_set_whitelist_dc_filtering,
        cass_cluster_set_whitelist_dc_filtering_n,
        cass_cluster_set_whitelist_filtering,
        cass_cluster_set_whitelist_filtering_n,
        // cass_cluster_set_write_bytes_high_water_mark, UNIMPLEMENTED
        // cass_cluster_set_write_bytes_low_water_mark, UNIMPLEMENTED
        // cass_cluster_set_pending_requests_high_water_mark, UNIMPLEMENTED
        // cass_cluster_set_pending_requests_low_water_mark, UNIMPLEMENTED
    };
}

pub mod session {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::session::{
        CassSession,
        cass_session_close,
        cass_session_connect,
        cass_session_connect_keyspace,
        cass_session_connect_keyspace_n,
        cass_session_execute,
        cass_session_execute_batch,
        cass_session_free,
        cass_session_get_client_id,
        cass_session_get_metrics,
        cass_session_get_schema_meta,
        // cass_session_get_speculative_execution_metrics, UNIMPLEMENTED
        cass_session_new,
        cass_session_prepare,
        cass_session_prepare_from_existing,
        cass_session_prepare_n,
    };
}

pub mod schema_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::metadata::{
        CassSchemaMeta,
        cass_schema_meta_free,
        // cass_schema_meta_snapshot_version, UNIMPLEMENTED
        // cass_schema_meta_version, UNIMPLEMENTED
        cass_schema_meta_keyspace_by_name,
        cass_schema_meta_keyspace_by_name_n,
    };
}

pub mod keyspace_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::metadata::{
        CassKeyspaceMeta,
        // cass_keyspace_meta_aggregate_by_name, UNIMPLEMENTED
        // cass_keyspace_meta_aggregate_by_name_n, UNIMPLEMENTED
        // cass_keyspace_meta_field_by_name, UNIMPLEMENTED
        // cass_keyspace_meta_field_by_name_n, UNIMPLEMENTED
        // cass_keyspace_meta_function_by_name, UNIMPLEMENTED
        // cass_keyspace_meta_function_by_name_n, UNIMPLEMENTED
        // cass_keyspace_meta_is_virtual, UNIMPLEMENTED
        cass_keyspace_meta_materialized_view_by_name,
        cass_keyspace_meta_materialized_view_by_name_n,
        cass_keyspace_meta_name,
        cass_keyspace_meta_table_by_name,
        cass_keyspace_meta_table_by_name_n,
        cass_keyspace_meta_user_type_by_name,
        cass_keyspace_meta_user_type_by_name_n,
    };
}

pub mod table_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::metadata::{
        CassTableMeta,
        cass_table_meta_clustering_key_count,
        cass_table_meta_clustering_key,
        // cass_table_meta_clustering_key_order, UNIMPLEMENTED
        cass_table_meta_column,
        cass_table_meta_column_by_name,
        cass_table_meta_column_by_name_n,
        cass_table_meta_column_count,
        // cass_table_meta_field_by_name, UNIMPLEMENTED
        // cass_table_meta_field_by_name_n, UNIMPLEMENTED
        // cass_table_meta_index, UNIMPLEMENTED
        // cass_table_meta_index_by_name, UNIMPLEMENTED
        // cass_table_meta_index_by_name_n, UNIMPLEMENTED
        // cass_table_meta_index_count, UNIMPLEMENTED
        // cass_table_meta_is_virtual, UNIMPLEMENTED
        cass_table_meta_materialized_view,
        cass_table_meta_materialized_view_by_name,
        cass_table_meta_materialized_view_by_name_n,
        cass_table_meta_materialized_view_count,
        cass_table_meta_name,
        cass_table_meta_partition_key_count,
        cass_table_meta_partition_key,
    };
}

pub mod materialized_view_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::metadata::{
        CassMaterializedViewMeta,
        cass_materialized_view_meta_base_table,
        cass_materialized_view_meta_column,
        cass_materialized_view_meta_column_by_name,
        cass_materialized_view_meta_column_by_name_n,
        cass_materialized_view_meta_column_count,
        cass_materialized_view_meta_name,
        cass_materialized_view_meta_clustering_key,
        cass_materialized_view_meta_clustering_key_count,
        // cass_materialized_view_meta_clustering_key_order, UNIMPLEMENTED
        // cass_materialized_view_meta_field_by_name, UNIMPLEMENTED
        // cass_materialized_view_meta_field_by_name_n, UNIMPLEMENTED
        cass_materialized_view_meta_partition_key,
        cass_materialized_view_meta_partition_key_count,
    };
}

pub mod column_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::metadata::{
        CassColumnMeta,
        cass_column_meta_data_type,
        // cass_column_meta_field_by_name, UNIMPLEMENTED
        // cass_column_meta_field_by_name_n, UNIMPLEMENTED
        cass_column_meta_name,
        cass_column_meta_type,
    };
}

pub mod index_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    #[expect(unused_imports)]
    pub use crate::metadata::{
        // CassIndexMeta,
        // cass_index_meta_name, UNIMPLEMENTED
        // cass_index_meta_field_by_name, UNIMPLEMENTED
        // cass_index_meta_field_by_name_n, UNIMPLEMENTED
        // cass_index_meta_options, UNIMPLEMENTED
        // cass_index_meta_target, UNIMPLEMENTED
        // cass_index_meta_type, UNIMPLEMENTED
    };
}

pub mod function_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    #[expect(unused_imports)]
    pub use crate::metadata::{
        // CassFunctionMeta,
        // cass_function_meta_argument_count, UNIMPLEMENTED
        // cass_function_meta_argument, UNIMPLEMENTED
        // cass_function_meta_argument_type_by_name, UNIMPLEMENTED
        // cass_function_meta_argument_type_by_name_n, UNIMPLEMENTED
        // cass_function_meta_body, UNIMPLEMENTED
        // cass_function_meta_called_on_null_input, UNIMPLEMENTED
        // cass_function_meta_field_by_name, UNIMPLEMENTED
        // cass_function_meta_field_by_name_n, UNIMPLEMENTED
        // cass_function_meta_full_name, UNIMPLEMENTED
        // cass_function_meta_language, UNIMPLEMENTED
        // cass_function_meta_name, UNIMPLEMENTED
        // cass_function_meta_return_type, UNIMPLEMENTED
    };
}

pub mod aggregate_meta {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    #[expect(unused_imports)]
    pub use crate::metadata::{
        // CassAggregateMeta,
        // cass_aggregate_meta_argument, UNIMPLEMENTED
        // cass_aggregate_meta_argument_count, UNIMPLEMENTED
        // cass_aggregate_meta_argument_type, UNIMPLEMENTED
        // cass_aggregate_meta_field_by_name, UNIMPLEMENTED
        // cass_aggregate_meta_field_by_name_n, UNIMPLEMENTED
        // cass_aggregate_meta_final_func, UNIMPLEMENTED
        // cass_aggregate_meta_full_name, UNIMPLEMENTED
        // cass_aggregate_meta_init_cond, UNIMPLEMENTED
        // cass_aggregate_meta_name, UNIMPLEMENTED
        // cass_aggregate_meta_return_type, UNIMPLEMENTED
        // cass_aggregate_meta_state_func, UNIMPLEMENTED
        // cass_aggregate_meta_state_type, UNIMPLEMENTED
    };
}

pub mod ssl {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::ssl::{
        CassSsl,
        cass_ssl_add_trusted_cert,
        cass_ssl_add_trusted_cert_n,
        cass_ssl_free,
        cass_ssl_new,
        cass_ssl_new_no_lib_init,
        cass_ssl_set_cert,
        cass_ssl_set_cert_n,
        cass_ssl_set_private_key,
        cass_ssl_set_private_key_n,
        cass_ssl_set_verify_flags,
    };
}

pub mod authenticator {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    #[expect(unused_imports)]
    pub use crate::{
        // CassAuthenticator,
        // cass_authenticator_address, UNIMPLEMENTED
        // cass_authenticator_hostname, UNIMPLEMENTED
        // cass_authenticator_class_name, UNIMPLEMENTED
        // cass_authenticator_exchange_data, UNIMPLEMENTED
        // cass_authenticator_set_exchange_data, UNIMPLEMENTED
        // cass_authenticator_response, UNIMPLEMENTED
        // cass_authenticator_set_response, UNIMPLEMENTED
        // cass_authenticator_set_error, UNIMPLEMENTED
        // cass_authenticator_set_error_n, UNIMPLEMENTED
    };
}

pub mod future {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::future::{
        CassFuture,
        CassFutureCallback,
        cass_future_coordinator,
        // cass_future_custom_payload_item, UNIMPLEMENTED, stub present
        // cass_future_custom_payload_item_count,  UNIMPLEMENTED, stub present
        cass_future_error_code,
        cass_future_error_message,
        cass_future_get_error_result,
        cass_future_free,
        cass_future_get_prepared,
        cass_future_get_result,
        cass_future_ready,
        cass_future_set_callback,
        cass_future_tracing_id,
        cass_future_wait,
        cass_future_wait_timed,
    };

    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::query_result::{
        CassNode, // `cass_future_coordinator()` returns a `CassNode`.
    };
}

pub mod statement {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::statement::{
        CassStatement,
        cass_statement_bind_bool,
        cass_statement_bind_bool_by_name,
        cass_statement_bind_bool_by_name_n,
        cass_statement_bind_bytes,
        cass_statement_bind_bytes_by_name,
        cass_statement_bind_bytes_by_name_n,
        // cass_statement_bind_custom, UNIMPLEMENTED
        // cass_statement_bind_custom_by_name, UNIMPLEMENTED
        // cass_statement_bind_custom_n, UNIMPLEMENTED
        cass_statement_bind_collection,
        cass_statement_bind_collection_by_name,
        cass_statement_bind_collection_by_name_n,
        cass_statement_bind_decimal,
        cass_statement_bind_decimal_by_name,
        cass_statement_bind_decimal_by_name_n,
        cass_statement_bind_double,
        cass_statement_bind_double_by_name,
        cass_statement_bind_double_by_name_n,
        cass_statement_bind_duration,
        cass_statement_bind_duration_by_name,
        cass_statement_bind_duration_by_name_n,
        cass_statement_bind_float,
        cass_statement_bind_float_by_name,
        cass_statement_bind_float_by_name_n,
        cass_statement_bind_inet,
        cass_statement_bind_inet_by_name,
        cass_statement_bind_inet_by_name_n,
        cass_statement_bind_int8,
        cass_statement_bind_int8_by_name,
        cass_statement_bind_int8_by_name_n,
        cass_statement_bind_int16,
        cass_statement_bind_int16_by_name,
        cass_statement_bind_int16_by_name_n,
        cass_statement_bind_int32,
        cass_statement_bind_int32_by_name,
        cass_statement_bind_int32_by_name_n,
        cass_statement_bind_int64,
        cass_statement_bind_int64_by_name,
        cass_statement_bind_int64_by_name_n,
        cass_statement_bind_null,
        cass_statement_bind_null_by_name,
        cass_statement_bind_null_by_name_n,
        cass_statement_bind_string,
        cass_statement_bind_string_by_name,
        cass_statement_bind_string_by_name_n,
        cass_statement_bind_string_n,
        cass_statement_bind_tuple,
        cass_statement_bind_tuple_by_name,
        cass_statement_bind_tuple_by_name_n,
        cass_statement_bind_uint32,
        cass_statement_bind_uint32_by_name,
        cass_statement_bind_uint32_by_name_n,
        cass_statement_bind_user_type,
        cass_statement_bind_user_type_by_name,
        cass_statement_bind_user_type_by_name_n,
        cass_statement_bind_uuid,
        cass_statement_bind_uuid_by_name,
        cass_statement_bind_uuid_by_name_n,
        // cass_statement_add_key_index, UNIMPLEMENTED
        cass_statement_free,
        cass_statement_new,
        cass_statement_new_n,
        cass_statement_reset_parameters,
        cass_statement_set_consistency,
        // cass_statement_set_custom_payload, UNIMPLEMENTED
        // cass_statement_set_custom_payload_n, UNIMPLEMENTED
        cass_statement_set_host,
        cass_statement_set_host_inet,
        cass_statement_set_host_n,
        cass_statement_set_is_idempotent,
        // cass_statement_set_keyspace, UNIMPLEMENTED
        // cass_statement_set_keyspace_n, UNIMPLEMENTED
        cass_statement_set_node,
        cass_statement_set_paging_size,
        cass_statement_set_paging_state,
        cass_statement_set_paging_state_token,
        cass_statement_set_request_timeout,
        cass_statement_set_retry_policy,
        cass_statement_set_serial_consistency,
        cass_statement_set_timestamp,
        cass_statement_set_tracing,
    };

    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::exec_profile::{
        cass_statement_set_execution_profile,
        cass_statement_set_execution_profile_n,
    };
}

pub mod prepared {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::prepared::{
        CassPrepared,
        cass_prepared_bind,
        cass_prepared_free,
        cass_prepared_parameter_data_type,
        cass_prepared_parameter_data_type_by_name,
        cass_prepared_parameter_data_type_by_name_n,
        cass_prepared_parameter_name,
    };
}

pub mod batch {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::batch::{
        CassBatch,
        CassBatchType,
        cass_batch_add_statement,
        cass_batch_free,
        cass_batch_new,
        cass_batch_set_consistency,
        // cass_batch_set_custom_payload, UNIMPLEMENTED
        // cass_batch_set_keyspace, UNIMPLEMENTED
        // cass_batch_set_keyspace_n, UNIMPLEMENTED
        cass_batch_set_serial_consistency,
        cass_batch_set_request_timeout,
        cass_batch_set_is_idempotent,
        cass_batch_set_retry_policy,
        cass_batch_set_timestamp,
        cass_batch_set_tracing,
    };

    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::exec_profile::{
        cass_batch_set_execution_profile,
        cass_batch_set_execution_profile_n,
    };
}

pub mod data_type {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::cass_types::{
        CassDataType,
        cass_data_sub_type_count,
        cass_data_type_add_sub_type,
        cass_data_type_add_sub_type_by_name,
        cass_data_type_add_sub_type_by_name_n,
        cass_data_type_add_sub_value_type,
        cass_data_type_add_sub_value_type_by_name,
        cass_data_type_add_sub_value_type_by_name_n,
        cass_data_type_class_name,
        cass_data_type_free,
        cass_data_type_is_frozen,
        cass_data_type_keyspace,
        cass_data_type_new,
        cass_data_type_new_from_existing,
        cass_data_type_new_tuple,
        cass_data_type_new_udt,
        cass_data_type_set_class_name,
        cass_data_type_set_class_name_n,
        cass_data_type_set_keyspace,
        cass_data_type_set_keyspace_n,
        cass_data_type_set_type_name,
        cass_data_type_set_type_name_n,
        cass_data_type_sub_data_type,
        cass_data_type_sub_data_type_by_name,
        cass_data_type_sub_data_type_by_name_n,
        cass_data_type_sub_type_count,
        cass_data_type_sub_type_name,
        cass_data_type_type,
        cass_data_type_type_name,
    };
}

pub mod collection {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::collection::{
        CassCollection,
        cass_collection_append_bool,
        cass_collection_append_bytes,
        cass_collection_append_collection,
        // cass_collection_append_custom, UNIMPLEMENTED
        // cass_collection_append_custom_n, UNIMPLEMENTED
        cass_collection_append_decimal,
        cass_collection_append_duration,
        cass_collection_append_double,
        cass_collection_append_float,
        cass_collection_append_inet,
        cass_collection_append_int8,
        cass_collection_append_int16,
        cass_collection_append_int32,
        cass_collection_append_int64,
        cass_collection_append_string,
        cass_collection_append_string_n,
        cass_collection_append_tuple,
        cass_collection_append_uint32,
        cass_collection_append_user_type,
        cass_collection_append_uuid,
        cass_collection_data_type,
        cass_collection_free,
        cass_collection_new,
        cass_collection_new_from_data_type,
    };
}

pub mod tuple {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::tuple::{
        CassTuple,
        cass_tuple_data_type,
        cass_tuple_free,
        cass_tuple_new,
        cass_tuple_new_from_data_type,
        cass_tuple_set_bool,
        cass_tuple_set_bytes,
        cass_tuple_set_collection,
        // cass_tuple_set_custom, UNIMPLEMENTED
        // cass_tuple_set_custom_n, UNIMPLEMENTED
        cass_tuple_set_decimal,
        cass_tuple_set_duration,
        cass_tuple_set_double,
        cass_tuple_set_float,
        cass_tuple_set_inet,
        cass_tuple_set_int8,
        cass_tuple_set_int16,
        cass_tuple_set_int32,
        cass_tuple_set_int64,
        cass_tuple_set_null,
        cass_tuple_set_string,
        cass_tuple_set_string_n,
        cass_tuple_set_tuple,
        cass_tuple_set_uint32,
        cass_tuple_set_user_type,
        cass_tuple_set_uuid,
    };
}

pub mod user_type {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::user_type::{
        CassUserType,
        cass_user_type_data_type,
        cass_user_type_free,
        cass_user_type_new_from_data_type,
        cass_user_type_set_bool,
        cass_user_type_set_bool_by_name,
        cass_user_type_set_bool_by_name_n,
        cass_user_type_set_bytes,
        cass_user_type_set_bytes_by_name,
        cass_user_type_set_bytes_by_name_n,
        cass_user_type_set_collection,
        cass_user_type_set_collection_by_name,
        cass_user_type_set_collection_by_name_n,
        // cass_user_type_set_custom, UNIMPLEMENTED
        // cass_user_type_set_custom_by_name, UNIMPLEMENTED
        // cass_user_type_set_custom_by_name_n, UNIMPLEMENTED
        // cass_user_type_set_custom_n, UNIMPLEMENTED
        cass_user_type_set_decimal,
        cass_user_type_set_decimal_by_name,
        cass_user_type_set_decimal_by_name_n,
        cass_user_type_set_duration,
        cass_user_type_set_duration_by_name,
        cass_user_type_set_duration_by_name_n,
        cass_user_type_set_double,
        cass_user_type_set_double_by_name,
        cass_user_type_set_double_by_name_n,
        cass_user_type_set_float,
        cass_user_type_set_float_by_name,
        cass_user_type_set_float_by_name_n,
        cass_user_type_set_inet,
        cass_user_type_set_inet_by_name,
        cass_user_type_set_inet_by_name_n,
        cass_user_type_set_int8,
        cass_user_type_set_int8_by_name,
        cass_user_type_set_int8_by_name_n,
        cass_user_type_set_int16,
        cass_user_type_set_int16_by_name,
        cass_user_type_set_int16_by_name_n,
        cass_user_type_set_int32,
        cass_user_type_set_int32_by_name,
        cass_user_type_set_int32_by_name_n,
        cass_user_type_set_int64,
        cass_user_type_set_int64_by_name,
        cass_user_type_set_int64_by_name_n,
        cass_user_type_set_null,
        cass_user_type_set_null_by_name,
        cass_user_type_set_null_by_name_n,
        cass_user_type_set_string,
        cass_user_type_set_string_by_name,
        cass_user_type_set_string_by_name_n,
        cass_user_type_set_string_n,
        cass_user_type_set_tuple,
        cass_user_type_set_tuple_by_name,
        cass_user_type_set_tuple_by_name_n,
        cass_user_type_set_uint32,
        cass_user_type_set_uint32_by_name,
        cass_user_type_set_uint32_by_name_n,
        cass_user_type_set_user_type,
        cass_user_type_set_user_type_by_name,
        cass_user_type_set_user_type_by_name_n,
        cass_user_type_set_uuid,
        cass_user_type_set_uuid_by_name,
        cass_user_type_set_uuid_by_name_n,
    };
}

pub mod result {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::query_result::{
        CassResult,
        cass_result_column_count,
        cass_result_column_data_type,
        cass_result_column_name,
        cass_result_column_type,
        cass_result_first_row,
        cass_result_free,
        cass_result_has_more_pages,
        cass_result_paging_state_token,
        cass_result_row_count,
    };
}

pub mod error {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::execution_error::{
        CassError,
        CassErrorResult,
        CassErrorSource,
        cass_error_desc,
        cass_error_num_arg_types,
        cass_error_result_arg_type,
        cass_error_result_code,
        cass_error_result_consistency,
        cass_error_result_data_present,
        cass_error_result_free,
        cass_error_result_function,
        cass_error_result_keyspace,
        cass_error_result_num_failures,
        cass_error_result_responses_received,
        cass_error_result_responses_required,
        cass_error_result_table,
        cass_error_result_write_type,
    };
}

pub mod iterator {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::iterator::{
        CassIterator,
        cass_iterator_fields_from_user_type,
        cass_iterator_free,
        cass_iterator_from_collection,
        cass_iterator_from_map,
        cass_iterator_from_result,
        cass_iterator_from_row,
        cass_iterator_from_tuple,
        // cass_iterator_aggregates_from_keyspace_meta, UNIMPLEMENTED
        cass_iterator_columns_from_materialized_view_meta,
        cass_iterator_columns_from_table_meta,
        // cass_iterator_functions_from_keyspace_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_keyspace_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_table_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_materialized_view_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_column_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_index_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_function_meta, UNIMPLEMENTED
        // cass_iterator_fields_from_aggregate_meta, UNIMPLEMENTED
        // cass_iterator_get_aggregate_meta, UNIMPLEMENTED
        cass_iterator_get_column,
        cass_iterator_get_column_meta,
        // cass_iterator_get_function_meta, UNIMPLEMENTED
        // cass_iterator_get_index_meta, UNIMPLEMENTED
        // cass_iterator_get_meta_field_name, UNIMPLEMENTED
        cass_iterator_get_map_key,
        cass_iterator_get_map_value,
        cass_iterator_get_materialized_view_meta,
        cass_iterator_get_user_type_field_name,
        cass_iterator_get_user_type_field_value,
        cass_iterator_get_keyspace_meta,
        // cass_iterator_get_meta_field_value, UNIMPLEMENTED
        cass_iterator_get_row,
        cass_iterator_get_table_meta,
        cass_iterator_get_value,
        cass_iterator_get_user_type,
        // cass_iterator_indexes_from_table_meta, UNIMPLEMENTED
        cass_iterator_keyspaces_from_schema_meta,
        cass_iterator_materialized_views_from_keyspace_meta,
        cass_iterator_materialized_views_from_table_meta,
        cass_iterator_next,
        cass_iterator_tables_from_keyspace_meta,
        cass_iterator_type,
        cass_iterator_user_types_from_keyspace_meta,
    };
}

pub mod row {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::query_result::{
        CassRow,
        cass_row_get_column,
        cass_row_get_column_by_name,
        cass_row_get_column_by_name_n,
    };
}

pub mod value {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::query_result::{
        CassValue,
        CassValueType,
        cass_value_data_type,
        cass_value_get_bool,
        cass_value_get_bytes,
        // cass_value_get_custom, UNIMPLEMENTED
        cass_value_get_decimal,
        cass_value_get_duration,
        cass_value_get_double,
        cass_value_get_float,
        cass_value_get_inet,
        cass_value_get_int8,
        cass_value_get_int16,
        cass_value_get_int32,
        cass_value_get_int64,
        cass_value_get_string,
        cass_value_get_uint32,
        cass_value_get_uuid,
        cass_value_is_collection,
        cass_value_is_duration,
        cass_value_is_null,
        cass_value_item_count,
        cass_value_primary_sub_type,
        cass_value_secondary_sub_type,
        cass_value_type,
    };
}

pub mod uuid_gen {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::uuid::{
        CassUuidGen,
        cass_uuid_gen_free,
        cass_uuid_gen_from_time,
        cass_uuid_gen_new,
        cass_uuid_gen_new_with_node,
        cass_uuid_gen_random,
        cass_uuid_gen_time,
    };
}

pub mod uuid {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::uuid::{
        cass_uuid_from_string,
        cass_uuid_from_string_n,
        cass_uuid_min_from_time,
        cass_uuid_max_from_time,
        cass_uuid_timestamp,
        cass_uuid_string,
        cass_uuid_version,
    };
}

pub mod timestamp_gen {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::timestamp_generator::{
        CassTimestampGen,
        cass_timestamp_gen_free,
        cass_timestamp_gen_monotonic_new,
        cass_timestamp_gen_monotonic_new_with_settings,
        cass_timestamp_gen_server_side_new,
    };
}

pub mod retry_policy {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::retry_policy::{
        CassRetryPolicy,
        cass_retry_policy_default_new,
        cass_retry_policy_downgrading_consistency_new,
        cass_retry_policy_fallthrough_new,
        cass_retry_policy_free,
        cass_retry_policy_logging_new,
    };
}

pub mod custom_payload {
    // CassCustomPayload, UNIMPLEMENTED
    // cass_custom_payload_free, UNIMPLEMENTED
    // cass_custom_payload_new, UNIMPLEMENTED, stub present
    // cass_custom_payload_remove, UNIMPLEMENTED
    // cass_custom_payload_remove_n, UNIMPLEMENTED
    // cass_custom_payload_set, UNIMPLEMENTED
    // cass_custom_payload_set_n, UNIMPLEMENTED
}

pub mod consistency {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::misc::{
        CassConsistency,
        cass_consistency_string
    };
}

pub mod write_type {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::misc::{
        CassWriteType,
        cass_write_type_string
    };
}

pub mod log {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::logging::{
        CassLogCallback,
        cass_log_cleanup,
        cass_log_get_callback_and_data,
        cass_log_level_string,
        cass_log_set_callback,
        cass_log_set_level,
        cass_log_set_queue_size,
    };
}

pub mod inet {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::inet::{
        CassInet,
        cass_inet_from_string,
        cass_inet_from_string_n,
        cass_inet_init_v4,
        cass_inet_init_v6,
        cass_inet_string,
    };
}

pub mod date_time {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::date_time::{
        cass_date_from_epoch,
        cass_date_time_to_epoch,
        cass_time_from_epoch
    };
}

pub mod alloc {
    // cass_alloc_set_functions, UNIMPLEMENTED
}

#[cfg(cpp_integration_testing)]
pub mod integration_testing {
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::integration_testing::{
        IgnoringRetryPolicy,
        testing_batch_set_sleeping_history_listener,
        testing_cluster_get_connect_timeout,
        testing_cluster_get_contact_points,
        testing_cluster_get_port,
        testing_free_cstring,
        testing_future_get_attempted_hosts,
        testing_future_get_host,
        testing_retry_policy_ignoring_new,
        testing_statement_set_recording_history_listener,
        testing_statement_set_sleeping_history_listener,
    };

    /// Stubs of functions that must be implemented for the integration tests to compile,
    /// but the proper implementation is not needed for the tests to run,
    /// and at the same time the functions are not yet implemented in the wrapper.
    // Disabling rustfmt to have one item per line for better readability.
    #[rustfmt::skip]
    pub use crate::integration_testing::stubs::{
        CassCustomPayload,
        cass_cluster_set_cloud_secure_connection_bundle_n,
        cass_cluster_set_exponential_reconnect,
        cass_cluster_set_use_randomized_contact_points,
        cass_custom_payload_new,
        cass_future_custom_payload_item,
        cass_future_custom_payload_item_count,
    };
}
