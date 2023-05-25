#include "cassandra.h"
#include <stdexcept>

CASS_EXPORT size_t
cass_aggregate_meta_argument_count(const CassAggregateMeta* aggregate_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_argument_count\n");
}
CASS_EXPORT const CassDataType*
cass_aggregate_meta_argument_type(const CassAggregateMeta* aggregate_meta,
                                  size_t index){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_argument_type\n");
}
CASS_EXPORT const CassValue*
cass_aggregate_meta_field_by_name(const CassAggregateMeta* aggregate_meta,
                                  const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_field_by_name\n");
}
CASS_EXPORT const CassFunctionMeta*
cass_aggregate_meta_final_func(const CassAggregateMeta* aggregate_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_final_func\n");
}
CASS_EXPORT void
cass_aggregate_meta_full_name(const CassAggregateMeta* aggregate_meta,
                              const char** full_name,
                              size_t* full_name_length){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_full_name\n");
}
CASS_EXPORT const CassValue*
cass_aggregate_meta_init_cond(const CassAggregateMeta* aggregate_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_init_cond\n");
}
CASS_EXPORT void
cass_aggregate_meta_name(const CassAggregateMeta* aggregate_meta,
                         const char** name,
                         size_t* name_length){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_name\n");
}
CASS_EXPORT const CassDataType*
cass_aggregate_meta_return_type(const CassAggregateMeta* aggregate_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_return_type\n");
}
CASS_EXPORT const CassFunctionMeta*
cass_aggregate_meta_state_func(const CassAggregateMeta* aggregate_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_state_func\n");
}
CASS_EXPORT const CassDataType*
cass_aggregate_meta_state_type(const CassAggregateMeta* aggregate_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_aggregate_meta_state_type\n");
}
CASS_EXPORT void
cass_authenticator_set_error(CassAuthenticator* auth,
                             const char* message){
	throw std::runtime_error("UNIMPLEMENTED cass_authenticator_set_error\n");
}
CASS_EXPORT CassError
cass_batch_set_execution_profile(CassBatch* batch,
                                 const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_batch_set_execution_profile\n");
}
CASS_EXPORT CassError
cass_batch_set_keyspace(CassBatch* batch,
                        const char* keyspace){
	throw std::runtime_error("UNIMPLEMENTED cass_batch_set_keyspace\n");
}
CASS_EXPORT CassError
cass_cluster_set_authenticator_callbacks(CassCluster* cluster,
                                         const CassAuthenticatorCallbacks* exchange_callbacks,
                                         CassAuthenticatorDataCleanupCallback cleanup_callback,
                                         void* data){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_authenticator_callbacks\n");
}
CASS_EXPORT void
cass_cluster_set_blacklist_dc_filtering(CassCluster* cluster,
                                        const char* dcs){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_blacklist_dc_filtering\n");
}
CASS_EXPORT void
cass_cluster_set_blacklist_filtering(CassCluster* cluster,
                                     const char* hosts){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_blacklist_filtering\n");
}
CASS_EXPORT CassError
cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(CassCluster* cluster,
                                                                const char* path){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init\n");
}
CASS_EXPORT void
cass_cluster_set_connection_heartbeat_interval(CassCluster* cluster,
                                               unsigned interval_secs){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_connection_heartbeat_interval\n");
}
CASS_EXPORT void
cass_cluster_set_connection_idle_timeout(CassCluster* cluster,
                                         unsigned timeout_secs){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_connection_idle_timeout\n");
}
CASS_EXPORT void
cass_cluster_set_constant_reconnect(CassCluster* cluster,
                                    cass_uint64_t delay_ms){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_constant_reconnect\n");
}
CASS_EXPORT CassError
cass_cluster_set_core_connections_per_host(CassCluster* cluster,
                                           unsigned num_connections){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_core_connections_per_host\n");
}
CASS_EXPORT CassError
cass_cluster_set_execution_profile(CassCluster* cluster,
                                   const char* name,
                                   CassExecProfile* profile){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_execution_profile\n");
}
CASS_EXPORT CassError
cass_cluster_set_host_listener_callback(CassCluster* cluster,
                                        CassHostListenerCallback callback,
                                        void* data){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_host_listener_callback\n");
}
CASS_EXPORT CassError
cass_cluster_set_local_address(CassCluster* cluster,
                               const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_local_address\n");
}
CASS_EXPORT CassError
cass_cluster_set_no_compact(CassCluster* cluster,
                            cass_bool_t enabled){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_no_compact\n");
}
CASS_EXPORT CassError
cass_cluster_set_num_threads_io(CassCluster* cluster,
                                unsigned num_threads){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_num_threads_io\n");
}
CASS_EXPORT CassError
cass_cluster_set_prepare_on_all_hosts(CassCluster* cluster,
                                      cass_bool_t enabled){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_prepare_on_all_hosts\n");
}
CASS_EXPORT CassError
cass_cluster_set_prepare_on_up_or_add_host(CassCluster* cluster,
                                           cass_bool_t enabled){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_prepare_on_up_or_add_host\n");
}
CASS_EXPORT void
cass_cluster_set_request_timeout(CassCluster* cluster,
                                 unsigned timeout_ms){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_request_timeout\n");
}
CASS_EXPORT void
cass_cluster_set_retry_policy(CassCluster* cluster,
                              CassRetryPolicy* retry_policy){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_retry_policy\n");
}
CASS_EXPORT void
cass_cluster_set_timestamp_gen(CassCluster* cluster,
                               CassTimestampGen* timestamp_gen){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_timestamp_gen\n");
}
CASS_EXPORT void
cass_cluster_set_whitelist_dc_filtering(CassCluster* cluster,
                                        const char* dcs){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_whitelist_dc_filtering\n");
}
CASS_EXPORT void
cass_cluster_set_whitelist_filtering(CassCluster* cluster,
                                     const char* hosts){
	throw std::runtime_error("UNIMPLEMENTED cass_cluster_set_whitelist_filtering\n");
}
CASS_EXPORT CassError
cass_collection_append_custom(CassCollection* collection,
                              const char* class_name,
                              const cass_byte_t* value,
                              size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_collection_append_custom\n");
}
CASS_EXPORT CassError
cass_collection_append_decimal(CassCollection* collection,
                               const cass_byte_t* varint,
                               size_t varint_size,
                               cass_int32_t scale){
	throw std::runtime_error("UNIMPLEMENTED cass_collection_append_decimal\n");
}
CASS_EXPORT CassError
cass_collection_append_duration(CassCollection* collection,
                                cass_int32_t months,
                                cass_int32_t days,
                                cass_int64_t nanos){
	throw std::runtime_error("UNIMPLEMENTED cass_collection_append_duration\n");
}
CASS_EXPORT const CassValue*
cass_column_meta_field_by_name(const CassColumnMeta* column_meta,
                               const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_column_meta_field_by_name\n");
}
CASS_EXPORT void
cass_custom_payload_free(CassCustomPayload* payload){
	throw std::runtime_error("UNIMPLEMENTED cass_custom_payload_free\n");
}
CASS_EXPORT void
cass_custom_payload_set(CassCustomPayload* payload,
                        const char* name,
                        const cass_byte_t* value,
                        size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_custom_payload_set\n");
}
CASS_EXPORT void
cass_execution_profile_free(CassExecProfile* profile){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_free\n");
}
CASS_EXPORT CassExecProfile*
cass_execution_profile_new(){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_new\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_blacklist_dc_filtering(CassExecProfile* profile,
                                                  const char* dcs){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_blacklist_dc_filtering\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_blacklist_filtering(CassExecProfile* profile,
                                               const char* hosts){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_blacklist_filtering\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_consistency(CassExecProfile* profile,
                                       CassConsistency consistency){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_consistency\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_constant_speculative_execution_policy(CassExecProfile* profile,
                                                                 cass_int64_t constant_delay_ms,
                                                                 int max_speculative_executions){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_constant_speculative_execution_policy\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_latency_aware_routing(CassExecProfile* profile,
                                                 cass_bool_t enabled){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_latency_aware_routing\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_load_balance_dc_aware(CassExecProfile* profile,
                                                 const char* local_dc,
                                                 unsigned used_hosts_per_remote_dc,
                                                 cass_bool_t allow_remote_dcs_for_local_cl){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_load_balance_dc_aware\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_load_balance_round_robin(CassExecProfile* profile){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_load_balance_round_robin\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_request_timeout(CassExecProfile* profile,
                                           cass_uint64_t timeout_ms){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_request_timeout\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_retry_policy(CassExecProfile* profile,
                                        CassRetryPolicy* retry_policy){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_retry_policy\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_serial_consistency(CassExecProfile* profile,
                                              CassConsistency serial_consistency){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_serial_consistency\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_token_aware_routing(CassExecProfile* profile,
                                               cass_bool_t enabled){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_token_aware_routing\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_whitelist_dc_filtering(CassExecProfile* profile,
                                                  const char* dcs){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_whitelist_dc_filtering\n");
}
CASS_EXPORT CassError
cass_execution_profile_set_whitelist_filtering(CassExecProfile* profile,
                                               const char* hosts){
	throw std::runtime_error("UNIMPLEMENTED cass_execution_profile_set_whitelist_filtering\n");
}
CASS_EXPORT CassError
cass_function_meta_argument(const CassFunctionMeta* function_meta,
                            size_t index,
                            const char** name,
                            size_t* name_length,
                            const CassDataType** type){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_argument\n");
}
CASS_EXPORT size_t
cass_function_meta_argument_count(const CassFunctionMeta* function_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_argument_count\n");
}
CASS_EXPORT const CassDataType*
cass_function_meta_argument_type_by_name(const CassFunctionMeta* function_meta,
                                         const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_argument_type_by_name\n");
}
CASS_EXPORT void
cass_function_meta_body(const CassFunctionMeta* function_meta,
                        const char** body,
                        size_t* body_length){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_body\n");
}
CASS_EXPORT cass_bool_t
cass_function_meta_called_on_null_input(const CassFunctionMeta* function_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_called_on_null_input\n");
}
CASS_EXPORT const CassValue*
cass_function_meta_field_by_name(const CassFunctionMeta* function_meta,
                                 const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_field_by_name\n");
}
CASS_EXPORT void
cass_function_meta_full_name(const CassFunctionMeta* function_meta,
                             const char** full_name,
                             size_t* full_name_length){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_full_name\n");
}
CASS_EXPORT void
cass_function_meta_language(const CassFunctionMeta* function_meta,
                            const char** language,
                            size_t* language_length){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_language\n");
}
CASS_EXPORT void
cass_function_meta_name(const CassFunctionMeta* function_meta,
                        const char** name,
                        size_t* name_length){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_name\n");
}
CASS_EXPORT const CassDataType*
cass_function_meta_return_type(const CassFunctionMeta* function_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_function_meta_return_type\n");
}
CASS_EXPORT const CassNode*
cass_future_coordinator(CassFuture* future){
	throw std::runtime_error("UNIMPLEMENTED cass_future_coordinator\n");
}
CASS_EXPORT cass_bool_t
cass_future_wait_timed(CassFuture* future,
                       cass_duration_t timeout_us){
	throw std::runtime_error("UNIMPLEMENTED cass_future_wait_timed\n");
}
CASS_EXPORT const CassValue*
cass_index_meta_field_by_name(const CassIndexMeta* index_meta,
                               const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_index_meta_field_by_name\n");
}
CASS_EXPORT const CassAggregateMeta*
cass_keyspace_meta_aggregate_by_name(const CassKeyspaceMeta* keyspace_meta,
                                     const char* name,
                                     const char* arguments){
	throw std::runtime_error("UNIMPLEMENTED cass_keyspace_meta_aggregate_by_name\n");
}
CASS_EXPORT const CassValue*
cass_keyspace_meta_field_by_name(const CassKeyspaceMeta* keyspace_meta,
                                 const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_keyspace_meta_field_by_name\n");
}
CASS_EXPORT const CassFunctionMeta*
cass_keyspace_meta_function_by_name(const CassKeyspaceMeta* keyspace_meta,
                                    const char* name,
                                    const char* arguments){
	throw std::runtime_error("UNIMPLEMENTED cass_keyspace_meta_function_by_name\n");
}
CASS_EXPORT cass_bool_t
cass_keyspace_meta_is_virtual(const CassKeyspaceMeta* keyspace_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_keyspace_meta_is_virtual\n");
}
CASS_EXPORT const CassValue*
cass_materialized_view_meta_field_by_name(const CassMaterializedViewMeta* view_meta,
                                          const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_materialized_view_meta_field_by_name\n");
}
CASS_EXPORT const CassDataType*
cass_prepared_parameter_data_type_by_name(const CassPrepared* prepared,
                                          const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_prepared_parameter_data_type_by_name\n");
}
CASS_EXPORT CassRetryPolicy*
cass_retry_policy_logging_new(CassRetryPolicy* child_retry_policy){
	throw std::runtime_error("UNIMPLEMENTED cass_retry_policy_logging_new\n");
}
CASS_EXPORT CassVersion
cass_schema_meta_version(const CassSchemaMeta* schema_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_schema_meta_version\n");
}
CASS_EXPORT CassFuture*
cass_session_connect_keyspace(CassSession* session,
                              const CassCluster* cluster,
                              const char* keyspace){
	throw std::runtime_error("UNIMPLEMENTED cass_session_connect_keyspace\n");
}
CASS_EXPORT void
cass_session_get_metrics(const CassSession* session,
                         CassMetrics* output){
	throw std::runtime_error("UNIMPLEMENTED cass_session_get_metrics\n");
}
CASS_EXPORT void
cass_session_get_speculative_execution_metrics(const CassSession* session,
                                               CassSpeculativeExecutionMetrics* output){
	throw std::runtime_error("UNIMPLEMENTED cass_session_get_speculative_execution_metrics\n");
}
CASS_EXPORT CassError
cass_statement_add_key_index(CassStatement* statement,
                             size_t index){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_add_key_index\n");
}
CASS_EXPORT CassError
cass_statement_bind_custom(CassStatement* statement,
                           size_t index,
                           const char* class_name,
                           const cass_byte_t* value,
                           size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_bind_custom\n");
}
CASS_EXPORT CassError
cass_statement_bind_custom_by_name(CassStatement* statement,
                                   const char* name,
                                   const char* class_name,
                                   const cass_byte_t* value,
                                   size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_bind_custom_by_name\n");
}
CASS_EXPORT CassError
cass_statement_bind_decimal(CassStatement* statement,
                            size_t index,
                            const cass_byte_t* varint,
                            size_t varint_size,
                            cass_int32_t scale){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_bind_decimal\n");
}
CASS_EXPORT CassError
cass_statement_bind_decimal_by_name(CassStatement* statement,
                                    const char* name,
                                    const cass_byte_t* varint,
                                    size_t varint_size,
                                    cass_int32_t scale){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_bind_decimal_by_name\n");
}
CASS_EXPORT CassError
cass_statement_bind_duration(CassStatement* statement,
                             size_t index,
                             cass_int32_t months,
                             cass_int32_t days,
                             cass_int64_t nanos){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_bind_duration\n");
}
CASS_EXPORT CassError
cass_statement_bind_duration_by_name(CassStatement* statement,
                                     const char* name,
                                     cass_int32_t months,
                                     cass_int32_t days,
                                     cass_int64_t nanos){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_bind_duration_by_name\n");
}
CASS_EXPORT CassError
cass_statement_set_custom_payload(CassStatement* statement,
                                  const CassCustomPayload* payload){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_set_custom_payload\n");
}
CASS_EXPORT CassError
cass_statement_set_execution_profile(CassStatement* statement,
                                     const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_set_execution_profile\n");
}
CASS_EXPORT CassError
cass_statement_set_host(CassStatement* statement,
                        const char* host,
                        int port){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_set_host\n");
}
CASS_EXPORT CassError
cass_statement_set_host_inet(CassStatement* statement,
                             const CassInet* host,
                             int port){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_set_host_inet\n");
}
CASS_EXPORT CassError
cass_statement_set_keyspace(CassStatement* statement,
                            const char* keyspace){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_set_keyspace\n");
}
CASS_EXPORT CassError
cass_statement_set_node(CassStatement* statement,
                        const CassNode* node){
	throw std::runtime_error("UNIMPLEMENTED cass_statement_set_node\n");
}
CASS_EXPORT CassClusteringOrder
cass_table_meta_clustering_key_order(const CassTableMeta* table_meta,
                                     size_t index){
	throw std::runtime_error("UNIMPLEMENTED cass_table_meta_clustering_key_order\n");
}
CASS_EXPORT const CassValue*
cass_table_meta_field_by_name(const CassTableMeta* table_meta,
                              const char* name){
	throw std::runtime_error("UNIMPLEMENTED cass_table_meta_field_by_name\n");
}
CASS_EXPORT const CassIndexMeta*
cass_table_meta_index_by_name(const CassTableMeta* table_meta,
                               const char* index){
	throw std::runtime_error("UNIMPLEMENTED cass_table_meta_index_by_name\n");
}
CASS_EXPORT size_t
cass_table_meta_index_count(const CassTableMeta* table_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_table_meta_index_count\n");
}
CASS_EXPORT cass_bool_t
cass_table_meta_is_virtual(const CassTableMeta* table_meta){
	throw std::runtime_error("UNIMPLEMENTED cass_table_meta_is_virtual\n");
}
CASS_EXPORT CassError
cass_tuple_set_custom(CassTuple* tuple,
                      size_t index,
                      const char* class_name,
                      const cass_byte_t* value,
                      size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_tuple_set_custom\n");
}
CASS_EXPORT CassError
cass_tuple_set_decimal(CassTuple* tuple,
                       size_t index,
                       const cass_byte_t* varint,
                       size_t varint_size,
                       cass_int32_t scale){
	throw std::runtime_error("UNIMPLEMENTED cass_tuple_set_decimal\n");
}
CASS_EXPORT CassError
cass_tuple_set_duration(CassTuple* tuple,
                        size_t index,
                        cass_int32_t months,
                        cass_int32_t days,
                        cass_int64_t nanos){
	throw std::runtime_error("UNIMPLEMENTED cass_tuple_set_duration\n");
}
CASS_EXPORT CassError
cass_user_type_set_custom(CassUserType* user_type,
                          size_t index,
                          const char* class_name,
                          const cass_byte_t* value,
                          size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_user_type_set_custom\n");
}
CASS_EXPORT CassError
cass_user_type_set_custom_by_name(CassUserType* user_type,
                                  const char* name,
                                  const char* class_name,
                                  const cass_byte_t* value,
                                  size_t value_size){
	throw std::runtime_error("UNIMPLEMENTED cass_user_type_set_custom_by_name\n");
}
CASS_EXPORT CassError
cass_user_type_set_decimal_by_name(CassUserType* user_type,
                                   const char* name,
                                   const cass_byte_t* varint,
                                   size_t varint_size,
                                   int scale){
	throw std::runtime_error("UNIMPLEMENTED cass_user_type_set_decimal_by_name\n");
}
CASS_EXPORT CassError
cass_user_type_set_duration_by_name(CassUserType* user_type,
                                    const char* name,
                                    cass_int32_t months,
                                    cass_int32_t days,
                                    cass_int64_t nanos){
	throw std::runtime_error("UNIMPLEMENTED cass_user_type_set_duration_by_name\n");
}
CASS_EXPORT CassError
cass_value_get_decimal(const CassValue* value,
                       const cass_byte_t** varint,
                       size_t* varint_size,
                       cass_int32_t* scale){
	throw std::runtime_error("UNIMPLEMENTED cass_value_get_decimal\n");
}
CASS_EXPORT CassError
cass_value_get_duration(const CassValue* value,
                        cass_int32_t* months,
                        cass_int32_t* days,
                        cass_int64_t* nanos){
	throw std::runtime_error("UNIMPLEMENTED cass_value_get_duration\n");
}