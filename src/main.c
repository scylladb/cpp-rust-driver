#include <stddef.h>
#include <assert.h>
#include <stdio.h>

#include "cassandra.h"

void do_prepared_query(CassSession* session, const char* query_text) {
    CassFuture* prepare_future = cass_session_prepare(session, query_text);
    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    CassStatement* statement = cass_prepared_bind(prepared);
    CassFuture* statement_future = cass_session_execute(session, statement);
    printf("prepared query code: %d\n", cass_future_error_code(statement_future));
    cass_future_free(statement_future);
    cass_future_free(prepare_future);
    cass_prepared_free(prepared);
    cass_statement_free(statement);
}

void do_simple_query(CassSession* session, const char* query_text) {
    CassStatement* statement = cass_statement_new(query_text, 0);
    cass_statement_set_tracing(statement, 1);

    CassFuture* statement_future = cass_session_execute(session, statement);
    printf("simple query code: %d\n", cass_future_error_code(statement_future));
    cass_future_free(statement_future);
    cass_statement_free(statement);
}

static void print_error_cb(CassFuture* future, void* data) {
    printf("code: %d\n", cass_future_error_code(future));
}

int main() {
    CassFuture* connect_future = NULL;
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    cass_cluster_set_contact_points(cluster, "127.0.1.1");
    cass_cluster_set_load_balance_round_robin(cluster);
    cass_cluster_set_token_aware_routing(cluster, 1);
    connect_future = cass_session_connect(session, cluster);
    cass_future_set_callback(connect_future, print_error_cb, NULL);
    cass_future_wait(connect_future);
    cass_future_free(connect_future);

    do_simple_query(session, "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    do_simple_query(session, "DROP TABLE IF EXISTS ks.t");
    do_simple_query(session, "CREATE TABLE IF NOT EXISTS ks.t (pk int, ck int, v int, v2 text, primary key (pk, ck))");
    do_simple_query(session, "INSERT INTO ks.t(pk, ck, v, v2) VALUES (7, 8, 9, 'hello world')");
    do_prepared_query(session, "INSERT INTO ks.t(pk, ck, v, v2) VALUES (69, 69, 69, 'greetings from Rust!')");

    CassStatement* statement = cass_statement_new("INSERT INTO ks.t(pk, ck, v, v2) VALUES (?, ?, ?, ?)", 4);
    cass_statement_bind_int32(statement, 0, 100);
    cass_statement_bind_int32(statement, 1, 200);
    cass_statement_bind_int32(statement, 2, 300);
    cass_statement_bind_string(statement, 3, "We love Rust!");

    CassFuture* statement_future = cass_session_execute(session, statement);
    cass_future_set_callback(statement_future, print_error_cb, NULL);
    cass_future_wait(statement_future);
    cass_future_free(statement_future);
    cass_statement_free(statement);

    do_simple_query(session, "DROP TABLE IF EXISTS ks.t2");
    do_simple_query(session, "DROP TYPE IF EXISTS ks.my_type");
    do_simple_query(session, "CREATE TYPE IF NOT EXISTS ks.my_type(c text, a int, b float)");
    do_simple_query(session, "CREATE TABLE IF NOT EXISTS ks.t2 (pk int, ck int, v list<int>, v2 map<text, float>, v3 my_type, primary key (pk, ck))");

    CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, 3);
    cass_collection_append_int32(list, 123);
    cass_collection_append_int32(list, 456);
    cass_collection_append_int32(list, 789);

    CassCollection* map = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 2);
    cass_collection_append_string(map, "k1");
    cass_collection_append_float(map, 10.0);
    cass_collection_append_string(map, "k2");
    cass_collection_append_float(map, 20.0);

    CassDataType* udt_type = cass_data_type_new_udt(3);
    cass_data_type_add_sub_value_type_by_name(udt_type, "c", CASS_VALUE_TYPE_TEXT);
    cass_data_type_add_sub_value_type_by_name(udt_type, "a", CASS_VALUE_TYPE_INT);
    cass_data_type_add_sub_value_type_by_name(udt_type, "b", CASS_VALUE_TYPE_FLOAT);

    CassUserType* user_type = cass_user_type_new_from_data_type(udt_type);
    cass_data_type_free(udt_type);
    cass_user_type_set_string_by_name(user_type, "c", "UDT!");
    cass_user_type_set_int32_by_name(user_type, "a", 15);
    cass_user_type_set_float_by_name(user_type, "b", 3.14);

    CassStatement* collection_statement = cass_statement_new("INSERT INTO ks.t2(pk, ck, v, v2, v3) VALUES (?, ?, ?, ?, ?)", 5);
    cass_statement_bind_int32(collection_statement, 0, 1);
    cass_statement_bind_int32(collection_statement, 1, 2);
    cass_statement_bind_collection(collection_statement, 2, list);
    cass_statement_bind_collection(collection_statement, 3, map);
    cass_statement_bind_user_type(collection_statement, 4, user_type);
    cass_collection_free(list);
    cass_collection_free(map);
    cass_user_type_free(user_type);

    CassFuture* collection_statement_future = cass_session_execute(session, collection_statement);
    cass_future_set_callback(collection_statement_future, print_error_cb, NULL);
    cass_future_wait(collection_statement_future);
    cass_future_free(collection_statement_future);
    cass_statement_free(collection_statement);

    CassStatement* select_statement = cass_statement_new("SELECT pk, ck, v FROM ks.t", 0);
    CassFuture* select_future = cass_session_execute(session, select_statement);
    printf("select code: %d\n", cass_future_error_code(select_future));
    
    const CassResult* select_result = cass_future_get_result(select_future);
    CassIterator* res_iterator = cass_iterator_from_result(select_result);
    while (cass_iterator_next(res_iterator)) {
        const CassRow* row = cass_iterator_get_row(res_iterator);

        int32_t pk, ck, v;
        cass_value_get_int32(cass_row_get_column(row, 0), &pk);
        cass_value_get_int32(cass_row_get_column(row, 1), &ck);
        cass_value_get_int32(cass_row_get_column(row, 2), &v);
        printf("pk: %d, ck: %d, v: %d\n", pk, ck, v);
    }

    cass_iterator_free(res_iterator);
    cass_result_free(select_result);
    cass_future_free(select_future);
    cass_statement_free(select_statement);

    cass_cluster_free(cluster);
    cass_session_free(session);
}
