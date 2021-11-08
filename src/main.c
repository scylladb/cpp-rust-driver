#include <stddef.h>
#include <assert.h>
#include <stdio.h>

#include "cassandra.h"

void print_error_result(const CassErrorResult *err) {
    printf("[error_result] code: %d\n", cass_error_result_code(err));
    printf("[error_result] consistency: %d\n", cass_error_result_consistency(err));
    printf("[error_result] responses received: %d\n", cass_error_result_responses_received(err));
    printf("[error_result] responses required: %d\n", cass_error_result_responses_required(err));
    printf("[error_result] num_failures: %d\n", cass_error_result_num_failures(err));
    printf("[error_result] data_present: %d\n", cass_error_result_data_present(err));
    printf("[error_result] write type: %d\n", cass_error_result_write_type(err));
    const char *str;
    size_t len;
    cass_error_result_keyspace(err, &str, &len);
    printf("[error_result] keyspace: %.*s\n", len, str);
    cass_error_result_table(err, &str, &len);
    printf("[error_result] table: %.*s\n", len, str);
    cass_error_result_function(err, &str, &len);
    printf("[error_result] function: %.*s\n", len, str);
    printf("[error_result] num arg types: %d\n", cass_error_num_arg_types(err));
    for(int i = 0; i < cass_error_num_arg_types(err); i++) {
        cass_error_result_arg_type(err, i, &str, &len);
        printf("[error_result] arg %d: %.*s\n", i, len, str);
    }
}

void do_prepared_query(CassSession* session, const char* query_text) {
    CassFuture* prepare_future = cass_session_prepare(session, query_text);
    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    CassStatement* statement = cass_prepared_bind(prepared);
    CassFuture* statement_future = cass_session_execute(session, statement);
    printf("prepared query code: %d\n", cass_future_error_code(statement_future));
    const CassErrorResult *err = cass_future_get_error_result(statement_future);
    if(err != NULL) {
        print_error_result(err);
    }
    cass_error_result_free(err);
    cass_future_free(statement_future);
    cass_future_free(prepare_future);
    cass_prepared_free(prepared);
    cass_statement_free(statement);
}

void do_simple_query(CassSession* session, const char* query_text) {
    CassStatement* statement = cass_statement_new(query_text, 0);
    cass_statement_set_tracing(statement, 1);

    CassFuture* statement_future = cass_session_execute(session, statement);
    printf("simple query ready: %d\n", cass_future_ready(statement_future));
    const char *msg;
    size_t len;
    cass_future_error_message(statement_future, &msg, &len);
    const CassErrorResult *err = cass_future_get_error_result(statement_future);
    if(err != NULL) {
        print_error_result(err);
    }
    cass_error_result_free(err);
    printf("simple query code: %d, message: %.*s\n", cass_future_error_code(statement_future), len, msg);
    printf("simple query ready: %d\n", cass_future_ready(statement_future));
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

    // Create already existing table
    do_simple_query(session, "CREATE TABLE ks.t (pk int, ck int, v int, v2 text, primary key (pk, ck))");
    // Some garbage as request
    do_simple_query(session, "asdasdafdsdfguhvcsdrhjgvf");

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
    do_simple_query(session, "CREATE TABLE IF NOT EXISTS ks.t2 (pk int, ck int, v list<int>, v2 map<text, float>, primary key (pk, ck))");

    CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, 3);
    cass_collection_append_int32(list, 123);
    cass_collection_append_int32(list, 456);
    cass_collection_append_int32(list, 789);

    CassCollection* map = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 2);
    cass_collection_append_string(map, "k1");
    cass_collection_append_float(map, 10.0);
    cass_collection_append_string(map, "k2");
    cass_collection_append_float(map, 20.0);

    CassStatement* collection_statement = cass_statement_new("INSERT INTO ks.t2(pk, ck, v, v2) VALUES (?, ?, ?, ?)", 4);
    cass_statement_bind_int32(collection_statement, 0, 1);
    cass_statement_bind_int32(collection_statement, 1, 2);
    cass_statement_bind_collection(collection_statement, 2, list);
    cass_statement_bind_collection(collection_statement, 3, map);
    cass_collection_free(list);
    cass_collection_free(map);

    CassFuture* collection_statement_future = cass_session_execute(session, collection_statement);
    cass_future_set_callback(collection_statement_future, print_error_cb, NULL);
    cass_future_wait(collection_statement_future);
    cass_future_free(collection_statement_future);
    cass_statement_free(collection_statement);

    CassStatement* batch_statement = cass_statement_new("INSERT INTO ks.t(pk, ck, v, v2) VALUES (?, ?, ?, ?)", 4);
    cass_statement_bind_int32(batch_statement, 0, 900);
    cass_statement_bind_int32(batch_statement, 1, 800);
    cass_statement_bind_int32(batch_statement, 2, 700);

    CassFuture* prepare_future = cass_session_prepare(session, "INSERT INTO ks.t(pk, ck, v, v2) VALUES (?, ?, ?, ?)");
    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    CassStatement* batch_prepared_statement = cass_prepared_bind(prepared);
    cass_statement_bind_int32(batch_prepared_statement, 0, 321);
    cass_statement_bind_int32(batch_prepared_statement, 1, 654);
    cass_statement_bind_int32(batch_prepared_statement, 2, 987);

    CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    cass_batch_add_statement(batch, batch_prepared_statement);
    cass_batch_add_statement(batch, batch_statement);

    CassFuture* batch_execute_future = cass_session_execute_batch(session, batch);
    const CassErrorResult *err = cass_future_get_error_result(batch_execute_future);
    if (err != NULL) {
        print_error_result(err);
    }
    cass_error_result_free(err);
    cass_future_free(batch_execute_future);

    cass_batch_free(batch);
    cass_statement_free(batch_prepared_statement);
    cass_statement_free(batch_statement);
    cass_prepared_free(prepared);

    CassStatement* select_statement = cass_statement_new("SELECT pk, ck, v, v2 FROM ks.t", 0);
    CassFuture* select_future = cass_session_execute(session, select_statement);
    printf("select code: %d\n", cass_future_error_code(select_future));
    
    const CassResult* select_result = cass_future_get_result(select_future);
    CassIterator* res_iterator = cass_iterator_from_result(select_result);
    while (cass_iterator_next(res_iterator)) {
        const CassRow* row = cass_iterator_get_row(res_iterator);

        int32_t pk, ck, v;
        const char *s;
        size_t s_len;
        cass_value_get_int32(cass_row_get_column(row, 0), &pk);
        cass_value_get_int32(cass_row_get_column(row, 1), &ck);
        cass_value_get_int32(cass_row_get_column(row, 2), &v);
        cass_value_get_string(cass_row_get_column(row, 3), &s, &s_len);
        printf("pk: %d, ck: %d, v: %d, v2: %.*s\n", pk, ck, v, s_len, s);
    }

    cass_iterator_free(res_iterator);
    cass_result_free(select_result);
    cass_future_free(select_future);
    cass_statement_free(select_statement);

    CassStatement* select_paged_statement = cass_statement_new("SELECT pk, ck, v FROM ks.t", 0);
    cass_statement_set_paging_size(select_paged_statement, 1);

    puts("");

    cass_bool_t has_more_pages = cass_true;
    while (has_more_pages) {
        CassFuture* page_future = cass_session_execute(session, select_paged_statement);

        const CassResult* page_result = cass_future_get_result(page_future);

        if (page_result == NULL) {
            puts("Error!");
            return 1;
        }

        CassIterator* page_iterator = cass_iterator_from_result(page_result);
        while (cass_iterator_next(page_iterator)) {
            const CassRow* row = cass_iterator_get_row(page_iterator);

            int32_t pk, ck, v;
            cass_value_get_int32(cass_row_get_column(row, 0), &pk);
            cass_value_get_int32(cass_row_get_column(row, 1), &ck);
            cass_value_get_int32(cass_row_get_column(row, 2), &v);
            printf("pk: %d, ck: %d, v: %d\n", pk, ck, v);
        }

        puts("[PAGE END]");

        has_more_pages = cass_result_has_more_pages(page_result);

        if (has_more_pages) {
            cass_statement_set_paging_state(select_paged_statement, page_result);
        }

        cass_result_free(page_result);
        cass_future_free(page_future);
    }

    cass_statement_free(select_paged_statement);

    cass_cluster_free(cluster);
    cass_session_free(session);
}
