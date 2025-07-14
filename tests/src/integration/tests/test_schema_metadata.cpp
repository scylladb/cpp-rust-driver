/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "cassandra.h"
#include "integration.hpp"

// Name of materialized view used in this test file.
#define VIEW_NAME "my_view"

class SchemaMetadataTest : public Integration {
public:
  SchemaMetadataTest() {
    is_schema_metadata_ = true;
    // Materialized views do not work with tablets.
    disable_tablets_ = true;
  }

  void SetUp() {
    SKIP_IF_CASSANDRA_VERSION_LT(2.2.0);
    Integration::SetUp();
    populateSchema();
    schema_meta_ = session_.schema();
  }

  void populateSchema() {
    session_.execute(format_string("CREATE TABLE %s (key text, value bigint, "
                                   "PRIMARY KEY (key))",
                                   table_name_.c_str()));

/*
 * Support for UDF should be manually enabled to successfully execute the below code.
 * These tests are also disabled for C++ driver. Additionally, Scylla does no support Java language in UDFs.
 * It seems that the created aggregate and functions are not checked in these tests, so currently it is commented out.
 *
    session_.execute("CREATE FUNCTION avg_state(state tuple<int, bigint>, val int) "
                     "CALLED ON NULL INPUT RETURNS tuple<int, bigint> "
                     "LANGUAGE java AS "
                     "  'if (val != null) { "
                     "    state.setInt(0, state.getInt(0) + 1); "
                     "    state.setLong(1, state.getLong(1) + val.intValue()); "
                     "  } ;"
                     "  return state;'"
                     ";");
    session_.execute("CREATE FUNCTION avg_final (state tuple<int, bigint>) "
                     "CALLED ON NULL INPUT RETURNS double "
                     "LANGUAGE java AS "
                     "  'double r = 0; "
                     "  if (state.getInt(0) == 0) return null; "
                     "  r = state.getLong(1); "
                     "  r /= state.getInt(0); "
                     "  return Double.valueOf(r);' "
                     ";");

    session_.execute("CREATE AGGREGATE average(int) "
                     "SFUNC avg_state STYPE tuple<int, bigint> FINALFUNC avg_final "
                     "INITCOND(0, 0);");
*/

    if (server_version_ >= "3.0.0") {
      session_.execute(format_string("CREATE MATERIALIZED VIEW %s "
                                     "AS SELECT value, key "
                                     "   FROM %s"
                                     "   WHERE value IS NOT NULL and key IS NOT NULL "
                                     "PRIMARY KEY(value, key)",
                                     VIEW_NAME, table_name_.c_str()));
    }
    session_.execute("CREATE TYPE address (street text, city text)");
/*
 * The below part is commented out as the created index is not being checked in these tests.
 *
    session_.execute(
        format_string("CREATE INDEX schema_meta_index ON %s (value)", table_name_.c_str()));
*/
  }

protected:
  Schema schema_meta_;
};

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, Views) {
  SKIP_IF_CASSANDRA_VERSION_LT(3.0.0);
  Keyspace keyspace_meta = schema_meta_.keyspace(keyspace_name_);
  Table table_meta = keyspace_meta.table(table_name_);

  // Verify that the view exists in the keyspace and table.
  const CassMaterializedViewMeta* view_from_keyspace =
      cass_keyspace_meta_materialized_view_by_name(keyspace_meta.get(), VIEW_NAME);
  EXPECT_TRUE(view_from_keyspace != NULL);

  // Now from the table, and it should be the same CassMaterializedViewMeta object.
  const CassMaterializedViewMeta* view_from_table =
      cass_table_meta_materialized_view_by_name(table_meta.get(), VIEW_NAME);
  EXPECT_EQ(view_from_keyspace, view_from_table);

  // Verify that the view's back-pointer references this table object.
  EXPECT_EQ(table_meta.get(), cass_materialized_view_meta_base_table(view_from_keyspace));

  // Alter the view, which will cause a new event, and make sure the new
  // view object is available in our metadata (in a new schema snapshot).

  session_.execute(format_string("ALTER MATERIALIZED VIEW %s "
                                 "WITH comment = 'my view rocks'",
                                 VIEW_NAME));

  Schema new_schema = session_.schema();
  Keyspace new_keyspace_meta = new_schema.keyspace(keyspace_name_);

  const CassMaterializedViewMeta* updated_view =
      cass_keyspace_meta_materialized_view_by_name(new_keyspace_meta.get(), VIEW_NAME);
  EXPECT_NE(updated_view, view_from_keyspace);
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, DropView) {
  SKIP_IF_CASSANDRA_VERSION_LT(3.0.0);
  Table table_meta = schema_meta_.keyspace(keyspace_name_).table(table_name_);

  // Verify that the table contains the view
  EXPECT_TRUE(cass_table_meta_materialized_view_by_name(table_meta.get(), VIEW_NAME) != NULL);

  session_.execute(format_string("DROP MATERIALIZED VIEW %s", VIEW_NAME));

  Schema new_schema = session_.schema();
  Table new_table_meta = new_schema.keyspace(keyspace_name_).table(table_name_);

  // Verify that the view has been removed from the table
  EXPECT_TRUE(cass_table_meta_materialized_view_by_name(new_table_meta.get(), VIEW_NAME) == NULL);

  // Verify that a new table metadata instance has been created
  EXPECT_NE(table_meta.get(), new_table_meta.get());
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, RegularMetadataNotMarkedVirtual) {
  SKIP_IF_CASSANDRA_VERSION_LT(2.2.0);
  // Check non-virtual keyspace/table is correctly not set
  Keyspace keyspace_meta = schema_meta_.keyspace("system");
  ASSERT_TRUE(keyspace_meta);
  EXPECT_FALSE(keyspace_meta.is_virtual());

  Table table_meta = keyspace_meta.table("peers");
  ASSERT_TRUE(table_meta);
  EXPECT_FALSE(table_meta.is_virtual());
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, VirtualMetadata) {
  SKIP_IF_CASSANDRA_VERSION_LT(4.0.0);

  // Check virtual keyspace/table is correctly set
  Keyspace keyspace_meta = schema_meta_.keyspace("system_views");
  ASSERT_TRUE(keyspace_meta);
  EXPECT_TRUE(keyspace_meta.is_virtual());

  Table table_meta = keyspace_meta.table("sstable_tasks");
  ASSERT_TRUE(table_meta);
  EXPECT_TRUE(table_meta.is_virtual());

  // Verify virtual table's metadata
  EXPECT_EQ(cass_table_meta_column_count(table_meta.get()), 7u);
  EXPECT_EQ(cass_table_meta_index_count(table_meta.get()), 0u);
  EXPECT_EQ(cass_table_meta_materialized_view_count(table_meta.get()), 0u);

  EXPECT_EQ(cass_table_meta_partition_key_count(table_meta.get()), 1u);
  EXPECT_EQ(cass_table_meta_clustering_key_count(table_meta.get()), 2u);

  EXPECT_EQ(cass_table_meta_clustering_key_order(table_meta.get(), 0), CASS_CLUSTERING_ORDER_ASC);
  EXPECT_EQ(cass_table_meta_clustering_key_order(table_meta.get(), 1), CASS_CLUSTERING_ORDER_ASC);

  const CassColumnMeta* column_meta;

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "keyspace_name");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "table_name");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "task_id");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_UUID);

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "kind");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "progress");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_BIGINT);

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "total");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_BIGINT);

  column_meta = cass_table_meta_column_by_name(table_meta.get(), "unit");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, KeyspaceMetadata) {
  const CassSchemaMeta* schema_meta = session_.schema_meta();

  // Schema Metadata
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace_name_.c_str());
  ASSERT_TRUE(keyspace_meta);

  // Keyspace Metadata
  const char* keyspace_name;
  size_t keyspace_name_length;
  cass_keyspace_meta_name(keyspace_meta, &keyspace_name, &keyspace_name_length);
  std::string keyspace_meta_name(keyspace_name, keyspace_name_length);
  ASSERT_EQ(keyspace_meta_name, keyspace_name_);

  // User Type Metadata
  const CassDataType* user_type_meta = cass_keyspace_meta_user_type_by_name(keyspace_meta, "address");
  const char* user_type_name;
  size_t user_type_name_length;
  cass_data_type_type_name(user_type_meta, &user_type_name, &user_type_name_length);
  std::string user_type_meta_name(user_type_name, user_type_name_length);
  ASSERT_EQ(user_type_meta_name, "address");

  const CassDataType* user_type_field1 = cass_data_type_sub_data_type_by_name(user_type_meta, "street");
  ASSERT_EQ(cass_data_type_type(user_type_field1), CASS_VALUE_TYPE_VARCHAR);

  const CassDataType* user_type_field2 = cass_data_type_sub_data_type_by_name(user_type_meta, "city");
  ASSERT_EQ(cass_data_type_type(user_type_field2), CASS_VALUE_TYPE_VARCHAR);

  // Table Metadata
  const CassTableMeta* table_meta = cass_keyspace_meta_table_by_name(keyspace_meta, table_name_.c_str());
  ASSERT_TRUE(table_meta);

  EXPECT_EQ(cass_table_meta_column_count(table_meta), 2u);

  EXPECT_EQ(cass_table_meta_partition_key_count(table_meta), 1u);
  EXPECT_EQ(cass_table_meta_clustering_key_count(table_meta), 0u);

  // Column Metadata
  const CassColumnMeta* column_meta;
  column_meta = cass_table_meta_column_by_name(table_meta, "key");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);

  column_meta = cass_table_meta_column_by_name(table_meta, "value");
  ASSERT_TRUE(column_meta);
  EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_BIGINT);

  cass_schema_meta_free(schema_meta);
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, TableMetadataColumnOrder) {
  // The order should be: d, a, j, h, i, b, c, f, g
  // First pks by position: d, a, j
  // Then cks by position: h, i
  // Then remaining columns alphabetically: b, c, f, g

  session_.execute(format_string("CREATE TABLE %s "
    "(i int, f int, g int STATIC, b int, c int STATIC, a int, d int, j int, h int, "
    "PRIMARY KEY( (d, a, j), h, i ) )", "column_order_test"));

  const CassSchemaMeta* schema_meta = session_.schema_meta();

  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace_name_.c_str());
  ASSERT_TRUE(keyspace_meta);

  const CassTableMeta* table_meta = cass_keyspace_meta_table_by_name(keyspace_meta, "column_order_test");
  ASSERT_TRUE(table_meta);

  ASSERT_EQ(cass_table_meta_column_count(table_meta), 9u);

  ASSERT_EQ(cass_table_meta_partition_key_count(table_meta), 3u);
  ASSERT_EQ(cass_table_meta_clustering_key_count(table_meta), 2u);

  auto check_column = [&](size_t index, const char* name) {
    const CassColumnMeta* column_meta;
    const char* column_meta_name;
    size_t column_meta_name_length;

    column_meta = cass_table_meta_column(table_meta, index);
    ASSERT_TRUE(column_meta);
    cass_column_meta_name(column_meta, &column_meta_name, &column_meta_name_length);
    ASSERT_EQ(std::string(column_meta_name, column_meta_name_length), name);
  };

  check_column(0, "d");
  check_column(1, "a");
  check_column(2, "j");
  check_column(3, "h");
  check_column(4, "i");
  check_column(5, "b");
  check_column(6, "c");
  check_column(7, "f");
  check_column(8, "g");

  cass_schema_meta_free(schema_meta);
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, MetadataIterator) {
  const CassSchemaMeta* schema_meta = session_.schema_meta();

  // Schema Metadata
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, keyspace_name_.c_str());
  ASSERT_TRUE(keyspace_meta);

  // Keyspace Metadata
  const char* keyspace_name;
  size_t keyspace_name_length;
  cass_keyspace_meta_name(keyspace_meta, &keyspace_name, &keyspace_name_length);
  std::string keyspace_meta_name(keyspace_name, keyspace_name_length);
  ASSERT_EQ(keyspace_meta_name, keyspace_name_);

  // User Type Metadata
  CassIterator* keyspace_user_types_iterator = cass_iterator_user_types_from_keyspace_meta(keyspace_meta);
  cass_iterator_next(keyspace_user_types_iterator);
  const CassDataType* user_type_meta = cass_iterator_get_user_type(keyspace_user_types_iterator);
  const char* user_type_name;
  size_t user_type_name_length;
  cass_data_type_type_name(user_type_meta, &user_type_name, &user_type_name_length);
  std::string user_type_meta_name(user_type_name, user_type_name_length);
  ASSERT_EQ(user_type_meta_name, "address");

  const CassDataType* user_type_field1 = cass_data_type_sub_data_type_by_name(user_type_meta, "street");
  ASSERT_EQ(cass_data_type_type(user_type_field1), CASS_VALUE_TYPE_VARCHAR);

  const CassDataType* user_type_field2 = cass_data_type_sub_data_type_by_name(user_type_meta, "city");
  ASSERT_EQ(cass_data_type_type(user_type_field2), CASS_VALUE_TYPE_VARCHAR);

  ASSERT_FALSE(cass_iterator_next(keyspace_user_types_iterator));

  cass_iterator_free(keyspace_user_types_iterator);

  // Table Metadata
  CassIterator* keyspace_tables_iterator = cass_iterator_tables_from_keyspace_meta(keyspace_meta);
  cass_iterator_next(keyspace_tables_iterator);
  const CassTableMeta* table_meta = cass_iterator_get_table_meta(keyspace_tables_iterator);
  ASSERT_TRUE(table_meta);

  EXPECT_EQ(cass_table_meta_column_count(table_meta), 2u);

  EXPECT_EQ(cass_table_meta_partition_key_count(table_meta), 1u);
  EXPECT_EQ(cass_table_meta_clustering_key_count(table_meta), 0u);

  ASSERT_FALSE(cass_iterator_next(keyspace_tables_iterator));

  cass_iterator_free(keyspace_tables_iterator);

  // Column Metadata
  CassIterator* table_columns_iterator = cass_iterator_columns_from_table_meta(table_meta);
  const CassColumnMeta* column_meta;
  const char* column_meta_name;
  size_t column_meta_name_length;

  cass_iterator_next(table_columns_iterator);
  column_meta = cass_iterator_get_column_meta(table_columns_iterator);
  ASSERT_TRUE(column_meta);
  cass_column_meta_name(column_meta, &column_meta_name, &column_meta_name_length);
  std::string column1_name(column_meta_name, column_meta_name_length);

  if (column1_name == "key") {
    EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);
  } else if (column1_name == "value") {
    EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_BIGINT);
  }

  cass_iterator_next(table_columns_iterator);
  column_meta = cass_iterator_get_column_meta(table_columns_iterator);
  ASSERT_TRUE(column_meta);
  cass_column_meta_name(column_meta, &column_meta_name, &column_meta_name_length);
  std::string column2_name(column_meta_name, column_meta_name_length);

  if (column2_name == "key") {
    EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_VARCHAR);
  } else if (column2_name == "value") {
    EXPECT_EQ(cass_data_type_type(cass_column_meta_data_type(column_meta)), CASS_VALUE_TYPE_BIGINT);
  }

  ASSERT_FALSE(cass_iterator_next(table_columns_iterator));

  cass_iterator_free(table_columns_iterator);

  // Materialized View Metadata
  CassIterator* keyspace_views_iterator = cass_iterator_materialized_views_from_keyspace_meta(keyspace_meta);
  cass_iterator_next(keyspace_views_iterator);
  const CassMaterializedViewMeta* view_meta = cass_iterator_get_materialized_view_meta(keyspace_views_iterator);
  ASSERT_TRUE(view_meta);

  EXPECT_EQ(cass_materialized_view_meta_column_count(view_meta), 2u);

  EXPECT_EQ(cass_materialized_view_meta_partition_key_count(view_meta), 1u);
  EXPECT_EQ(cass_materialized_view_meta_clustering_key_count(view_meta), 1u);

  ASSERT_FALSE(cass_iterator_next(keyspace_views_iterator));

  cass_iterator_free(keyspace_views_iterator);

  cass_schema_meta_free(schema_meta);
}
