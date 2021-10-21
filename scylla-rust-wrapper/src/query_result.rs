use crate::cass_error::CassError;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use std::sync::Arc;

pub type CassResult = Arc<scylla::QueryResult>;

pub struct CassIterator {}

pub struct CassRow {}

pub type CassValue = Option<CqlValue>;

pub unsafe extern "C" fn cass_iterator_from_result(
    _result: *const CassResult,
) -> *mut CassIterator {
    unimplemented!();
}

pub unsafe extern "C" fn cass_result_free(_result: *const CassResult) {
    unimplemented!();
}

pub unsafe extern "C" fn cass_iterator_free(_iterator: *mut CassIterator) {
    unimplemented!();
}

pub unsafe extern "C" fn cass_iterator_next(_iterator: *mut CassIterator) -> cass_bool_t {
    unimplemented!();
}

pub unsafe extern "C" fn cass_iterator_get_row(_iterator: *const CassIterator) -> *const CassRow {
    unimplemented!();
}

pub unsafe extern "C" fn cass_row_get_column(
    _row: *const CassRow,
    _index: size_t,
) -> *const CassValue {
    unimplemented!();
}

pub unsafe extern "C" fn cass_value_get_int32(
    _value: *const CassValue,
    _output: *mut cass_int32_t,
) -> CassError {
    unimplemented!();
}

pub unsafe extern "C" fn cass_value_is_null(_value: *const CassValue) -> cass_bool_t {
    unimplemented!();
}

// CassResult functions:
/*
extern "C" {
    pub fn cass_statement_set_paging_state(
        statement: *mut CassStatement,
        result: *const CassResult,
    ) -> CassError;
}
extern "C" {
    pub fn cass_result_row_count(result: *const CassResult) -> size_t;
}
extern "C" {
    pub fn cass_result_column_count(result: *const CassResult) -> size_t;
}
extern "C" {
    pub fn cass_result_column_name(
        result: *const CassResult,
        index: size_t,
        name: *mut *const ::std::os::raw::c_char,
        name_length: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_result_column_type(result: *const CassResult, index: size_t) -> CassValueType;
}
extern "C" {
    pub fn cass_result_column_data_type(
        result: *const CassResult,
        index: size_t,
    ) -> *const CassDataType;
}
extern "C" {
    pub fn cass_result_first_row(result: *const CassResult) -> *const CassRow;
}
extern "C" {
    pub fn cass_result_has_more_pages(result: *const CassResult) -> cass_bool_t;
}
extern "C" {
    pub fn cass_result_paging_state_token(
        result: *const CassResult,
        paging_state: *mut *const ::std::os::raw::c_char,
        paging_state_size: *mut size_t,
    ) -> CassError;
}
*/

// CassIterator functions:
/*
extern "C" {
    pub fn cass_iterator_type(iterator: *mut CassIterator) -> CassIteratorType;
}

extern "C" {
    pub fn cass_iterator_from_row(row: *const CassRow) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_from_collection(value: *const CassValue) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_from_map(value: *const CassValue) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_from_tuple(value: *const CassValue) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_user_type(value: *const CassValue) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_keyspaces_from_schema_meta(
        schema_meta: *const CassSchemaMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_tables_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_materialized_views_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_user_types_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_functions_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_aggregates_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_keyspace_meta(
        keyspace_meta: *const CassKeyspaceMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_columns_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_indexes_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_materialized_views_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_table_meta(
        table_meta: *const CassTableMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_columns_from_materialized_view_meta(
        view_meta: *const CassMaterializedViewMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_materialized_view_meta(
        view_meta: *const CassMaterializedViewMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_column_meta(
        column_meta: *const CassColumnMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_index_meta(
        index_meta: *const CassIndexMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_function_meta(
        function_meta: *const CassFunctionMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_fields_from_aggregate_meta(
        aggregate_meta: *const CassAggregateMeta,
    ) -> *mut CassIterator;
}
extern "C" {
    pub fn cass_iterator_get_column(iterator: *const CassIterator) -> *const CassValue;
}
extern "C" {
    pub fn cass_iterator_get_value(iterator: *const CassIterator) -> *const CassValue;
}
extern "C" {
    pub fn cass_iterator_get_map_key(iterator: *const CassIterator) -> *const CassValue;
}
extern "C" {
    pub fn cass_iterator_get_map_value(iterator: *const CassIterator) -> *const CassValue;
}
extern "C" {
    pub fn cass_iterator_get_user_type_field_name(
        iterator: *const CassIterator,
        name: *mut *const ::std::os::raw::c_char,
        name_length: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_iterator_get_user_type_field_value(
        iterator: *const CassIterator,
    ) -> *const CassValue;
}
extern "C" {
    pub fn cass_iterator_get_keyspace_meta(
        iterator: *const CassIterator,
    ) -> *const CassKeyspaceMeta;
}
extern "C" {
    pub fn cass_iterator_get_table_meta(iterator: *const CassIterator) -> *const CassTableMeta;
}
extern "C" {
    pub fn cass_iterator_get_materialized_view_meta(
        iterator: *const CassIterator,
    ) -> *const CassMaterializedViewMeta;
}
extern "C" {
    pub fn cass_iterator_get_user_type(iterator: *const CassIterator) -> *const CassDataType;
}
extern "C" {
    pub fn cass_iterator_get_function_meta(
        iterator: *const CassIterator,
    ) -> *const CassFunctionMeta;
}
extern "C" {
    pub fn cass_iterator_get_aggregate_meta(
        iterator: *const CassIterator,
    ) -> *const CassAggregateMeta;
}
extern "C" {
    pub fn cass_iterator_get_column_meta(iterator: *const CassIterator) -> *const CassColumnMeta;
}
extern "C" {
    pub fn cass_iterator_get_index_meta(iterator: *const CassIterator) -> *const CassIndexMeta;
}
extern "C" {
    pub fn cass_iterator_get_meta_field_name(
        iterator: *const CassIterator,
        name: *mut *const ::std::os::raw::c_char,
        name_length: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_iterator_get_meta_field_value(iterator: *const CassIterator) -> *const CassValue;
}
*/

// CassRow functions:
/*
extern "C" {
    pub fn cass_row_get_column_by_name(
        row: *const CassRow,
        name: *const ::std::os::raw::c_char,
    ) -> *const CassValue;
}
extern "C" {
    pub fn cass_row_get_column_by_name_n(
        row: *const CassRow,
        name: *const ::std::os::raw::c_char,
        name_length: size_t,
    ) -> *const CassValue;
}
*/

// CassValue functions:
/*
extern "C" {
    pub fn cass_value_data_type(value: *const CassValue) -> *const CassDataType;
}
extern "C" {
    pub fn cass_value_get_float(value: *const CassValue, output: *mut cass_float_t) -> CassError;
}
extern "C" {
    pub fn cass_value_get_double(value: *const CassValue, output: *mut cass_double_t) -> CassError;
}
extern "C" {
    pub fn cass_value_get_bool(value: *const CassValue, output: *mut cass_bool_t) -> CassError;
}
extern "C" {
    pub fn cass_value_get_int8(
        _value: *const CassValue,
        _output: *mut cass_int8_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_int16(
        _value: *const CassValue,
        _output: *mut cass_int16_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_uint32(
        _value: *const CassValue,
        _output: *mut cass_uint32_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_int64(
        _value: *const CassValue,
        _output: *mut cass_int64_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_uuid(value: *const CassValue, output: *mut CassUuid) -> CassError;
}
extern "C" {
    pub fn cass_value_get_inet(value: *const CassValue, output: *mut CassInet) -> CassError;
}
extern "C" {
    pub fn cass_value_get_string(
        value: *const CassValue,
        output: *mut *const ::std::os::raw::c_char,
        output_size: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_bytes(
        value: *const CassValue,
        output: *mut *const cass_byte_t,
        output_size: *mut size_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_decimal(
        value: *const CassValue,
        varint: *mut *const cass_byte_t,
        varint_size: *mut size_t,
        scale: *mut cass_int32_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_get_duration(
        value: *const CassValue,
        months: *mut cass_int32_t,
        days: *mut cass_int32_t,
        nanos: *mut cass_int64_t,
    ) -> CassError;
}
extern "C" {
    pub fn cass_value_type(value: *const CassValue) -> CassValueType;
}
extern "C" {
    pub fn cass_value_is_collection(value: *const CassValue) -> cass_bool_t;
}
extern "C" {
    pub fn cass_value_is_duration(value: *const CassValue) -> cass_bool_t;
}
extern "C" {
    pub fn cass_value_item_count(collection: *const CassValue) -> size_t;
}
extern "C" {
    pub fn cass_value_primary_sub_type(collection: *const CassValue) -> CassValueType;
}
extern "C" {
    pub fn cass_value_secondary_sub_type(collection: *const CassValue) -> CassValueType;
}
*/
