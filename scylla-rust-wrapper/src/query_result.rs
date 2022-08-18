use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::{cass_data_type_type, CassDataType, CassValueType};
use crate::inet::CassInet;
use crate::statement::CassStatement;
use crate::types::*;
use crate::uuid::CassUuid;
use scylla::frame::response::result::{ColumnSpec, CqlValue};
use scylla::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;
use std::os::raw::c_char;
use std::slice;
use std::sync::Arc;

pub struct CassResult {
    pub rows: Option<Vec<CassRow>>,
    pub metadata: Arc<CassResultData>,
}

pub struct CassResultData {
    pub paging_state: Option<Bytes>,
    pub col_specs: Vec<ColumnSpec>,
}

pub type CassResult_ = Arc<CassResult>;

pub type CassRow_ = &'static CassRow;

pub struct CassRow {
    pub columns: Vec<CassValue>,
    pub result_metadata: Arc<CassResultData>,
}

pub struct CassValue {
    pub value: Option<CqlValue>,
    pub value_type: CassDataType,
}

pub struct CassResultIterator {
    result: CassResult_,
    position: Option<usize>,
}

pub struct CassRowIterator {
    row: CassRow_,
    position: Option<usize>,
}

pub enum CassIterator {
    CassResultIterator(CassResultIterator),
    CassRowIterator(CassRowIterator),
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_free(iterator: *mut CassIterator) {
    free_boxed(iterator);
}

// After creating an iterator we have to call next() before accessing the value
#[no_mangle]
pub unsafe extern "C" fn cass_iterator_next(iterator: *mut CassIterator) -> cass_bool_t {
    let mut iter = ptr_to_ref_mut(iterator);

    match &mut iter {
        CassIterator::CassResultIterator(result_iterator) => {
            let new_pos: usize = result_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            result_iterator.position = Some(new_pos);

            match &result_iterator.result.rows {
                Some(rs) => (new_pos < rs.len()) as cass_bool_t,
                None => false as cass_bool_t,
            }
        }
        CassIterator::CassRowIterator(row_iterator) => {
            let new_pos: usize = row_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            row_iterator.position = Some(new_pos);

            (new_pos < row_iterator.row.columns.len()) as cass_bool_t
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_row(iterator: *const CassIterator) -> *const CassRow {
    let iter = ptr_to_ref(iterator);

    // Defined only for result iterator, for other types should return null
    if let CassIterator::CassResultIterator(result_iterator) = iter {
        let iter_position = match result_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let row: &CassRow = match result_iterator
            .result
            .rows
            .as_ref()
            .and_then(|rs| rs.get(iter_position))
        {
            Some(row) => row,
            None => return std::ptr::null(),
        };

        return row;
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    // Defined only for row iterator, for other types should return null
    if let CassIterator::CassRowIterator(row_iterator) = iter {
        let iter_position = match row_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let value = match row_iterator.row.columns.get(iter_position) {
            Some(col) => col,
            None => return std::ptr::null(),
        };

        return value as *const CassValue;
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_result(result: *const CassResult) -> *mut CassIterator {
    let result_from_raw: CassResult_ = clone_arced(result);

    let iterator = CassResultIterator {
        result: result_from_raw,
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassResultIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_row(row: *const CassRow) -> *mut CassIterator {
    let row_from_raw: CassRow_ = ptr_to_ref(row);

    let iterator = CassRowIterator {
        row: row_from_raw,
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassRowIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_free(result_raw: *const CassResult) {
    free_arced(result_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_has_more_pages(result: *const CassResult) -> cass_bool_t {
    let result = ptr_to_ref(result);
    result.metadata.paging_state.is_some() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column(
    row_raw: *const CassRow,
    index: size_t,
) -> *const CassValue {
    let row: &CassRow = ptr_to_ref(row_raw);

    let index_usize: usize = index.try_into().unwrap();
    let column_value = match row.columns.get(index_usize) {
        Some(val) => val,
        None => return std::ptr::null(),
    };

    column_value as *const CassValue
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column_by_name(
    row: *const CassRow,
    name: *const c_char,
) -> *const CassValue {
    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_row_get_column_by_name_n(row, name, name_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column_by_name_n(
    row: *const CassRow,
    name: *const c_char,
    name_length: size_t,
) -> *const CassValue {
    let row_from_raw = ptr_to_ref(row);
    let name_str = ptr_to_cstr_n(name, name_length).unwrap().to_lowercase();

    return row_from_raw
        .result_metadata
        .col_specs
        .iter()
        .enumerate()
        .find(|(_, spec)| spec.name.to_lowercase() == name_str)
        .map(|(index, _)| {
            return match row_from_raw.columns.get(index) {
                Some(value) => value as *const CassValue,
                None => std::ptr::null(),
            };
        })
        .unwrap_or(std::ptr::null());
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_name(
    result: *const CassResult,
    index: size_t,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let result_from_raw = ptr_to_ref(result);
    let index_usize: usize = index.try_into().unwrap();

    if index_usize >= result_from_raw.metadata.col_specs.len() {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }

    let column_spec: &ColumnSpec = result_from_raw.metadata.col_specs.get(index_usize).unwrap();
    let column_name = column_spec.name.as_str();

    write_str_to_c(column_name, name, name_length);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_type(value: *const CassValue) -> CassValueType {
    let value_from_raw = ptr_to_ref(value);

    cass_data_type_type(&value_from_raw.value_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_data_type(value: *const CassValue) -> *const CassDataType {
    let value_from_raw = ptr_to_ref(value);

    &value_from_raw.value_type as *const CassDataType
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_float(
    value: *const CassValue,
    output: *mut cass_float_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_float_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Float(f)) => *out = f,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_double(
    value: *const CassValue,
    output: *mut cass_double_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_double_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Double(d)) => *out = d,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bool(
    value: *const CassValue,
    output: *mut cass_bool_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_bool_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Boolean(b)) => *out = b as cass_bool_t,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int8(
    value: *const CassValue,
    output: *mut cass_int8_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_int8_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::TinyInt(i)) => *out = i,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int16(
    value: *const CassValue,
    output: *mut cass_int16_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_int16_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::SmallInt(i)) => *out = i,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_uint32(
    value: *const CassValue,
    output: *mut cass_uint32_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_uint32_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Date(u)) => *out = u, // FIXME: hack
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int32(
    value: *const CassValue,
    output: *mut cass_int32_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_int32_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Int(i)) => *out = i,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_int64(
    value: *const CassValue,
    output: *mut cass_int64_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut cass_int64_t = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::BigInt(i)) => *out = i,
        Some(CqlValue::Counter(i)) => *out = i.0 as cass_int64_t,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_uuid(
    value: *const CassValue,
    output: *mut CassUuid,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut CassUuid = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Uuid(uuid)) => *out = uuid.into(),
        Some(CqlValue::Timeuuid(uuid)) => *out = uuid.into(),
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_inet(
    value: *const CassValue,
    output: *mut CassInet,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    let out: &mut CassInet = ptr_to_ref_mut(output);
    match val.value {
        Some(CqlValue::Inet(inet)) => *out = inet.into(),
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_string(
    value: *const CassValue,
    output: *mut *const c_char,
    output_size: *mut size_t,
) -> CassError {
    let val: &CassValue = ptr_to_ref(value);
    match &val.value {
        // It seems that cpp driver doesn't check the type - you can call _get_string
        // on any type and get internal represenation. I don't see how to do it easily in
        // a compatible way in rust, so let's do something sensible - only return result
        // for string values.
        Some(CqlValue::Ascii(s)) => write_str_to_c(s.as_str(), output, output_size),
        Some(CqlValue::Text(s)) => write_str_to_c(s.as_str(), output, output_size),
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bytes(
    value: *const CassValue,
    output: *mut *const cass_byte_t,
    output_size: *mut size_t,
) -> CassError {
    if value.is_null() {
        return CassError::CASS_ERROR_LIB_NULL_VALUE;
    }

    let value_from_raw: &CassValue = ptr_to_ref(value);

    // FIXME: This should be implemented for all CQL types
    // Note: currently rust driver does not allow to get raw bytes of the CQL value.
    match &value_from_raw.value {
        Some(CqlValue::Blob(bytes)) => {
            *output = bytes.as_ptr() as *const cass_byte_t;
            *output_size = bytes.len() as u64;
        }
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_null(value: *const CassValue) -> cass_bool_t {
    let val: &CassValue = ptr_to_ref(value);
    val.value.is_none() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_row_count(result_raw: *const CassResult) -> size_t {
    let result = ptr_to_ref(result_raw);

    if result.rows.as_ref().is_none() {
        return 0;
    }

    result.rows.as_ref().unwrap().len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_count(result_raw: *const CassResult) -> size_t {
    let result = ptr_to_ref(result_raw);

    result.metadata.col_specs.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_first_row(result_raw: *const CassResult) -> *const CassRow {
    let result = ptr_to_ref(result_raw);

    if result.rows.is_some() || result.rows.as_ref().unwrap().is_empty() {
        return result.rows.as_ref().unwrap().first().unwrap();
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_paging_state_token(
    result: *const CassResult,
    paging_state: *mut *const c_char,
    paging_state_size: *mut size_t,
) -> CassError {
    if cass_result_has_more_pages(result) == cass_false {
        return CassError::CASS_ERROR_LIB_NO_PAGING_STATE;
    }

    let result_from_raw = ptr_to_ref(result);

    match &result_from_raw.metadata.paging_state {
        Some(result_paging_state) => {
            *paging_state_size = result_paging_state.len() as u64;
            *paging_state = result_paging_state.as_ptr() as *const c_char;
        }
        None => {
            *paging_state_size = 0;
            *paging_state = std::ptr::null();
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_state_token(
    statement: *mut CassStatement,
    paging_state: *const c_char,
    paging_state_size: size_t,
) -> CassError {
    let statement_from_raw = ptr_to_ref_mut(statement);

    if paging_state.is_null() {
        statement_from_raw.paging_state = None;
        return CassError::CASS_ERROR_LIB_NULL_VALUE;
    }

    let paging_state_usize: usize = paging_state_size.try_into().unwrap();
    let mut b = BytesMut::with_capacity(paging_state_usize + 1);
    b.put_slice(slice::from_raw_parts(
        paging_state as *const u8,
        paging_state_usize,
    ));
    b.extend_from_slice(b"\0");
    statement_from_raw.paging_state = Some(b.freeze());

    CassError::CASS_OK
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
#[no_mangle]
pub unsafe extern "C" fn cass_value_get_bytes(
    value: *const CassValue,
    output: *mut *const cass_byte_t,
    output_size: *mut size_t,
) -> CassError {
}
#[no_mangle]
pub unsafe extern "C" fn cass_value_get_decimal(
    value: *const CassValue,
    varint: *mut *const cass_byte_t,
    varint_size: *mut size_t,
    scale: *mut cass_int32_t,
) -> CassError {
}
#[no_mangle]
pub unsafe extern "C" fn cass_value_get_duration(
    value: *const CassValue,
    months: *mut cass_int32_t,
    days: *mut cass_int32_t,
    nanos: *mut cass_int64_t,
) -> CassError {
}
extern "C" {
    pub fn cass_value_data_type(value: *const CassValue) -> *const CassDataType;
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
