use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::{
    cass_data_type_type, get_column_type, CassColumnSpec, CassDataType, CassDataTypeInner,
    CassValueType, MapDataType,
};
use crate::inet::CassInet;
use crate::query_error::CassErrorResult;
use crate::query_result::Value::{CollectionValue, RegularValue};
use crate::types::*;
use crate::uuid::CassUuid;
use scylla::errors::IntoRowsResultError;
use scylla::response::query_result::{ColumnSpecs, QueryResult};
use scylla::response::PagingStateResponse;
use scylla::value::{CqlValue, Row};
use std::convert::TryInto;
use std::os::raw::c_char;
use std::sync::Arc;
use uuid::Uuid;

pub enum CassResultKind {
    NonRows,
    Rows(CassRowsResult),
}

pub struct CassRowsResult {
    pub rows: Vec<CassRow>,
    pub metadata: Arc<CassResultMetadata>,
}

pub struct CassResult {
    pub tracing_id: Option<Uuid>,
    pub paging_state_response: PagingStateResponse,
    pub kind: CassResultKind,
}

impl CassResult {
    /// It creates CassResult object based on the:
    /// - query result
    /// - paging state response
    /// - optional cached result metadata - it's provided for prepared statements
    pub fn from_result_payload(
        result: QueryResult,
        paging_state_response: PagingStateResponse,
        maybe_result_metadata: Option<Arc<CassResultMetadata>>,
    ) -> Result<Self, CassErrorResult> {
        match result.into_rows_result() {
            Ok(rows_result) => {
                // maybe_result_metadata is:
                // - Some(_) for prepared statements
                // - None for unprepared statements
                let metadata = maybe_result_metadata.unwrap_or_else(|| {
                    Arc::new(CassResultMetadata::from_column_specs(
                        rows_result.column_specs(),
                    ))
                });

                // For now, let's eagerly deserialize rows into type-erased CqlValues.
                // Lazy deserialization requires a non-trivial refactor that needs to be discussed.
                let rows: Vec<Row> = rows_result
                    .rows::<Row>()
                    // SAFETY: this unwrap is safe, because `Row` always
                    // passes the typecheck, no matter the type of the columns.
                    .unwrap()
                    .collect::<Result<_, _>>()?;
                let cass_rows = create_cass_rows_from_rows(rows, &metadata);

                let cass_result = CassResult {
                    tracing_id: rows_result.tracing_id(),
                    paging_state_response,
                    kind: CassResultKind::Rows(CassRowsResult {
                        rows: cass_rows,
                        metadata,
                    }),
                };

                Ok(cass_result)
            }
            Err(IntoRowsResultError::ResultNotRows(result)) => {
                let cass_result = CassResult {
                    tracing_id: result.tracing_id(),
                    paging_state_response,
                    kind: CassResultKind::NonRows,
                };

                Ok(cass_result)
            }
            Err(IntoRowsResultError::ResultMetadataLazyDeserializationError(err)) => {
                Err(err.into())
            }
        }
    }
}

impl ArcFFI for CassResult {}

#[derive(Debug)]
pub struct CassResultMetadata {
    pub col_specs: Vec<CassColumnSpec>,
}

impl CassResultMetadata {
    pub fn from_column_specs(col_specs: ColumnSpecs<'_, '_>) -> CassResultMetadata {
        let col_specs = col_specs
            .iter()
            .map(|col_spec| {
                let name = col_spec.name().to_owned();
                let data_type = Arc::new(get_column_type(col_spec.typ()));

                CassColumnSpec { name, data_type }
            })
            .collect();

        CassResultMetadata { col_specs }
    }
}

/// The lifetime of CassRow is bound to CassResult.
/// It will be freed, when CassResult is freed.(see #[cass_result_free])
pub struct CassRow {
    pub columns: Vec<CassValue>,
    pub result_metadata: Arc<CassResultMetadata>,
}

impl RefFFI for CassRow {}

pub fn create_cass_rows_from_rows(
    rows: Vec<Row>,
    metadata: &Arc<CassResultMetadata>,
) -> Vec<CassRow> {
    rows.into_iter()
        .map(|r| CassRow {
            columns: create_cass_row_columns(r, metadata),
            result_metadata: metadata.clone(),
        })
        .collect()
}

pub enum Value {
    RegularValue(CqlValue),
    CollectionValue(Collection),
}

pub enum Collection {
    List(Vec<CassValue>),
    Map(Vec<(CassValue, CassValue)>),
    Set(Vec<CassValue>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
        fields: Vec<(String, Option<CassValue>)>,
    },
    Tuple(Vec<Option<CassValue>>),
}

pub struct CassValue {
    pub value: Option<Value>,
    pub value_type: Arc<CassDataType>,
}

impl RefFFI for CassValue {}

fn create_cass_row_columns(row: Row, metadata: &Arc<CassResultMetadata>) -> Vec<CassValue> {
    row.columns
        .into_iter()
        .zip(metadata.col_specs.iter())
        .map(|(val, col_spec)| {
            let column_type = Arc::clone(&col_spec.data_type);
            CassValue {
                value: val.map(|col_val| get_column_value(col_val, &column_type)),
                value_type: column_type,
            }
        })
        .collect()
}

fn get_column_value(column: CqlValue, column_type: &Arc<CassDataType>) -> Value {
    match (column, unsafe { column_type.get_unchecked() }) {
        (
            CqlValue::List(list),
            CassDataTypeInner::List {
                typ: Some(list_type),
                ..
            },
        ) => CollectionValue(Collection::List(
            list.into_iter()
                .map(|val| CassValue {
                    value_type: list_type.clone(),
                    value: Some(get_column_value(val, list_type)),
                })
                .collect(),
        )),
        (
            CqlValue::Map(map),
            CassDataTypeInner::Map {
                typ: MapDataType::KeyAndValue(key_type, value_type),
                ..
            },
        ) => CollectionValue(Collection::Map(
            map.into_iter()
                .map(|(key, val)| {
                    (
                        CassValue {
                            value_type: key_type.clone(),
                            value: Some(get_column_value(key, key_type)),
                        },
                        CassValue {
                            value_type: value_type.clone(),
                            value: Some(get_column_value(val, value_type)),
                        },
                    )
                })
                .collect(),
        )),
        (
            CqlValue::Set(set),
            CassDataTypeInner::Set {
                typ: Some(set_type),
                ..
            },
        ) => CollectionValue(Collection::Set(
            set.into_iter()
                .map(|val| CassValue {
                    value_type: set_type.clone(),
                    value: Some(get_column_value(val, set_type)),
                })
                .collect(),
        )),
        (
            CqlValue::UserDefinedType {
                keyspace,
                name,
                fields,
            },
            CassDataTypeInner::UDT(udt_type),
        ) => CollectionValue(Collection::UserDefinedType {
            keyspace,
            type_name: name,
            fields: fields
                .into_iter()
                .enumerate()
                .map(|(index, (name, val_opt))| {
                    let udt_field_type_opt = udt_type.get_field_by_index(index);
                    if let (Some(val), Some(udt_field_type)) = (val_opt, udt_field_type_opt) {
                        return (
                            name,
                            Some(CassValue {
                                value_type: udt_field_type.clone(),
                                value: Some(get_column_value(val, udt_field_type)),
                            }),
                        );
                    }
                    (name, None)
                })
                .collect(),
        }),
        (CqlValue::Tuple(tuple), CassDataTypeInner::Tuple(tuple_types)) => {
            CollectionValue(Collection::Tuple(
                tuple
                    .into_iter()
                    .enumerate()
                    .map(|(index, val_opt)| {
                        val_opt
                            .zip(tuple_types.get(index))
                            .map(|(val, tuple_field_type)| CassValue {
                                value_type: tuple_field_type.clone(),
                                value: Some(get_column_value(val, tuple_field_type)),
                            })
                    })
                    .collect(),
            ))
        }
        (regular_value, _) => RegularValue(regular_value),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_free(result_raw: *const CassResult) {
    ArcFFI::free(result_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_has_more_pages(result: *const CassResult) -> cass_bool_t {
    let result = ArcFFI::as_ref(result);
    (!result.paging_state_response.finished()) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_row_get_column(
    row_raw: *const CassRow,
    index: size_t,
) -> *const CassValue {
    let row: &CassRow = RefFFI::as_ref(row_raw);

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
    let row_from_raw = RefFFI::as_ref(row);
    let mut name_str = ptr_to_cstr_n(name, name_length).unwrap();
    let mut is_case_sensitive = false;

    if name_str.starts_with('\"') && name_str.ends_with('\"') {
        name_str = name_str.strip_prefix('\"').unwrap();
        name_str = name_str.strip_suffix('\"').unwrap();
        is_case_sensitive = true;
    }

    row_from_raw
        .result_metadata
        .col_specs
        .iter()
        .enumerate()
        .find(|(_, col_spec)| {
            is_case_sensitive && col_spec.name == name_str
                || !is_case_sensitive && col_spec.name.eq_ignore_ascii_case(name_str)
        })
        .map(|(index, _)| match row_from_raw.columns.get(index) {
            Some(value) => value as *const CassValue,
            None => std::ptr::null(),
        })
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_name(
    result: *const CassResult,
    index: size_t,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let result_from_raw = ArcFFI::as_ref(result);
    let index_usize: usize = index.try_into().unwrap();

    let CassResultKind::Rows(CassRowsResult { metadata, .. }) = &result_from_raw.kind else {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    };

    if index_usize >= metadata.col_specs.len() {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }

    let column_name = &metadata.col_specs.get(index_usize).unwrap().name;

    write_str_to_c(column_name, name, name_length);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_type(
    result: *const CassResult,
    index: size_t,
) -> CassValueType {
    let data_type_ptr = cass_result_column_data_type(result, index);
    if data_type_ptr.is_null() {
        return CassValueType::CASS_VALUE_TYPE_UNKNOWN;
    }
    cass_data_type_type(data_type_ptr)
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_data_type(
    result: *const CassResult,
    index: size_t,
) -> *const CassDataType {
    let result_from_raw: &CassResult = ArcFFI::as_ref(result);
    let index_usize: usize = index
        .try_into()
        .expect("Provided index is out of bounds. Max possible value is usize::MAX");

    let CassResultKind::Rows(CassRowsResult { metadata, .. }) = &result_from_raw.kind else {
        return std::ptr::null();
    };

    metadata
        .col_specs
        .get(index_usize)
        .map(|col_spec| ArcFFI::as_ptr(&col_spec.data_type))
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_type(value: *const CassValue) -> CassValueType {
    let value_from_raw = RefFFI::as_ref(value);

    cass_data_type_type(ArcFFI::as_ptr(&value_from_raw.value_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_data_type(value: *const CassValue) -> *const CassDataType {
    let value_from_raw = RefFFI::as_ref(value);

    ArcFFI::as_ptr(&value_from_raw.value_type)
}

macro_rules! val_ptr_to_ref_ensure_non_null {
    ($ptr:ident) => {{
        if $ptr.is_null() {
            return CassError::CASS_ERROR_LIB_NULL_VALUE;
        }
        RefFFI::as_ref($ptr)
    }};
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_float(
    value: *const CassValue,
    output: *mut cass_float_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Float(f))) => std::ptr::write(output, f),
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Double(d))) => std::ptr::write(output, d),
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Boolean(b))) => {
            std::ptr::write(output, b as cass_bool_t)
        }
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::TinyInt(i))) => std::ptr::write(output, i),
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::SmallInt(i))) => std::ptr::write(output, i),
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Date(u))) => std::ptr::write(output, u.0), // FIXME: hack
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Int(i))) => std::ptr::write(output, i),
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::BigInt(i))) => std::ptr::write(output, i),
        Some(Value::RegularValue(CqlValue::Counter(i))) => {
            std::ptr::write(output, i.0 as cass_int64_t)
        }
        Some(Value::RegularValue(CqlValue::Time(d))) => std::ptr::write(output, d.0),
        Some(Value::RegularValue(CqlValue::Timestamp(d))) => {
            std::ptr::write(output, d.0 as cass_int64_t)
        }
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Uuid(uuid))) => std::ptr::write(output, uuid.into()),
        Some(Value::RegularValue(CqlValue::Timeuuid(uuid))) => {
            std::ptr::write(output, Into::<Uuid>::into(uuid).into())
        }
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
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match val.value {
        Some(Value::RegularValue(CqlValue::Inet(inet))) => std::ptr::write(output, inet.into()),
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_decimal(
    value: *const CassValue,
    varint: *mut *const cass_byte_t,
    varint_size: *mut size_t,
    scale: *mut cass_int32_t,
) -> CassError {
    let val: &CassValue = RefFFI::as_ref(value);
    let decimal = match &val.value {
        Some(Value::RegularValue(CqlValue::Decimal(decimal))) => decimal,
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    };

    let (varint_value, scale_value) = decimal.as_signed_be_bytes_slice_and_exponent();
    std::ptr::write(varint_size, varint_value.len() as size_t);
    std::ptr::write(varint, varint_value.as_ptr());
    std::ptr::write(scale, scale_value);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_string(
    value: *const CassValue,
    output: *mut *const c_char,
    output_size: *mut size_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);
    match &val.value {
        // It seems that cpp driver doesn't check the type - you can call _get_string
        // on any type and get internal represenation. I don't see how to do it easily in
        // a compatible way in rust, so let's do something sensible - only return result
        // for string values.
        Some(Value::RegularValue(CqlValue::Ascii(s))) => {
            write_str_to_c(s.as_str(), output, output_size)
        }
        Some(Value::RegularValue(CqlValue::Text(s))) => {
            write_str_to_c(s.as_str(), output, output_size)
        }
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_get_duration(
    value: *const CassValue,
    months: *mut cass_int32_t,
    days: *mut cass_int32_t,
    nanos: *mut cass_int64_t,
) -> CassError {
    let val: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    match &val.value {
        Some(Value::RegularValue(CqlValue::Duration(duration))) => {
            std::ptr::write(months, duration.months);
            std::ptr::write(days, duration.days);
            std::ptr::write(nanos, duration.nanoseconds);
        }
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
    let value_from_raw: &CassValue = val_ptr_to_ref_ensure_non_null!(value);

    // FIXME: This should be implemented for all CQL types
    // Note: currently rust driver does not allow to get raw bytes of the CQL value.
    match &value_from_raw.value {
        Some(Value::RegularValue(CqlValue::Blob(bytes))) => {
            *output = bytes.as_ptr() as *const cass_byte_t;
            *output_size = bytes.len() as u64;
        }
        Some(Value::RegularValue(CqlValue::Varint(varint))) => {
            let bytes = varint.as_signed_bytes_be_slice();
            std::ptr::write(output, bytes.as_ptr());
            std::ptr::write(output_size, bytes.len() as size_t);
        }
        Some(_) => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        None => return CassError::CASS_ERROR_LIB_NULL_VALUE,
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_null(value: *const CassValue) -> cass_bool_t {
    let val: &CassValue = RefFFI::as_ref(value);
    val.value.is_none() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_collection(value: *const CassValue) -> cass_bool_t {
    let val = RefFFI::as_ref(value);

    matches!(
        val.value_type.get_unchecked().get_value_type(),
        CassValueType::CASS_VALUE_TYPE_LIST
            | CassValueType::CASS_VALUE_TYPE_SET
            | CassValueType::CASS_VALUE_TYPE_MAP
    ) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_duration(value: *const CassValue) -> cass_bool_t {
    let val = RefFFI::as_ref(value);

    (val.value_type.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_DURATION)
        as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_item_count(collection: *const CassValue) -> size_t {
    let val = RefFFI::as_ref(collection);

    match &val.value {
        Some(Value::CollectionValue(Collection::List(list))) => list.len() as size_t,
        Some(Value::CollectionValue(Collection::Map(map))) => map.len() as size_t,
        Some(Value::CollectionValue(Collection::Set(set))) => set.len() as size_t,
        Some(Value::CollectionValue(Collection::Tuple(tuple))) => tuple.len() as size_t,
        Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) => {
            fields.len() as size_t
        }
        _ => 0 as size_t,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_primary_sub_type(
    collection: *const CassValue,
) -> CassValueType {
    let val = RefFFI::as_ref(collection);

    match val.value_type.get_unchecked() {
        CassDataTypeInner::List {
            typ: Some(list), ..
        } => list.get_unchecked().get_value_type(),
        CassDataTypeInner::Set { typ: Some(set), .. } => set.get_unchecked().get_value_type(),
        CassDataTypeInner::Map {
            typ: MapDataType::Key(key) | MapDataType::KeyAndValue(key, _),
            ..
        } => key.get_unchecked().get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_secondary_sub_type(
    collection: *const CassValue,
) -> CassValueType {
    let val = RefFFI::as_ref(collection);

    match val.value_type.get_unchecked() {
        CassDataTypeInner::Map {
            typ: MapDataType::KeyAndValue(_, value),
            ..
        } => value.get_unchecked().get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_row_count(result_raw: *const CassResult) -> size_t {
    let result = ArcFFI::as_ref(result_raw);

    let CassResultKind::Rows(CassRowsResult { rows, .. }) = &result.kind else {
        return 0;
    };

    rows.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_count(result_raw: *const CassResult) -> size_t {
    let result = ArcFFI::as_ref(result_raw);

    let CassResultKind::Rows(CassRowsResult { metadata, .. }) = &result.kind else {
        return 0;
    };

    metadata.col_specs.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_first_row(result_raw: *const CassResult) -> *const CassRow {
    let result = ArcFFI::as_ref(result_raw);

    let CassResultKind::Rows(CassRowsResult { rows, .. }) = &result.kind else {
        return std::ptr::null();
    };

    rows.first()
        .map(|row| row as *const CassRow)
        .unwrap_or(std::ptr::null())
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

    let result_from_raw = ArcFFI::as_ref(result);

    match &result_from_raw.paging_state_response {
        PagingStateResponse::HasMorePages { state } => match state.as_bytes_slice() {
            Some(result_paging_state) => {
                *paging_state_size = result_paging_state.len() as u64;
                *paging_state = result_paging_state.as_ptr() as *const c_char;
            }
            None => {
                *paging_state_size = 0;
                *paging_state = std::ptr::null();
            }
        },
        PagingStateResponse::NoMorePages => {
            *paging_state_size = 0;
            *paging_state = std::ptr::null();
        }
    }

    CassError::CASS_OK
}

#[cfg(test)]
mod tests {
    use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};
    use scylla::frame::response::result::{ColumnSpec, TableSpec};
    use scylla::response::query_result::ColumnSpecs;
    use scylla::response::PagingStateResponse;
    use scylla::value::{CqlValue, Row};

    use crate::{
        argconv::ArcFFI,
        cass_error::CassError,
        cass_types::{CassDataType, CassDataTypeInner, CassValueType},
        query_result::{
            cass_result_column_data_type, cass_result_column_name, cass_result_first_row,
            ptr_to_cstr_n, size_t,
        },
    };
    use std::{ffi::c_char, ptr::addr_of_mut, sync::Arc};

    use super::{
        cass_result_column_count, cass_result_column_type, create_cass_rows_from_rows, CassResult,
        CassResultKind, CassResultMetadata, CassRowsResult,
    };

    fn col_spec(name: &'static str, typ: ColumnType<'static>) -> ColumnSpec<'static> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
    }

    const FIRST_COLUMN_NAME: &str = "bigint_col";
    const SECOND_COLUMN_NAME: &str = "varint_col";
    const THIRD_COLUMN_NAME: &str = "list_double_col";
    fn create_cass_rows_result() -> CassResult {
        let metadata = Arc::new(CassResultMetadata::from_column_specs(ColumnSpecs::new(&[
            col_spec(FIRST_COLUMN_NAME, ColumnType::Native(NativeType::BigInt)),
            col_spec(SECOND_COLUMN_NAME, ColumnType::Native(NativeType::Varint)),
            col_spec(
                THIRD_COLUMN_NAME,
                ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Double))),
                },
            ),
        ])));

        let rows = create_cass_rows_from_rows(
            vec![Row {
                columns: vec![
                    Some(CqlValue::BigInt(42)),
                    None,
                    Some(CqlValue::List(vec![
                        CqlValue::Float(0.5),
                        CqlValue::Float(42.42),
                        CqlValue::Float(9999.9999),
                    ])),
                ],
            }],
            &metadata,
        );

        CassResult {
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
            kind: CassResultKind::Rows(CassRowsResult { rows, metadata }),
        }
    }

    unsafe fn cass_result_column_name_rust_str(
        result_ptr: *const CassResult,
        column_index: u64,
    ) -> Option<&'static str> {
        let mut name_ptr: *const c_char = std::ptr::null();
        let mut name_length: size_t = 0;
        let cass_err = cass_result_column_name(
            result_ptr,
            column_index,
            addr_of_mut!(name_ptr),
            addr_of_mut!(name_length),
        );
        assert_eq!(CassError::CASS_OK, cass_err);
        ptr_to_cstr_n(name_ptr, name_length)
    }

    #[test]
    fn rows_cass_result_api_test() {
        let result = create_cass_rows_result();

        unsafe {
            let result_ptr = std::ptr::addr_of!(result);

            // cass_result_column_count test
            {
                let column_count = cass_result_column_count(result_ptr);
                assert_eq!(3, column_count);
            }

            // cass_result_column_name test
            {
                let first_column_name = cass_result_column_name_rust_str(result_ptr, 0).unwrap();
                assert_eq!(FIRST_COLUMN_NAME, first_column_name);
                let second_column_name = cass_result_column_name_rust_str(result_ptr, 1).unwrap();
                assert_eq!(SECOND_COLUMN_NAME, second_column_name);
                let third_column_name = cass_result_column_name_rust_str(result_ptr, 2).unwrap();
                assert_eq!(THIRD_COLUMN_NAME, third_column_name);
            }

            // cass_result_column_type test
            {
                let first_col_type = cass_result_column_type(result_ptr, 0);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_BIGINT, first_col_type);
                let second_col_type = cass_result_column_type(result_ptr, 1);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_VARINT, second_col_type);
                let third_col_type = cass_result_column_type(result_ptr, 2);
                assert_eq!(CassValueType::CASS_VALUE_TYPE_LIST, third_col_type);
                let out_of_bound_col_type = cass_result_column_type(result_ptr, 555);
                assert_eq!(
                    CassValueType::CASS_VALUE_TYPE_UNKNOWN,
                    out_of_bound_col_type
                );
            }

            // cass_result_column_data_type test
            {
                let first_col_data_type =
                    ArcFFI::as_ref(cass_result_column_data_type(result_ptr, 0));
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_BIGINT
                    )),
                    first_col_data_type
                );
                let second_col_data_type =
                    ArcFFI::as_ref(cass_result_column_data_type(result_ptr, 1));
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_VARINT
                    )),
                    second_col_data_type
                );
                let third_col_data_type =
                    ArcFFI::as_ref(cass_result_column_data_type(result_ptr, 2));
                assert_eq!(
                    &CassDataType::new(CassDataTypeInner::List {
                        typ: Some(CassDataType::new_arced(CassDataTypeInner::Value(
                            CassValueType::CASS_VALUE_TYPE_DOUBLE
                        ))),
                        frozen: false
                    }),
                    third_col_data_type
                );
                let out_of_bound_col_data_type = cass_result_column_data_type(result_ptr, 555);
                assert!(out_of_bound_col_data_type.is_null());
            }
        }
    }

    fn create_non_rows_cass_result() -> CassResult {
        CassResult {
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
            kind: CassResultKind::NonRows,
        }
    }

    #[test]
    fn non_rows_cass_result_api_test() {
        let result = create_non_rows_cass_result();

        // Check that API functions do not panic when rows are empty - e.g. for INSERT queries.
        unsafe {
            let result_ptr = std::ptr::addr_of!(result);

            assert_eq!(0, cass_result_column_count(result_ptr));
            assert_eq!(
                CassValueType::CASS_VALUE_TYPE_UNKNOWN,
                cass_result_column_type(result_ptr, 0)
            );
            assert!(cass_result_column_data_type(result_ptr, 0).is_null());
            assert!(cass_result_first_row(result_ptr).is_null());

            {
                let mut name_ptr: *const c_char = std::ptr::null();
                let mut name_length: size_t = 0;
                let cass_err = cass_result_column_name(
                    result_ptr,
                    0,
                    addr_of_mut!(name_ptr),
                    addr_of_mut!(name_length),
                );
                assert_eq!(CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS, cass_err);
            }
        }
    }
}
