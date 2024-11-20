use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::{
    cass_data_type_type, get_column_type, CassColumnSpec, CassDataType, CassValueType, MapDataType,
};
use crate::inet::CassInet;
use crate::metadata::{
    CassColumnMeta, CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta, CassTableMeta,
};
use crate::types::*;
use crate::uuid::CassUuid;
use scylla::frame::response::result::{ColumnSpec, CqlValue};
use scylla::transport::PagingStateResponse;
use std::convert::TryInto;
use std::os::raw::c_char;
use std::sync::Arc;
use uuid::Uuid;

pub struct CassResult {
    pub rows: Option<Vec<CassRow>>,
    pub metadata: Arc<CassResultData>,
    pub tracing_id: Option<Uuid>,
    pub paging_state_response: PagingStateResponse,
}

pub struct CassResultData {
    pub col_specs: Vec<CassColumnSpec>,
}

impl CassResultData {
    pub fn from_column_specs(col_specs: &[ColumnSpec<'_>]) -> CassResultData {
        let col_specs = col_specs
            .iter()
            .map(|col_spec| {
                let name = col_spec.name().to_owned();
                let data_type = Arc::new(get_column_type(col_spec.typ()));

                CassColumnSpec { name, data_type }
            })
            .collect();

        CassResultData { col_specs }
    }
}

/// The lifetime of CassRow is bound to CassResult.
/// It will be freed, when CassResult is freed.(see #[cass_result_free])
pub struct CassRow {
    pub columns: Vec<CassValue>,
    pub result_metadata: Arc<CassResultData>,
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

pub struct CassResultIterator {
    result: Arc<CassResult>,
    position: Option<usize>,
}

pub struct CassRowIterator {
    row: &'static CassRow,
    position: Option<usize>,
}

pub struct CassCollectionIterator {
    value: &'static CassValue,
    count: u64,
    position: Option<usize>,
}

pub struct CassMapIterator {
    value: &'static CassValue,
    count: u64,
    position: Option<usize>,
}

pub struct CassUdtIterator {
    value: &'static CassValue,
    count: u64,
    position: Option<usize>,
}

pub struct CassSchemaMetaIterator {
    value: &'static CassSchemaMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassKeyspaceMetaIterator {
    value: &'static CassKeyspaceMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassTableMetaIterator {
    value: &'static CassTableMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassViewMetaIterator {
    value: &'static CassMaterializedViewMeta,
    count: usize,
    position: Option<usize>,
}

pub enum CassIterator {
    CassResultIterator(CassResultIterator),
    CassRowIterator(CassRowIterator),
    CassCollectionIterator(CassCollectionIterator),
    CassMapIterator(CassMapIterator),
    CassUdtIterator(CassUdtIterator),
    CassSchemaMetaIterator(CassSchemaMetaIterator),
    CassKeyspaceMetaTableIterator(CassKeyspaceMetaIterator),
    CassKeyspaceMetaUserTypeIterator(CassKeyspaceMetaIterator),
    CassKeyspaceMetaViewIterator(CassKeyspaceMetaIterator),
    CassTableMetaIterator(CassTableMetaIterator),
    CassViewMetaIterator(CassViewMetaIterator),
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
        CassIterator::CassCollectionIterator(collection_iterator) => {
            let new_pos: usize = collection_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            collection_iterator.position = Some(new_pos);

            (new_pos < collection_iterator.count.try_into().unwrap()) as cass_bool_t
        }
        CassIterator::CassMapIterator(map_iterator) => {
            let new_pos: usize = map_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            map_iterator.position = Some(new_pos);

            (new_pos < map_iterator.count.try_into().unwrap()) as cass_bool_t
        }
        CassIterator::CassUdtIterator(udt_iterator) => {
            let new_pos: usize = udt_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            udt_iterator.position = Some(new_pos);

            (new_pos < udt_iterator.count.try_into().unwrap()) as cass_bool_t
        }
        CassIterator::CassSchemaMetaIterator(schema_meta_iterator) => {
            let new_pos: usize = schema_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            schema_meta_iterator.position = Some(new_pos);

            (new_pos < schema_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassKeyspaceMetaTableIterator(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassKeyspaceMetaUserTypeIterator(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassKeyspaceMetaViewIterator(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::CassTableMetaIterator(table_iterator) => {
            let new_pos: usize = table_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            table_iterator.position = Some(new_pos);

            (new_pos < table_iterator.count) as cass_bool_t
        }
        CassIterator::CassViewMetaIterator(view_iterator) => {
            let new_pos: usize = view_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            view_iterator.position = Some(new_pos);

            (new_pos < view_iterator.count) as cass_bool_t
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
pub unsafe extern "C" fn cass_iterator_get_value(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    // Defined only for collections(list, set and map) or tuple iterator, for other types should return null
    if let CassIterator::CassCollectionIterator(collection_iterator) = iter {
        let iter_position = match collection_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let value = match &collection_iterator.value.value {
            Some(Value::CollectionValue(Collection::List(list))) => list.get(iter_position),
            Some(Value::CollectionValue(Collection::Set(set))) => set.get(iter_position),
            Some(Value::CollectionValue(Collection::Tuple(tuple))) => {
                tuple.get(iter_position).and_then(|x| x.as_ref())
            }
            Some(Value::CollectionValue(Collection::Map(map))) => {
                let map_entry_index = iter_position / 2;
                map.get(map_entry_index)
                    .map(|(key, value)| if iter_position % 2 == 0 { key } else { value })
            }
            _ => return std::ptr::null(),
        };

        if value.is_none() {
            return std::ptr::null();
        }

        return value.unwrap() as *const CassValue;
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_key(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassMapIterator(map_iterator) = iter {
        let iter_position = match map_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let entry = match &map_iterator.value.value {
            Some(Value::CollectionValue(Collection::Map(map))) => map.get(iter_position),
            _ => return std::ptr::null(),
        };

        if entry.is_none() {
            return std::ptr::null();
        }

        return &entry.unwrap().0 as *const CassValue;
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_value(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassMapIterator(map_iterator) = iter {
        let iter_position = match map_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let entry = match &map_iterator.value.value {
            Some(Value::CollectionValue(Collection::Map(map))) => map.get(iter_position),
            _ => return std::ptr::null(),
        };

        if entry.is_none() {
            return std::ptr::null();
        }

        return &entry.unwrap().1 as *const CassValue;
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_name(
    iterator: *const CassIterator,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassUdtIterator(udt_iterator) = iter {
        let iter_position = match udt_iterator.position {
            Some(pos) => pos,
            None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
        };

        let udt_entry_opt = match &udt_iterator.value.value {
            Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) => {
                fields.get(iter_position)
            }
            _ => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
        };

        match udt_entry_opt {
            Some(udt_entry) => {
                let field_name = &udt_entry.0;
                write_str_to_c(field_name.as_str(), name, name_length);
            }
            None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
        }

        return CassError::CASS_OK;
    }

    CassError::CASS_ERROR_LIB_BAD_PARAMS
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_value(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassUdtIterator(udt_iterator) = iter {
        let iter_position = match udt_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let udt_entry_opt = match &udt_iterator.value.value {
            Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) => {
                fields.get(iter_position)
            }
            _ => return std::ptr::null(),
        };

        return match udt_entry_opt {
            Some(udt_entry) => match &udt_entry.1 {
                Some(value) => value as *const CassValue,
                None => std::ptr::null(),
            },
            None => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_keyspace_meta(
    iterator: *const CassIterator,
) -> *const CassKeyspaceMeta {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassSchemaMetaIterator(schema_meta_iterator) = iter {
        let iter_position = match schema_meta_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let schema_meta_entry_opt = &schema_meta_iterator
            .value
            .keyspaces
            .iter()
            .nth(iter_position);

        return match schema_meta_entry_opt {
            Some(schema_meta_entry) => schema_meta_entry.1 as *const CassKeyspaceMeta,
            None => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_table_meta(
    iterator: *const CassIterator,
) -> *const CassTableMeta {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassKeyspaceMetaTableIterator(keyspace_meta_iterator) = iter {
        let iter_position = match keyspace_meta_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let table_meta_entry_opt = keyspace_meta_iterator
            .value
            .tables
            .iter()
            .nth(iter_position);

        return match table_meta_entry_opt {
            Some(table_meta_entry) => Arc::as_ptr(table_meta_entry.1),
            None => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type(
    iterator: *const CassIterator,
) -> *const CassDataType {
    let iter = ptr_to_ref(iterator);

    if let CassIterator::CassKeyspaceMetaUserTypeIterator(keyspace_meta_iterator) = iter {
        let iter_position = match keyspace_meta_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let udt_to_type_entry_opt = keyspace_meta_iterator
            .value
            .user_defined_type_data_type
            .iter()
            .nth(iter_position);

        return match udt_to_type_entry_opt {
            Some(udt_to_type_entry) => Arc::as_ptr(udt_to_type_entry.1),
            None => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column_meta(
    iterator: *const CassIterator,
) -> *const CassColumnMeta {
    let iter = ptr_to_ref(iterator);

    match iter {
        CassIterator::CassTableMetaIterator(table_meta_iterator) => {
            let iter_position = match table_meta_iterator.position {
                Some(pos) => pos,
                None => return std::ptr::null(),
            };

            let column_meta_entry_opt = table_meta_iterator
                .value
                .columns_metadata
                .iter()
                .nth(iter_position);

            match column_meta_entry_opt {
                Some(column_meta_entry) => column_meta_entry.1 as *const CassColumnMeta,
                None => std::ptr::null(),
            }
        }
        CassIterator::CassViewMetaIterator(view_meta_iterator) => {
            let iter_position = match view_meta_iterator.position {
                Some(pos) => pos,
                None => return std::ptr::null(),
            };

            let column_meta_entry_opt = view_meta_iterator
                .value
                .view_metadata
                .columns_metadata
                .iter()
                .nth(iter_position);

            match column_meta_entry_opt {
                Some(column_meta_entry) => column_meta_entry.1 as *const CassColumnMeta,
                None => std::ptr::null(),
            }
        }
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_materialized_view_meta(
    iterator: *const CassIterator,
) -> *const CassMaterializedViewMeta {
    let iter = ptr_to_ref(iterator);

    match iter {
        CassIterator::CassKeyspaceMetaViewIterator(keyspace_meta_iterator) => {
            let iter_position = match keyspace_meta_iterator.position {
                Some(pos) => pos,
                None => return std::ptr::null(),
            };

            let view_meta_entry_opt = keyspace_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => Arc::as_ptr(view_meta_entry.1),
                None => std::ptr::null(),
            }
        }
        CassIterator::CassTableMetaIterator(table_meta_iterator) => {
            let iter_position = match table_meta_iterator.position {
                Some(pos) => pos,
                None => return std::ptr::null(),
            };

            let view_meta_entry_opt = table_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => Arc::as_ptr(view_meta_entry.1),
                None => std::ptr::null(),
            }
        }
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_result(result: *const CassResult) -> *mut CassIterator {
    let result_from_raw = clone_arced(result);

    let iterator = CassResultIterator {
        result: result_from_raw,
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassResultIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_row(row: *const CassRow) -> *mut CassIterator {
    let row_from_raw = ptr_to_ref(row);

    let iterator = CassRowIterator {
        row: row_from_raw,
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassRowIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_collection(
    value: *const CassValue,
) -> *mut CassIterator {
    let is_collection = cass_value_is_collection(value) != 0;

    if value.is_null() || !is_collection {
        return std::ptr::null_mut();
    }

    let val = ptr_to_ref(value);
    let item_count = cass_value_item_count(value);
    let item_count = match cass_value_type(value) {
        CassValueType::CASS_VALUE_TYPE_MAP => item_count * 2,
        _ => item_count,
    };

    let iterator = CassCollectionIterator {
        value: val,
        count: item_count,
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassCollectionIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_tuple(value: *const CassValue) -> *mut CassIterator {
    let tuple = ptr_to_ref(value);

    if let Some(Value::CollectionValue(Collection::Tuple(val))) = &tuple.value {
        let item_count = val.len();
        let iterator = CassCollectionIterator {
            value: tuple,
            count: item_count as u64,
            position: None,
        };

        return Box::into_raw(Box::new(CassIterator::CassCollectionIterator(iterator)));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_map(value: *const CassValue) -> *mut CassIterator {
    let map = ptr_to_ref(value);

    if let Some(Value::CollectionValue(Collection::Map(val))) = &map.value {
        let item_count = val.len();
        let iterator = CassMapIterator {
            value: map,
            count: item_count as u64,
            position: None,
        };

        return Box::into_raw(Box::new(CassIterator::CassMapIterator(iterator)));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_fields_from_user_type(
    value: *const CassValue,
) -> *mut CassIterator {
    let udt = ptr_to_ref(value);

    if let Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) = &udt.value {
        let item_count = fields.len();
        let iterator = CassUdtIterator {
            value: udt,
            count: item_count as u64,
            position: None,
        };

        return Box::into_raw(Box::new(CassIterator::CassUdtIterator(iterator)));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_keyspaces_from_schema_meta(
    schema_meta: *const CassSchemaMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(schema_meta);

    let iterator = CassSchemaMetaIterator {
        value: metadata,
        count: metadata.keyspaces.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassSchemaMetaIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_tables_from_keyspace_meta(
    keyspace_meta: *const CassKeyspaceMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(keyspace_meta);

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.tables.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassKeyspaceMetaTableIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_keyspace_meta(
    keyspace_meta: *const CassKeyspaceMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(keyspace_meta);

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassKeyspaceMetaViewIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_user_types_from_keyspace_meta(
    keyspace_meta: *const CassKeyspaceMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(keyspace_meta);

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.user_defined_type_data_type.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassKeyspaceMetaUserTypeIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_columns_from_table_meta(
    table_meta: *const CassTableMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(table_meta);

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.columns_metadata.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassTableMetaIterator(iterator)))
}

pub unsafe extern "C" fn cass_iterator_materialized_views_from_table_meta(
    table_meta: *const CassTableMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(table_meta);

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassTableMetaIterator(iterator)))
}

pub unsafe extern "C" fn cass_iterator_columns_from_materialized_view_meta(
    view_meta: *const CassMaterializedViewMeta,
) -> *mut CassIterator {
    let metadata = ptr_to_ref(view_meta);

    let iterator = CassViewMetaIterator {
        value: metadata,
        count: metadata.view_metadata.columns_metadata.len(),
        position: None,
    };

    Box::into_raw(Box::new(CassIterator::CassViewMetaIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_free(result_raw: *const CassResult) {
    free_arced(result_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_has_more_pages(result: *const CassResult) -> cass_bool_t {
    let result = ptr_to_ref(result);
    (!result.paging_state_response.finished()) as cass_bool_t
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
    let mut name_str = ptr_to_cstr_n(name, name_length).unwrap();
    let mut is_case_sensitive = false;

    if name_str.starts_with('\"') && name_str.ends_with('\"') {
        name_str = name_str.strip_prefix('\"').unwrap();
        name_str = name_str.strip_suffix('\"').unwrap();
        is_case_sensitive = true;
    }

    return row_from_raw
        .result_metadata
        .col_specs
        .iter()
        .enumerate()
        .find(|(_, col_spec)| {
            is_case_sensitive && col_spec.name == name_str
                || !is_case_sensitive && col_spec.name.eq_ignore_ascii_case(name_str)
        })
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

    let column_name = &result_from_raw
        .metadata
        .col_specs
        .get(index_usize)
        .unwrap()
        .name;

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
    let result_from_raw: &CassResult = ptr_to_ref(result);
    let index_usize: usize = index
        .try_into()
        .expect("Provided index is out of bounds. Max possible value is usize::MAX");

    result_from_raw
        .metadata
        .col_specs
        .get(index_usize)
        .map(|col_spec| Arc::as_ptr(&col_spec.data_type))
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_type(value: *const CassValue) -> CassValueType {
    let value_from_raw = ptr_to_ref(value);

    cass_data_type_type(Arc::as_ptr(&value_from_raw.value_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_data_type(value: *const CassValue) -> *const CassDataType {
    let value_from_raw = ptr_to_ref(value);

    Arc::as_ptr(&value_from_raw.value_type)
}

macro_rules! val_ptr_to_ref_ensure_non_null {
    ($ptr:ident) => {{
        if $ptr.is_null() {
            return CassError::CASS_ERROR_LIB_NULL_VALUE;
        }
        ptr_to_ref($ptr)
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
    let val: &CassValue = ptr_to_ref(value);
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
    let val: &CassValue = ptr_to_ref(value);
    val.value.is_none() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_collection(value: *const CassValue) -> cass_bool_t {
    let val = ptr_to_ref(value);

    matches!(
        val.value_type.get_value_type(),
        CassValueType::CASS_VALUE_TYPE_LIST
            | CassValueType::CASS_VALUE_TYPE_SET
            | CassValueType::CASS_VALUE_TYPE_MAP
    ) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_duration(value: *const CassValue) -> cass_bool_t {
    let val = ptr_to_ref(value);

    (val.value_type.get_value_type() == CassValueType::CASS_VALUE_TYPE_DURATION) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_item_count(collection: *const CassValue) -> size_t {
    let val = ptr_to_ref(collection);

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
    let val = ptr_to_ref(collection);

    match val.value_type.as_ref() {
        CassDataType::List {
            typ: Some(list), ..
        } => list.get_value_type(),
        CassDataType::Set { typ: Some(set), .. } => set.get_value_type(),
        CassDataType::Map {
            typ: MapDataType::Key(key) | MapDataType::KeyAndValue(key, _),
            ..
        } => key.get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_secondary_sub_type(
    collection: *const CassValue,
) -> CassValueType {
    let val = ptr_to_ref(collection);

    match val.value_type.as_ref() {
        CassDataType::Map {
            typ: MapDataType::KeyAndValue(_, value),
            ..
        } => value.get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
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

    result
        .rows
        .as_ref()
        .and_then(|rows| rows.first())
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

    let result_from_raw = ptr_to_ref(result);

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
    use std::{ffi::c_char, ptr::addr_of_mut, sync::Arc};

    use scylla::{
        frame::response::result::{ColumnSpec, ColumnType, CqlValue, Row, TableSpec},
        transport::PagingStateResponse,
    };

    use crate::{
        cass_error::CassError,
        cass_types::{CassDataType, CassValueType},
        query_result::{
            cass_result_column_data_type, cass_result_column_name, cass_result_first_row,
            ptr_to_cstr_n, ptr_to_ref, size_t,
        },
        session::create_cass_rows_from_rows,
    };

    use super::{cass_result_column_count, cass_result_column_type, CassResult, CassResultData};

    fn col_spec(name: &'static str, typ: ColumnType<'static>) -> ColumnSpec<'static> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
    }

    const FIRST_COLUMN_NAME: &str = "bigint_col";
    const SECOND_COLUMN_NAME: &str = "varint_col";
    const THIRD_COLUMN_NAME: &str = "list_double_col";
    fn create_cass_rows_result() -> CassResult {
        let metadata = Arc::new(CassResultData::from_column_specs(&[
            col_spec(FIRST_COLUMN_NAME, ColumnType::BigInt),
            col_spec(SECOND_COLUMN_NAME, ColumnType::Varint),
            col_spec(
                THIRD_COLUMN_NAME,
                ColumnType::List(Box::new(ColumnType::Double)),
            ),
        ]));

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
            rows: Some(rows),
            metadata,
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
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
                let first_col_data_type = ptr_to_ref(cass_result_column_data_type(result_ptr, 0));
                assert_eq!(
                    &CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BIGINT),
                    first_col_data_type
                );
                let second_col_data_type = ptr_to_ref(cass_result_column_data_type(result_ptr, 1));
                assert_eq!(
                    &CassDataType::Value(CassValueType::CASS_VALUE_TYPE_VARINT),
                    second_col_data_type
                );
                let third_col_data_type = ptr_to_ref(cass_result_column_data_type(result_ptr, 2));
                assert_eq!(
                    &CassDataType::List {
                        typ: Some(Arc::new(CassDataType::Value(
                            CassValueType::CASS_VALUE_TYPE_DOUBLE
                        ))),
                        frozen: false
                    },
                    third_col_data_type
                );
                let out_of_bound_col_data_type = cass_result_column_data_type(result_ptr, 555);
                assert!(out_of_bound_col_data_type.is_null());
            }
        }
    }

    fn create_non_rows_cass_result() -> CassResult {
        let metadata = Arc::new(CassResultData::from_column_specs(&[]));
        CassResult {
            rows: None,
            metadata,
            tracing_id: None,
            paging_state_response: PagingStateResponse::NoMorePages,
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
extern "C" {
    pub fn cass_value_data_type(value: *const CassValue) -> *const CassDataType;
}
extern "C" {
    pub fn cass_value_type(value: *const CassValue) -> CassValueType;
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
