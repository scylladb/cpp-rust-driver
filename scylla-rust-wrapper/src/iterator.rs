use crate::argconv::{write_str_to_c, ArcFFI, BoxFFI, RefFFI};
use crate::cass_error::CassError;
use crate::cass_types::{CassDataType, CassValueType};
use crate::metadata::{
    CassColumnMeta, CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta, CassTableMeta,
};
use crate::query_result::{
    cass_value_is_collection, cass_value_item_count, cass_value_type, CassResult, CassResultKind,
    CassRow, CassRowsResult, CassValue, Collection, Value,
};
use crate::types::{cass_bool_t, size_t};

use std::os::raw::c_char;

pub struct CassResultIterator<'result> {
    result: &'result CassResult,
    position: Option<usize>,
}

pub struct CassRowIterator<'result> {
    row: &'result CassRow,
    position: Option<usize>,
}

pub struct CassCollectionIterator<'result> {
    value: &'result CassValue,
    count: u64,
    position: Option<usize>,
}

pub struct CassMapIterator<'result> {
    value: &'result CassValue,
    count: u64,
    position: Option<usize>,
}

pub struct CassUdtIterator<'result> {
    value: &'result CassValue,
    count: u64,
    position: Option<usize>,
}

pub struct CassSchemaMetaIterator<'schema> {
    value: &'schema CassSchemaMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassKeyspaceMetaIterator<'schema> {
    value: &'schema CassKeyspaceMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassTableMetaIterator<'schema> {
    value: &'schema CassTableMeta,
    count: usize,
    position: Option<usize>,
}

pub struct CassViewMetaIterator<'schema> {
    value: &'schema CassMaterializedViewMeta,
    count: usize,
    position: Option<usize>,
}

pub enum CassIterator<'result_or_schema> {
    CassResultIterator(CassResultIterator<'result_or_schema>),
    CassRowIterator(CassRowIterator<'result_or_schema>),
    CassCollectionIterator(CassCollectionIterator<'result_or_schema>),
    CassMapIterator(CassMapIterator<'result_or_schema>),
    CassUdtIterator(CassUdtIterator<'result_or_schema>),
    CassSchemaMetaIterator(CassSchemaMetaIterator<'result_or_schema>),
    CassKeyspaceMetaTableIterator(CassKeyspaceMetaIterator<'result_or_schema>),
    CassKeyspaceMetaUserTypeIterator(CassKeyspaceMetaIterator<'result_or_schema>),
    CassKeyspaceMetaViewIterator(CassKeyspaceMetaIterator<'result_or_schema>),
    CassTableMetaIterator(CassTableMetaIterator<'result_or_schema>),
    CassViewMetaIterator(CassViewMetaIterator<'result_or_schema>),
}

impl BoxFFI for CassIterator<'_> {}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_free(iterator: *mut CassIterator) {
    BoxFFI::free(iterator);
}

// After creating an iterator we have to call next() before accessing the value
#[no_mangle]
pub unsafe extern "C" fn cass_iterator_next(iterator: *mut CassIterator) -> cass_bool_t {
    let mut iter = BoxFFI::as_mut_ref(iterator);

    match &mut iter {
        CassIterator::CassResultIterator(result_iterator) => {
            let new_pos: usize = result_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            result_iterator.position = Some(new_pos);

            match &result_iterator.result.kind {
                CassResultKind::Rows(rows_result) => {
                    (new_pos < rows_result.rows.len()) as cass_bool_t
                }
                CassResultKind::NonRows => false as cass_bool_t,
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
    let iter = BoxFFI::as_ref(iterator);

    // Defined only for result iterator, for other types should return null
    if let CassIterator::CassResultIterator(result_iterator) = iter {
        let iter_position = match result_iterator.position {
            Some(pos) => pos,
            None => return std::ptr::null(),
        };

        let CassResultKind::Rows(CassRowsResult { rows, .. }) = &result_iterator.result.kind else {
            return std::ptr::null();
        };

        let row: &CassRow = match rows.get(iter_position) {
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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

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
            Some(table_meta_entry) => RefFFI::as_ptr(table_meta_entry.1.as_ref()),
            None => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type(
    iterator: *const CassIterator,
) -> *const CassDataType {
    let iter = BoxFFI::as_ref(iterator);

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
            Some(udt_to_type_entry) => ArcFFI::as_ptr(udt_to_type_entry.1),
            None => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column_meta(
    iterator: *const CassIterator,
) -> *const CassColumnMeta {
    let iter = BoxFFI::as_ref(iterator);

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
    let iter = BoxFFI::as_ref(iterator);

    match iter {
        CassIterator::CassKeyspaceMetaViewIterator(keyspace_meta_iterator) => {
            let iter_position = match keyspace_meta_iterator.position {
                Some(pos) => pos,
                None => return std::ptr::null(),
            };

            let view_meta_entry_opt = keyspace_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => RefFFI::as_ptr(view_meta_entry.1.as_ref()),
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
                Some(view_meta_entry) => RefFFI::as_ptr(view_meta_entry.1.as_ref()),
                None => std::ptr::null(),
            }
        }
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_result<'result>(
    result: *const CassResult,
) -> *mut CassIterator<'result> {
    let result_from_raw = ArcFFI::as_ref(result);

    let iterator = CassResultIterator {
        result: result_from_raw,
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassResultIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_row<'result>(
    row: *const CassRow,
) -> *mut CassIterator<'result> {
    let row_from_raw = RefFFI::as_ref(row);

    let iterator = CassRowIterator {
        row: row_from_raw,
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassRowIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_collection<'result>(
    value: *const CassValue,
) -> *mut CassIterator<'result> {
    let is_collection = cass_value_is_collection(value) != 0;

    if value.is_null() || !is_collection {
        return std::ptr::null_mut();
    }

    let val = RefFFI::as_ref(value);
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

    BoxFFI::into_ptr(Box::new(CassIterator::CassCollectionIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_tuple<'result>(
    value: *const CassValue,
) -> *mut CassIterator<'result> {
    let tuple = RefFFI::as_ref(value);

    if let Some(Value::CollectionValue(Collection::Tuple(val))) = &tuple.value {
        let item_count = val.len();
        let iterator = CassCollectionIterator {
            value: tuple,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::CassCollectionIterator(iterator)));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_map<'result>(
    value: *const CassValue,
) -> *mut CassIterator<'result> {
    let map = RefFFI::as_ref(value);

    if let Some(Value::CollectionValue(Collection::Map(val))) = &map.value {
        let item_count = val.len();
        let iterator = CassMapIterator {
            value: map,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::CassMapIterator(iterator)));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_fields_from_user_type<'result>(
    value: *const CassValue,
) -> *mut CassIterator<'result> {
    let udt = RefFFI::as_ref(value);

    if let Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) = &udt.value {
        let item_count = fields.len();
        let iterator = CassUdtIterator {
            value: udt,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::CassUdtIterator(iterator)));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_keyspaces_from_schema_meta<'schema>(
    schema_meta: *const CassSchemaMeta,
) -> *mut CassIterator<'schema> {
    let metadata = BoxFFI::as_ref(schema_meta);

    let iterator = CassSchemaMetaIterator {
        value: metadata,
        count: metadata.keyspaces.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassSchemaMetaIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_tables_from_keyspace_meta<'schema>(
    keyspace_meta: *const CassKeyspaceMeta,
) -> *mut CassIterator<'schema> {
    let metadata = RefFFI::as_ref(keyspace_meta);

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.tables.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassKeyspaceMetaTableIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_keyspace_meta<'schema>(
    keyspace_meta: *const CassKeyspaceMeta,
) -> *mut CassIterator<'schema> {
    let metadata = RefFFI::as_ref(keyspace_meta);

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassKeyspaceMetaViewIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_user_types_from_keyspace_meta<'schema>(
    keyspace_meta: *const CassKeyspaceMeta,
) -> *mut CassIterator<'schema> {
    let metadata = RefFFI::as_ref(keyspace_meta);

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.user_defined_type_data_type.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassKeyspaceMetaUserTypeIterator(
        iterator,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_columns_from_table_meta<'schema>(
    table_meta: *const CassTableMeta,
) -> *mut CassIterator<'schema> {
    let metadata = RefFFI::as_ref(table_meta);

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassTableMetaIterator(iterator)))
}

pub unsafe extern "C" fn cass_iterator_materialized_views_from_table_meta<'schema>(
    table_meta: *const CassTableMeta,
) -> *mut CassIterator<'schema> {
    let metadata = RefFFI::as_ref(table_meta);

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassTableMetaIterator(iterator)))
}

pub unsafe extern "C" fn cass_iterator_columns_from_materialized_view_meta<'schema>(
    view_meta: *const CassMaterializedViewMeta,
) -> *mut CassIterator<'schema> {
    let metadata = RefFFI::as_ref(view_meta);

    let iterator = CassViewMetaIterator {
        value: metadata,
        count: metadata.view_metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::CassViewMetaIterator(iterator)))
}
