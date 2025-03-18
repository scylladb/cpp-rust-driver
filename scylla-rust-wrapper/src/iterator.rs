use scylla::deserialize::result::TypedRowIterator;
use scylla::value::Row;

use crate::argconv::{
    write_str_to_c, ArcFFI, BoxFFI, CConst, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr,
    CassOwnedExclusivePtr, FromBox, RefFFI, FFI,
};
use crate::cass_error::CassError;
use crate::cass_types::{CassDataType, CassValueType};
use crate::metadata::{
    CassColumnMeta, CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta, CassTableMeta,
};
use crate::query_result::{
    cass_value_is_collection, cass_value_item_count, cass_value_type, CassResult, CassResultKind,
    CassResultMetadata, CassRow, CassValue, Collection, Value,
};
use crate::types::{cass_bool_t, size_t};

pub use crate::cass_iterator_types::CassIteratorType;

use std::os::raw::c_char;

pub struct CassRowsResultIterator<'result> {
    iterator: TypedRowIterator<'result, 'result, Row>,
    result_metadata: &'result CassResultMetadata,
    current_row: Option<CassRow<'result>>,
}

pub enum CassResultIterator<'result> {
    NonRows,
    Rows(CassRowsResultIterator<'result>),
}

pub struct CassRowIterator<'result> {
    row: &'result CassRow<'result>,
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

/// An iterator over columns metadata.
/// Can be constructed from either table ([`cass_iterator_columns_from_table_meta()`])
/// or view metadata ([`cass_iterator_columns_from_materialized_view_meta()`]).
/// To be used by [`cass_iterator_get_column_meta()`].
pub enum CassColumnsMetaIterator<'schema> {
    FromTable(CassTableMetaIterator<'schema>),
    FromView(CassViewMetaIterator<'schema>),
}

/// An iterator over materialized views.
/// Can be constructed from either keyspace ([`cass_iterator_materialized_views_from_keyspace_meta()`])
/// or table ([`cass_iterator_materialized_views_from_table_meta()`]) metadata.
/// To be used by [`cass_iterator_get_materialized_view_meta()`].
pub enum CassMaterializedViewsMetaIterator<'schema> {
    FromKeyspace(CassKeyspaceMetaIterator<'schema>),
    FromTable(CassTableMetaIterator<'schema>),
}

pub enum CassIterator<'result_or_schema> {
    // Iterators derived from CassResult.
    // Naming convention of the variants: the name of the collection.
    /// Iterator over rows in a result.
    Result(CassResultIterator<'result_or_schema>),
    /// Iterator over columns (values) in a row.
    Row(CassRowIterator<'result_or_schema>),
    /// Iterator over values in a collection.
    Collection(CassCollectionIterator<'result_or_schema>),
    /// Iterator over key-value pairs in a map.
    Map(CassMapIterator<'result_or_schema>),
    /// Iterator over values in a tuple.
    Tuple(CassCollectionIterator<'result_or_schema>),

    // Iterators derived from CassSchemaMeta.
    // Naming convention of the variants: name of item in the collection (plural).
    /// Iterator over fields in UDT.
    UdtFields(CassUdtIterator<'result_or_schema>),
    /// Iterator over keyspaces in schema metadata.
    KeyspacesMeta(CassSchemaMetaIterator<'result_or_schema>),
    /// Iterator over tables in keyspace metadata.
    TablesMeta(CassKeyspaceMetaIterator<'result_or_schema>),
    /// Iterator over UDTs in keyspace metadata.
    UserTypes(CassKeyspaceMetaIterator<'result_or_schema>),
    /// Iterator over materialized views in either keyspace or table metadata.
    MaterializedViewsMeta(CassMaterializedViewsMetaIterator<'result_or_schema>),
    /// Iterator over columns metadata in either table or view metadata.
    ColumnsMeta(CassColumnsMetaIterator<'result_or_schema>),
}

impl FFI for CassIterator<'_> {
    type Origin = FromBox;
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_free(iterator: CassOwnedExclusivePtr<CassIterator, CMut>) {
    BoxFFI::free(iterator);
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_type(
    iterator: CassBorrowedExclusivePtr<CassIterator, CMut>,
) -> CassIteratorType {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    match iter {
        CassIterator::Result(_) => CassIteratorType::CASS_ITERATOR_TYPE_RESULT,
        CassIterator::Row(_) => CassIteratorType::CASS_ITERATOR_TYPE_ROW,
        CassIterator::Collection(_) => CassIteratorType::CASS_ITERATOR_TYPE_COLLECTION,
        CassIterator::Map(_) => CassIteratorType::CASS_ITERATOR_TYPE_MAP,
        CassIterator::Tuple(_) => CassIteratorType::CASS_ITERATOR_TYPE_TUPLE,
        CassIterator::UdtFields(_) => CassIteratorType::CASS_ITERATOR_TYPE_USER_TYPE_FIELD,
        CassIterator::KeyspacesMeta(_) => CassIteratorType::CASS_ITERATOR_TYPE_KEYSPACE_META,
        CassIterator::TablesMeta(_) => CassIteratorType::CASS_ITERATOR_TYPE_TABLE_META,
        CassIterator::UserTypes(_) => CassIteratorType::CASS_ITERATOR_TYPE_TYPE_META,
        CassIterator::MaterializedViewsMeta(_) => {
            CassIteratorType::CASS_ITERATOR_TYPE_MATERIALIZED_VIEW_META
        }
        CassIterator::ColumnsMeta(_) => CassIteratorType::CASS_ITERATOR_TYPE_COLUMN_META,
    }
}

// After creating an iterator we have to call next() before accessing the value
#[no_mangle]
pub unsafe extern "C" fn cass_iterator_next(
    iterator: CassBorrowedExclusivePtr<CassIterator, CMut>,
) -> cass_bool_t {
    let mut iter = BoxFFI::as_mut_ref(iterator).unwrap();

    match &mut iter {
        CassIterator::Result(result_iterator) => {
            let CassResultIterator::Rows(rows_result_iterator) = result_iterator else {
                return false as cass_bool_t;
            };

            let new_row = rows_result_iterator
                .iterator
                .next()
                .and_then(|res| match res {
                    Ok(row) => Some(row),
                    Err(e) => {
                        // We have no way to propagate the error (return type is bool).
                        // Let's at least log the deserialization error.
                        tracing::error!("Failed to deserialize next row: {e}");
                        None
                    }
                })
                .map(|row| {
                    CassRow::from_row_and_metadata(row, rows_result_iterator.result_metadata)
                });

            rows_result_iterator.current_row = new_row;

            rows_result_iterator.current_row.is_some() as cass_bool_t
        }
        CassIterator::Row(row_iterator) => {
            let new_pos: usize = row_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            row_iterator.position = Some(new_pos);

            (new_pos < row_iterator.row.columns.len()) as cass_bool_t
        }
        CassIterator::Collection(collection_iterator)
        | CassIterator::Tuple(collection_iterator) => {
            let new_pos: usize = collection_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            collection_iterator.position = Some(new_pos);

            (new_pos < collection_iterator.count.try_into().unwrap()) as cass_bool_t
        }
        CassIterator::Map(map_iterator) => {
            let new_pos: usize = map_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            map_iterator.position = Some(new_pos);

            (new_pos < map_iterator.count.try_into().unwrap()) as cass_bool_t
        }
        CassIterator::UdtFields(udt_iterator) => {
            let new_pos: usize = udt_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            udt_iterator.position = Some(new_pos);

            (new_pos < udt_iterator.count.try_into().unwrap()) as cass_bool_t
        }
        CassIterator::KeyspacesMeta(schema_meta_iterator) => {
            let new_pos: usize = schema_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            schema_meta_iterator.position = Some(new_pos);

            (new_pos < schema_meta_iterator.count) as cass_bool_t
        }
        CassIterator::TablesMeta(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::UserTypes(keyspace_meta_iterator) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::MaterializedViewsMeta(CassMaterializedViewsMetaIterator::FromKeyspace(
            keyspace_meta_iterator,
        )) => {
            let new_pos: usize = keyspace_meta_iterator
                .position
                .map_or(0, |prev_pos| prev_pos + 1);

            keyspace_meta_iterator.position = Some(new_pos);

            (new_pos < keyspace_meta_iterator.count) as cass_bool_t
        }
        CassIterator::MaterializedViewsMeta(CassMaterializedViewsMetaIterator::FromTable(
            table_iterator,
        ))
        | CassIterator::ColumnsMeta(CassColumnsMetaIterator::FromTable(table_iterator)) => {
            let new_pos: usize = table_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            table_iterator.position = Some(new_pos);

            (new_pos < table_iterator.count) as cass_bool_t
        }
        CassIterator::ColumnsMeta(CassColumnsMetaIterator::FromView(view_iterator)) => {
            let new_pos: usize = view_iterator.position.map_or(0, |prev_pos| prev_pos + 1);

            view_iterator.position = Some(new_pos);

            (new_pos < view_iterator.count) as cass_bool_t
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_row<'result>(
    iterator: CassBorrowedSharedPtr<'result, CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassRow<'result>, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    // Defined only for result iterator, for other types should return null
    let CassIterator::Result(CassResultIterator::Rows(rows_result_iterator)) = iter else {
        return RefFFI::null();
    };

    rows_result_iterator
        .current_row
        .as_ref()
        .map(RefFFI::as_ptr)
        .unwrap_or(RefFFI::null())
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column<'result>(
    iterator: CassBorrowedSharedPtr<CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    // Defined only for row iterator, for other types should return null
    if let CassIterator::Row(row_iterator) = iter {
        let iter_position = match row_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let value = match row_iterator.row.columns.get(iter_position) {
            Some(col) => col,
            None => return RefFFI::null(),
        };

        return RefFFI::as_ptr(value);
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_value<'result>(
    iterator: CassBorrowedSharedPtr<CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    // Defined only for collections(list, set and map) or tuple iterator, for other types should return null
    if let CassIterator::Collection(collection_iterator)
    | CassIterator::Tuple(collection_iterator) = iter
    {
        let iter_position = match collection_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
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
            _ => return RefFFI::null(),
        };

        if value.is_none() {
            return RefFFI::null();
        }

        return RefFFI::as_ptr(value.unwrap());
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_key<'result>(
    iterator: CassBorrowedSharedPtr<CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::Map(map_iterator) = iter {
        let iter_position = match map_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let entry = match &map_iterator.value.value {
            Some(Value::CollectionValue(Collection::Map(map))) => map.get(iter_position),
            _ => return RefFFI::null(),
        };

        if entry.is_none() {
            return RefFFI::null();
        }

        return RefFFI::as_ptr(&entry.unwrap().0);
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_value<'result>(
    iterator: CassBorrowedSharedPtr<CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::Map(map_iterator) = iter {
        let iter_position = match map_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let entry = match &map_iterator.value.value {
            Some(Value::CollectionValue(Collection::Map(map))) => map.get(iter_position),
            _ => return RefFFI::null(),
        };

        if entry.is_none() {
            return RefFFI::null();
        }

        return RefFFI::as_ptr(&entry.unwrap().1);
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_name(
    iterator: CassBorrowedSharedPtr<CassIterator, CConst>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::UdtFields(udt_iterator) = iter {
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
                unsafe { write_str_to_c(field_name.as_str(), name, name_length) };
            }
            None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
        }

        return CassError::CASS_OK;
    }

    CassError::CASS_ERROR_LIB_BAD_PARAMS
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_value<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassValue, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::UdtFields(udt_iterator) = iter {
        let iter_position = match udt_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let udt_entry_opt = match &udt_iterator.value.value {
            Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) => {
                fields.get(iter_position)
            }
            _ => return RefFFI::null(),
        };

        return match udt_entry_opt {
            Some(udt_entry) => match &udt_entry.1 {
                Some(value) => RefFFI::as_ptr(value),
                None => RefFFI::null(),
            },
            None => RefFFI::null(),
        };
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_keyspace_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::KeyspacesMeta(schema_meta_iterator) = iter {
        let iter_position = match schema_meta_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let schema_meta_entry_opt = &schema_meta_iterator
            .value
            .keyspaces
            .iter()
            .nth(iter_position);

        return match schema_meta_entry_opt {
            Some(schema_meta_entry) => RefFFI::as_ptr(schema_meta_entry.1),
            None => RefFFI::null(),
        };
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_table_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassTableMeta, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::TablesMeta(keyspace_meta_iterator) = iter {
        let iter_position = match keyspace_meta_iterator.position {
            Some(pos) => pos,
            None => return RefFFI::null(),
        };

        let table_meta_entry_opt = keyspace_meta_iterator
            .value
            .tables
            .iter()
            .nth(iter_position);

        return match table_meta_entry_opt {
            Some(table_meta_entry) => RefFFI::as_ptr(table_meta_entry.1.as_ref()),
            None => RefFFI::null(),
        };
    }

    RefFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassDataType, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    if let CassIterator::UserTypes(keyspace_meta_iterator) = iter {
        let iter_position = match keyspace_meta_iterator.position {
            Some(pos) => pos,
            None => return ArcFFI::null(),
        };

        let udt_to_type_entry_opt = keyspace_meta_iterator
            .value
            .user_defined_type_data_type
            .iter()
            .nth(iter_position);

        return match udt_to_type_entry_opt {
            Some(udt_to_type_entry) => ArcFFI::as_ptr(udt_to_type_entry.1),
            None => ArcFFI::null(),
        };
    }

    ArcFFI::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassColumnMeta, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    match iter {
        CassIterator::ColumnsMeta(CassColumnsMetaIterator::FromTable(table_meta_iterator)) => {
            let iter_position = match table_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let column_meta_entry_opt = table_meta_iterator
                .value
                .columns_metadata
                .iter()
                .nth(iter_position);

            match column_meta_entry_opt {
                Some(column_meta_entry) => RefFFI::as_ptr(column_meta_entry.1),
                None => RefFFI::null(),
            }
        }
        CassIterator::ColumnsMeta(CassColumnsMetaIterator::FromView(view_meta_iterator)) => {
            let iter_position = match view_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let column_meta_entry_opt = view_meta_iterator
                .value
                .view_metadata
                .columns_metadata
                .iter()
                .nth(iter_position);

            match column_meta_entry_opt {
                Some(column_meta_entry) => RefFFI::as_ptr(column_meta_entry.1),
                None => RefFFI::null(),
            }
        }
        _ => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_materialized_view_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassMaterializedViewMeta, CConst> {
    let iter = BoxFFI::as_ref(iterator).unwrap();

    match iter {
        CassIterator::MaterializedViewsMeta(CassMaterializedViewsMetaIterator::FromKeyspace(
            keyspace_meta_iterator,
        )) => {
            let iter_position = match keyspace_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let view_meta_entry_opt = keyspace_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => RefFFI::as_ptr(view_meta_entry.1.as_ref()),
                None => RefFFI::null(),
            }
        }
        CassIterator::MaterializedViewsMeta(CassMaterializedViewsMetaIterator::FromTable(
            table_meta_iterator,
        )) => {
            let iter_position = match table_meta_iterator.position {
                Some(pos) => pos,
                None => return RefFFI::null(),
            };

            let view_meta_entry_opt = table_meta_iterator.value.views.iter().nth(iter_position);

            match view_meta_entry_opt {
                Some(view_meta_entry) => RefFFI::as_ptr(view_meta_entry.1.as_ref()),
                None => RefFFI::null(),
            }
        }
        _ => RefFFI::null(),
    }
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_result<'result>(
    result: CassBorrowedSharedPtr<'result, CassResult, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let result_from_raw = ArcFFI::as_ref(result).unwrap();

    let iterator = match &result_from_raw.kind {
        CassResultKind::NonRows => CassResultIterator::NonRows,
        CassResultKind::Rows(cass_rows_result) => {
            CassResultIterator::Rows(CassRowsResultIterator {
                // unwrap: Row always passes the typecheck.
                iterator: cass_rows_result.raw_rows.rows_iter::<Row>().unwrap(),
                result_metadata: &cass_rows_result.metadata,
                current_row: None,
            })
        }
    };

    BoxFFI::into_ptr(Box::new(CassIterator::Result(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_row<'result>(
    row: CassBorrowedSharedPtr<'result, CassRow<'result>, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let row_from_raw = RefFFI::as_ref(row).unwrap();

    let iterator = CassRowIterator {
        row: row_from_raw,
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::Row(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_collection<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let is_collection = unsafe { cass_value_is_collection(value.borrow()) } != 0;

    if RefFFI::is_null(&value) || !is_collection {
        return BoxFFI::null_mut();
    }

    let item_count = unsafe { cass_value_item_count(value.borrow()) };
    let item_count = match unsafe { cass_value_type(value.borrow()) } {
        CassValueType::CASS_VALUE_TYPE_MAP => item_count * 2,
        _ => item_count,
    };
    let val = RefFFI::as_ref(value).unwrap();

    let iterator = CassCollectionIterator {
        value: val,
        count: item_count,
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::Collection(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_tuple<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let tuple = RefFFI::as_ref(value).unwrap();

    if let Some(Value::CollectionValue(Collection::Tuple(val))) = &tuple.value {
        let item_count = val.len();
        let iterator = CassCollectionIterator {
            value: tuple,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::Tuple(iterator)));
    }

    BoxFFI::null_mut()
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_map<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let map = RefFFI::as_ref(value).unwrap();

    if let Some(Value::CollectionValue(Collection::Map(val))) = &map.value {
        let item_count = val.len();
        let iterator = CassMapIterator {
            value: map,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::Map(iterator)));
    }

    BoxFFI::null_mut()
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_fields_from_user_type<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let udt = RefFFI::as_ref(value).unwrap();

    if let Some(Value::CollectionValue(Collection::UserDefinedType { fields, .. })) = &udt.value {
        let item_count = fields.len();
        let iterator = CassUdtIterator {
            value: udt,
            count: item_count as u64,
            position: None,
        };

        return BoxFFI::into_ptr(Box::new(CassIterator::UdtFields(iterator)));
    }

    BoxFFI::null_mut()
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_keyspaces_from_schema_meta<'schema>(
    schema_meta: CassBorrowedSharedPtr<'schema, CassSchemaMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = BoxFFI::as_ref(schema_meta).unwrap();

    let iterator = CassSchemaMetaIterator {
        value: metadata,
        count: metadata.keyspaces.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::KeyspacesMeta(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_tables_from_keyspace_meta<'schema>(
    keyspace_meta: CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = RefFFI::as_ref(keyspace_meta).unwrap();

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.tables.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::TablesMeta(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_keyspace_meta<'schema>(
    keyspace_meta: CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = RefFFI::as_ref(keyspace_meta).unwrap();

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::MaterializedViewsMeta(
        CassMaterializedViewsMetaIterator::FromKeyspace(iterator),
    )))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_user_types_from_keyspace_meta<'schema>(
    keyspace_meta: CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = RefFFI::as_ref(keyspace_meta).unwrap();

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.user_defined_type_data_type.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::UserTypes(iterator)))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_columns_from_table_meta<'schema>(
    table_meta: CassBorrowedSharedPtr<'schema, CassTableMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = RefFFI::as_ref(table_meta).unwrap();

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::ColumnsMeta(
        CassColumnsMetaIterator::FromTable(iterator),
    )))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_table_meta<'schema>(
    table_meta: CassBorrowedSharedPtr<'schema, CassTableMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = RefFFI::as_ref(table_meta).unwrap();

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::MaterializedViewsMeta(
        CassMaterializedViewsMetaIterator::FromTable(iterator),
    )))
}

#[no_mangle]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_columns_from_materialized_view_meta<'schema>(
    view_meta: CassBorrowedSharedPtr<'schema, CassMaterializedViewMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let metadata = RefFFI::as_ref(view_meta).unwrap();

    let iterator = CassViewMetaIterator {
        value: metadata,
        count: metadata.view_metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::ColumnsMeta(
        CassColumnsMetaIterator::FromView(iterator),
    )))
}
