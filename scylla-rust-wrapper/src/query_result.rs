use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::{cass_data_type_type, get_column_type, CassDataType, CassValueType};
use crate::inet::CassInet;
use crate::metadata::{
    CassColumnMeta, CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta, CassTableMeta,
};
use crate::types::*;
use crate::uuid::CassUuid;
use num_traits::Zero;
use scylla::frame::frame_errors::ParseError;
use scylla::frame::response::result::{ColumnSpec, ColumnType};
use scylla::frame::types;
use scylla::frame::value::{CqlDuration, Date};
use scylla::types::deserialize::row::ColumnIterator;
use scylla::types::deserialize::value::{
    DeserializeCql, MapIterator, SequenceIterator, UdtIterator,
};
use scylla::types::deserialize::FrameSlice;
use scylla::QueryResult;
use std::convert::TryInto;
use std::net::IpAddr;
use std::os::raw::c_char;
use std::sync::{Arc, Weak};
use uuid::Uuid;

pub struct CassResult {
    pub result: Arc<QueryResult>,
    pub first_row: CassRow,
}

/// The lifetime of CassRow is bound to CassResult.
/// It will be freed, when CassResult is freed.(see #[cass_result_free])
pub struct CassRow {
    pub result: Weak<CassResult>,
    pub columns: Vec<CassValue>,
}

pub struct RawValue<'frame> {
    pub spec: &'frame ColumnType,
    pub slice: Option<FrameSlice<'frame>>,
}

impl<'frame> DeserializeCql<'frame> for RawValue<'frame> {
    fn type_check(_typ: &ColumnType) -> Result<(), ParseError> {
        // Raw bytes can be returned for all types
        Ok(())
    }

    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError> {
        Ok(RawValue {
            spec: typ,
            slice: v,
        })
    }
}

#[derive(Clone)]
pub struct CassValue {
    /// Represents a raw, unparsed column value.
    pub frame_slice: Option<FrameSlice<'static>>,
    pub is_null: bool,
    pub count: usize,
    pub value_type: Arc<CassDataType>,
    pub column_type: &'static ColumnType,
}

enum CassIteratorStateInfo<T> {
    NoValue,
    ValueNoPosition { value: T },
    PositionNoValue { position: usize },
    Value { value: T, position: usize },
}

impl<T> CassIteratorStateInfo<T> {
    /// Update iterator's state and return new state info.
    /// Increments an existing position, otherwise sets it to 0.
    fn advance(&mut self) {
        // Store a dummy NoValue temporarily as we cannot move out StateInfo fields.
        let old_state_info = std::mem::replace(self, CassIteratorStateInfo::NoValue);
        *self = match old_state_info {
            CassIteratorStateInfo::Value { value, position } => CassIteratorStateInfo::Value {
                value,
                position: position + 1,
            },
            CassIteratorStateInfo::PositionNoValue { .. } => {
                panic!("Cannot advance the iterator. Value is empty!")
            }
            CassIteratorStateInfo::ValueNoPosition { value } => {
                CassIteratorStateInfo::Value { value, position: 0 }
            }
            CassIteratorStateInfo::NoValue => {
                CassIteratorStateInfo::PositionNoValue { position: 0 }
            }
        };
    }

    /// Update iterator's state and return new state info.
    /// Sets or replaces the old value with the `new_value` without changing the position.
    fn update_value(&mut self, new_value: T) {
        // Store a dummy NoValue temporarily as we cannot move state_info fields.
        let old_state_info = std::mem::replace(self, CassIteratorStateInfo::NoValue);
        *self = match old_state_info {
            CassIteratorStateInfo::Value { position, .. } => CassIteratorStateInfo::Value {
                value: new_value,
                position,
            },
            CassIteratorStateInfo::PositionNoValue { position } => CassIteratorStateInfo::Value {
                value: new_value,
                position,
            },
            CassIteratorStateInfo::ValueNoPosition { .. } => CassIteratorStateInfo::Value {
                value: new_value,
                position: 0,
            },
            CassIteratorStateInfo::NoValue => CassIteratorStateInfo::Value {
                value: new_value,
                position: 0,
            },
        };
    }
}

pub struct CassResultIterator {
    result: Arc<CassResult>,
    state_info: CassIteratorStateInfo<CassRow>,
}

pub struct CassRowIterator {
    state_info: CassIteratorStateInfo<&'static CassRow>,
}

/// For sequential iteration over collection types
pub enum CassCollectionIterator {
    SequenceIterator(CassSequenceIterator),
    SeqMapIterator(CassMapIterator),
}

pub struct CassSequenceIterator {
    sequence_iterator: SequenceIterator<'static, RawValue<'static>>,
    count: usize,
    state_info: CassIteratorStateInfo<CassValue>,
}

pub struct CassTupleIterator {
    sequence_iterator: SequenceIterator<'static, RawValue<'static>>,
    count: usize,
    state_info: CassIteratorStateInfo<CassValue>,
}

pub struct CassMapIterator {
    map_iterator: MapIterator<'static, RawValue<'static>, RawValue<'static>>,
    count: usize,
    state_info: CassIteratorStateInfo<(CassValue, CassValue)>, // (key, value)
}

pub struct CassUdtIterator {
    udt_iterator: UdtIterator<'static>,
    count: usize,
    state_info: CassIteratorStateInfo<(String, CassValue)>, // (field_name, field_value)
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
    CassTupleIterator(CassTupleIterator),
    CassMapIterator(CassMapIterator),
    CassUdtIterator(CassUdtIterator),
    CassSchemaMetaIterator(CassSchemaMetaIterator),
    CassKeyspaceMetaTableIterator(CassKeyspaceMetaIterator),
    CassKeyspaceMetaUserTypeIterator(CassKeyspaceMetaIterator),
    CassKeyspaceMetaViewIterator(CassKeyspaceMetaIterator),
    CassTableMetaIterator(CassTableMetaIterator),
    CassViewMetaIterator(CassViewMetaIterator),
}

fn decode_next_row(result: &'static CassResult, row: &mut CassRow) -> bool {
    // Errors are ignored, but logging them may come in handy in the future.
    let mut rows_iter = unwrap_or_return_false!(result.result.rows::<ColumnIterator>());
    let next_cols_iter = unwrap_or_return_false!(rows_iter.next().unwrap());

    for (i, raw_col) in next_cols_iter.into_iter().enumerate() {
        let raw_col = unwrap_or_return_false!(raw_col);
        let raw_value = RawValue {
            spec: &raw_col.spec.typ,
            slice: raw_col.slice,
        };
        let cass_value = decode_value(raw_value, &raw_col.spec.typ);
        match cass_value {
            Some(value) => {
                // Below assignment is safe from out of bounds panic, as
                // first [decode_first_row] call already initialized the columns vec
                row.columns[i] = value;
            }
            _ => return false,
        }
    }

    true
}

pub fn decode_value(
    raw_value: RawValue<'static>,
    val_type: &'static ColumnType,
) -> Option<CassValue> {
    let data_type = get_column_type(val_type);
    let frame_slice = raw_value.slice;
    let is_null = frame_slice.map_or(true, |f| f.is_empty());
    let mut count = 0;

    match frame_slice {
        Some(frame) if data_type.is_collection() => {
            // Frame is immutable, reading value len will not modify mem
            let mut mem = frame.as_slice();
            match types::read_int_length(&mut mem) {
                Ok(len) => count = len,
                Err(_) => return None,
            }
        }
        Some(_frame) if data_type.is_user_type() => {
            count = data_type.get_udt_type().field_types.len();
        }
        Some(_frame) if data_type.is_tuple() => count = data_type.get_tuple_types().len(),
        _ => {}
    }

    let cass_value = CassValue {
        frame_slice,
        is_null,
        count,
        value_type: Arc::new(data_type),
        column_type: val_type,
    };

    Some(cass_value)
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_free(iterator: *mut CassIterator) {
    free_boxed(iterator);
}

// After creating an iterator we have to call next() before accessing the value
#[no_mangle]
pub unsafe extern "C" fn cass_iterator_next(iterator: *mut CassIterator) -> cass_bool_t {
    let iter: &mut CassIterator = ptr_to_ref_mut(iterator);

    match iter {
        CassIterator::CassResultIterator(result_iterator) => {
            result_iterator.state_info.advance();

            if let CassIteratorStateInfo::Value { value, position } =
                &mut result_iterator.state_info
            {
                return match result_iterator.result.result.rows_num() {
                    Some(rs) if *position < rs => {
                        decode_next_row(result_iterator.result.as_ref(), value) as cass_bool_t
                    }
                    _ => false as cass_bool_t,
                };
            }

            false as cass_bool_t
        }
        CassIterator::CassRowIterator(row_iterator) => {
            row_iterator.state_info.advance();

            if let CassIteratorStateInfo::Value { value, position } = row_iterator.state_info {
                return (position < value.columns.len()) as cass_bool_t;
            }

            false as cass_bool_t
        }
        CassIterator::CassCollectionIterator(collection_iterator) => match collection_iterator {
            CassCollectionIterator::SequenceIterator(seq_iterator) => {
                seq_iterator.state_info.advance();

                if let CassIteratorStateInfo::Value { position, .. }
                | CassIteratorStateInfo::PositionNoValue { position } = seq_iterator.state_info
                {
                    if position < seq_iterator.count {
                        let raw_value = seq_iterator.sequence_iterator.next().unwrap();
                        if let Ok(raw) = raw_value {
                            let raw_value_type = raw.spec;
                            let new_value = decode_value(raw, raw_value_type);

                            if let Some(val) = new_value {
                                seq_iterator.state_info.update_value(val);
                                true as cass_bool_t // Value on new position is deserialized
                            } else {
                                false as cass_bool_t // New value is empty
                            }
                        } else {
                            false as cass_bool_t // Raw value is empty
                        }
                    } else {
                        false as cass_bool_t // Iterator reached the end
                    }
                } else {
                    false as cass_bool_t // Iterator position is unknown
                }
            }
            CassCollectionIterator::SeqMapIterator(seq_map_iterator) => {
                seq_map_iterator.state_info.advance();

                if let CassIteratorStateInfo::Value { position, .. }
                | CassIteratorStateInfo::PositionNoValue { position } =
                    seq_map_iterator.state_info
                {
                    // Decoding in pair (key, value) on even positions
                    if position < seq_map_iterator.count {
                        if position % 2 == 0 {
                            let raw_value = seq_map_iterator.map_iterator.next().unwrap();
                            if let Ok((raw_key, raw_value)) = raw_value {
                                let key_type = raw_key.spec;
                                let new_key = decode_value(raw_key, key_type);
                                let value_type = raw_value.spec;
                                let new_value = decode_value(raw_value, value_type);

                                if let (Some(k), Some(v)) = (new_key, new_value) {
                                    seq_map_iterator.state_info.update_value((k, v));
                                    true as cass_bool_t // (Key, Value) on new position is deserialized
                                } else {
                                    false as cass_bool_t // New (key, value) is empty
                                }
                            } else {
                                false as cass_bool_t // Raw value is empty
                            }
                        } else {
                            true as cass_bool_t // Do not deserialize on odd positions
                        }
                    } else {
                        false as cass_bool_t // Iterator reached the end
                    }
                } else {
                    false as cass_bool_t // Iterator position is unknown
                }
            }
        },
        CassIterator::CassTupleIterator(tuple_iterator) => {
            tuple_iterator.state_info.advance();

            if let CassIteratorStateInfo::Value { position, .. }
            | CassIteratorStateInfo::PositionNoValue { position } = tuple_iterator.state_info
            {
                if position < tuple_iterator.count {
                    let raw_value = tuple_iterator.sequence_iterator.next().unwrap();
                    if let Ok(raw) = raw_value {
                        let type_in_pos = match raw.spec {
                            ColumnType::Tuple(type_defs) => type_defs.get(position),
                            _ => panic!("Cannot get tuple out of non-tuple column type"),
                        };
                        if let Some(spec) = type_in_pos {
                            let new_value = decode_value(raw, spec);

                            if let Some(val) = new_value {
                                tuple_iterator.state_info.update_value(val);
                                true as cass_bool_t // Value on new position is deserialized
                            } else {
                                false as cass_bool_t // New value is empty
                            }
                        } else {
                            false as cass_bool_t // Value type is not known
                        }
                    } else {
                        false as cass_bool_t // Raw value is empty
                    }
                } else {
                    false as cass_bool_t // Iterator reached the end
                }
            } else {
                false as cass_bool_t // Iterator position is unknown
            }
        }
        CassIterator::CassMapIterator(map_iterator) => {
            map_iterator.state_info.advance();

            if let CassIteratorStateInfo::Value { position, .. }
            | CassIteratorStateInfo::PositionNoValue { position } = map_iterator.state_info
            {
                if position < map_iterator.count {
                    let raw_value = map_iterator.map_iterator.next().unwrap();
                    if let Ok((raw_key, raw_value)) = raw_value {
                        let key_type = raw_key.spec;
                        let new_key = decode_value(raw_key, key_type);
                        let value_type = raw_value.spec;
                        let new_value = decode_value(raw_value, value_type);

                        if let (Some(k), Some(v)) = (new_key, new_value) {
                            map_iterator.state_info.update_value((k, v));
                            true as cass_bool_t // (Key, Value) on new position is deserialized
                        } else {
                            false as cass_bool_t // New (key, value) is empty
                        }
                    } else {
                        false as cass_bool_t // Raw value is empty
                    }
                } else {
                    false as cass_bool_t // Iterator reached the end
                }
            } else {
                false as cass_bool_t // Iterator position is unknown
            }
        }
        CassIterator::CassUdtIterator(udt_iterator) => {
            udt_iterator.state_info.advance();

            if let CassIteratorStateInfo::Value { position, .. }
            | CassIteratorStateInfo::PositionNoValue { position } = udt_iterator.state_info
            {
                if position < udt_iterator.count {
                    let raw_value = udt_iterator.udt_iterator.next().unwrap();
                    if let Ok((name_type, Some(frame_slice))) = raw_value {
                        let name = &name_type.0;
                        let field_type = &name_type.1;
                        let raw = RawValue {
                            spec: field_type,
                            slice: frame_slice,
                        };
                        let new_value = decode_value(raw, field_type);

                        if let Some(val) = new_value {
                            udt_iterator.state_info.update_value((name.clone(), val));
                            true as cass_bool_t // Value on new position is deserialized
                        } else {
                            false as cass_bool_t // New value is empty
                        }
                    } else {
                        false as cass_bool_t // Raw value is empty
                    }
                } else {
                    false as cass_bool_t // Iterator reached the end
                }
            } else {
                false as cass_bool_t // Iterator position is unknown
            }
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
    if let CassIterator::CassResultIterator(CassResultIterator {
        result,
        state_info: CassIteratorStateInfo::Value { value, position },
    }) = iter
    {
        return match result.result.rows_num() {
            Some(rows_count) if *position < rows_count => value,
            _ => std::ptr::null(),
        };
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_column(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    // Defined only for row iterator, for other types should return null
    if let CassIterator::CassRowIterator(CassRowIterator {
        state_info: CassIteratorStateInfo::Value { value, position },
    }) = iter
    {
        let value = match value.columns.get(*position) {
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

    // Defined only for collections(list and set) or tuple iterator, for other types should return null
    match iter {
        CassIterator::CassCollectionIterator(collection_iterator) => match collection_iterator {
            CassCollectionIterator::SequenceIterator(CassSequenceIterator {
                state_info: CassIteratorStateInfo::Value { value, .. },
                ..
            }) => value,
            CassCollectionIterator::SeqMapIterator(CassMapIterator {
                state_info:
                    CassIteratorStateInfo::Value {
                        value: (key, value),
                        position,
                    },
                ..
            }) => {
                if position % 2 == 0 {
                    key
                } else {
                    value
                }
            }
            _ => std::ptr::null(),
        },
        CassIterator::CassTupleIterator(CassTupleIterator {
            state_info: CassIteratorStateInfo::Value { value, .. },
            ..
        }) => value,
        _ => std::ptr::null(), // null is returned if value in iterator is not set
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_key(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    match iter {
        CassIterator::CassMapIterator(CassMapIterator {
            count,
            state_info:
                CassIteratorStateInfo::Value {
                    value: (key, _value),
                    position,
                },
            ..
        }) => {
            assert!(*position < *count); // assertion copied from c++ driver
            key
        }
        CassIterator::CassCollectionIterator(CassCollectionIterator::SeqMapIterator(
            CassMapIterator {
                state_info:
                    CassIteratorStateInfo::Value {
                        value: (key, _value),
                        ..
                    },
                ..
            },
        )) => key,
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_map_value(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    match iter {
        CassIterator::CassMapIterator(CassMapIterator {
            count,
            state_info:
                CassIteratorStateInfo::Value {
                    value: (_key, value),
                    position,
                },
            ..
        }) => {
            assert!(*position < *count); // assertion copied from c++ driver
            value
        }
        CassIterator::CassCollectionIterator(CassCollectionIterator::SeqMapIterator(
            CassMapIterator {
                state_info:
                    CassIteratorStateInfo::Value {
                        value: (_key, value),
                        ..
                    },
                ..
            },
        )) => value,
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_name(
    iterator: *const CassIterator,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let iter = ptr_to_ref(iterator);

    match iter {
        CassIterator::CassUdtIterator(CassUdtIterator {
            count,
            state_info:
                CassIteratorStateInfo::Value {
                    value: (field_name, _field_value),
                    position,
                },
            ..
        }) => {
            assert!(*position < *count); // assertion copied from c++ driver
            write_str_to_c(field_name.as_str(), name, name_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_BAD_PARAMS,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_value(
    iterator: *const CassIterator,
) -> *const CassValue {
    let iter = ptr_to_ref(iterator);

    match iter {
        CassIterator::CassUdtIterator(CassUdtIterator {
            count,
            state_info:
                CassIteratorStateInfo::Value {
                    value: (_field_name, field_value),
                    position,
                },
            ..
        }) => {
            assert!(*position < *count); // assertion copied from c++ driver
            field_value
        }
        _ => std::ptr::null(),
    }
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
            Some(table_meta_entry) => Arc::as_ptr(table_meta_entry.1) as *const CassTableMeta,
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
                Some(view_meta_entry) => {
                    Arc::as_ptr(view_meta_entry.1) as *const CassMaterializedViewMeta
                }
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
    let row = CassRow {
        result: Arc::downgrade(&result_from_raw),
        columns: result_from_raw.first_row.columns.clone(), // C++ driver also clones columns of the first row into the iterator.
    };

    let iterator = CassResultIterator {
        result: result_from_raw,
        state_info: CassIteratorStateInfo::ValueNoPosition { value: row },
    };

    Box::into_raw(Box::new(CassIterator::CassResultIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_row(row: *const CassRow) -> *mut CassIterator {
    let row_from_raw = ptr_to_ref(row);

    let iterator = CassRowIterator {
        state_info: CassIteratorStateInfo::ValueNoPosition {
            value: row_from_raw,
        },
    };

    Box::into_raw(Box::new(CassIterator::CassRowIterator(iterator)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_collection(
    value: *const CassValue,
) -> *mut CassIterator {
    let collection = ptr_to_ref(value);

    if !collection.is_null && collection.value_type.is_collection() {
        let item_count = collection.count;
        let column_type = collection.column_type;
        match column_type {
            ColumnType::Map(_, _) => {
                let map_iterator = MapIterator::deserialize(column_type, collection.frame_slice);
                if let Ok(map_iter) = map_iterator {
                    let iterator = CassCollectionIterator::SeqMapIterator(CassMapIterator {
                        map_iterator: map_iter,
                        count: item_count * 2,
                        state_info: CassIteratorStateInfo::NoValue,
                    });

                    return Box::into_raw(Box::new(CassIterator::CassCollectionIterator(iterator)));
                }
            }
            ColumnType::Set(_) | ColumnType::List(_) => {
                let sequence_iterator =
                    SequenceIterator::deserialize(column_type, collection.frame_slice);
                if let Ok(seq_iterator) = sequence_iterator {
                    let iterator = CassCollectionIterator::SequenceIterator(CassSequenceIterator {
                        sequence_iterator: seq_iterator,
                        count: item_count,
                        state_info: CassIteratorStateInfo::NoValue,
                    });

                    return Box::into_raw(Box::new(CassIterator::CassCollectionIterator(iterator)));
                }
            }
            _ => panic!("Cannot create collection iterator from non-collection value"),
        }
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_tuple(value: *const CassValue) -> *mut CassIterator {
    let tuple = ptr_to_ref(value);

    if !tuple.is_null && tuple.value_type.is_tuple() {
        if let Some(frame_slice) = tuple.frame_slice {
            let item_count = tuple.count;
            let column_type = tuple.column_type;
            let sequence_iterator = SequenceIterator::new(column_type, item_count, frame_slice);
            let iterator = CassTupleIterator {
                sequence_iterator,
                count: item_count,
                state_info: CassIteratorStateInfo::NoValue,
            };

            return Box::into_raw(Box::new(CassIterator::CassTupleIterator(iterator)));
        }
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_from_map(value: *const CassValue) -> *mut CassIterator {
    let map = ptr_to_ref(value);

    if !map.is_null && map.value_type.is_map() {
        let item_count = map.count;
        let map_iterator = MapIterator::deserialize(map.column_type, map.frame_slice);
        if let Ok(map_iter) = map_iterator {
            let iterator = CassMapIterator {
                map_iterator: map_iter,
                count: item_count,
                state_info: CassIteratorStateInfo::NoValue,
            };

            return Box::into_raw(Box::new(CassIterator::CassMapIterator(iterator)));
        }
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn cass_iterator_fields_from_user_type(
    value: *const CassValue,
) -> *mut CassIterator {
    let udt = ptr_to_ref(value);

    if !udt.is_null && udt.value_type.is_user_type() {
        if let Some(frame_slice) = udt.frame_slice {
            let item_count = udt.count;
            let fields = match udt.column_type {
                ColumnType::UserDefinedType { field_types, .. } => field_types.as_slice(),
                _ => panic!("Unexpected column type for map collection"),
            };
            let udt_iterator = UdtIterator::new(fields, frame_slice);
            let iterator = CassUdtIterator {
                udt_iterator,
                count: item_count,
                state_info: CassIteratorStateInfo::NoValue,
            };

            return Box::into_raw(Box::new(CassIterator::CassUdtIterator(iterator)));
        }
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
    result.result.paging_state().is_some() as cass_bool_t
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
    let result = row_from_raw.result.upgrade().unwrap(); // safe to unwrap as result lives longer than row.
    let col_specs = result.result.column_specs();

    if name_str.starts_with('\"') && name_str.ends_with('\"') {
        name_str = name_str.strip_prefix('\"').unwrap();
        name_str = name_str.strip_suffix('\"').unwrap();
        is_case_sensitive = true;
    }

    col_specs
        .and_then(|col_specs| {
            col_specs
                .iter()
                .enumerate()
                .find(|(_, spec)| {
                    is_case_sensitive && spec.name == name_str
                        || !is_case_sensitive && spec.name.eq_ignore_ascii_case(name_str)
                })
                .map(|(index, _)| {
                    if let Some(value) = row_from_raw.columns.get(index) {
                        value as *const CassValue
                    } else {
                        std::ptr::null()
                    }
                })
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
    let result_from_raw = ptr_to_ref(result);
    let index_usize: usize = index.try_into().unwrap();
    let col_specs = if let Some(specs) = result_from_raw.result.column_specs() {
        specs
    } else {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    };

    if index_usize >= col_specs.len() {
        return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
    }

    let column_spec: &ColumnSpec = col_specs.get(index_usize).unwrap();
    let column_name = column_spec.name.as_str();

    write_str_to_c(column_name, name, name_length);

    CassError::CASS_OK
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

macro_rules! cass_value_get_strict_type {
    ($name:ident, $t:ty, $cass_t:ty, $cass_value_type:pat, $col_type:expr, $conv:expr $(, $arg:tt : $arg_ty:ty)*) => {
        #[no_mangle]
        #[allow(unreachable_patterns)] // cass_value_type may match all patterns
        pub unsafe extern "C" fn $name(value: *const CassValue, output: *mut $cass_t $(, $arg: $arg_ty)*) -> CassError {
            if !cass_value_is_null(value).is_zero() {
                return CassError::CASS_ERROR_LIB_NULL_VALUE;
            }

            match cass_value_type(value) {
                $cass_value_type => {}
                _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
            }

            let cass_value: &CassValue = ptr_to_ref(value);
            let decoded_val: Result<$t, ParseError> =
                DeserializeCql::deserialize(&$col_type, cass_value.frame_slice);

            match decoded_val {
                Ok(val) => $conv(value, output $(, $arg)*, val),
                Err(_) => CassError::CASS_ERROR_LIB_NOT_ENOUGH_DATA,
            }
        }
    };
}

// fixed numeric types

macro_rules! cass_value_get_numeric_type {
    ($name:ident, $t:ty, $cass_value_type:pat, $col_type:expr) => {
        cass_value_get_strict_type!(
            $name,
            $t,
            $t,
            $cass_value_type,
            $col_type,
            |_value: *const CassValue, output: *mut $t, val: $t| {
                std::ptr::write(output, val);
                CassError::CASS_OK
            }
        );
    };
}

cass_value_get_strict_type!(
    cass_value_get_bool,
    bool,
    cass_bool_t,
    CassValueType::CASS_VALUE_TYPE_BOOLEAN,
    ColumnType::Boolean,
    |_value: *const CassValue, output: *mut cass_bool_t, val: bool| {
        std::ptr::write(output, val as cass_bool_t);
        CassError::CASS_OK
    }
);

cass_value_get_numeric_type!(
    cass_value_get_float,
    cass_float_t,
    CassValueType::CASS_VALUE_TYPE_FLOAT,
    ColumnType::Float
);

cass_value_get_numeric_type!(
    cass_value_get_double,
    cass_double_t,
    CassValueType::CASS_VALUE_TYPE_DOUBLE,
    ColumnType::Double
);

cass_value_get_numeric_type!(
    cass_value_get_int8,
    cass_int8_t,
    CassValueType::CASS_VALUE_TYPE_TINY_INT,
    ColumnType::TinyInt
);

cass_value_get_numeric_type!(
    cass_value_get_int16,
    cass_int16_t,
    CassValueType::CASS_VALUE_TYPE_SMALL_INT,
    ColumnType::SmallInt
);

cass_value_get_numeric_type!(
    cass_value_get_int32,
    cass_int32_t,
    CassValueType::CASS_VALUE_TYPE_INT,
    ColumnType::Int
);

cass_value_get_numeric_type!(
    cass_value_get_int64,
    cass_int64_t,
    CassValueType::CASS_VALUE_TYPE_BIGINT
        | CassValueType::CASS_VALUE_TYPE_COUNTER
        | CassValueType::CASS_VALUE_TYPE_TIMESTAMP
        | CassValueType::CASS_VALUE_TYPE_TIME,
    ColumnType::BigInt // or `Counter` types can be deserialized as i64 in Rust driver
);

// other numeric types
#[no_mangle]
pub unsafe extern "C" fn cass_value_get_decimal(
    value: *const CassValue,
    varint: *mut *const cass_byte_t,
    varint_size: *mut size_t,
    scale: *mut cass_int32_t,
) -> CassError {
    if !cass_value_is_null(value).is_zero() {
        return CassError::CASS_ERROR_LIB_NULL_VALUE;
    }

    match cass_value_type(value) {
        CassValueType::CASS_VALUE_TYPE_DECIMAL => {}
        _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }

    let cass_value: &CassValue = ptr_to_ref(value);
    if let Some(frame) = cass_value.frame_slice {
        let mut val = frame.as_slice();
        let scale_res = types::read_int(&mut val);

        if let Ok(s) = scale_res {
            let decimal_len = val.len();

            *scale = s;
            *varint_size = decimal_len as size_t;
            *varint = val.as_ptr();

            return CassError::CASS_OK;
        }
    }

    CassError::CASS_ERROR_LIB_NOT_ENOUGH_DATA
}

// string
cass_value_get_strict_type!(
    cass_value_get_string,
    &str,
    *const c_char,
    CassValueType::CASS_VALUE_TYPE_ASCII
        | CassValueType::CASS_VALUE_TYPE_TEXT
        | CassValueType::CASS_VALUE_TYPE_VARCHAR,
    ColumnType::Text,
    |_value: *const CassValue, output: *mut *const c_char, output_size: *mut size_t, val: &str| {
        write_str_to_c(val, output, output_size);
        CassError::CASS_OK
    },
    output_size: *mut size_t // additional arguments
);

cass_value_get_strict_type!(
    cass_value_get_bytes,
    &[u8],
    *const cass_byte_t,
    _,
    ColumnType::Blob,
    |_value: *const CassValue,
     output: *mut *const cass_byte_t,
     output_size: *mut size_t,
     val: &[u8]| {
        *output = val.as_ptr() as *const cass_byte_t;
        *output_size = val.len() as size_t;
        CassError::CASS_OK
    },
    output_size: *mut size_t // additional arguments
);

// date and time types
cass_value_get_strict_type!(
    cass_value_get_duration,
    CqlDuration,
    cass_int32_t,
    CassValueType::CASS_VALUE_TYPE_DURATION,
    ColumnType::Duration,
    |_value: *const CassValue,
     months: *mut cass_int32_t,
     days: *mut cass_int32_t,
     nanos: *mut cass_int64_t,
     val: CqlDuration| {
        std::ptr::write(months, val.months);
        std::ptr::write(days, val.days);
        std::ptr::write(nanos, val.nanoseconds);
        CassError::CASS_OK
    },
    days: *mut cass_int32_t, // additional arguments
    nanos: *mut cass_int64_t
);

cass_value_get_strict_type!(
    cass_value_get_uint32,
    Date,
    cass_uint32_t,
    CassValueType::CASS_VALUE_TYPE_DATE,
    ColumnType::Date,
    |_value: *const CassValue, output: *mut cass_uint32_t, val: Date| {
        *output = val.0;
        CassError::CASS_OK
    }
);

// inet
cass_value_get_strict_type!(
    cass_value_get_inet,
    IpAddr,
    CassInet,
    CassValueType::CASS_VALUE_TYPE_INET,
    ColumnType::Inet,
    |_value: *const CassValue, output: *mut CassInet, val: IpAddr| {
        std::ptr::write(output, val.into());
        CassError::CASS_OK
    }
);

// uuid
cass_value_get_strict_type!(
    cass_value_get_uuid,
    Uuid,
    CassUuid,
    CassValueType::CASS_VALUE_TYPE_UUID | CassValueType::CASS_VALUE_TYPE_TIMEUUID,
    ColumnType::Uuid, // or `Timeuuid` types can be deserialized as `Uuid` in Rust driver
    |_value: *const CassValue, output: *mut CassUuid, val: Uuid| {
        std::ptr::write(output, val.into());
        CassError::CASS_OK
    }
);

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_null(value: *const CassValue) -> cass_bool_t {
    value.as_ref().map_or(true, |val| val.is_null) as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_is_collection(value: *const CassValue) -> cass_bool_t {
    let val = ptr_to_ref(value);
    val.value_type.is_collection() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_item_count(collection: *const CassValue) -> size_t {
    let val = ptr_to_ref(collection);
    val.count as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_primary_sub_type(
    collection: *const CassValue,
) -> CassValueType {
    let val = ptr_to_ref(collection);

    match val.value_type.as_ref() {
        CassDataType::List(Some(list)) => list.get_value_type(),
        CassDataType::Set(Some(set)) => set.get_value_type(),
        CassDataType::Map(Some(key), _) => key.get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_value_secondary_sub_type(
    collection: *const CassValue,
) -> CassValueType {
    let val = ptr_to_ref(collection);

    match val.value_type.as_ref() {
        CassDataType::Map(_, Some(value)) => value.get_value_type(),
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_row_count(result_raw: *const CassResult) -> size_t {
    let result = ptr_to_ref(result_raw);

    result.result.rows_num().as_ref().copied().unwrap_or(0) as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_column_count(result_raw: *const CassResult) -> size_t {
    let result = ptr_to_ref(result_raw);

    result
        .result
        .column_specs()
        .map_or(0, |col_specs| col_specs.len()) as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_result_first_row(result_raw: *const CassResult) -> *const CassRow {
    let result = ptr_to_ref(result_raw);

    &result.first_row
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

    match &result_from_raw.result.paging_state() {
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

#[cfg(test)]
mod tests {
    use crate::cass_error::CassError;
    use crate::query_result::{
        cass_iterator_get_value, cass_iterator_next, cass_value_get_bool, CassCollectionIterator,
        CassIterator, CassIteratorStateInfo, CassMapIterator, CassSequenceIterator,
        CassTupleIterator, CassUdtIterator,
    };
    use crate::testing::assert_cass_error_eq;
    use crate::types::cass_bool_t;
    use bytes::{BufMut, Bytes, BytesMut};
    use scylla::frame::response::result::ColumnType;
    use scylla::frame::value::Value;
    use scylla::types::deserialize::value::{DeserializeCql, MapIterator};
    use scylla::types::deserialize::value::{SequenceIterator, UdtIterator};
    use scylla::types::deserialize::FrameSlice;
    use std::collections::HashMap;

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_seq_iterator_empty_raw_value() {
        unsafe {
            let mut bytes_mut = BytesMut::new();
            bytes_mut.put_i32(1); // Number of values
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType = &ColumnType::List(Box::new(ColumnType::Text));
            let sequence_iterator =
                SequenceIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame))
                    .unwrap();
            let mut collection_iterator = CassIterator::CassCollectionIterator(
                CassCollectionIterator::SequenceIterator(CassSequenceIterator {
                    sequence_iterator,
                    count: 1,
                    state_info: CassIteratorStateInfo::NoValue,
                }),
            );
            let has_next = cass_iterator_next(&mut collection_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_seq_iterator_reached_the_end() {
        unsafe {
            let mut bytes_mut = Vec::new();
            let text = vec!["test"];
            text.serialize(&mut bytes_mut).unwrap();
            bytes_mut.drain(0..4); // Drop number of bytes before deserialization
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType = &ColumnType::List(Box::new(ColumnType::Text));
            let sequence_iterator =
                SequenceIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame))
                    .unwrap();
            let mut collection_iterator = CassIterator::CassCollectionIterator(
                CassCollectionIterator::SequenceIterator(CassSequenceIterator {
                    sequence_iterator,
                    count: 1,
                    state_info: CassIteratorStateInfo::NoValue,
                }),
            );
            let has_next = cass_iterator_next(&mut collection_iterator);
            assert_ne!(has_next, 0);
            // Reached the end
            let has_next = cass_iterator_next(&mut collection_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_map_iterator_empty_raw_value() {
        unsafe {
            let mut bytes_mut = BytesMut::new();
            bytes_mut.put_i32(1); // Number of values
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType =
                &ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Text));
            let map_iterator =
                MapIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame)).unwrap();
            let mut collection_iterator = CassIterator::CassCollectionIterator(
                CassCollectionIterator::SeqMapIterator(CassMapIterator {
                    map_iterator,
                    count: 2,
                    state_info: CassIteratorStateInfo::NoValue,
                }),
            );
            let has_next = cass_iterator_next(&mut collection_iterator);
            assert_eq!(has_next, 0);

            let map_iterator =
                MapIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame)).unwrap();
            let mut map_iterator = CassIterator::CassMapIterator(CassMapIterator {
                map_iterator,
                count: 1,
                state_info: CassIteratorStateInfo::NoValue,
            });
            let has_next = cass_iterator_next(&mut map_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_map_iterator_reached_the_end() {
        unsafe {
            let mut bytes_mut = Vec::new();
            let mut map = HashMap::new();
            map.insert("key", true);
            map.serialize(&mut bytes_mut).unwrap();
            bytes_mut.drain(0..4); // Drop number of bytes before deserialization
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType =
                &ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Boolean));
            let map_iterator =
                MapIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame)).unwrap();
            let mut collection_iterator = CassIterator::CassCollectionIterator(
                CassCollectionIterator::SeqMapIterator(CassMapIterator {
                    map_iterator,
                    count: 2,
                    state_info: CassIteratorStateInfo::NoValue,
                }),
            );
            let has_next = cass_iterator_next(&mut collection_iterator); // Position on key
            assert_ne!(has_next, 0);
            let has_next = cass_iterator_next(&mut collection_iterator); // Position on value
            assert_ne!(has_next, 0);
            // Reached the end
            let has_next = cass_iterator_next(&mut collection_iterator);
            assert_eq!(has_next, 0);

            let map_iterator =
                MapIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame)).unwrap();
            let mut map_iterator = CassIterator::CassMapIterator(CassMapIterator {
                map_iterator,
                count: 1,
                state_info: CassIteratorStateInfo::NoValue,
            });
            let has_next = cass_iterator_next(&mut map_iterator);
            assert_ne!(has_next, 0);
            // Reached the end
            let has_next = cass_iterator_next(&mut map_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_seq_map_iterator_deserialize_pair() {
        // To test that sequential iterator deserializes in pairs,
        // after the first `cass_iterator_next` call the value of the first pair
        // will be retrieved.
        unsafe {
            let mut bytes_mut = Vec::new();
            let mut map = HashMap::new();
            map.insert("key", true);
            map.serialize(&mut bytes_mut).unwrap();
            bytes_mut.drain(0..4); // Drop number of bytes before deserialization
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType =
                &ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Boolean));
            let map_iterator =
                MapIterator::deserialize(column_type.as_ref().unwrap(), Some(slice_frame)).unwrap();
            let mut collection_iterator = CassIterator::CassCollectionIterator(
                CassCollectionIterator::SeqMapIterator(CassMapIterator {
                    map_iterator,
                    count: 2,
                    state_info: CassIteratorStateInfo::NoValue,
                }),
            );

            let has_next = cass_iterator_next(&mut collection_iterator);
            assert_ne!(has_next, 0);

            // Hacking the iterator to not change the value but increment the position
            if let CassIterator::CassCollectionIterator(CassCollectionIterator::SeqMapIterator(
                CassMapIterator { state_info, .. },
            )) = &mut collection_iterator
            {
                state_info.advance();
            } else {
                unreachable!()
            }

            // Reached the end
            let value = cass_iterator_get_value(&collection_iterator);
            let mut output = cass_bool_t::default();
            assert_cass_error_eq!(cass_value_get_bool(value, &mut output), CassError::CASS_OK);
            assert_ne!(output, 0); // Value should be true
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_tuple_iterator_empty_raw_value() {
        unsafe {
            let mut bytes_mut = BytesMut::new();
            bytes_mut.put_i32(1); // Number of values
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType = &ColumnType::Tuple(vec![ColumnType::Text]);
            let sequence_iterator =
                SequenceIterator::new(column_type.as_ref().unwrap(), 1, slice_frame);
            let mut tuple_iterator = CassIterator::CassTupleIterator(CassTupleIterator {
                sequence_iterator,
                count: 1,
                state_info: CassIteratorStateInfo::NoValue,
            });
            let has_next = cass_iterator_next(&mut tuple_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_tuple_iterator_reached_the_end() {
        unsafe {
            let mut bytes_mut = Vec::new();
            let tuple = ("test",);
            tuple.serialize(&mut bytes_mut).unwrap();
            bytes_mut.drain(0..4); // Drop number of bytes before deserialization
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let column_type: *const ColumnType = &ColumnType::Tuple(vec![ColumnType::Text]);
            let sequence_iterator =
                SequenceIterator::new(column_type.as_ref().unwrap(), 1, slice_frame);
            let mut tuple_iterator = CassIterator::CassTupleIterator(CassTupleIterator {
                sequence_iterator,
                count: 1,
                state_info: CassIteratorStateInfo::NoValue,
            });
            let has_next = cass_iterator_next(&mut tuple_iterator);
            assert_ne!(has_next, 0);
            // Reached the end
            let has_next = cass_iterator_next(&mut tuple_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_udt_iterator_empty_raw_value() {
        unsafe {
            let mut bytes_mut = BytesMut::new();
            bytes_mut.put_i32(1); // Number of values
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let fields: *const Vec<(String, ColumnType)> =
                &vec![("string".to_owned(), ColumnType::Text)];
            let mut udt_iterator = CassIterator::CassUdtIterator(CassUdtIterator {
                udt_iterator: UdtIterator::new(fields.as_ref().unwrap(), slice_frame),
                count: 1,
                state_info: CassIteratorStateInfo::NoValue,
            });
            let has_next = cass_iterator_next(&mut udt_iterator);
            assert_eq!(has_next, 0);
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_collection_udt_iterator_reached_the_end() {
        unsafe {
            let mut bytes_mut = Vec::new();
            let tuple = ("test",);
            tuple.serialize(&mut bytes_mut).unwrap();
            bytes_mut.drain(0..4); // Drop number of bytes before deserialization
            let bytes: *const Bytes = &Bytes::from(bytes_mut);
            let slice_frame = FrameSlice::new(bytes.as_ref().unwrap());
            let fields: *const Vec<(String, ColumnType)> =
                &vec![("string".to_owned(), ColumnType::Text)];
            let mut udt_iterator = CassIterator::CassUdtIterator(CassUdtIterator {
                udt_iterator: UdtIterator::new(fields.as_ref().unwrap(), slice_frame),
                count: 1,
                state_info: CassIteratorStateInfo::NoValue,
            });
            let has_next = cass_iterator_next(&mut udt_iterator);
            assert_ne!(has_next, 0);
            // Reached the end
            let has_next = cass_iterator_next(&mut udt_iterator);
            assert_eq!(has_next, 0);
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
