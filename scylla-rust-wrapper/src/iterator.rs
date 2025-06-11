use scylla::deserialize::result::TypedRowIterator;
use scylla::deserialize::value::{DeserializeValue, ListlikeIterator, MapIterator, UdtIterator};

use crate::argconv::{
    ArcFFI, BoxFFI, CConst, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr,
    CassOwnedExclusivePtr, FFI, FromBox, RefFFI, write_str_to_c,
};
use crate::cass_error::CassError;
use crate::cass_types::{CassDataType, CassDataTypeInner, CassValueType, MapDataType};
use crate::metadata::{
    CassColumnMeta, CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta, CassTableMeta,
};
use crate::query_result::cass_raw_value::CassRawValue;
use crate::query_result::{
    CassRawRow, CassResult, CassResultKind, CassResultMetadata, CassRow, CassValue,
    NonNullDeserializationError, cass_value_type,
};
use crate::types::{cass_bool_t, cass_false, size_t};

pub(crate) use crate::cass_iterator_types::CassIteratorType;

use std::os::raw::c_char;
use std::sync::Arc;

pub(crate) struct CassRowsResultIterator<'result> {
    iterator: TypedRowIterator<'result, 'result, CassRawRow<'result, 'result>>,
    result_metadata: &'result CassResultMetadata,
    current_row: Option<CassRow<'result>>,
}

pub(crate) enum CassResultIterator<'result> {
    NonRows,
    Rows(CassRowsResultIterator<'result>),
}

impl CassResultIterator<'_> {
    fn next(&mut self) -> bool {
        let CassResultIterator::Rows(rows_result_iterator) = self else {
            return false;
        };

        let new_row =
            rows_result_iterator
                .iterator
                .next()
                .and_then(|raw_row_res: Result<CassRawRow, _>| {
                    raw_row_res
                        .and_then(|raw_row| {
                            CassRow::from_raw_row_and_metadata(
                                raw_row,
                                rows_result_iterator.result_metadata,
                            )
                        })
                        .inspect_err(|e| {
                            // We have no way to propagate the error (return type is bool).
                            // Let's at least log the deserialization error.
                            tracing::error!("Failed to deserialize next row: {e}");
                        })
                        .ok()
                });

        rows_result_iterator.current_row = new_row;

        rows_result_iterator.current_row.is_some()
    }
}

pub(crate) struct CassRowIterator<'result> {
    row: &'result CassRow<'result>,
    position: Option<usize>,
}

impl CassRowIterator<'_> {
    fn next(&mut self) -> bool {
        let new_pos: usize = self.position.map_or(0, |prev_pos| prev_pos + 1);

        self.position = Some(new_pos);

        new_pos < self.row.columns.len()
    }
}

/// An iterator created from [`cass_iterator_from_collection()`] with list or set provided as a value.
pub(crate) struct CassListlikeIterator<'result> {
    iterator: ListlikeIterator<'result, 'result, CassRawValue<'result, 'result>>,
    value_data_type: &'result Arc<CassDataType>,
    current_value: Option<CassValue<'result>>,
}

impl<'result> CassListlikeIterator<'result> {
    fn new_from_value(
        value: &'result CassValue<'result>,
    ) -> Result<Self, NonNullDeserializationError> {
        let listlike_iterator = value.get_non_null::<ListlikeIterator<CassRawValue>>()?;

        // SAFETY: `CassDataType` is obtained from `CassResultMetadata`, which is immutable.
        let item_type = match unsafe { value.value_type.get_unchecked() } {
            CassDataTypeInner::List { typ, .. } | CassDataTypeInner::Set { typ, .. } => {
                // Expect: `typ` is an `Option<Arc<CassDataType>>`. It is None, for untyped set or list.
                // There is no such thing as untyped list/set in CQL protocol, thus we do not expect it in result metadata.
                typ.as_ref()
                    .expect("List or set type provided from result metadata should be fully typed!")
            }
            _ => {
                panic!("Expected list or set type. Typecheck should have prevented such scenario!")
            }
        };

        Ok(Self {
            iterator: listlike_iterator,
            value_data_type: item_type,
            current_value: None,
        })
    }

    fn next(&mut self) -> bool {
        let next_value = self.iterator.next().and_then(|res| match res {
            Ok(value) => Some(CassValue {
                value,
                value_type: self.value_data_type,
            }),
            Err(e) => {
                tracing::error!("Failed to deserialize next listlike entry: {e}");
                None
            }
        });

        self.current_value = next_value;

        self.current_value.is_some()
    }
}

/// Iterator created from [`cass_iterator_from_collection()`] with map provided as a collection.
/// Single iteration (call to [`cass_iterator_next()`]) moves the iterator to the next value (either key or value).
pub(crate) struct CassMapCollectionIterator<'result> {
    iterator: CassMapIterator<'result>,
    state: Option<CassMapCollectionIteratorState>,
}

/// Tells the [`CassMapCollectionIterator`] at which part of the singular entry it is currently at.
enum CassMapCollectionIteratorState {
    Key,
    Value,
}

impl<'result> CassMapCollectionIterator<'result> {
    fn new_from_value(
        value: &'result CassValue<'result>,
    ) -> Result<Self, NonNullDeserializationError> {
        let iterator = CassMapIterator::new_from_value(value)?;

        Ok(Self {
            iterator,
            state: None,
        })
    }

    fn next(&mut self) -> bool {
        let (new_state, next_result) = match self.state {
            // First call to cass_iterator_next(). Move underlying CassMapIterator to the next entry.
            None => (CassMapCollectionIteratorState::Key, self.iterator.next()),
            // Move the state to the value.
            // Do not forward the underlying iterator.
            Some(CassMapCollectionIteratorState::Key) => {
                (CassMapCollectionIteratorState::Value, true)
            }
            // Moving to the key of the next entry. Forwards the underlying iterator.
            Some(CassMapCollectionIteratorState::Value) => {
                (CassMapCollectionIteratorState::Key, self.iterator.next())
            }
        };

        if next_result {
            self.state = Some(new_state);
        }

        next_result
    }
}

/// Iterator created from [`cass_iterator_from_collection()`] with list, set or map provided as a collection.
pub(crate) enum CassCollectionIterator<'result> {
    /// Listlike iterator for list or set.
    Listlike(CassListlikeIterator<'result>),
    /// Map iterator.
    Map(CassMapCollectionIterator<'result>),
}

impl CassCollectionIterator<'_> {
    fn next(&mut self) -> bool {
        match self {
            CassCollectionIterator::Listlike(listlike_iterator) => listlike_iterator.next(),
            CassCollectionIterator::Map(map_collection_iterator) => map_collection_iterator.next(),
        }
    }
}

// TODO: consider introducing this to Rust driver.
mod tuple_iterator {
    use scylla::cluster::metadata::ColumnType;
    use scylla::deserialize::FrameSlice;
    use scylla::deserialize::value::{self, DeserializeValue};
    use scylla::errors::{DeserializationError, TypeCheckError};
    use scylla::frame::frame_errors::LowLevelDeserializationError;
    use thiserror::Error;

    pub(super) struct TupleIterator<'frame, 'metadata> {
        all_metadata: &'metadata [ColumnType<'metadata>],
        remaining_metadata: &'metadata [ColumnType<'metadata>],
        iterator: BytesSequenceIterator<'frame>,
    }

    impl<'frame, 'metadata> TupleIterator<'frame, 'metadata> {
        fn new(metadata: &'metadata [ColumnType<'metadata>], slice: FrameSlice<'frame>) -> Self {
            Self {
                all_metadata: metadata,
                remaining_metadata: metadata,
                iterator: BytesSequenceIterator::new(slice),
            }
        }
    }

    impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for TupleIterator<'frame, 'metadata> {
        fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
            match typ {
                ColumnType::Tuple { .. } => Ok(()),
                _ => Err(TypeCheckError::new(value::BuiltinTypeCheckError {
                    rust_name: std::any::type_name::<Self>(),
                    cql_type: typ.clone().into_owned(),
                    kind: value::BuiltinTypeCheckErrorKind::TupleError(
                        value::TupleTypeCheckErrorKind::NotTuple,
                    ),
                })),
            }
        }

        fn deserialize(
            typ: &'metadata ColumnType<'metadata>,
            v: Option<FrameSlice<'frame>>,
        ) -> Result<Self, DeserializationError> {
            let slice: FrameSlice<'frame> = v.ok_or_else(|| {
                DeserializationError::new(value::BuiltinDeserializationError {
                    rust_name: std::any::type_name::<Self>(),
                    cql_type: typ.clone().into_owned(),
                    kind: value::BuiltinDeserializationErrorKind::ExpectedNonNull,
                })
            })?;

            let metadata = match typ {
                ColumnType::Tuple(types) => types.as_slice(),
                _ => {
                    unreachable!("Typecheck should have prevented this scenario!")
                }
            };

            Ok(Self::new(metadata, slice))
        }
    }

    impl<'frame, 'metadata> Iterator for TupleIterator<'frame, 'metadata> {
        type Item = Result<
            (&'metadata ColumnType<'metadata>, Option<FrameSlice<'frame>>),
            DeserializationError,
        >;

        fn next(&mut self) -> Option<Self::Item> {
            let pos = self.all_metadata.len() - self.remaining_metadata.len();

            // We don't fail if there are more serialized fields than declared in the metadata.
            // This is what we do for static-size tuple deserialization.
            // For example, (a, b, c) does not fail if slice contains 4 serialized values (in Rust-driver).
            let (head, metadata) = self.remaining_metadata.split_first()?;
            self.remaining_metadata = metadata;

            let raw_res = match self.iterator.next() {
                Some(Ok(raw)) => Ok((head, raw)),
                Some(Err(e)) => Err(DeserializationError::new(
                    value::BuiltinDeserializationError {
                        rust_name: std::any::type_name::<Self>(),
                        cql_type: ColumnType::Tuple(self.all_metadata.to_owned()).into_owned(),
                        kind: value::BuiltinDeserializationErrorKind::RawCqlBytesReadError(e),
                    },
                )),
                // Value is missing.
                None => Err(DeserializationError::new(TupleMissingValue(pos))),
            };

            Some(raw_res)
        }
    }

    // This would be a variant of TupleDeserializationErrorKind in rust-driver.
    #[derive(Error, Debug)]
    #[error("Failed to deserialize tuple: serialized value is missing as position {0}")]
    struct TupleMissingValue(usize);

    // --------------------------
    // COPIED FROM RUST-DRIVER!!!
    // --------------------------
    #[derive(Clone, Copy, Debug)]
    struct BytesSequenceIterator<'frame> {
        slice: FrameSlice<'frame>,
    }

    impl<'frame> BytesSequenceIterator<'frame> {
        fn new(slice: FrameSlice<'frame>) -> Self {
            Self { slice }
        }
    }

    impl<'frame> From<FrameSlice<'frame>> for BytesSequenceIterator<'frame> {
        #[inline]
        fn from(slice: FrameSlice<'frame>) -> Self {
            Self::new(slice)
        }
    }

    impl<'frame> Iterator for BytesSequenceIterator<'frame> {
        type Item = Result<Option<FrameSlice<'frame>>, LowLevelDeserializationError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.slice.as_slice().is_empty() {
                None
            } else {
                Some(self.slice.read_cql_bytes())
            }
        }
    }
}

/// Iterator created from [`cass_iterator_from_tuple()`].
pub(crate) struct CassTupleIterator<'result> {
    iterator: tuple_iterator::TupleIterator<'result, 'result>,
    metadata: &'result [Arc<CassDataType>],
    current_entry: Option<CassTupleIteratorEntry<'result>>,
}

pub(crate) struct CassTupleIteratorEntry<'result> {
    field_value: CassValue<'result>,
    metadata_types_index: usize,
}

impl<'result> CassTupleIterator<'result> {
    fn new_from_value(
        value: &'result CassValue<'result>,
    ) -> Result<Self, NonNullDeserializationError> {
        let tuple_iterator = value.get_non_null::<tuple_iterator::TupleIterator>()?;

        // SAFETY: `CassDataType` is obtained from `CassResultMetadata`, which is immutable.
        let metadata = match unsafe { value.value_type.get_unchecked() } {
            CassDataTypeInner::Tuple(typ) => typ.as_slice(),
            _ => panic!("Expected tuple type. Typecheck should have prevented such scenario!"),
        };

        Ok(Self {
            iterator: tuple_iterator,
            metadata,
            current_entry: None,
        })
    }

    fn next(&mut self) -> bool {
        // Handle scenario where underlying iterator is exhausted.
        let Some(deser_result) = self.iterator.next() else {
            return false;
        };

        // Handle the deserialization error from underlying iterator.
        let Ok((column_type, slice)) = deser_result else {
            tracing::error!("Failed to deserialize next tuple value: {deser_result:?}");
            return false;
        };

        // Deserialize to CassRawValue.
        let raw_value: CassRawValue = match <_ as DeserializeValue>::deserialize(column_type, slice)
        {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Failed to deserialize next tuple value: {e}");
                return false;
            }
        };

        // Update the current entry.
        let new_metadata_types_index = self
            .current_entry
            .as_ref()
            .map(|entry| entry.metadata_types_index + 1)
            .unwrap_or(0);

        let new_value = CassValue {
            value: raw_value,
            value_type: &self.metadata[new_metadata_types_index],
        };

        self.current_entry = Some(CassTupleIteratorEntry {
            field_value: new_value,
            metadata_types_index: new_metadata_types_index,
        });

        true
    }
}

/// Iterator created from [`cass_iterator_from_map()`].
/// Single iteration (call to [`cass_iterator_next()`]) moves the iterator to the next entry (key-value pair).
pub(crate) struct CassMapIterator<'result> {
    iterator: MapIterator<
        'result,
        'result,
        CassRawValue<'result, 'result>,
        CassRawValue<'result, 'result>,
    >,
    key_value_types: (&'result Arc<CassDataType>, &'result Arc<CassDataType>),
    current_entry: Option<(CassValue<'result>, CassValue<'result>)>,
}

impl<'result> CassMapIterator<'result> {
    fn new_from_value(
        value: &'result CassValue<'result>,
    ) -> Result<Self, NonNullDeserializationError> {
        let map_iterator = value.get_non_null::<MapIterator<CassRawValue, CassRawValue>>()?;

        // SAFETY: `CassDataType` is obtained from `CassResultMetadata`, which is immutable.
        let key_value_types = match unsafe { value.value_type.get_unchecked() } {
            CassDataTypeInner::Map { typ, .. } => match typ {
                MapDataType::KeyAndValue(key_type, val_type) => (key_type, val_type),
                _ => panic!(
                    "Untyped or half-typed map received in result metadata. Something really bad happened!"
                ),
            },
            _ => panic!("Expected map type. Typecheck should have prevented such scenario!"),
        };

        Ok(Self {
            iterator: map_iterator,
            key_value_types,
            current_entry: None,
        })
    }

    fn next(&mut self) -> bool {
        let new_entry = self
            .iterator
            .next()
            .and_then(|res| match res {
                Ok((key, value)) => Some((key, value)),
                Err(e) => {
                    tracing::error!("Failed to deserialize next map entry: {e}");
                    None
                }
            })
            .map(|(key, value)| {
                (
                    CassValue {
                        value: key,
                        value_type: self.key_value_types.0,
                    },
                    CassValue {
                        value,
                        value_type: self.key_value_types.1,
                    },
                )
            });

        self.current_entry = new_entry;

        self.current_entry.is_some()
    }
}

pub(crate) struct CassUdtIterator<'result> {
    iterator: UdtIterator<'result, 'result>,
    metadata: &'result [(String, Arc<CassDataType>)],
    current_entry: Option<CassUdtIteratorEntry<'result>>,
}

struct CassUdtIteratorEntry<'result> {
    field_value: CassValue<'result>,
    metadata_types_index: usize,
}

impl<'result> CassUdtIterator<'result> {
    fn new_from_value(
        value: &'result CassValue<'result>,
    ) -> Result<Self, NonNullDeserializationError> {
        let udt_iterator = value.get_non_null::<UdtIterator>()?;

        // SAFETY: `CassDataType` is obtained from `CassResultMetadata`, which is immutable.
        let metadata = match unsafe { value.value_type.get_unchecked() } {
            CassDataTypeInner::Udt(udt) => udt.field_types.as_slice(),
            _ => panic!("Expected UDT type. Typecheck should have prevented such scenario!"),
        };

        Ok(Self {
            iterator: udt_iterator,
            metadata,
            current_entry: None,
        })
    }

    fn next(&mut self) -> bool {
        // Handle scenario where underlying iterator is exhausted.
        let Some(((_field_name, field_type), deser_result)) = self.iterator.next() else {
            return false;
        };

        // Handle the deserialization error from underlying iterator.
        let Ok(field_value) = deser_result else {
            tracing::error!("Failed to deserialize next UDT field: {deser_result:?}");
            return false;
        };
        // Flatten: Treat missing UDT field as null.
        // This is a bit different from what cpp-driver does.
        // cpp-driver fails in case when field metadata is provided, but the serialized value is missing.
        // After the discussion, we concluded that this is not the correct behaviour. CQL protocol
        // clearly states that this is a valid scenario: https://github.com/apache/cassandra/blob/4a80daf32eb4226d9870b914779a1fc007479da6/doc/native_protocol_v4.spec#L1003.
        let field_slice = field_value.flatten();

        // Deserialize to CassRawValue.
        let raw_value: CassRawValue =
            match <_ as DeserializeValue>::deserialize(field_type, field_slice) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("Failed to deserialize UDT field value: {e}");
                    return false;
                }
            };

        // Update the current entry.
        let new_metadata_types_index = self
            .current_entry
            .as_ref()
            .map(|entry| entry.metadata_types_index + 1)
            .unwrap_or(0);

        let new_value = CassValue {
            value: raw_value,
            value_type: &self.metadata[new_metadata_types_index].1,
        };

        self.current_entry = Some(CassUdtIteratorEntry {
            field_value: new_value,
            metadata_types_index: new_metadata_types_index,
        });

        true
    }
}

pub(crate) struct CassSchemaMetaIterator<'schema> {
    value: &'schema CassSchemaMeta,
    count: usize,
    position: Option<usize>,
}

impl CassSchemaMetaIterator<'_> {
    fn next(&mut self) -> bool {
        let new_pos: usize = self.position.map_or(0, |prev_pos| prev_pos + 1);

        self.position = Some(new_pos);

        new_pos < self.count
    }
}

pub(crate) struct CassKeyspaceMetaIterator<'schema> {
    value: &'schema CassKeyspaceMeta,
    count: usize,
    position: Option<usize>,
}

impl CassKeyspaceMetaIterator<'_> {
    fn next(&mut self) -> bool {
        let new_pos: usize = self.position.map_or(0, |prev_pos| prev_pos + 1);

        self.position = Some(new_pos);

        new_pos < self.count
    }
}

pub(crate) struct CassTableMetaIterator<'schema> {
    value: &'schema CassTableMeta,
    count: usize,
    position: Option<usize>,
}

impl CassTableMetaIterator<'_> {
    fn next(&mut self) -> bool {
        let new_pos: usize = self.position.map_or(0, |prev_pos| prev_pos + 1);

        self.position = Some(new_pos);

        new_pos < self.count
    }
}

pub(crate) struct CassViewMetaIterator<'schema> {
    value: &'schema CassMaterializedViewMeta,
    count: usize,
    position: Option<usize>,
}

impl CassViewMetaIterator<'_> {
    fn next(&mut self) -> bool {
        let new_pos: usize = self.position.map_or(0, |prev_pos| prev_pos + 1);

        self.position = Some(new_pos);

        new_pos < self.count
    }
}

/// An iterator over columns metadata.
/// Can be constructed from either table ([`cass_iterator_columns_from_table_meta()`])
/// or view metadata ([`cass_iterator_columns_from_materialized_view_meta()`]).
/// To be used by [`cass_iterator_get_column_meta()`].
pub(crate) enum CassColumnsMetaIterator<'schema> {
    FromTable(CassTableMetaIterator<'schema>),
    FromView(CassViewMetaIterator<'schema>),
}

/// An iterator over materialized views.
/// Can be constructed from either keyspace ([`cass_iterator_materialized_views_from_keyspace_meta()`])
/// or table ([`cass_iterator_materialized_views_from_table_meta()`]) metadata.
/// To be used by [`cass_iterator_get_materialized_view_meta()`].
pub(crate) enum CassMaterializedViewsMetaIterator<'schema> {
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
    Tuple(CassTupleIterator<'result_or_schema>),
    /// Iterator over fields (values) in UDT.
    Udt(CassUdtIterator<'result_or_schema>),

    // Iterators derived from CassSchemaMeta.
    // Naming convention of the variants: name of item in the collection (plural).
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_free(iterator: CassOwnedExclusivePtr<CassIterator, CMut>) {
    BoxFFI::free(iterator);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_type(
    iterator: CassBorrowedExclusivePtr<CassIterator, CMut>,
) -> CassIteratorType {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_type!");
        // TYPE_RESULT corresponds to 0.
        return CassIteratorType::CASS_ITERATOR_TYPE_RESULT;
    };

    match iter {
        CassIterator::Result(_) => CassIteratorType::CASS_ITERATOR_TYPE_RESULT,
        CassIterator::Row(_) => CassIteratorType::CASS_ITERATOR_TYPE_ROW,
        CassIterator::Collection(_) => CassIteratorType::CASS_ITERATOR_TYPE_COLLECTION,
        CassIterator::Map(_) => CassIteratorType::CASS_ITERATOR_TYPE_MAP,
        CassIterator::Tuple(_) => CassIteratorType::CASS_ITERATOR_TYPE_TUPLE,
        CassIterator::Udt(_) => CassIteratorType::CASS_ITERATOR_TYPE_USER_TYPE_FIELD,
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_next(
    iterator: CassBorrowedExclusivePtr<CassIterator, CMut>,
) -> cass_bool_t {
    let Some(mut iter) = BoxFFI::as_mut_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_next!");
        return cass_false;
    };

    let result = match &mut iter {
        CassIterator::Result(result_iterator) => result_iterator.next(),
        CassIterator::Row(row_iterator) => row_iterator.next(),
        CassIterator::Collection(collection_iterator) => collection_iterator.next(),
        CassIterator::Tuple(tuple_iterator) => tuple_iterator.next(),
        CassIterator::Map(map_iterator) => map_iterator.next(),
        CassIterator::Udt(udt_iterator) => udt_iterator.next(),
        CassIterator::KeyspacesMeta(schema_meta_iterator) => schema_meta_iterator.next(),
        CassIterator::TablesMeta(keyspace_meta_iterator)
        | CassIterator::UserTypes(keyspace_meta_iterator)
        | CassIterator::MaterializedViewsMeta(CassMaterializedViewsMetaIterator::FromKeyspace(
            keyspace_meta_iterator,
        )) => keyspace_meta_iterator.next(),
        CassIterator::MaterializedViewsMeta(CassMaterializedViewsMetaIterator::FromTable(
            table_iterator,
        ))
        | CassIterator::ColumnsMeta(CassColumnsMetaIterator::FromTable(table_iterator)) => {
            table_iterator.next()
        }
        CassIterator::ColumnsMeta(CassColumnsMetaIterator::FromView(view_iterator)) => {
            view_iterator.next()
        }
    };

    result as cass_bool_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_row<'result>(
    iterator: CassBorrowedSharedPtr<'result, CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassRow<'result>, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_row!");
        return RefFFI::null();
    };

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_column<'result>(
    iterator: CassBorrowedSharedPtr<CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_column!");
        return RefFFI::null();
    };

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_value<'result>(
    iterator: CassBorrowedSharedPtr<'result, CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_value!");
        return RefFFI::null();
    };

    // Defined only for collections(list, set and map) or tuple iterator, for other types should return null
    match iter {
        CassIterator::Collection(CassCollectionIterator::Listlike(listlike_iterator)) => {
            listlike_iterator
                .current_value
                .as_ref()
                .map(RefFFI::as_ptr)
                .unwrap_or(RefFFI::null())
        }
        CassIterator::Collection(CassCollectionIterator::Map(map_collection_iterator)) => {
            map_collection_iterator
                .state
                .as_ref()
                .and_then(|state| {
                    let current_entry = map_collection_iterator.iterator.current_entry.as_ref();
                    let v = match state {
                        CassMapCollectionIteratorState::Key => current_entry.map(|entry| &entry.0),
                        CassMapCollectionIteratorState::Value => {
                            current_entry.map(|entry| &entry.1)
                        }
                    };

                    v.map(RefFFI::as_ptr)
                })
                .unwrap_or(RefFFI::null())
        }
        CassIterator::Tuple(tuple_iterator) => tuple_iterator
            .current_entry
            .as_ref()
            .map(|entry| RefFFI::as_ptr(&entry.field_value))
            .unwrap_or(RefFFI::null()),
        _ => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_map_key<'result>(
    iterator: CassBorrowedSharedPtr<'result, CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_map_key!");
        return RefFFI::null();
    };

    let CassIterator::Map(map_iterator) = iter else {
        return RefFFI::null();
    };

    map_iterator
        .current_entry
        .as_ref()
        .map(|entry| RefFFI::as_ptr(&entry.0))
        .unwrap_or(RefFFI::null())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_map_value<'result>(
    iterator: CassBorrowedSharedPtr<'result, CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_map_value!");
        return RefFFI::null();
    };

    let CassIterator::Map(map_iterator) = iter else {
        return RefFFI::null();
    };

    map_iterator
        .current_entry
        .as_ref()
        .map(|entry| RefFFI::as_ptr(&entry.1))
        .unwrap_or(RefFFI::null())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_name(
    iterator: CassBorrowedSharedPtr<CassIterator, CConst>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!(
            "Provided null iterator pointer to cass_iterator_get_user_type_field_name!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let CassIterator::Udt(udt_iterator) = iter else {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match &udt_iterator.current_entry {
        Some(entry) => {
            let field_name = &udt_iterator.metadata[entry.metadata_types_index].0;

            unsafe { write_str_to_c(field_name.as_str(), name, name_length) };

            CassError::CASS_OK
        }
        None => CassError::CASS_ERROR_LIB_BAD_PARAMS,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_user_type_field_value<'result>(
    iterator: CassBorrowedSharedPtr<'result, CassIterator<'result>, CConst>,
) -> CassBorrowedSharedPtr<'result, CassValue<'result>, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!(
            "Provided null iterator pointer to cass_iterator_get_user_type_field_value!"
        );
        return RefFFI::null();
    };

    let CassIterator::Udt(udt_iterator) = iter else {
        return RefFFI::null();
    };

    udt_iterator
        .current_entry
        .as_ref()
        .map(|entry| RefFFI::as_ptr(&entry.field_value))
        .unwrap_or(RefFFI::null())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_keyspace_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_keyspace_meta!");
        return RefFFI::null();
    };

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_table_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassTableMeta, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_table_meta!");
        return RefFFI::null();
    };

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_user_type<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassDataType, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_user_type!");
        return ArcFFI::null();
    };

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_column_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassColumnMeta, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!("Provided null iterator pointer to cass_iterator_get_column_meta!");
        return RefFFI::null();
    };

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_iterator_get_materialized_view_meta<'schema>(
    iterator: CassBorrowedSharedPtr<CassIterator<'schema>, CConst>,
) -> CassBorrowedSharedPtr<'schema, CassMaterializedViewMeta, CConst> {
    let Some(iter) = BoxFFI::as_ref(iterator) else {
        tracing::error!(
            "Provided null iterator pointer to cass_iterator_get_materialized_view_meta!"
        );
        return RefFFI::null();
    };

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

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_result<'result>(
    result: CassBorrowedSharedPtr<'result, CassResult, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let Some(result_from_raw) = ArcFFI::as_ref(result) else {
        tracing::error!("Provided null result pointer to cass_iterator_from_result!");
        return BoxFFI::null_mut();
    };

    let iterator = match &result_from_raw.kind {
        CassResultKind::NonRows => CassResultIterator::NonRows,
        CassResultKind::Rows(cass_rows_result) => {
            CassResultIterator::Rows(CassRowsResultIterator {
                // unwrap: CassRawRow always passes the typecheck.
                iterator: cass_rows_result
                    .shared_data
                    .raw_rows
                    .rows_iter::<CassRawRow>()
                    .unwrap(),
                result_metadata: &cass_rows_result.shared_data.metadata,
                current_row: None,
            })
        }
    };

    BoxFFI::into_ptr(Box::new(CassIterator::Result(iterator)))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_row<'result>(
    row: CassBorrowedSharedPtr<'result, CassRow<'result>, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let Some(row_from_raw) = RefFFI::as_ref(row) else {
        tracing::error!("Provided null row pointer to cass_iterator_from_row!");
        return BoxFFI::null_mut();
    };

    let iterator = CassRowIterator {
        row: row_from_raw,
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::Row(iterator)))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_collection<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue<'result>, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    if RefFFI::is_null(&value) {
        return BoxFFI::null_mut();
    }

    // SAFETY: We assume that user provided a valid pointer to the value.
    // The `value.value_type` is a `CassDataType` obtained from `CassResultMetadata`, which is immutable
    // (thus, underlying `get_unchecked()` is safe).
    let value_type = unsafe { cass_value_type(value.borrow()) };

    let val = RefFFI::as_ref(value).unwrap();

    let iterator_result = match value_type {
        CassValueType::CASS_VALUE_TYPE_SET | CassValueType::CASS_VALUE_TYPE_LIST => {
            CassListlikeIterator::new_from_value(val).map(CassCollectionIterator::Listlike)
        }
        CassValueType::CASS_VALUE_TYPE_MAP => {
            CassMapCollectionIterator::new_from_value(val).map(CassCollectionIterator::Map)
        }
        _ => return BoxFFI::null_mut(),
    };

    match iterator_result {
        Ok(iterator) => BoxFFI::into_ptr(Box::new(CassIterator::Collection(iterator))),
        Err(e) => {
            tracing::error!("Failed to create collection iterator: {e}");
            BoxFFI::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_tuple<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue<'result>, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let Some(tuple) = RefFFI::as_ref(value) else {
        tracing::error!("Provided null tuple pointer to cass_iterator_from_tuple!");
        return BoxFFI::null_mut();
    };

    let iterator_result = CassTupleIterator::new_from_value(tuple);
    match iterator_result {
        Ok(iterator) => BoxFFI::into_ptr(Box::new(CassIterator::Tuple(iterator))),
        Err(e) => {
            tracing::error!("Failed to create tuple iterator: {e}");
            BoxFFI::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_from_map<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue<'result>, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let Some(map) = RefFFI::as_ref(value) else {
        tracing::error!("Provided null map pointer to cass_iterator_from_map!");
        return BoxFFI::null_mut();
    };

    let iterator_result = CassMapIterator::new_from_value(map);

    match iterator_result {
        Ok(iterator) => BoxFFI::into_ptr(Box::new(CassIterator::Map(iterator))),
        Err(e) => {
            tracing::error!("Failed to create map iterator: {e}");
            BoxFFI::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_fields_from_user_type<'result>(
    value: CassBorrowedSharedPtr<'result, CassValue<'result>, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'result>, CMut> {
    let Some(udt) = RefFFI::as_ref(value) else {
        tracing::error!("Provided null UDT pointer to cass_iterator_fields_from_user_type!");
        return BoxFFI::null_mut();
    };

    let iterator_result = CassUdtIterator::new_from_value(udt);
    match iterator_result {
        Ok(iterator) => BoxFFI::into_ptr(Box::new(CassIterator::Udt(iterator))),
        Err(e) => {
            tracing::error!("Failed to create UDT iterator: {e}");
            BoxFFI::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_keyspaces_from_schema_meta<'schema>(
    schema_meta: CassBorrowedSharedPtr<'schema, CassSchemaMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = BoxFFI::as_ref(schema_meta) else {
        tracing::error!(
            "Provided null schema metadata pointer to cass_iterator_keyspaces_from_schema_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassSchemaMetaIterator {
        value: metadata,
        count: metadata.keyspaces.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::KeyspacesMeta(iterator)))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_tables_from_keyspace_meta<'schema>(
    keyspace_meta: CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!(
            "Provided null keyspace metadata pointer to cass_iterator_tables_from_keyspace_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.tables.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::TablesMeta(iterator)))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_keyspace_meta<'schema>(
    keyspace_meta: CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!(
            "Provided null keyspace metadata pointer to cass_iterator_materialized_views_from_keyspace_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::MaterializedViewsMeta(
        CassMaterializedViewsMetaIterator::FromKeyspace(iterator),
    )))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_user_types_from_keyspace_meta<'schema>(
    keyspace_meta: CassBorrowedSharedPtr<'schema, CassKeyspaceMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!(
            "Provided null keyspace metadata pointer to cass_iterator_user_types_from_keyspace_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassKeyspaceMetaIterator {
        value: metadata,
        count: metadata.user_defined_type_data_type.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::UserTypes(iterator)))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_columns_from_table_meta<'schema>(
    table_meta: CassBorrowedSharedPtr<'schema, CassTableMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_iterator_columns_from_table_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::ColumnsMeta(
        CassColumnsMetaIterator::FromTable(iterator),
    )))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_materialized_views_from_table_meta<'schema>(
    table_meta: CassBorrowedSharedPtr<'schema, CassTableMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_iterator_materialized_views_from_table_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassTableMetaIterator {
        value: metadata,
        count: metadata.views.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::MaterializedViewsMeta(
        CassMaterializedViewsMetaIterator::FromTable(iterator),
    )))
}

#[unsafe(no_mangle)]
#[allow(clippy::needless_lifetimes)]
pub unsafe extern "C" fn cass_iterator_columns_from_materialized_view_meta<'schema>(
    view_meta: CassBorrowedSharedPtr<'schema, CassMaterializedViewMeta, CConst>,
) -> CassOwnedExclusivePtr<CassIterator<'schema>, CMut> {
    let Some(metadata) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null view metadata pointer to cass_iterator_columns_from_materialized_view_meta!"
        );
        return BoxFFI::null_mut();
    };

    let iterator = CassViewMetaIterator {
        value: metadata,
        count: metadata.view_metadata.columns_metadata.len(),
        position: None,
    };

    BoxFFI::into_ptr(Box::new(CassIterator::ColumnsMeta(
        CassColumnsMetaIterator::FromView(iterator),
    )))
}
