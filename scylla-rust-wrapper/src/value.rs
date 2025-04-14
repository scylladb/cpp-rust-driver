use std::{convert::TryInto, net::IpAddr, sync::Arc};

use scylla::cluster::metadata::NativeType;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::SerializationError;
use scylla::serialize::value::{
    BuiltinSerializationErrorKind, MapSerializationErrorKind, SerializeValue,
    SetOrListSerializationErrorKind, TupleSerializationErrorKind, UdtSerializationErrorKind,
};
use scylla::serialize::writers::{CellWriter, WrittenCellProof};
use scylla::value::{CqlDate, CqlDecimal, CqlDuration};
use uuid::Uuid;

use crate::cass_types::{CassDataType, CassValueType};

/// A narrower version of rust driver's CqlValue.
///
/// cpp-driver's API allows to map single rust type to
/// multiple CQL types. For example `cass_statement_bind_int64`
/// can be used to bind a value to the column of following CQL types:
/// - bigint
/// - time
/// - counter
/// - timestamp
///
/// There is no such method as `cass_statement_bind_counter`, and so
/// we need to serialize the counter value using `CassCqlValue::BigInt`.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum CassCqlValue {
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
    Uuid(Uuid),
    Date(CqlDate),
    Inet(IpAddr),
    Duration(CqlDuration),
    Decimal(CqlDecimal),
    Tuple {
        data_type: Option<Arc<CassDataType>>,
        fields: Vec<Option<CassCqlValue>>,
    },
    List {
        data_type: Option<Arc<CassDataType>>,
        values: Vec<CassCqlValue>,
    },
    Map {
        data_type: Option<Arc<CassDataType>>,
        values: Vec<(CassCqlValue, CassCqlValue)>,
    },
    Set {
        data_type: Option<Arc<CassDataType>>,
        values: Vec<CassCqlValue>,
    },
    UserDefinedType {
        data_type: Arc<CassDataType>,
        /// Order of `fields` vector must match the order of fields as defined in the UDT. The
        /// driver does not check it by itself, so incorrect data will be written if the order is
        /// wrong.
        fields: Vec<(String, Option<CassCqlValue>)>,
    },
    // TODO: custom (?), duration and decimal
}

pub fn is_type_compatible(value: &Option<CassCqlValue>, typ: &CassDataType) -> bool {
    match value {
        Some(v) => v.is_type_compatible(typ),
        None => true,
    }
}

impl CassCqlValue {
    pub fn is_type_compatible(&self, typ: &CassDataType) -> bool {
        match self {
            CassCqlValue::TinyInt(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_TINY_INT
            },
            CassCqlValue::SmallInt(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_SMALL_INT
            },
            CassCqlValue::Int(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_INT
            },
            CassCqlValue::BigInt(_) => unsafe {
                matches!(
                    typ.get_unchecked().get_value_type(),
                    CassValueType::CASS_VALUE_TYPE_BIGINT
                        | CassValueType::CASS_VALUE_TYPE_COUNTER
                        | CassValueType::CASS_VALUE_TYPE_TIMESTAMP
                        | CassValueType::CASS_VALUE_TYPE_TIME
                )
            },
            CassCqlValue::Float(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_FLOAT
            },
            CassCqlValue::Double(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_DOUBLE
            },
            CassCqlValue::Boolean(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_BOOLEAN
            },
            CassCqlValue::Text(_) => unsafe {
                matches!(
                    typ.get_unchecked().get_value_type(),
                    CassValueType::CASS_VALUE_TYPE_TEXT
                        | CassValueType::CASS_VALUE_TYPE_VARCHAR
                        | CassValueType::CASS_VALUE_TYPE_ASCII
                        | CassValueType::CASS_VALUE_TYPE_BLOB
                        | CassValueType::CASS_VALUE_TYPE_VARINT
                )
            },
            CassCqlValue::Blob(_) => unsafe {
                matches!(
                    typ.get_unchecked().get_value_type(),
                    CassValueType::CASS_VALUE_TYPE_BLOB | CassValueType::CASS_VALUE_TYPE_VARINT
                )
            },
            CassCqlValue::Uuid(_) => unsafe {
                matches!(
                    typ.get_unchecked().get_value_type(),
                    CassValueType::CASS_VALUE_TYPE_UUID | CassValueType::CASS_VALUE_TYPE_TIMEUUID
                )
            },
            CassCqlValue::Date(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_DATE
            },
            CassCqlValue::Inet(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_INET
            },
            CassCqlValue::Duration(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_DURATION
            },
            CassCqlValue::Decimal(_) => unsafe {
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_DECIMAL
            },
            CassCqlValue::Tuple { data_type, .. } => unsafe {
                if let Some(dt) = data_type {
                    return dt.get_unchecked().typecheck_equals(typ.get_unchecked());
                }
                // Untyped tuple.
                typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_TUPLE
            },
            CassCqlValue::List { data_type, .. } => unsafe {
                if let Some(dt) = data_type {
                    dt.get_unchecked().typecheck_equals(typ.get_unchecked())
                } else {
                    // Untyped list.
                    typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_LIST
                }
            },
            CassCqlValue::Map { data_type, .. } => unsafe {
                if let Some(dt) = data_type {
                    dt.get_unchecked().typecheck_equals(typ.get_unchecked())
                } else {
                    // Untyped map.
                    typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_MAP
                }
            },
            CassCqlValue::Set { data_type, .. } => unsafe {
                if let Some(dt) = data_type {
                    dt.get_unchecked().typecheck_equals(typ.get_unchecked())
                } else {
                    // Untyped set.
                    typ.get_unchecked().get_value_type() == CassValueType::CASS_VALUE_TYPE_SET
                }
            },
            CassCqlValue::UserDefinedType { data_type, .. } => unsafe {
                data_type
                    .get_unchecked()
                    .typecheck_equals(typ.get_unchecked())
            },
        }
    }
}

impl SerializeValue for CassCqlValue {
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        // _typ is not used, since we do the typechecks during binding.
        // This is the same approach as cpp-driver.
        self.do_serialize(writer)
    }
}

impl CassCqlValue {
    fn do_serialize<'b>(
        &self,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        use NativeType::*;
        match self {
            // Notice:
            // We make use of builtin rust-driver serialization for simple types.
            // Keep in mind, that rust's implementation includes typechecks.
            //
            // We don't want to perform the typechecks here, because we handle it
            // earlier, during binding.
            // This is why, we make use of a hack, and provide a valid (in rust-driver's ser implementation)
            // ColumnType for each variant. This way, we can make sure that rust-driver's typecheck
            // will never fail. Thanks to that, we do not have to reimplement low-level serialization
            // for each type.
            CassCqlValue::TinyInt(v) => {
                <i8 as SerializeValue>::serialize(v, &ColumnType::Native(TinyInt), writer)
            }
            CassCqlValue::SmallInt(v) => {
                <i16 as SerializeValue>::serialize(v, &ColumnType::Native(SmallInt), writer)
            }
            CassCqlValue::Int(v) => {
                <i32 as SerializeValue>::serialize(v, &ColumnType::Native(Int), writer)
            }
            CassCqlValue::BigInt(v) => {
                <i64 as SerializeValue>::serialize(v, &ColumnType::Native(BigInt), writer)
            }
            CassCqlValue::Float(v) => {
                <f32 as SerializeValue>::serialize(v, &ColumnType::Native(Float), writer)
            }
            CassCqlValue::Double(v) => {
                <f64 as SerializeValue>::serialize(v, &ColumnType::Native(Double), writer)
            }
            CassCqlValue::Boolean(v) => {
                <bool as SerializeValue>::serialize(v, &ColumnType::Native(Boolean), writer)
            }
            CassCqlValue::Text(v) => {
                <String as SerializeValue>::serialize(v, &ColumnType::Native(Text), writer)
            }
            CassCqlValue::Blob(v) => {
                <Vec<u8> as SerializeValue>::serialize(v, &ColumnType::Native(Blob), writer)
            }
            CassCqlValue::Uuid(v) => {
                <uuid::Uuid as SerializeValue>::serialize(v, &ColumnType::Native(Uuid), writer)
            }
            CassCqlValue::Date(v) => {
                <CqlDate as SerializeValue>::serialize(v, &ColumnType::Native(Date), writer)
            }
            CassCqlValue::Inet(v) => {
                <IpAddr as SerializeValue>::serialize(v, &ColumnType::Native(Inet), writer)
            }
            CassCqlValue::Duration(v) => {
                <CqlDuration as SerializeValue>::serialize(v, &ColumnType::Native(Duration), writer)
            }
            CassCqlValue::Decimal(v) => {
                <CqlDecimal as SerializeValue>::serialize(v, &ColumnType::Native(Decimal), writer)
            }
            CassCqlValue::Tuple { fields, .. } => serialize_tuple_like(fields.iter(), writer),
            CassCqlValue::List { values, .. } => {
                serialize_sequence(values.len(), values.iter(), writer)
            }
            CassCqlValue::Map { values, .. } => {
                serialize_mapping(values.len(), values.iter().map(|p| (&p.0, &p.1)), writer)
            }
            CassCqlValue::Set { values, .. } => {
                serialize_sequence(values.len(), values.iter(), writer)
            }
            CassCqlValue::UserDefinedType { fields, .. } => serialize_udt(fields, writer),
        }
    }
}

/// Serialization of one of the built-in types failed.
#[derive(Debug, Clone)]
pub struct CassSerializationError {
    /// Name of the Rust type being serialized.
    pub rust_name: &'static str,

    /// Detailed information about the failure.
    pub kind: BuiltinSerializationErrorKind,
}

impl std::fmt::Display for CassSerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to serialize Rust type {}: {}",
            self.rust_name, self.kind
        )
    }
}

impl std::error::Error for CassSerializationError {}

fn mk_ser_err<T>(kind: impl Into<BuiltinSerializationErrorKind>) -> SerializationError {
    mk_ser_err_named(std::any::type_name::<T>(), kind)
}

fn mk_ser_err_named(
    name: &'static str,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(CassSerializationError {
        rust_name: name,
        kind: kind.into(),
    })
}

fn serialize_tuple_like<'t, 'b>(
    field_values: impl Iterator<Item = &'t Option<CassCqlValue>>,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let mut builder = writer.into_value_builder();

    for (index, el) in field_values.enumerate() {
        let sub = builder.make_sub_writer();
        match el {
            None => sub.set_null(),
            Some(el) => el.do_serialize(sub).map_err(|err| {
                mk_ser_err::<CassCqlValue>(
                    TupleSerializationErrorKind::ElementSerializationFailed { index, err },
                )
            })?,
        };
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<CassCqlValue>(BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_sequence<'t, 'b>(
    len: usize,
    iter: impl Iterator<Item = &'t CassCqlValue>,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let rust_name = std::any::type_name::<CassCqlValue>();

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len.try_into().map_err(|_| {
        mk_ser_err_named(rust_name, SetOrListSerializationErrorKind::TooManyElements)
    })?;
    builder.append_bytes(&element_count.to_be_bytes());

    for el in iter {
        el.do_serialize(builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                SetOrListSerializationErrorKind::ElementSerializationFailed(err),
            )
        })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_mapping<'t, 'b>(
    len: usize,
    iter: impl Iterator<Item = (&'t CassCqlValue, &'t CassCqlValue)>,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let rust_name = std::any::type_name::<CassCqlValue>();

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len
        .try_into()
        .map_err(|_| mk_ser_err_named(rust_name, MapSerializationErrorKind::TooManyElements))?;
    builder.append_bytes(&element_count.to_be_bytes());

    for (k, v) in iter {
        k.do_serialize(builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                MapSerializationErrorKind::KeySerializationFailed(err),
            )
        })?;
        v.do_serialize(builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                MapSerializationErrorKind::ValueSerializationFailed(err),
            )
        })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_udt<'b>(
    values: &[(String, Option<CassCqlValue>)],
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let mut builder = writer.into_value_builder();
    for (fname, fvalue) in values {
        let writer = builder.make_sub_writer();
        match fvalue {
            None => writer.set_null(),
            Some(v) => v.do_serialize(writer).map_err(|err| {
                mk_ser_err::<CassCqlValue>(UdtSerializationErrorKind::FieldSerializationFailed {
                    field_name: fname.clone(),
                    err,
                })
            })?,
        };
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<CassCqlValue>(BuiltinSerializationErrorKind::SizeOverflow))
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use scylla::value::{CqlDate, CqlDecimal, CqlDuration};

    use crate::{
        cass_types::{CassDataType, CassDataTypeInner, CassValueType, MapDataType, UDTDataType},
        value::{CassCqlValue, is_type_compatible},
    };

    fn all_value_data_types() -> Vec<CassDataType> {
        let from = |v_typ: CassValueType| CassDataType::new(CassDataTypeInner::Value(v_typ));

        vec![
            from(CassValueType::CASS_VALUE_TYPE_TINY_INT),
            from(CassValueType::CASS_VALUE_TYPE_SMALL_INT),
            from(CassValueType::CASS_VALUE_TYPE_INT),
            from(CassValueType::CASS_VALUE_TYPE_BIGINT),
            from(CassValueType::CASS_VALUE_TYPE_COUNTER),
            from(CassValueType::CASS_VALUE_TYPE_TIME),
            from(CassValueType::CASS_VALUE_TYPE_TIMESTAMP),
            from(CassValueType::CASS_VALUE_TYPE_FLOAT),
            from(CassValueType::CASS_VALUE_TYPE_DOUBLE),
            from(CassValueType::CASS_VALUE_TYPE_BOOLEAN),
            from(CassValueType::CASS_VALUE_TYPE_TEXT),
            from(CassValueType::CASS_VALUE_TYPE_VARCHAR),
            from(CassValueType::CASS_VALUE_TYPE_ASCII),
            from(CassValueType::CASS_VALUE_TYPE_BLOB),
            from(CassValueType::CASS_VALUE_TYPE_UUID),
            from(CassValueType::CASS_VALUE_TYPE_TIMEUUID),
            from(CassValueType::CASS_VALUE_TYPE_DATE),
            from(CassValueType::CASS_VALUE_TYPE_INET),
            from(CassValueType::CASS_VALUE_TYPE_DURATION),
            from(CassValueType::CASS_VALUE_TYPE_DECIMAL),
            from(CassValueType::CASS_VALUE_TYPE_VARINT),
            from(CassValueType::CASS_VALUE_TYPE_TUPLE),
            from(CassValueType::CASS_VALUE_TYPE_LIST),
            from(CassValueType::CASS_VALUE_TYPE_SET),
            from(CassValueType::CASS_VALUE_TYPE_MAP),
            from(CassValueType::CASS_VALUE_TYPE_UDT),
        ]
    }

    #[test]
    fn typecheck_simple_test() {
        let from = |v_typ: CassValueType| CassDataType::new(CassDataTypeInner::Value(v_typ));
        struct TestCase {
            value: Option<CassCqlValue>,
            compatible_types: Vec<CassDataType>,
        }

        let test_cases = [
            // Null -> all types
            TestCase {
                value: None,
                compatible_types: all_value_data_types(),
            },
            // i8 -> tinyint
            TestCase {
                value: Some(CassCqlValue::TinyInt(Default::default())),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_TINY_INT)],
            },
            // i16 -> smallint
            TestCase {
                value: Some(CassCqlValue::SmallInt(Default::default())),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_SMALL_INT)],
            },
            // i32 -> int
            TestCase {
                value: Some(CassCqlValue::Int(Default::default())),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_INT)],
            },
            // i64 -> bigint/counter/time/timestamp
            TestCase {
                value: Some(CassCqlValue::BigInt(Default::default())),
                compatible_types: vec![
                    from(CassValueType::CASS_VALUE_TYPE_BIGINT),
                    from(CassValueType::CASS_VALUE_TYPE_COUNTER),
                    from(CassValueType::CASS_VALUE_TYPE_TIME),
                    from(CassValueType::CASS_VALUE_TYPE_TIMESTAMP),
                ],
            },
            // f32 -> float
            TestCase {
                value: Some(CassCqlValue::Float(Default::default())),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_FLOAT)],
            },
            // f64 -> double
            TestCase {
                value: Some(CassCqlValue::Double(Default::default())),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_DOUBLE)],
            },
            // bool -> boolean
            TestCase {
                value: Some(CassCqlValue::Boolean(Default::default())),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_BOOLEAN)],
            },
            TestCase {
                value: Some(CassCqlValue::Text(Default::default())),
                compatible_types: vec![
                    from(CassValueType::CASS_VALUE_TYPE_TEXT),
                    from(CassValueType::CASS_VALUE_TYPE_VARCHAR),
                    from(CassValueType::CASS_VALUE_TYPE_ASCII),
                    from(CassValueType::CASS_VALUE_TYPE_BLOB),
                    from(CassValueType::CASS_VALUE_TYPE_VARINT),
                ],
            },
            // Vec<u8> -> blob/varint
            TestCase {
                value: Some(CassCqlValue::Blob(Default::default())),
                compatible_types: vec![
                    from(CassValueType::CASS_VALUE_TYPE_BLOB),
                    from(CassValueType::CASS_VALUE_TYPE_VARINT),
                ],
            },
            // uuid -> uuid/timeuuid
            TestCase {
                value: Some(CassCqlValue::Uuid(Default::default())),
                compatible_types: vec![
                    from(CassValueType::CASS_VALUE_TYPE_UUID),
                    from(CassValueType::CASS_VALUE_TYPE_TIMEUUID),
                ],
            },
            // u32 -> date
            TestCase {
                value: Some(CassCqlValue::Date(CqlDate(Default::default()))),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_DATE)],
            },
            // IpAddr -> inet
            TestCase {
                value: Some(CassCqlValue::Inet(std::net::IpAddr::V4(
                    Ipv4Addr::LOCALHOST,
                ))),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_INET)],
            },
            // CqlDuration -> duration
            TestCase {
                value: Some(CassCqlValue::Duration(CqlDuration {
                    months: 0,
                    days: 0,
                    nanoseconds: 0,
                })),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_DURATION)],
            },
            // CqlDecimal -> decimal
            TestCase {
                value: Some(CassCqlValue::Decimal(
                    CqlDecimal::from_signed_be_bytes_slice_and_exponent(&[], 0),
                )),
                compatible_types: vec![from(CassValueType::CASS_VALUE_TYPE_DECIMAL)],
            },
        ];
        let all_simple_types = all_value_data_types();

        for case in test_cases {
            for typ in all_simple_types.iter() {
                let result = is_type_compatible(&case.value, typ);
                let expected = case.compatible_types.iter().any(|t| t == typ);
                assert_eq!(
                    expected, result,
                    "Typecheck test for value {:?} and type {:?} failed. Expected result for the typecheck: {}",
                    case.value, typ, expected,
                );
            }
        }
    }

    #[test]
    fn typecheck_complex_test() {
        struct TestCase {
            value: CassCqlValue,
            compatible_types: Vec<Arc<CassDataType>>,
            incompatible_types: Vec<Arc<CassDataType>>,
        }

        let run_test_cases = |test_cases: &[TestCase]| {
            for case in test_cases {
                for typ in case.compatible_types.iter() {
                    assert!(
                        case.value.is_type_compatible(typ),
                        "Typecheck failed, when it should pass. Value: {:?}, Type: {:?}",
                        case.value,
                        typ
                    );
                }
                for typ in case.incompatible_types.iter() {
                    assert!(
                        !case.value.is_type_compatible(typ),
                        "Typecheck passed, when it should fail. Value: {:?}, Type: {:?}",
                        case.value,
                        typ
                    )
                }
            }
        };

        // Let's make some types accessible for all test cases.
        // To make sure that e.g. Tuple against UDT typecheck fails.
        let data_type_float = CassDataType::new_arced(CassDataTypeInner::Value(
            CassValueType::CASS_VALUE_TYPE_FLOAT,
        ));
        let data_type_int =
            CassDataType::new_arced(CassDataTypeInner::Value(CassValueType::CASS_VALUE_TYPE_INT));
        let data_type_bool = CassDataType::new_arced(CassDataTypeInner::Value(
            CassValueType::CASS_VALUE_TYPE_BOOLEAN,
        ));
        let data_type_tuple = CassDataType::new_arced(CassDataTypeInner::Tuple(vec![
            data_type_float.clone(),
            data_type_int.clone(),
            data_type_bool.clone(),
        ]));

        let simple_fields = vec![
            ("foo".to_owned(), data_type_float.clone()),
            ("bar".to_owned(), data_type_bool.clone()),
            ("baz".to_owned(), data_type_int.clone()),
        ];
        let ks_keyspace_name = "ks".to_owned();
        let user_udt_name = "user".to_owned();
        let empty_str = "".to_owned();

        let data_type_udt_simple = CassDataType::new_arced(CassDataTypeInner::UDT(UDTDataType {
            field_types: simple_fields.clone(),
            keyspace: ks_keyspace_name.clone(),
            name: user_udt_name.clone(),
            frozen: false,
        }));

        let data_type_int_list = CassDataType::new_arced(CassDataTypeInner::List {
            typ: Some(data_type_int.clone()),
            frozen: false,
        });

        let data_type_int_set = CassDataType::new_arced(CassDataTypeInner::Set {
            typ: Some(data_type_int.clone()),
            frozen: false,
        });

        let data_type_bool_float_map = CassDataType::new_arced(CassDataTypeInner::Map {
            typ: MapDataType::KeyAndValue(data_type_bool.clone(), data_type_float.clone()),
            frozen: false,
        });

        // TUPLES
        {
            let data_type_untyped_tuple = CassDataType::new_arced(CassDataTypeInner::Tuple(vec![]));
            let data_type_small_tuple =
                CassDataType::new_arced(CassDataTypeInner::Tuple(vec![data_type_bool.clone()]));
            let data_type_nested_tuple = CassDataType::new_arced(CassDataTypeInner::Tuple(vec![
                data_type_small_tuple.clone(),
                data_type_int.clone(),
                data_type_tuple.clone(),
            ]));
            let data_type_nested_untyped_tuple =
                CassDataType::new_arced(CassDataTypeInner::Tuple(vec![
                    data_type_untyped_tuple.clone(),
                    data_type_int.clone(),
                    data_type_untyped_tuple.clone(),
                ]));

            let test_cases = &[
                // Untyped tuple -> created via `cass_tuple_new`
                TestCase {
                    value: CassCqlValue::Tuple {
                        data_type: None,
                        fields: vec![],
                    },
                    compatible_types: vec![
                        data_type_untyped_tuple.clone(),
                        data_type_small_tuple.clone(),
                        data_type_tuple.clone(),
                        data_type_nested_tuple.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_int_set.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Untyped tuple -> used created an untyped tuple data type, and then
                // created a tuple value via `cass_tuple_new_from_data_type`.
                TestCase {
                    value: CassCqlValue::Tuple {
                        data_type: Some(data_type_untyped_tuple.clone()),
                        fields: vec![],
                    },
                    compatible_types: vec![
                        data_type_untyped_tuple.clone(),
                        data_type_small_tuple.clone(),
                        data_type_tuple.clone(),
                        data_type_nested_tuple.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_int_set.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Fully typed tuple.
                TestCase {
                    value: CassCqlValue::Tuple {
                        data_type: Some(data_type_tuple.clone()),
                        fields: vec![],
                    },
                    compatible_types: vec![
                        data_type_tuple.clone(),
                        data_type_untyped_tuple.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_small_tuple.clone(),
                        data_type_nested_tuple.clone(),
                        data_type_nested_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_int_set.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Nested tuple.
                TestCase {
                    value: CassCqlValue::Tuple {
                        data_type: Some(data_type_nested_tuple.clone()),
                        fields: vec![],
                    },
                    compatible_types: vec![
                        data_type_nested_tuple.clone(),
                        data_type_untyped_tuple.clone(),
                        data_type_nested_untyped_tuple.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_small_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_int_set.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
            ];

            run_test_cases(test_cases);
        }

        // UDT
        {
            let data_type_udt_simple_empty_keyspace =
                CassDataType::new_arced(CassDataTypeInner::UDT(UDTDataType {
                    field_types: simple_fields.clone(),
                    keyspace: empty_str.to_owned(),
                    name: user_udt_name.clone(),
                    frozen: false,
                }));
            let data_type_udt_simple_empty_name =
                CassDataType::new_arced(CassDataTypeInner::UDT(UDTDataType {
                    field_types: simple_fields.clone(),
                    keyspace: ks_keyspace_name.clone(),
                    name: empty_str.clone(),
                    frozen: false,
                }));

            // A prefix of simple_fields.
            let small_fields = vec![
                ("foo".to_owned(), data_type_float.clone()),
                ("bar".to_owned(), data_type_bool.clone()),
            ];
            let data_type_udt_small =
                CassDataType::new_arced(CassDataTypeInner::UDT(UDTDataType {
                    field_types: small_fields.clone(),
                    keyspace: ks_keyspace_name.clone(),
                    name: user_udt_name.clone(),
                    frozen: false,
                }));

            let test_cases = &[TestCase {
                value: CassCqlValue::UserDefinedType {
                    data_type: data_type_udt_simple.clone(),
                    fields: vec![],
                },
                compatible_types: vec![
                    data_type_udt_simple.clone(),
                    data_type_udt_simple_empty_keyspace.clone(),
                    data_type_udt_simple_empty_name.clone(),
                    data_type_udt_small.clone(),
                ],
                incompatible_types: vec![
                    data_type_float.clone(),
                    data_type_int.clone(),
                    data_type_bool.clone(),
                    data_type_tuple.clone(),
                    data_type_int_list.clone(),
                    data_type_int_set.clone(),
                    data_type_bool_float_map.clone(),
                ],
            }];

            run_test_cases(test_cases);
        }

        // COLLECTIONS
        {
            let data_type_untyped_list = CassDataType::new_arced(CassDataTypeInner::List {
                typ: None,
                frozen: false,
            });
            let data_type_float_list = CassDataType::new_arced(CassDataTypeInner::List {
                typ: Some(data_type_float.clone()),
                frozen: false,
            });

            let data_type_untyped_set = CassDataType::new_arced(CassDataTypeInner::Set {
                typ: None,
                frozen: false,
            });
            let data_type_float_set = CassDataType::new_arced(CassDataTypeInner::Set {
                typ: Some(data_type_float.clone()),
                frozen: false,
            });

            let data_type_untyped_map = CassDataType::new_arced(CassDataTypeInner::Map {
                typ: MapDataType::Untyped,
                frozen: false,
            });
            let data_type_typed_key_float_map = CassDataType::new_arced(CassDataTypeInner::Map {
                typ: MapDataType::Key(data_type_float.clone()),

                frozen: false,
            });
            let data_type_float_int_map = CassDataType::new_arced(CassDataTypeInner::Map {
                typ: MapDataType::KeyAndValue(data_type_float.clone(), data_type_int.clone()),
                frozen: false,
            });

            let test_cases = &[
                // Untyped list -> user created it via `cass_collection_new`.
                TestCase {
                    value: CassCqlValue::List {
                        data_type: None,
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_float_list.clone(),
                        data_type_int_list.clone(),
                        data_type_untyped_list.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                        data_type_int_set.clone(),
                        data_type_untyped_map.clone(),
                        data_type_typed_key_float_map.clone(),
                        data_type_float_int_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Typed list.
                TestCase {
                    value: CassCqlValue::List {
                        data_type: Some(data_type_float_list.clone()),
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_float_list.clone(),
                        data_type_untyped_list.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                        data_type_int_set.clone(),
                        data_type_untyped_map.clone(),
                        data_type_typed_key_float_map.clone(),
                        data_type_float_int_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Untyped set (via cass_collection_new).
                TestCase {
                    value: CassCqlValue::Set {
                        data_type: None,
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                        data_type_int_set.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_float_list.clone(),
                        data_type_untyped_list.clone(),
                        data_type_untyped_map.clone(),
                        data_type_typed_key_float_map.clone(),
                        data_type_float_int_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Typed set.
                TestCase {
                    value: CassCqlValue::Set {
                        data_type: Some(data_type_float_set.clone()),
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_int_list.clone(),
                        data_type_float_list.clone(),
                        data_type_untyped_list.clone(),
                        data_type_int_set.clone(),
                        data_type_untyped_map.clone(),
                        data_type_typed_key_float_map.clone(),
                        data_type_float_int_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Untyped map (via cass_collection_new).
                TestCase {
                    value: CassCqlValue::Map {
                        data_type: None,
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_untyped_map.clone(),
                        data_type_typed_key_float_map.clone(),
                        data_type_float_int_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_float_list.clone(),
                        data_type_int_list.clone(),
                        data_type_untyped_list.clone(),
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                        data_type_int_set.clone(),
                    ],
                },
                // Only key-typed map.
                TestCase {
                    value: CassCqlValue::Map {
                        data_type: Some(data_type_typed_key_float_map.clone()),
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_typed_key_float_map.clone(),
                        data_type_untyped_map.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_float_list.clone(),
                        data_type_int_list.clone(),
                        data_type_untyped_list.clone(),
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                        data_type_int_set.clone(),
                        data_type_float_int_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
                // Fully typed map
                TestCase {
                    value: CassCqlValue::Map {
                        data_type: Some(data_type_float_int_map.clone()),
                        values: vec![],
                    },
                    compatible_types: vec![
                        data_type_float_int_map.clone(),
                        data_type_untyped_map.clone(),
                    ],
                    incompatible_types: vec![
                        data_type_float.clone(),
                        data_type_int.clone(),
                        data_type_bool.clone(),
                        data_type_tuple.clone(),
                        data_type_udt_simple.clone(),
                        data_type_float_list.clone(),
                        data_type_int_list.clone(),
                        data_type_untyped_list.clone(),
                        data_type_untyped_set.clone(),
                        data_type_float_set.clone(),
                        data_type_int_set.clone(),
                        data_type_typed_key_float_map.clone(),
                        data_type_bool_float_map.clone(),
                    ],
                },
            ];

            run_test_cases(test_cases)
        }
    }
}
