use std::{convert::TryInto, net::IpAddr};

use scylla::{
    frame::{
        response::result::ColumnType,
        value::{CqlDate, CqlDecimal, CqlDuration},
    },
    serialize::{
        value::{
            BuiltinSerializationErrorKind, MapSerializationErrorKind, SerializeValue,
            SetOrListSerializationErrorKind, TupleSerializationErrorKind,
            UdtSerializationErrorKind,
        },
        writers::WrittenCellProof,
        CellWriter, SerializationError,
    },
};
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
#[derive(Clone, Debug, PartialEq)]
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
    Tuple(Vec<Option<CassCqlValue>>),
    List(Vec<CassCqlValue>),
    Map(Vec<(CassCqlValue, CassCqlValue)>),
    Set(Vec<CassCqlValue>),
    UserDefinedType {
        keyspace: String,
        type_name: String,
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
            CassCqlValue::TinyInt(_) => {
                typ.get_value_type() == CassValueType::CASS_VALUE_TYPE_TINY_INT
            }
            CassCqlValue::SmallInt(_) => {
                typ.get_value_type() == CassValueType::CASS_VALUE_TYPE_SMALL_INT
            }
            CassCqlValue::Int(_) => todo!(),
            CassCqlValue::BigInt(_) => todo!(),
            CassCqlValue::Float(_) => todo!(),
            CassCqlValue::Double(_) => todo!(),
            CassCqlValue::Boolean(_) => todo!(),
            CassCqlValue::Text(_) => todo!(),
            CassCqlValue::Blob(_) => todo!(),
            CassCqlValue::Uuid(_) => todo!(),
            CassCqlValue::Date(_) => todo!(),
            CassCqlValue::Inet(_) => todo!(),
            CassCqlValue::Duration(_) => todo!(),
            CassCqlValue::Decimal(_) => todo!(),
            CassCqlValue::Tuple(_) => todo!(),
            CassCqlValue::List(_) => todo!(),
            CassCqlValue::Map(_) => todo!(),
            CassCqlValue::Set(_) => todo!(),
            CassCqlValue::UserDefinedType { .. } => todo!(),
        }
    }
}

impl SerializeValue for CassCqlValue {
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        // _typ is not used, since we do the typechecks during binding (this is still a TODO, high priority).
        // This is the same approach as cpp-driver.
        self.do_serialize(writer)
    }
}

impl CassCqlValue {
    fn do_serialize<'b>(
        &self,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
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
                <i8 as SerializeValue>::serialize(v, &ColumnType::TinyInt, writer)
            }
            CassCqlValue::SmallInt(v) => {
                <i16 as SerializeValue>::serialize(v, &ColumnType::SmallInt, writer)
            }
            CassCqlValue::Int(v) => <i32 as SerializeValue>::serialize(v, &ColumnType::Int, writer),
            CassCqlValue::BigInt(v) => {
                <i64 as SerializeValue>::serialize(v, &ColumnType::BigInt, writer)
            }
            CassCqlValue::Float(v) => {
                <f32 as SerializeValue>::serialize(v, &ColumnType::Float, writer)
            }
            CassCqlValue::Double(v) => {
                <f64 as SerializeValue>::serialize(v, &ColumnType::Double, writer)
            }
            CassCqlValue::Boolean(v) => {
                <bool as SerializeValue>::serialize(v, &ColumnType::Boolean, writer)
            }
            CassCqlValue::Text(v) => {
                <String as SerializeValue>::serialize(v, &ColumnType::Text, writer)
            }
            CassCqlValue::Blob(v) => {
                <Vec<u8> as SerializeValue>::serialize(v, &ColumnType::Blob, writer)
            }
            CassCqlValue::Uuid(v) => {
                <Uuid as SerializeValue>::serialize(v, &ColumnType::Uuid, writer)
            }
            CassCqlValue::Date(v) => {
                <CqlDate as SerializeValue>::serialize(v, &ColumnType::Date, writer)
            }
            CassCqlValue::Inet(v) => {
                <IpAddr as SerializeValue>::serialize(v, &ColumnType::Inet, writer)
            }
            CassCqlValue::Duration(v) => {
                <CqlDuration as SerializeValue>::serialize(v, &ColumnType::Duration, writer)
            }
            CassCqlValue::Decimal(v) => {
                <CqlDecimal as SerializeValue>::serialize(v, &ColumnType::Decimal, writer)
            }
            CassCqlValue::Tuple(fields) => serialize_tuple_like(fields.iter(), writer),
            CassCqlValue::List(l) => serialize_sequence(l.len(), l.iter(), writer),
            CassCqlValue::Map(m) => {
                serialize_mapping(m.len(), m.iter().map(|p| (&p.0, &p.1)), writer)
            }
            CassCqlValue::Set(s) => serialize_sequence(s.len(), s.iter(), writer),
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
    use crate::{
        cass_types::{CassDataType, CassValueType},
        value::{is_type_compatible, CassCqlValue},
    };

    fn all_value_data_types() -> [CassDataType; 26] {
        let from = |v_typ: CassValueType| CassDataType::Value(v_typ);

        [
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
        let from = |v_typ: CassValueType| CassDataType::Value(v_typ);
        struct TestCase {
            value: Option<CassCqlValue>,
            compatible_types: Vec<CassDataType>,
        }

        let test_cases = [
            // Null -> all types
            TestCase {
                value: None,
                compatible_types: all_value_data_types().to_vec(),
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
}
