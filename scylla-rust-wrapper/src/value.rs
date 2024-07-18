use std::{convert::TryInto, net::IpAddr};

use scylla::{
    frame::{response::result::ColumnType, value::CqlDate},
    serialize::{
        value::{
            BuiltinSerializationErrorKind, MapSerializationErrorKind, SerializeCql,
            SetOrListSerializationErrorKind, TupleSerializationErrorKind,
            UdtSerializationErrorKind,
        },
        writers::WrittenCellProof,
        CellWriter, SerializationError,
    },
};
use uuid::Uuid;

/// A narrower version of rust driver's CqlValue.
///
/// cpp-driver's API allows to map single rust type to
/// multiple CQL types. For example `cass_statement_bind_int64`
/// can be used to bind a value to the column of following CQL types:
/// - bigint
/// - time
/// - counter
/// - timestamp
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

impl SerializeCql for CassCqlValue {
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
            CassCqlValue::TinyInt(v) => serialize_i8(v, writer),
            CassCqlValue::SmallInt(v) => serialize_i16(v, writer),
            CassCqlValue::Int(v) => serialize_i32(v, writer),
            CassCqlValue::BigInt(v) => serialize_i64(v, writer),
            CassCqlValue::Float(v) => serialize_f32(v, writer),
            CassCqlValue::Double(v) => serialize_f64(v, writer),
            CassCqlValue::Boolean(v) => serialize_bool(v, writer),
            CassCqlValue::Text(v) => serialize_text(v, writer),
            CassCqlValue::Blob(v) => serialize_blob(v, writer),
            CassCqlValue::Uuid(v) => serialize_uuid(v, writer),
            CassCqlValue::Date(v) => serialize_date(v, writer),
            CassCqlValue::Inet(v) => serialize_inet(v, writer),
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

/// Generates a function that serializes a value of given type.
macro_rules! fn_serialize_via_writer {
    ($ser_fn:ident,
     $rust_typ:tt$(<$($targ:tt),+>)?,
     |$ser_me:ident, $writer:ident| $ser:expr) => {
        fn $ser_fn<'b>(
            v: &$rust_typ$(<$($targ),+>)?,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            let $writer = writer;
            let $ser_me = v;
            let proof = $ser;
            Ok(proof)
        }
    };
}

fn_serialize_via_writer!(serialize_i8, i8, |me, writer| {
    writer.set_value(me.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_i16, i16, |me, writer| {
    writer.set_value(me.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_i32, i32, |me, writer| {
    writer.set_value(me.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_i64, i64, |me, writer| {
    writer.set_value(me.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_f32, f32, |me, writer| {
    writer.set_value(me.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_f64, f64, |me, writer| {
    writer.set_value(me.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_bool, bool, |me, writer| {
    writer.set_value(&[*me as u8]).unwrap()
});
fn_serialize_via_writer!(serialize_text, String, |me, writer| {
    writer
        .set_value(me.as_bytes())
        .map_err(|_| mk_ser_err::<String>(BuiltinSerializationErrorKind::SizeOverflow))?
});
fn_serialize_via_writer!(serialize_blob, Vec<u8>, |me, writer| {
    writer
        .set_value(me.as_ref())
        .map_err(|_| mk_ser_err::<Vec<u8>>(BuiltinSerializationErrorKind::SizeOverflow))?
});
fn_serialize_via_writer!(serialize_uuid, Uuid, |me, writer| {
    writer.set_value(me.as_bytes().as_ref()).unwrap()
});
fn_serialize_via_writer!(serialize_date, CqlDate, |me, writer| {
    writer.set_value(me.0.to_be_bytes().as_slice()).unwrap()
});
fn_serialize_via_writer!(serialize_inet, IpAddr, |me, writer| {
    match me {
        IpAddr::V4(ip) => writer.set_value(&ip.octets()).unwrap(),
        IpAddr::V6(ip) => writer.set_value(&ip.octets()).unwrap(),
    }
});

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
