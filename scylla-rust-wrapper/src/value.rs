use std::net::IpAddr;

use scylla::{
    frame::{response::result::ColumnType, value::CqlDate},
    serialize::{
        value::{BuiltinSerializationErrorKind, SerializeCql},
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
        _writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        todo!()
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
