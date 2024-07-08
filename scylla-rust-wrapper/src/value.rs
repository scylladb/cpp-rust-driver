use std::net::IpAddr;

use scylla::{
    frame::{response::result::ColumnType, value::CqlDate},
    serialize::{value::SerializeCql, writers::WrittenCellProof, CellWriter, SerializationError},
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
