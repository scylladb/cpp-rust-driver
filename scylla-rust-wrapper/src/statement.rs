use crate::argconv::*;
use crate::cass_error::CassError;
use crate::collection::CassCollection;
use crate::inet::CassInet;
use crate::query_result::CassResult;
use crate::types::*;
use crate::uuid::CassUuid;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::*;
use scylla::frame::types::Consistency;
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use scylla::query::Query;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::Bytes;
use std::convert::TryInto;
use std::os::raw::{c_char, c_int};
use std::sync::Arc;

#[derive(Clone)]
pub enum Statement {
    Simple(Query),
    // Arc is needed, because PreparedStatement is passed by reference to session.execute
    Prepared(Arc<PreparedStatement>),
}

pub struct CassStatement {
    pub statement: Statement,
    pub bound_values: Vec<MaybeUnset<Option<CqlValue>>>,
    pub paging_state: Option<Bytes>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new(
    query: *const c_char,
    parameter_count: size_t,
) -> *mut CassStatement {
    let query_str = match ptr_to_cstr(query) {
        Some(v) => v,
        None => return std::ptr::null_mut(),
    };
    let query_length = query_str.len();

    cass_statement_new_n(query, query_length as size_t, parameter_count)
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new_n(
    query: *const c_char,
    query_length: size_t,
    parameter_count: size_t,
) -> *mut CassStatement {
    let query_str = match ptr_to_cstr_n(query, query_length) {
        Some(v) => v,
        None => return std::ptr::null_mut(),
    };

    let mut query = Query::new(query_str.to_string());

    // Set Cpp Driver default configuration for queries:
    query.disable_paging();
    query.set_consistency(Consistency::One);

    Box::into_raw(Box::new(CassStatement {
        statement: Statement::Simple(query),
        bound_values: vec![Unset; parameter_count as usize],
        paging_state: None,
    }))
}

// TODO: Bind methods currently not implemented:
// cass_statement_bind_decimal
//
// cass_statement_bind_duration - DURATION not implemented in Rust Driver
//
// (methods requiring implementing cpp driver data structures)
// cass_statement_bind_user_type
// cass_statement_bind_custom
// cass_statement_bind_custom_n
// cass_statement_bind_tuple

unsafe fn cass_statement_bind_maybe_unset(
    statement_raw: *mut CassStatement,
    index: size_t,
    value: MaybeUnset<Option<CqlValue>>,
) -> CassError {
    let statement = ptr_to_ref_mut(statement_raw);

    if index as usize >= statement.bound_values.len() {
        CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS
    } else {
        statement.bound_values[index as usize] = value;
        CassError::CASS_OK
    }
}

unsafe fn cass_statement_bind_maybe_unset_by_name(
    statement: *mut CassStatement,
    name: *const c_char,
    value: MaybeUnset<Option<CqlValue>>,
) -> CassError {
    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_statement_bind_maybe_unset_by_name_n(statement, name, name_length as size_t, value)
}

unsafe fn cass_statement_bind_maybe_unset_by_name_n(
    statement_raw: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
    value: MaybeUnset<Option<CqlValue>>,
) -> CassError {
    let name_str = ptr_to_cstr_n(name, name_length).unwrap();
    let statement = &ptr_to_ref_mut(statement_raw).statement;

    match &statement {
        Statement::Prepared(prepared) => {
            for (i, col) in prepared.get_metadata().col_specs.iter().enumerate() {
                if col.name == name_str {
                    return cass_statement_bind_maybe_unset(statement_raw, i as size_t, value);
                }
            }
            CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
        }
        Statement::Simple(_) => CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
    }
}

unsafe fn cass_statement_bind_cql_value(
    statement: *mut CassStatement,
    index: size_t,
    value: CqlValue,
) -> CassError {
    cass_statement_bind_maybe_unset(statement, index, Set(Some(value)))
}

unsafe fn cass_statement_bind_cql_value_by_name(
    statement: *mut CassStatement,
    name: *const c_char,
    value: CqlValue,
) -> CassError {
    cass_statement_bind_maybe_unset_by_name(statement, name, Set(Some(value)))
}

unsafe fn cass_statement_bind_cql_value_by_name_n(
    statement: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
    value: CqlValue,
) -> CassError {
    cass_statement_bind_maybe_unset_by_name_n(statement, name, name_length, Set(Some(value)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_null(
    statement: *mut CassStatement,
    index: size_t,
) -> CassError {
    cass_statement_bind_maybe_unset(statement, index, Set(None))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_null_by_name(
    statement: *mut CassStatement,
    name: *const c_char,
) -> CassError {
    cass_statement_bind_maybe_unset_by_name(statement, name, Set(None))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_null_by_name_n(
    statement: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    cass_statement_bind_maybe_unset_by_name_n(statement, name, name_length, Set(None))
}

macro_rules! make_binders {
    ($fn_by_idx:ident, $fn_by_name:ident, $fn_by_name_n:ident, $t:ty, $e:expr) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_idx(
            statement: *mut CassStatement,
            index: size_t,
            value: $t,
        ) -> CassError {
            match ($e)(value) {
                Ok(v) => cass_statement_bind_cql_value(statement, index, v),
                Err(e) => e,
            }
        }

        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name(
            statement: *mut CassStatement,
            name: *const c_char,
            value: $t,
        ) -> CassError {
            match ($e)(value) {
                Ok(v) => cass_statement_bind_cql_value_by_name(statement, name, v),
                Err(e) => e,
            }
        }

        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name_n(
            statement: *mut CassStatement,
            name: *const c_char,
            name_length: size_t,
            value: $t,
        ) -> CassError {
            match ($e)(value) {
                Ok(v) => cass_statement_bind_cql_value_by_name_n(statement, name, name_length, v),
                Err(e) => e,
            }
        }
    };
}

make_binders!(
    cass_statement_bind_int8,
    cass_statement_bind_int8_by_name,
    cass_statement_bind_int8_by_name_n,
    cass_int8_t,
    |v| Ok(TinyInt(v))
);

make_binders!(
    cass_statement_bind_int16,
    cass_statement_bind_int16_by_name,
    cass_statement_bind_int16_by_name_n,
    cass_int16_t,
    |v| Ok(SmallInt(v))
);

make_binders!(
    cass_statement_bind_int32,
    cass_statement_bind_int32_by_name,
    cass_statement_bind_int32_by_name_n,
    cass_int32_t,
    |v| Ok(Int(v))
);

// cass_statement_bind_uint32 is only used to set a DATE.
make_binders!(
    cass_statement_bind_uint32,
    cass_statement_bind_uint32_by_name,
    cass_statement_bind_uint32_by_name_n,
    cass_uint32_t,
    |v| Ok(Date(v))
);

make_binders!(
    cass_statement_bind_int64,
    cass_statement_bind_int64_by_name,
    cass_statement_bind_int64_by_name_n,
    cass_int64_t,
    |v| Ok(BigInt(v))
);

make_binders!(
    cass_statement_bind_float,
    cass_statement_bind_float_by_name,
    cass_statement_bind_float_by_name_n,
    cass_float_t,
    |v| Ok(Float(v))
);

make_binders!(
    cass_statement_bind_double,
    cass_statement_bind_double_by_name,
    cass_statement_bind_double_by_name_n,
    cass_double_t,
    |v| Ok(Double(v))
);

make_binders!(
    cass_statement_bind_bool,
    cass_statement_bind_bool_by_name,
    cass_statement_bind_bool_by_name_n,
    cass_bool_t,
    |v| Ok(Boolean(v != 0))
);

make_binders!(
    cass_statement_bind_inet,
    cass_statement_bind_inet_by_name,
    cass_statement_bind_inet_by_name_n,
    CassInet,
    |v: CassInet| {
        // Err if length in struct is invalid.
        // cppdriver doesn't check this - it encodes any length given to it
        // but it doesn't seem like something we wanna do. Also, rust driver can't
        // really do it afaik.
        match v.try_into() {
            Ok(v) => Ok(Inet(v)),
            Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    }
);

make_binders!(
    cass_statement_bind_uuid,
    cass_statement_bind_uuid_by_name,
    cass_statement_bind_uuid_by_name_n,
    CassUuid,
    |v: CassUuid| Ok(Uuid(v.into()))
);

make_binders!(
    cass_statement_bind_collection,
    cass_statement_bind_collection_by_name,
    cass_statement_bind_collection_by_name_n,
    *const CassCollection,
    |p: *const CassCollection| {
        match ptr_to_ref(p).clone().try_into() {
            Ok(v) => Ok(v),
            Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    }
);

// The following four functions cannot be realized with make_binders!
// because of the string length for the value

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_string(
    statement: *mut CassStatement,
    index: size_t,
    value: *const c_char,
) -> CassError {
    let value_str = ptr_to_cstr(value).unwrap();
    let value_length = value_str.len();

    cass_statement_bind_string_n(statement, index, value, value_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_string_by_name(
    statement: *mut CassStatement,
    name: *const c_char,
    value: *const c_char,
) -> CassError {
    let value_str = ptr_to_cstr(value).unwrap();
    let value_length = value_str.len();

    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_statement_bind_string_by_name_n(
        statement,
        name,
        name_length as size_t,
        value,
        value_length as size_t,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_string_n(
    statement: *mut CassStatement,
    index: size_t,
    value: *const c_char,
    value_length: size_t,
) -> CassError {
    // TODO: Error handling
    let value_string = ptr_to_cstr_n(value, value_length).unwrap().to_string();
    cass_statement_bind_cql_value(statement, index, Text(value_string))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_string_by_name_n(
    statement: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
    value: *const c_char,
    value_length: size_t,
) -> CassError {
    // TODO: Error handling
    let value_string = ptr_to_cstr_n(value, value_length).unwrap().to_string();
    cass_statement_bind_cql_value_by_name_n(statement, name, name_length, Text(value_string))
}

// The following three functions cannot be realized with make_binders!
// because of the bytes length for the value

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_bytes(
    statement: *mut CassStatement,
    index: size_t,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_statement_bind_cql_value(statement, index, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_bytes_by_name(
    statement: *mut CassStatement,
    name: *const c_char,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_statement_bind_cql_value_by_name(statement, name, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_bind_bytes_by_name_n(
    statement: *mut CassStatement,
    name: *const c_char,
    name_length: size_t,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_statement_bind_cql_value_by_name_n(statement, name, name_length, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_tracing(
    statement_raw: *mut CassStatement,
    enabled: cass_bool_t,
) -> CassError {
    match &mut ptr_to_ref_mut(statement_raw).statement {
        Statement::Simple(inner) => inner.set_tracing(enabled != 0),
        Statement::Prepared(inner) => Arc::make_mut(inner).set_tracing(enabled != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_size(
    statement_raw: *mut CassStatement,
    page_size: c_int,
) -> CassError {
    // TODO: validate page_size
    match &mut ptr_to_ref_mut(statement_raw).statement {
        Statement::Simple(inner) => {
            if page_size == -1 {
                inner.disable_paging()
            } else {
                inner.set_page_size(page_size)
            }
        }
        Statement::Prepared(inner) => {
            if page_size == -1 {
                Arc::make_mut(inner).disable_paging()
            } else {
                Arc::make_mut(inner).set_page_size(page_size)
            }
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_state(
    statement: *mut CassStatement,
    result: *const CassResult,
) -> CassError {
    let statement = ptr_to_ref_mut(statement);
    let result = ptr_to_ref(result);

    statement.paging_state = result.paging_state.clone();
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_is_idempotent(
    statement_raw: *mut CassStatement,
    is_idempotent: cass_bool_t,
) -> CassError {
    match &mut ptr_to_ref_mut(statement_raw).statement {
        Statement::Simple(inner) => inner.set_is_idempotent(is_idempotent != 0),
        Statement::Prepared(inner) => Arc::make_mut(inner).set_is_idempotent(is_idempotent != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    free_boxed(statement_raw);
}
