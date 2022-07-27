use crate::argconv::*;
use crate::cass_error::CassError;
use crate::query_result::CassResult;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::types::Consistency;
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use scylla::query::Query;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::Bytes;
use std::os::raw::{c_char, c_int};
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_query_error.rs"));

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

impl CassStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CqlValue>) -> CassError {
        if index as usize >= self.bound_values.len() {
            CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS
        } else {
            self.bound_values[index] = Set(value);
            CassError::CASS_OK
        }
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CqlValue>) -> CassError {
        match &self.statement {
            Statement::Prepared(prepared) => {
                for (i, col) in prepared
                    .get_prepared_metadata()
                    .col_specs
                    .iter()
                    .enumerate()
                {
                    if col.name == name {
                        return self.bind_cql_value(i, value);
                    }
                }
                CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
            }
            Statement::Simple(_) => CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new(
    query: *const c_char,
    parameter_count: size_t,
) -> *mut CassStatement {
    cass_statement_new_n(query, strlen(query), parameter_count)
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

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    free_boxed(statement_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_consistency(
    _statement: *mut CassStatement,
    _consistency: CassConsistency,
) -> CassError {
    // FIXME: should return CASS_OK if successful, otherwise an error occurred.
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

    statement.paging_state = result.metadata.paging_state.clone();
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

prepare_binders_macro!(@index_and_name CassStatement,
    |s: &mut CassStatement, idx, v| s.bind_cql_value(idx, v),
    |s: &mut CassStatement, name, v| s.bind_cql_value_by_name(name, v));
make_binders!(
    null,
    cass_statement_bind_null,
    cass_statement_bind_null_by_name,
    cass_statement_bind_null_by_name_n
);
make_binders!(
    int8,
    cass_statement_bind_int8,
    cass_statement_bind_int8_by_name,
    cass_statement_bind_int8_by_name_n
);
make_binders!(
    int16,
    cass_statement_bind_int16,
    cass_statement_bind_int16_by_name,
    cass_statement_bind_int16_by_name_n
);
make_binders!(
    int32,
    cass_statement_bind_int32,
    cass_statement_bind_int32_by_name,
    cass_statement_bind_int32_by_name_n
);
make_binders!(
    uint32,
    cass_statement_bind_uint32,
    cass_statement_bind_uint32_by_name,
    cass_statement_bind_uint32_by_name_n
);
make_binders!(
    int64,
    cass_statement_bind_int64,
    cass_statement_bind_int64_by_name,
    cass_statement_bind_int64_by_name_n
);
make_binders!(
    float,
    cass_statement_bind_float,
    cass_statement_bind_float_by_name,
    cass_statement_bind_float_by_name_n
);
make_binders!(
    double,
    cass_statement_bind_double,
    cass_statement_bind_double_by_name,
    cass_statement_bind_double_by_name_n
);
make_binders!(
    bool,
    cass_statement_bind_bool,
    cass_statement_bind_bool_by_name,
    cass_statement_bind_bool_by_name_n
);
make_binders!(
    string,
    cass_statement_bind_string,
    string,
    cass_statement_bind_string_by_name,
    string_n,
    cass_statement_bind_string_by_name_n
);
make_binders!(@index string_n, cass_statement_bind_string_n);
make_binders!(
    bytes,
    cass_statement_bind_bytes,
    cass_statement_bind_bytes_by_name,
    cass_statement_bind_bytes_by_name_n
);
make_binders!(
    uuid,
    cass_statement_bind_uuid,
    cass_statement_bind_uuid_by_name,
    cass_statement_bind_uuid_by_name_n
);
make_binders!(
    inet,
    cass_statement_bind_inet,
    cass_statement_bind_inet_by_name,
    cass_statement_bind_inet_by_name_n
);
make_binders!(
    collection,
    cass_statement_bind_collection,
    cass_statement_bind_collection_by_name,
    cass_statement_bind_collection_by_name_n
);
make_binders!(
    tuple,
    cass_statement_bind_tuple,
    cass_statement_bind_tuple_by_name,
    cass_statement_bind_tuple_by_name_n
);
make_binders!(
    user_type,
    cass_statement_bind_user_type,
    cass_statement_bind_user_type_by_name,
    cass_statement_bind_user_type_by_name_n
);
