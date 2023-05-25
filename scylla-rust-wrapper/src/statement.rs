use crate::argconv::*;
use crate::cass_error::CassError;
use crate::query_result::CassResult;
use crate::retry_policy::CassRetryPolicy;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::types::LegacyConsistency::{Regular, Serial};
use scylla::frame::types::{Consistency, LegacyConsistency};
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use scylla::query::Query;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::statement::SerialConsistency;
use scylla::Bytes;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int};
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_query_error.rs"));

#[derive(Clone)]
pub enum Statement {
    Simple(SimpleQuery),
    // Arc is needed, because PreparedStatement is passed by reference to session.execute
    Prepared(Arc<PreparedStatement>),
}

#[derive(Clone)]
pub struct SimpleQuery {
    pub query: Query,
    pub name_to_bound_index: HashMap<String, usize>,
}

pub struct CassStatement {
    pub statement: Statement,
    pub bound_values: Vec<MaybeUnset<Option<CqlValue>>>,
    pub paging_state: Option<Bytes>,
    pub request_timeout_ms: Option<cass_uint64_t>,
}

impl CassStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CqlValue>) -> CassError {
        if index >= self.bound_values.len() {
            CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS
        } else {
            self.bound_values[index] = Set(value);
            CassError::CASS_OK
        }
    }

    fn bind_multiple_values_by_name(
        &mut self,
        indices: &[usize],
        value: Option<CqlValue>,
    ) -> CassError {
        for i in indices {
            let bind_status = self.bind_cql_value(*i, value.clone());

            if bind_status != CassError::CASS_OK {
                return bind_status;
            }
        }

        CassError::CASS_OK
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CqlValue>) -> CassError {
        let mut set_bound_val_index: Option<usize> = None;
        let mut name_str = name;
        let mut is_case_sensitive = false;

        if name_str.starts_with('\"') && name_str.ends_with('\"') {
            name_str = name_str.strip_prefix('\"').unwrap();
            name_str = name_str.strip_suffix('\"').unwrap();
            is_case_sensitive = true;
        }

        match &self.statement {
            Statement::Prepared(prepared) => {
                let indices: Vec<usize> = prepared
                    .get_prepared_metadata()
                    .col_specs
                    .iter()
                    .enumerate()
                    .filter(|(_, col)| {
                        is_case_sensitive && col.name == name_str
                            || !is_case_sensitive && col.name.eq_ignore_ascii_case(name_str)
                    })
                    .map(|(i, _)| i)
                    .collect();

                if indices.is_empty() {
                    return CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
                }

                return self.bind_multiple_values_by_name(&indices, value);
            }
            Statement::Simple(query) => {
                let index = query.name_to_bound_index.get(name);

                if let Some(idx) = index {
                    return self.bind_cql_value(*idx, value);
                } else {
                    for (index, bound_val) in self.bound_values.iter().enumerate() {
                        if let Unset = bound_val {
                            set_bound_val_index = Some(index);
                            break;
                        }
                    }
                }
            }
        }

        if let Some(index) = set_bound_val_index {
            if let Statement::Simple(query) = &mut self.statement {
                query.name_to_bound_index.insert(name.to_string(), index);
            }

            return self.bind_cql_value(index, value);
        }

        CassError::CASS_OK
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
    // TODO: Consider moving this configuration to the default execution profile.
    // With the current way, any changes to consistency on exec profile level will be
    // anyway overriden with this setting.
    query.set_consistency(Consistency::LocalOne);

    let simple_query = SimpleQuery {
        query,
        name_to_bound_index: HashMap::with_capacity(parameter_count as usize),
    };

    Box::into_raw(Box::new(CassStatement {
        statement: Statement::Simple(simple_query),
        bound_values: vec![Unset; parameter_count as usize],
        paging_state: None,
        request_timeout_ms: None,
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    free_boxed(statement_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_consistency(
    statement: *mut CassStatement,
    consistency: CassConsistency,
) -> CassError {
    let consistency_opt = get_consistency_from_cass_consistency(consistency);

    if let Some(Regular(regular_consistency)) = consistency_opt {
        match &mut ptr_to_ref_mut(statement).statement {
            Statement::Simple(inner) => inner.query.set_consistency(regular_consistency),
            Statement::Prepared(inner) => Arc::make_mut(inner).set_consistency(regular_consistency),
        }
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
                inner.query.disable_paging()
            } else {
                inner.query.set_page_size(page_size)
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
        Statement::Simple(inner) => inner.query.set_is_idempotent(is_idempotent != 0),
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
        Statement::Simple(inner) => inner.query.set_tracing(enabled != 0),
        Statement::Prepared(inner) => Arc::make_mut(inner).set_tracing(enabled != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_retry_policy(
    statement: *mut CassStatement,
    retry_policy: *const CassRetryPolicy,
) -> CassError {
    let arced_retry_policy: Arc<dyn scylla::retry_policy::RetryPolicy> =
        // Shouldn't this accept NULL ptr for unsetting retry policy on statement?
        match ptr_to_ref(retry_policy) {
            CassRetryPolicy::DefaultRetryPolicy(default) => default.clone(),
            CassRetryPolicy::FallthroughRetryPolicy(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistencyRetryPolicy(downgrading) => downgrading.clone(),
        };

    match &mut ptr_to_ref_mut(statement).statement {
        Statement::Simple(inner) => inner.query.set_retry_policy(Some(arced_retry_policy)),
        Statement::Prepared(inner) => {
            Arc::make_mut(inner).set_retry_policy(Some(arced_retry_policy))
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_serial_consistency(
    statement: *mut CassStatement,
    serial_consistency: CassConsistency,
) -> CassError {
    let consistency = get_consistency_from_cass_consistency(serial_consistency);

    let serial_consistency = match consistency {
        Some(Serial(s)) => Some(s),
        _ => None,
    };

    match &mut ptr_to_ref_mut(statement).statement {
        Statement::Simple(inner) => inner.query.set_serial_consistency(serial_consistency),
        Statement::Prepared(inner) => {
            Arc::make_mut(inner).set_serial_consistency(serial_consistency)
        }
    }

    CassError::CASS_OK
}

fn get_consistency_from_cass_consistency(
    consistency: CassConsistency,
) -> Option<LegacyConsistency> {
    match consistency {
        CassConsistency::CASS_CONSISTENCY_ANY => Some(Regular(Consistency::Any)),
        CassConsistency::CASS_CONSISTENCY_ONE => Some(Regular(Consistency::One)),
        CassConsistency::CASS_CONSISTENCY_TWO => Some(Regular(Consistency::Two)),
        CassConsistency::CASS_CONSISTENCY_THREE => Some(Regular(Consistency::Three)),
        CassConsistency::CASS_CONSISTENCY_QUORUM => Some(Regular(Consistency::Quorum)),
        CassConsistency::CASS_CONSISTENCY_ALL => Some(Regular(Consistency::All)),
        CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Some(Regular(Consistency::LocalQuorum)),
        CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Some(Regular(Consistency::EachQuorum)),
        CassConsistency::CASS_CONSISTENCY_SERIAL => Some(Serial(SerialConsistency::Serial)),
        CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => {
            Some(Serial(SerialConsistency::LocalSerial))
        }
        CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Some(Regular(Consistency::LocalOne)),
        _ => None,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_timestamp(
    statement: *mut CassStatement,
    timestamp: cass_int64_t,
) -> CassError {
    match &mut ptr_to_ref_mut(statement).statement {
        Statement::Simple(inner) => inner.query.set_timestamp(Some(timestamp)),
        Statement::Prepared(inner) => Arc::make_mut(inner).set_timestamp(Some(timestamp)),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_request_timeout(
    statement: *mut CassStatement,
    timeout_ms: cass_uint64_t,
) -> CassError {
    // The maximum duration for a sleep is 68719476734 milliseconds (approximately 2.2 years).
    // Note: this is limited by tokio::time:timout
    // https://github.com/tokio-rs/tokio/blob/4b1c4801b1383800932141d0f6508d5b3003323e/tokio/src/time/driver/wheel/mod.rs#L44-L50
    let request_timeout_limit = 2_u64.pow(36) - 1;
    if timeout_ms >= request_timeout_limit {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let statement_from_raw = ptr_to_ref_mut(statement);
    statement_from_raw.request_timeout_ms = Some(timeout_ms);

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
