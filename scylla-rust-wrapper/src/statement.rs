use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::exec_profile::PerStatementExecProfile;
use crate::prepared::CassPrepared;
use crate::query_result::CassResult;
use crate::retry_policy::CassRetryPolicy;
use crate::types::*;
use crate::value::CassCqlValue;
use crate::{argconv::*, value};
use scylla::frame::types::Consistency;
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::{PagingState, PagingStateResponse};
use std::collections::HashMap;
use std::convert::TryInto;
use std::os::raw::{c_char, c_int};
use std::slice;
use std::sync::Arc;

#[derive(Clone)]
pub enum Statement {
    Simple(SimpleQuery),
    // Arc is needed, because PreparedStatement is passed by reference to session.execute
    Prepared(Arc<CassPrepared>),
}

#[derive(Clone)]
pub struct SimpleQuery {
    pub query: Query,
    pub name_to_bound_index: HashMap<String, usize>,
}

pub struct CassStatement {
    pub statement: Statement,
    pub bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
    pub paging_state: PagingState,
    pub paging_enabled: bool,
    pub request_timeout_ms: Option<cass_uint64_t>,

    pub(crate) exec_profile: Option<PerStatementExecProfile>,
}

impl BoxFFI for CassStatement {}

impl CassStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        let (bound_value, maybe_data_type) = match &self.statement {
            Statement::Simple(_) => match self.bound_values.get_mut(index) {
                Some(v) => (v, None),
                None => return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            },
            Statement::Prepared(p) => match (
                self.bound_values.get_mut(index),
                p.variable_col_data_types.get(index),
            ) {
                (Some(v), Some(dt)) => (v, Some(dt)),
                (None, None) => return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
                // This indicates a length mismatch between col specs table and self.bound_values.
                //
                // It can only occur when user provides bad `count` value in `cass_statement_reset_parameters`.
                // Cpp-driver does not verify that both of these values are equal.
                // I believe returning CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS is best we can do here.
                _ => return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            },
        };

        // Perform the typecheck.
        if let Some(dt) = maybe_data_type {
            if !value::is_type_compatible(&value, dt) {
                return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
            }
        }

        *bound_value = Set(value);
        CassError::CASS_OK
    }

    fn bind_multiple_values_by_name(
        &mut self,
        indices: &[usize],
        value: Option<CassCqlValue>,
    ) -> CassError {
        for i in indices {
            let bind_status = self.bind_cql_value(*i, value.clone());

            if bind_status != CassError::CASS_OK {
                return bind_status;
            }
        }

        CassError::CASS_OK
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CassCqlValue>) -> CassError {
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
                    .statement
                    .get_variable_col_specs()
                    .iter()
                    .enumerate()
                    .filter(|(_, col)| {
                        is_case_sensitive && col.name() == name_str
                            || !is_case_sensitive && col.name().eq_ignore_ascii_case(name_str)
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

    fn reset_bound_values(&mut self, count: usize) {
        // Clear bound values and resize the vector - all values should be unset.
        self.bound_values.clear();
        self.bound_values.resize(count, Unset);
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

    let query = Query::new(query_str.to_string());

    let simple_query = SimpleQuery {
        query,
        name_to_bound_index: HashMap::with_capacity(parameter_count as usize),
    };

    BoxFFI::into_ptr(Box::new(CassStatement {
        statement: Statement::Simple(simple_query),
        bound_values: vec![Unset; parameter_count as usize],
        paging_state: PagingState::start(),
        // Cpp driver disables paging by default.
        paging_enabled: false,
        request_timeout_ms: None,
        exec_profile: None,
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: *mut CassStatement) {
    BoxFFI::free(statement_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_consistency(
    statement: *mut CassStatement,
    consistency: CassConsistency,
) -> CassError {
    let consistency_opt = get_consistency_from_cass_consistency(consistency);

    if let Some(consistency) = consistency_opt {
        match &mut BoxFFI::as_mut_ref(statement).statement {
            Statement::Simple(inner) => inner.query.set_consistency(consistency),
            Statement::Prepared(inner) => {
                Arc::make_mut(inner).statement.set_consistency(consistency)
            }
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_size(
    statement_raw: *mut CassStatement,
    page_size: c_int,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(statement_raw);
    if page_size <= 0 {
        // Cpp driver sets the page size flag only for positive page size provided by user.
        statement.paging_enabled = false;
    } else {
        statement.paging_enabled = true;
        match &mut statement.statement {
            Statement::Simple(inner) => inner.query.set_page_size(page_size),
            Statement::Prepared(inner) => Arc::make_mut(inner).statement.set_page_size(page_size),
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_state(
    statement: *mut CassStatement,
    result: *const CassResult,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(statement);
    let result = ArcFFI::as_ref(result);

    match &result.paging_state_response {
        PagingStateResponse::HasMorePages { state } => statement.paging_state.clone_from(state),
        PagingStateResponse::NoMorePages => statement.paging_state = PagingState::start(),
    }
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_state_token(
    statement: *mut CassStatement,
    paging_state: *const c_char,
    paging_state_size: size_t,
) -> CassError {
    let statement_from_raw = BoxFFI::as_mut_ref(statement);

    if paging_state.is_null() {
        statement_from_raw.paging_state = PagingState::start();
        return CassError::CASS_ERROR_LIB_NULL_VALUE;
    }

    let paging_state_usize: usize = paging_state_size.try_into().unwrap();
    let paging_state_bytes = slice::from_raw_parts(paging_state as *const u8, paging_state_usize);
    statement_from_raw.paging_state = PagingState::new_from_raw_bytes(paging_state_bytes);
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_is_idempotent(
    statement_raw: *mut CassStatement,
    is_idempotent: cass_bool_t,
) -> CassError {
    match &mut BoxFFI::as_mut_ref(statement_raw).statement {
        Statement::Simple(inner) => inner.query.set_is_idempotent(is_idempotent != 0),
        Statement::Prepared(inner) => Arc::make_mut(inner)
            .statement
            .set_is_idempotent(is_idempotent != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_tracing(
    statement_raw: *mut CassStatement,
    enabled: cass_bool_t,
) -> CassError {
    match &mut BoxFFI::as_mut_ref(statement_raw).statement {
        Statement::Simple(inner) => inner.query.set_tracing(enabled != 0),
        Statement::Prepared(inner) => Arc::make_mut(inner).statement.set_tracing(enabled != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_retry_policy(
    statement: *mut CassStatement,
    retry_policy: *const CassRetryPolicy,
) -> CassError {
    let maybe_arced_retry_policy: Option<Arc<dyn scylla::retry_policy::RetryPolicy>> =
        ArcFFI::as_maybe_ref(retry_policy).map(|policy| match policy {
            CassRetryPolicy::DefaultRetryPolicy(default) => {
                default.clone() as Arc<dyn scylla::retry_policy::RetryPolicy>
            }
            CassRetryPolicy::FallthroughRetryPolicy(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistencyRetryPolicy(downgrading) => downgrading.clone(),
        });

    match &mut BoxFFI::as_mut_ref(statement).statement {
        Statement::Simple(inner) => inner.query.set_retry_policy(maybe_arced_retry_policy),
        Statement::Prepared(inner) => Arc::make_mut(inner)
            .statement
            .set_retry_policy(maybe_arced_retry_policy),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_serial_consistency(
    statement: *mut CassStatement,
    serial_consistency: CassConsistency,
) -> CassError {
    // cpp-driver doesn't validate passed value in any way.
    // If it is an incorrect serial-consistency value then it will be set
    // and sent as-is.
    // Before adapting the driver to Rust Driver 0.12 this code
    // set serial consistency if a user passed correct value and set it to
    // None otherwise.
    // I think that failing explicitly is a better idea, so I decided to return
    // and error
    let consistency = match get_consistency_from_cass_consistency(serial_consistency) {
        Some(Consistency::Serial) => SerialConsistency::Serial,
        Some(Consistency::LocalSerial) => SerialConsistency::LocalSerial,
        _ => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    match &mut BoxFFI::as_mut_ref(statement).statement {
        Statement::Simple(inner) => inner.query.set_serial_consistency(Some(consistency)),
        Statement::Prepared(inner) => Arc::make_mut(inner)
            .statement
            .set_serial_consistency(Some(consistency)),
    }

    CassError::CASS_OK
}

fn get_consistency_from_cass_consistency(consistency: CassConsistency) -> Option<Consistency> {
    match consistency {
        CassConsistency::CASS_CONSISTENCY_ANY => Some(Consistency::Any),
        CassConsistency::CASS_CONSISTENCY_ONE => Some(Consistency::One),
        CassConsistency::CASS_CONSISTENCY_TWO => Some(Consistency::Two),
        CassConsistency::CASS_CONSISTENCY_THREE => Some(Consistency::Three),
        CassConsistency::CASS_CONSISTENCY_QUORUM => Some(Consistency::Quorum),
        CassConsistency::CASS_CONSISTENCY_ALL => Some(Consistency::All),
        CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Some(Consistency::LocalQuorum),
        CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Some(Consistency::EachQuorum),
        CassConsistency::CASS_CONSISTENCY_SERIAL => Some(Consistency::Serial),
        CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Some(Consistency::LocalSerial),
        CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Some(Consistency::LocalOne),
        _ => None,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_timestamp(
    statement: *mut CassStatement,
    timestamp: cass_int64_t,
) -> CassError {
    match &mut BoxFFI::as_mut_ref(statement).statement {
        Statement::Simple(inner) => inner.query.set_timestamp(Some(timestamp)),
        Statement::Prepared(inner) => Arc::make_mut(inner)
            .statement
            .set_timestamp(Some(timestamp)),
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

    let statement_from_raw = BoxFFI::as_mut_ref(statement);
    statement_from_raw.request_timeout_ms = Some(timeout_ms);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_reset_parameters(
    statement_raw: *mut CassStatement,
    count: size_t,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(statement_raw);
    statement.reset_bound_values(count as usize);

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
    duration,
    cass_statement_bind_duration,
    cass_statement_bind_duration_by_name,
    cass_statement_bind_duration_by_name_n
);
make_binders!(
    decimal,
    cass_statement_bind_decimal,
    cass_statement_bind_decimal_by_name,
    cass_statement_bind_decimal_by_name_n
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
