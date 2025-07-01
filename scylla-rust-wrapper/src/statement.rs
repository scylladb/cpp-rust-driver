use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::config_value::MaybeUnsetConfig;
use crate::exec_profile::PerStatementExecProfile;
use crate::inet::CassInet;
use crate::prepared::CassPrepared;
use crate::query_result::{CassNode, CassResult};
use crate::retry_policy::CassRetryPolicy;
use crate::types::*;
use crate::value::CassCqlValue;
use crate::{argconv::*, value};
use scylla::frame::types::Consistency;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::{PagingState, PagingStateResponse};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use scylla::statement::SerialConsistency;
use scylla::statement::unprepared::Statement;
use scylla::value::MaybeUnset;
use scylla::value::MaybeUnset::{Set, Unset};
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::{IpAddr, SocketAddr};
use std::os::raw::{c_char, c_int};
use std::slice;
use std::str::FromStr;
use std::sync::Arc;

use thiserror::Error;

#[derive(Clone)]
pub enum BoundStatement {
    Simple(BoundSimpleStatement),
    Prepared(BoundPreparedStatement),
}

#[derive(Clone)]
pub(crate) struct BoundPreparedStatement {
    // Arc is needed, because PreparedStatement is passed by reference to session.execute
    pub(crate) statement: Arc<CassPrepared>,
    pub(crate) bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
}

impl BoundPreparedStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        match (
            self.bound_values.get_mut(index),
            self.statement.variable_col_data_types.get(index),
        ) {
            (Some(v), Some(dt)) => {
                // Perform the typecheck.
                if !value::is_type_compatible(&value, dt) {
                    return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                }

                *v = Set(value);
                CassError::CASS_OK
            }
            (None, None) => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            // This indicates a length mismatch between col specs table and self.bound_values.
            //
            // It can only occur when user provides bad `count` value in `cass_statement_reset_parameters`.
            // Cpp-driver does not verify that both of these values are equal.
            // I believe returning CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS is best we can do here.
            _ => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
        }
    }

    fn bind_cql_value_by_name(
        &mut self,
        name: &str,
        is_case_sensitive: bool,
        value: Option<CassCqlValue>,
    ) -> CassError {
        let indices: Vec<usize> = self
            .statement
            .statement
            .get_variable_col_specs()
            .iter()
            .enumerate()
            .filter(|(_, col)| {
                is_case_sensitive && col.name() == name
                    || !is_case_sensitive && col.name().eq_ignore_ascii_case(name)
            })
            .map(|(i, _)| i)
            .collect();

        if indices.is_empty() {
            return CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
        }

        self.bind_multiple_values_by_name(&indices, value)
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
}

#[derive(Clone)]
pub(crate) struct BoundSimpleStatement {
    pub(crate) query: Statement,
    pub(crate) bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
    pub(crate) name_to_bound_index: HashMap<String, usize>,
}

impl BoundSimpleStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        match self.bound_values.get_mut(index) {
            Some(v) => {
                *v = Set(value);
                CassError::CASS_OK
            }
            None => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
        }
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CassCqlValue>) -> CassError {
        let index = self.name_to_bound_index.get(name);

        if let Some(idx) = index {
            self.bind_cql_value(*idx, value)
        } else {
            let index = {
                // If new name appeared, we want to append its value to the vector.
                // This can possibly overwrite some already Set value
                // (which was set via by_index binding). cpp-driver does the same.
                let free_index = self.name_to_bound_index.len();

                // New index exceeds the number of declared parameters.
                // cpp-driver returns this error as well.
                if free_index >= self.bound_values.len() {
                    return CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
                }
                free_index
            };

            self.name_to_bound_index.insert(name.to_string(), index);
            self.bind_cql_value(index, value)
        }
    }
}

/// Used to provide a custom serialization implementation for unprepared queries.
///
/// Users are allowed to bind values by either position, or name.
/// This is done via `cass_statement_bind_*` API.
///
/// In case of binding by position, it's really simple - we simply
/// append the value at the end of `bound_values` struct.
///
/// When binding by name, we append the value at the end of `bound_values`,
/// but we also store the name-to-index mapping in `name_to_bound_index` map.
/// Having this information, and prepared metadata provided in serialization context,
/// we can build a resulting vector of bound values.
pub(crate) struct SimpleQueryRowSerializer {
    pub(crate) bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
    pub(crate) name_to_bound_index: HashMap<String, usize>,
}

#[derive(Debug, Error)]
#[error("Unknown named parameter \"{0}\"")]
pub(crate) struct UnknownNamedParameterError(String);

impl SerializeRow for SimpleQueryRowSerializer {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        if self.name_to_bound_index.is_empty() {
            // Values bound by position - no need to reorder anything.
            <_ as SerializeRow>::serialize(&self.bound_values, ctx, writer)
        } else {
            // Values bound by names - we need to make use of col specs from metadata to
            // bind the values in correct order.
            ctx.columns().iter().try_for_each(|col_spec| {
                let bound_values_idx =
                    self.name_to_bound_index
                        .get(col_spec.name())
                        .ok_or_else(|| {
                            SerializationError::new(UnknownNamedParameterError(
                                col_spec.name().to_owned(),
                            ))
                        })?;

                let val = &self.bound_values[*bound_values_idx];

                <_ as SerializeValue>::serialize(val, col_spec.typ(), writer.make_cell_writer())?;

                Ok(())
            })
        }
    }

    fn is_empty(&self) -> bool {
        self.bound_values.is_empty()
    }
}

pub struct CassStatement {
    pub(crate) statement: BoundStatement,
    pub(crate) paging_state: PagingState,
    pub(crate) paging_enabled: bool,
    pub(crate) request_timeout_ms: Option<cass_uint64_t>,

    pub(crate) exec_profile: Option<PerStatementExecProfile>,
    #[cfg(cpp_integration_testing)]
    pub(crate) record_hosts: bool,
}

impl FFI for CassStatement {
    type Origin = FromBox;
}

impl CassStatement {
    fn new(statement: BoundStatement) -> Self {
        Self {
            statement,
            paging_state: PagingState::start(),
            // Cpp driver disables paging by default.
            paging_enabled: false,
            request_timeout_ms: None,
            exec_profile: None,
            #[cfg(cpp_integration_testing)]
            record_hosts: false,
        }
    }

    pub(crate) fn new_unprepared(statement: BoundSimpleStatement) -> Self {
        Self::new(BoundStatement::Simple(statement))
    }

    pub(crate) fn new_prepared(statement: BoundPreparedStatement) -> Self {
        Self::new(BoundStatement::Prepared(statement))
    }

    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        match &mut self.statement {
            BoundStatement::Simple(simple) => simple.bind_cql_value(index, value),
            BoundStatement::Prepared(prepared) => prepared.bind_cql_value(index, value),
        }
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CassCqlValue>) -> CassError {
        // If the name was quoted, then we should treat it as case sensitive.
        let (name_unquoted, is_case_sensitive) =
            match name.strip_prefix('\"').and_then(|s| s.strip_suffix('\"')) {
                Some(name_unquoted) => (name_unquoted, true),
                None => (name, false),
            };

        match &mut self.statement {
            BoundStatement::Simple(simple) => simple.bind_cql_value_by_name(name_unquoted, value),
            BoundStatement::Prepared(prepared) => {
                prepared.bind_cql_value_by_name(name_unquoted, is_case_sensitive, value)
            }
        }
    }

    fn reset_bound_values(&mut self, count: usize) {
        // Clear bound values and resize the vector - all values should be unset.
        match &mut self.statement {
            BoundStatement::Simple(simple) => {
                simple.bound_values.clear();
                simple.bound_values.resize(count, Unset);
                simple.name_to_bound_index.clear();
            }
            BoundStatement::Prepared(prepared) => {
                prepared.bound_values.clear();
                prepared.bound_values.resize(count, Unset);
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_new(
    query: *const c_char,
    parameter_count: size_t,
) -> CassOwnedExclusivePtr<CassStatement, CMut> {
    unsafe { cass_statement_new_n(query, strlen(query), parameter_count) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_new_n(
    query: *const c_char,
    query_length: size_t,
    parameter_count: size_t,
) -> CassOwnedExclusivePtr<CassStatement, CMut> {
    let query_str = match unsafe { ptr_to_cstr_n(query, query_length) } {
        Some(v) => v,
        None => return BoxFFI::null_mut(),
    };

    let query = Statement::new(query_str.to_string());

    let simple_query = BoundSimpleStatement {
        query,
        bound_values: vec![Unset; parameter_count as usize],
        name_to_bound_index: HashMap::with_capacity(parameter_count as usize),
    };

    BoxFFI::into_ptr(Box::new(CassStatement::new_unprepared(simple_query)))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_free(
    statement_raw: CassOwnedExclusivePtr<CassStatement, CMut>,
) {
    BoxFFI::free(statement_raw);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_consistency(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    consistency: CassConsistency,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_consistency!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Ok(maybe_set_consistency) = MaybeUnsetConfig::<Consistency>::from_c_value(consistency)
    else {
        // Invalid consistency value provided.
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match maybe_set_consistency {
        MaybeUnsetConfig::Unset => {
            // The correct semantics for `CASS_CONSISTENCY_UNKNOWN` is to
            // make statement not have any opinion at all about consistency.
            // Then, the default from the cluster/execution profile should be used.
            // Unfortunately, the Rust Driver does not support
            // "unsetting" consistency from a statement at the moment.
            //
            // FIXME: Implement unsetting consistency in the Rust Driver.
            // Then, fix this code.
            //
            // For now, we will throw an error in order to warn the user
            // about this limitation.
            tracing::warn!(
                "Passed `CASS_CONSISTENCY_UNKNOWN` to `cass_statement_set_consistency`. \
                This is not supported by the CPP Rust Driver yet: once you set some consistency \
                on a statement, you cannot unset it. This limitation will be fixed in the future. \
                As a workaround, you can refrain from setting consistency on a statement, which \
                will make the driver use the consistency set on execution profile or cluster level."
            );

            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
        MaybeUnsetConfig::Set(consistency) => match &mut statement.statement {
            BoundStatement::Simple(inner) => inner.query.set_consistency(consistency),
            BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
                .statement
                .set_consistency(consistency),
        },
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_paging_size(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    page_size: c_int,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_paging_size!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    if page_size <= 0 {
        // Cpp driver sets the page size flag only for positive page size provided by user.
        statement.paging_enabled = false;
    } else {
        statement.paging_enabled = true;
        match &mut statement.statement {
            BoundStatement::Simple(inner) => inner.query.set_page_size(page_size),
            BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
                .statement
                .set_page_size(page_size),
        }
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_paging_state(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    result: CassBorrowedSharedPtr<CassResult, CConst>,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_paging_state!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let Some(result) = ArcFFI::as_ref(result) else {
        tracing::error!("Provided null result pointer to cass_statement_set_paging_state!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match &result.paging_state_response {
        PagingStateResponse::HasMorePages { state } => statement.paging_state.clone_from(state),
        PagingStateResponse::NoMorePages => statement.paging_state = PagingState::start(),
    }
    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_paging_state_token(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    paging_state: *const c_char,
    paging_state_size: size_t,
) -> CassError {
    let Some(statement_from_raw) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!(
            "Provided null statement pointer to cass_statement_set_paging_state_token!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    if paging_state.is_null() {
        statement_from_raw.paging_state = PagingState::start();
        return CassError::CASS_ERROR_LIB_NULL_VALUE;
    }

    let paging_state_usize: usize = paging_state_size.try_into().unwrap();
    let paging_state_bytes =
        unsafe { slice::from_raw_parts(paging_state as *const u8, paging_state_usize) };
    statement_from_raw.paging_state = PagingState::new_from_raw_bytes(paging_state_bytes);
    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_is_idempotent(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    is_idempotent: cass_bool_t,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_is_idempotent!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner.query.set_is_idempotent(is_idempotent != 0),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_is_idempotent(is_idempotent != 0),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_tracing(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    enabled: cass_bool_t,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_tracing!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner.query.set_tracing(enabled != 0),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_tracing(enabled != 0),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_host(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    host: *const c_char,
    port: c_int,
) -> CassError {
    unsafe { cass_statement_set_host_n(statement_raw, host, strlen(host), port) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_host_n(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    host: *const c_char,
    host_length: size_t,
    port: c_int,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_host_n!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let host = match unsafe { ptr_to_cstr_n(host, host_length) } {
        Some(v) => v,
        None => {
            tracing::error!("Provided null or non-utf8 host pointer to cass_statement_set_host_n!");
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
    };
    let Ok(port): Result<u16, _> = port.try_into() else {
        tracing::error!("Provided invalid port value to cass_statement_set_host_n: {port}");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let address = match IpAddr::from_str(host) {
        Ok(ip_addr) => SocketAddr::new(ip_addr, port),
        Err(e) => {
            tracing::error!("Failed to parse ip address <{}>: {}", host, e);
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
    };
    let enforce_target_lbp =
        SingleTargetLoadBalancingPolicy::new(NodeIdentifier::NodeAddress(address), None);

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner
            .query
            .set_load_balancing_policy(Some(enforce_target_lbp)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_load_balancing_policy(Some(enforce_target_lbp)),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_host_inet(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    host: *const CassInet,
    port: c_int,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_host_inet!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    if host.is_null() {
        tracing::error!("Provided null host pointer to cass_statement_set_host_inet!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }
    // SAFETY: Assuming that user provided valid pointer.
    let ip_addr: IpAddr = match unsafe { *host }.try_into() {
        Ok(ip_addr) => ip_addr,
        Err(_) => {
            tracing::error!("Provided invalid CassInet value to cass_statement_set_host_inet!");
            return CassError::CASS_ERROR_LIB_BAD_PARAMS;
        }
    };
    let Ok(port): Result<u16, _> = port.try_into() else {
        tracing::error!("Provided invalid port value to cass_statement_set_host_n: {port}");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let address = SocketAddr::new(ip_addr, port);
    let enforce_target_lbp =
        SingleTargetLoadBalancingPolicy::new(NodeIdentifier::NodeAddress(address), None);

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner
            .query
            .set_load_balancing_policy(Some(enforce_target_lbp)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_load_balancing_policy(Some(enforce_target_lbp)),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_node(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    node_raw: CassBorrowedSharedPtr<CassNode, CConst>,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_node!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };
    let Some(node) = RefFFI::as_ref(node_raw) else {
        tracing::error!("Provided null node pointer to cass_statement_set_node!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let enforce_target_lbp =
        SingleTargetLoadBalancingPolicy::new(NodeIdentifier::Node(Arc::clone(node.node())), None);

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner
            .query
            .set_load_balancing_policy(Some(enforce_target_lbp)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_load_balancing_policy(Some(enforce_target_lbp)),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_retry_policy(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    retry_policy: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_retry_policy!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let maybe_arced_retry_policy: Option<Arc<dyn scylla::policies::retry::RetryPolicy>> =
        ArcFFI::as_ref(retry_policy).map(|policy| match policy {
            CassRetryPolicy::Default(default) => {
                default.clone() as Arc<dyn scylla::policies::retry::RetryPolicy>
            }
            CassRetryPolicy::Fallthrough(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistency(downgrading) => downgrading.clone(),
            CassRetryPolicy::Logging(logging) => Arc::clone(logging) as _,
            #[cfg(cpp_integration_testing)]
            CassRetryPolicy::Ignoring(ignoring) => Arc::clone(ignoring) as _,
        });

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner.query.set_retry_policy(maybe_arced_retry_policy),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_retry_policy(maybe_arced_retry_policy),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_serial_consistency(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    serial_consistency: CassConsistency,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!(
            "Provided null statement pointer to cass_statement_set_serial_consistency!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

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

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner.query.set_serial_consistency(Some(consistency)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_timestamp(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    timestamp: cass_int64_t,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_timestamp!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match &mut statement.statement {
        BoundStatement::Simple(inner) => inner.query.set_timestamp(Some(timestamp)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_timestamp(Some(timestamp)),
    }

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_set_request_timeout(
    statement: CassBorrowedExclusivePtr<CassStatement, CMut>,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let Some(statement_from_raw) = BoxFFI::as_mut_ref(statement) else {
        tracing::error!("Provided null statement pointer to cass_statement_set_request_timeout!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    // The maximum duration for a sleep is 68719476734 milliseconds (approximately 2.2 years).
    // Note: this is limited by tokio::time:timout
    // https://github.com/tokio-rs/tokio/blob/4b1c4801b1383800932141d0f6508d5b3003323e/tokio/src/time/driver/wheel/mod.rs#L44-L50
    let request_timeout_limit = 2_u64.pow(36) - 1;
    if timeout_ms >= request_timeout_limit {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    statement_from_raw.request_timeout_ms = Some(timeout_ms);

    CassError::CASS_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_statement_reset_parameters(
    statement_raw: CassBorrowedExclusivePtr<CassStatement, CMut>,
    count: size_t,
) -> CassError {
    let Some(statement) = BoxFFI::as_mut_ref(statement_raw) else {
        tracing::error!("Provided null statement pointer to cass_statement_reset_parameters!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

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

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::ptr::addr_of;
    use std::str::FromStr;

    use crate::argconv::{BoxFFI, RefFFI};
    use crate::cass_error::CassError;
    use crate::inet::CassInet;
    use crate::statement::{
        cass_statement_set_host, cass_statement_set_host_inet, cass_statement_set_node,
    };
    use crate::testing::assert_cass_error_eq;

    use super::{cass_statement_free, cass_statement_new};

    #[test]
    fn test_statement_set_host() {
        unsafe {
            let mut statement_raw = cass_statement_new(c"dummy".as_ptr(), 0);

            // cass_statement_set_host
            {
                // Null statement
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host(BoxFFI::null_mut(), c"127.0.0.1".as_ptr(), 9042)
                );

                // Null ip address
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host(statement_raw.borrow_mut(), std::ptr::null(), 9042)
                );

                // Unparsable ip address
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host(statement_raw.borrow_mut(), c"invalid".as_ptr(), 9042)
                );

                // Negative port
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host(statement_raw.borrow_mut(), c"127.0.0.1".as_ptr(), -1)
                );

                // Port too big
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host(
                        statement_raw.borrow_mut(),
                        c"127.0.0.1".as_ptr(),
                        70000
                    )
                );

                // Valid ip address and port
                assert_cass_error_eq!(
                    CassError::CASS_OK,
                    cass_statement_set_host(
                        statement_raw.borrow_mut(),
                        c"127.0.0.1".as_ptr(),
                        9042
                    )
                );
            }

            // cass_statement_set_host_inet
            {
                let valid_inet: CassInet = IpAddr::from_str("127.0.0.1").unwrap().into();

                let invalid_inet = CassInet {
                    address: [0; 16],
                    // invalid length - should be 4 or 16
                    address_length: 3,
                };

                // Null statement
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host_inet(BoxFFI::null_mut(), addr_of!(valid_inet), 9042)
                );

                // Null CassInet
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host_inet(
                        statement_raw.borrow_mut(),
                        std::ptr::null(),
                        9042
                    )
                );

                // Invalid CassInet
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host_inet(
                        statement_raw.borrow_mut(),
                        addr_of!(invalid_inet),
                        9042
                    )
                );

                // Negative port
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host_inet(
                        statement_raw.borrow_mut(),
                        addr_of!(valid_inet),
                        -1
                    )
                );

                // Port too big
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_host_inet(
                        statement_raw.borrow_mut(),
                        addr_of!(valid_inet),
                        70000
                    )
                );

                // Valid ip address and port
                assert_cass_error_eq!(
                    CassError::CASS_OK,
                    cass_statement_set_host_inet(
                        statement_raw.borrow_mut(),
                        addr_of!(valid_inet),
                        9042
                    )
                );
            }

            // cass_statement_set_node
            {
                // Null statement
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_node(BoxFFI::null_mut(), RefFFI::null())
                );

                // Null CassNode
                assert_cass_error_eq!(
                    CassError::CASS_ERROR_LIB_BAD_PARAMS,
                    cass_statement_set_node(statement_raw.borrow_mut(), RefFFI::null())
                );
            }

            cass_statement_free(statement_raw);
        }
    }
}
