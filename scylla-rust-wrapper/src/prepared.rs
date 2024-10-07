use scylla::{frame::value::MaybeUnset::Unset, transport::PagingState};
use std::sync::Arc;

use crate::{
    argconv::*,
    cass_types::{get_column_type, CassDataType},
    statement::{CassStatement, Statement},
};
use scylla::prepared_statement::PreparedStatement;

#[derive(Debug, Clone)]
pub struct CassPrepared {
    // Data types of columns from PreparedMetadata.
    pub variable_col_data_types: Vec<Arc<CassDataType>>,
    // Data types of columns from ResultMetadata.
    //
    // Arc<CassDataType> -> to share each data type with other structs such as `CassValue`
    // Arc<Vec<...>> -> to share the whole vector with `CassResultData`.
    pub result_col_data_types: Arc<Vec<Arc<CassDataType>>>,
    pub statement: PreparedStatement,
}

impl CassPrepared {
    pub fn new_from_prepared_statement(statement: PreparedStatement) -> Self {
        let variable_col_data_types = statement
            .get_variable_col_specs()
            .iter()
            .map(|col_spec| Arc::new(get_column_type(&col_spec.typ)))
            .collect();

        let result_col_data_types: Arc<Vec<Arc<CassDataType>>> = Arc::new(
            statement
                .get_result_set_col_specs()
                .iter()
                .map(|col_spec| Arc::new(get_column_type(&col_spec.typ)))
                .collect(),
        );

        Self {
            variable_col_data_types,
            result_col_data_types,
            statement,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_prepared_free(prepared_raw: *const CassPrepared) {
    free_arced(prepared_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_prepared_bind(
    prepared_raw: *const CassPrepared,
) -> *mut CassStatement {
    let prepared: Arc<_> = clone_arced(prepared_raw);
    let bound_values_size = prepared.statement.get_variable_col_specs().len();

    // cloning prepared statement's arc, because creating CassStatement should not invalidate
    // the CassPrepared argument
    let statement = Statement::Prepared(prepared);

    Box::into_raw(Box::new(CassStatement {
        statement,
        bound_values: vec![Unset; bound_values_size],
        paging_state: PagingState::start(),
        // Cpp driver disables paging by default.
        paging_enabled: false,
        request_timeout_ms: None,
        exec_profile: None,
    }))
}
