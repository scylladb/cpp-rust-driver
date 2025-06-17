use scylla::value::MaybeUnset::Unset;
use std::{os::raw::c_char, sync::Arc};

use crate::{
    argconv::*,
    cass_error::CassError,
    cass_types::{CassDataType, get_column_type},
    query_result::CassResultMetadata,
    statement::{BoundPreparedStatement, CassStatement},
    types::size_t,
};
use scylla::statement::prepared::PreparedStatement;

#[derive(Debug, Clone)]
pub struct CassPrepared {
    // Data types of columns from PreparedMetadata.
    pub(crate) variable_col_data_types: Vec<Arc<CassDataType>>,

    // Cached result metadata. Arc'ed since we want to share it
    // with result metadata after execution.
    pub(crate) result_metadata: Arc<CassResultMetadata>,
    pub(crate) statement: PreparedStatement,
}

impl CassPrepared {
    pub(crate) fn new_from_prepared_statement(mut statement: PreparedStatement) -> Self {
        // We already cache the metadata on cpp-rust-driver side (see CassPrepared::result_metadata field),
        // thus we can enable the optimization on rust-driver side as well. This will prevent the server
        // from sending redundant bytes representing a result metadata during EXECUTE.
        //
        // NOTE: We are aware that it makes cached metadata immutable. It is expected, though - there
        // is an integration test for this for CQL protocol v4 (AlterDoesntUpdateColumnCount).
        // This issue is addressed in CQL protocol v5, but Scylla doesn't support it yet, and probably
        // won't support it in the near future.
        statement.set_use_cached_result_metadata(true);

        let variable_col_data_types = statement
            .get_variable_col_specs()
            .iter()
            .map(|col_spec| Arc::new(get_column_type(col_spec.typ())))
            .collect();

        let result_metadata = Arc::new(CassResultMetadata::from_column_specs(
            statement.get_result_set_col_specs(),
        ));

        Self {
            variable_col_data_types,
            result_metadata,
            statement,
        }
    }

    pub(crate) fn get_variable_data_type_by_name(&self, name: &str) -> Option<&Arc<CassDataType>> {
        let index = self
            .statement
            .get_variable_col_specs()
            .iter()
            .position(|col_spec| col_spec.name() == name)?;

        match self.variable_col_data_types.get(index) {
            Some(dt) => Some(dt),
            // This is a violation of driver's internal invariant.
            // Since `self.variable_col_data_types` is created based on prepared statement's
            // col specs, and we found an index with a corresponding name, we should
            // find a CassDataType at given index.
            None => panic!(
                "Cannot find a data type of parameter with given name: {}. This is a driver bug!",
                name
            ),
        }
    }
}

impl FFI for CassPrepared {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_prepared_free(
    prepared_raw: CassOwnedSharedPtr<CassPrepared, CConst>,
) {
    ArcFFI::free(prepared_raw);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_prepared_bind(
    prepared_raw: CassBorrowedSharedPtr<CassPrepared, CConst>,
) -> CassOwnedExclusivePtr<CassStatement, CMut> {
    let Some(prepared) = ArcFFI::cloned_from_ptr(prepared_raw) else {
        tracing::error!("Provided null prepared statement pointer to cass_prepared_bind!");
        return BoxFFI::null_mut();
    };

    let bound_values_size = prepared.statement.get_variable_col_specs().len();

    // cloning prepared statement's arc, because creating CassStatement should not invalidate
    // the CassPrepared argument

    let statement = BoundPreparedStatement {
        statement: prepared,
        bound_values: vec![Unset; bound_values_size],
    };

    BoxFFI::into_ptr(Box::new(CassStatement::new_prepared(statement)))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_prepared_parameter_name(
    prepared_raw: CassBorrowedSharedPtr<CassPrepared, CConst>,
    index: size_t,
    name: *mut *const c_char,
    name_length: *mut size_t,
) -> CassError {
    let Some(prepared) = ArcFFI::as_ref(prepared_raw) else {
        tracing::error!(
            "Provided null prepared statement pointer to cass_prepared_parameter_name!"
        );
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match prepared
        .statement
        .get_variable_col_specs()
        .get_by_index(index as usize)
    {
        Some(col_spec) => {
            unsafe { write_str_to_c(col_spec.name(), name, name_length) };
            CassError::CASS_OK
        }
        None => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_prepared_parameter_data_type(
    prepared_raw: CassBorrowedSharedPtr<CassPrepared, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let Some(prepared) = ArcFFI::as_ref(prepared_raw) else {
        tracing::error!(
            "Provided null prepared statement pointer to cass_prepared_parameter_data_type!"
        );
        return ArcFFI::null();
    };

    match prepared.variable_col_data_types.get(index as usize) {
        Some(dt) => ArcFFI::as_ptr(dt),
        None => ArcFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_prepared_parameter_data_type_by_name(
    prepared_raw: CassBorrowedSharedPtr<CassPrepared, CConst>,
    name: *const c_char,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    unsafe { cass_prepared_parameter_data_type_by_name_n(prepared_raw, name, strlen(name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_prepared_parameter_data_type_by_name_n(
    prepared_raw: CassBorrowedSharedPtr<CassPrepared, CConst>,
    name: *const c_char,
    name_length: size_t,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let Some(prepared) = ArcFFI::as_ref(prepared_raw) else {
        tracing::error!(
            "Provided null prepared statement pointer to cass_prepared_parameter_data_type_by_name!"
        );
        return ArcFFI::null();
    };

    let parameter_name =
        unsafe { ptr_to_cstr_n(name, name_length).expect("Prepared parameter name is not UTF-8") };

    let data_type = prepared.get_variable_data_type_by_name(parameter_name);
    match data_type {
        Some(dt) => ArcFFI::as_ptr(dt),
        None => ArcFFI::null(),
    }
}
