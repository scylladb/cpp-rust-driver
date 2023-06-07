use scylla::frame::value::MaybeUnset::Unset;
use std::sync::Arc;

use crate::{
    argconv::*,
    statement::{CassStatement, Statement},
};
use scylla::prepared_statement::PreparedStatement;

pub type CassPrepared = PreparedStatement;

#[no_mangle]
pub unsafe extern "C" fn cass_prepared_free(prepared_raw: *const CassPrepared) {
    free_arced(prepared_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_prepared_bind(
    prepared_raw: *const CassPrepared,
) -> *mut CassStatement {
    let prepared: Arc<_> = clone_arced(prepared_raw);
    let bound_values_size = prepared.get_prepared_metadata().col_count;

    // cloning prepared statement's arc, because creating CassStatement should not invalidate
    // the CassPrepared argument
    let statement = Statement::Prepared(prepared);

    Box::into_raw(Box::new(CassStatement {
        statement,
        bound_values: vec![Unset; bound_values_size],
        paging_state: None,
        request_timeout_ms: None,
        exec_profile: None,
    }))
}
