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
    let prepared: Arc<_> = Arc::from_raw(prepared_raw);

    // cloning prepared statement's arc, because creating CassStatement should not invalidate
    // the CassPrepared argument
    let statement = Statement::Prepared(prepared.clone());

    Box::into_raw(Box::new(CassStatement {
        statement,
        bound_values: Vec::new(),
    }))
}
