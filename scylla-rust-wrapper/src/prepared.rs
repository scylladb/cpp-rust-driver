use crate::argconv::*;
use scylla::prepared_statement::PreparedStatement;

pub type CassPrepared = PreparedStatement;

#[no_mangle]
pub unsafe extern "C" fn cass_prepared_free(prepared_raw: *const CassPrepared) {
    free_arced(prepared_raw);
}
