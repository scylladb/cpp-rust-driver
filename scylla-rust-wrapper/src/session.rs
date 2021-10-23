use crate::argconv::*;
use crate::cass_error;
use crate::cluster::build_session_builder;
use crate::cluster::CassCluster;
use crate::future::{CassFuture, CassResultValue};
use crate::statement::CassStatement;
use crate::statement::Statement;
use crate::types::size_t;
use scylla::query::Query;
use scylla::{QueryResult, Session};
use std::os::raw::c_char;
use std::sync::Arc;
use tokio::sync::RwLock;

type CassSession = Arc<RwLock<Option<Session>>>;

#[no_mangle]
pub unsafe extern "C" fn cass_session_new() -> *mut CassSession {
    Box::into_raw(Box::new(Arc::new(RwLock::new(None))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect(
    session_raw: *mut CassSession,
    cluster_raw: *const CassCluster,
) -> *const CassFuture {
    let session_opt = ptr_to_ref(session_raw);
    let cluster = ptr_to_ref(cluster_raw);

    CassFuture::make_raw(async move {
        // TODO: Proper error handling
        let session = build_session_builder(cluster)
            .build()
            .await
            .map_err(|_| cass_error::LIB_NO_HOSTS_AVAILABLE)?;

        *session_opt.write().await = Some(session);
        Ok(CassResultValue::Empty)
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_execute(
    session_raw: *mut CassSession,
    statement_raw: *const CassStatement,
) -> *const CassFuture {
    let session_opt = ptr_to_ref(session_raw);
    let statement_opt = ptr_to_ref(statement_raw);
    let bound_values = statement_opt.bound_values.clone();

    let statement = statement_opt.statement.clone();

    CassFuture::make_raw(async move {
        let session_guard = session_opt.read().await;
        let session = session_guard.as_ref().unwrap();

        let query_res: QueryResult = match statement {
            Statement::Simple(query) => session.query(query, bound_values).await,
            Statement::Prepared(prepared) => session.execute(&prepared, bound_values).await,
        }
        .map_err(|_| cass_error::LIB_NO_HOSTS_AVAILABLE)?; // TODO: Proper error handling

        Ok(CassResultValue::QueryResult(Arc::new(query_res)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare(
    session: *mut CassSession,
    query: *const c_char,
) -> *const CassFuture {
    // TODO: error handling
    let query_str = ptr_to_cstr(query).unwrap();
    let query_length = query_str.len();

    cass_session_prepare_n(session, query, query_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_n(
    cass_session_raw: *mut CassSession,
    query: *const c_char,
    query_length: size_t,
) -> *const CassFuture {
    // TODO: error handling
    let query_str = ptr_to_cstr_n(query, query_length).unwrap();
    let query = Query::new(query_str.to_string());
    let cass_session: &CassSession = ptr_to_ref(cass_session_raw);

    CassFuture::make_raw(async move {
        let session_guard = cass_session.read().await;
        let session = session_guard.as_ref().unwrap();

        let prepared = session
            .prepare(query)
            .await
            .map_err(|_| cass_error::LIB_NO_HOSTS_AVAILABLE)?; // TODO: Proper error handling

        Ok(CassResultValue::Prepared(Arc::new(prepared)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_free(session_raw: *mut CassSession) {
    free_boxed(session_raw);
}
