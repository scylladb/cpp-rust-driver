use crate::argconv::*;
use crate::cass_error::*;
use crate::cluster::build_session_builder;
use crate::cluster::CassCluster;
use crate::future::{CassFuture, CassResultValue};
use crate::statement::CassStatement;
use crate::statement::Statement;
use crate::types::size_t;
use scylla::query::Query;
use scylla::transport::errors::QueryError;
use scylla::{QueryResult, Session};
use std::os::raw::c_char;
use std::sync::Arc;
use tokio::sync::RwLock;

type CassSession = RwLock<Option<Session>>;
type CassSession_ = Arc<CassSession>;

#[no_mangle]
pub unsafe extern "C" fn cass_session_new() -> *const CassSession {
    let session: CassSession_ = Arc::new(RwLock::new(None));
    Arc::into_raw(session)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect(
    session_raw: *mut CassSession,
    cluster_raw: *const CassCluster,
) -> *const CassFuture {
    let session_opt = ptr_to_ref(session_raw);
    let cluster: CassCluster = (*ptr_to_ref(cluster_raw)).clone();

    CassFuture::make_raw(async move {
        // This can sleep for a long time, but only if someone connects/closes session
        // from more than 1 thread concurrently, which is inherently stupid thing to do.
        let mut session_guard = session_opt.write().await;
        if session_guard.is_some() {
            return Err((
                CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                "Already connecting, closing, or connected".msg(),
            ));
        }

        let session = build_session_builder(&cluster)
            .build()
            .await
            .map_err(|err| (CassError::from(&err), err.msg()))?;

        *session_guard = Some(session);
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
    let paging_state = statement_opt.paging_state.clone();
    let bound_values = statement_opt.bound_values.clone();

    let statement = statement_opt.statement.clone();

    CassFuture::make_raw(async move {
        let session_guard = session_opt.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }
        let session = session_guard.as_ref().unwrap();

        let query_res: Result<QueryResult, QueryError> = match statement {
            Statement::Simple(query) => {
                session.query_paged(query, bound_values, paging_state).await
            }
            Statement::Prepared(prepared) => {
                session
                    .execute_paged(&prepared, bound_values, paging_state)
                    .await
            }
        };

        match query_res {
            Ok(result) => Ok(CassResultValue::QueryResult(Arc::new(result))),
            Err(err) => Ok(CassResultValue::QueryError(Arc::new(err))),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare(
    session: *mut CassSession,
    query: *const c_char,
) -> *const CassFuture {
    // TODO: error handling
    let query_str = match ptr_to_cstr(query) {
        Some(v) => v,
        None => return std::ptr::null(),
    };
    let query_length = query_str.len();

    cass_session_prepare_n(session, query, query_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_n(
    cass_session_raw: *mut CassSession,
    query: *const c_char,
    query_length: size_t,
) -> *const CassFuture {
    let query_str = match ptr_to_cstr_n(query, query_length) {
        Some(v) => v,
        None => return std::ptr::null(),
    };
    let query = Query::new(query_str.to_string());
    let cass_session: &CassSession = ptr_to_ref(cass_session_raw);

    CassFuture::make_raw(async move {
        let session_guard = cass_session.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }
        let session = session_guard.as_ref().unwrap();

        let mut prepared = session
            .prepare(query)
            .await
            .map_err(|err| (CassError::from(&err), err.msg()))?;

        prepared.disable_paging(); // Cpp Driver by default disables paging

        Ok(CassResultValue::Prepared(Arc::new(prepared)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_free(session_raw: *mut CassSession) {
    free_arced(session_raw);
}
