use crate::argconv::*;
use crate::cass_error::*;
use crate::cass_types::{get_column_type, CassDataType, CassDataTypeArc};
use crate::cluster::build_session_builder;
use crate::cluster::CassCluster;
use crate::future::{CassFuture, CassResultValue};
use crate::query_result::Value::{CollectionValue, RegularValue};
use crate::query_result::{
    CassResult, CassResultData, CassResult_, CassRow, CassValue, Collection, Value,
};
use crate::statement::CassStatement;
use crate::statement::Statement;
use crate::types::size_t;
use scylla::frame::response::result::{CqlValue, Row};
use scylla::frame::types::Consistency;
use scylla::query::Query;
use scylla::transport::errors::QueryError;
use scylla::{QueryResult, Session};
use std::os::raw::c_char;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub type CassSession = RwLock<Option<Session>>;
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
    let request_timeout_ms = statement_opt.request_timeout_ms;

    let statement = statement_opt.statement.clone();

    let future = async move {
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
            Ok(result) => {
                let metadata = Arc::new(CassResultData {
                    paging_state: result.paging_state,
                    col_specs: result.col_specs,
                });
                let cass_rows = create_cass_rows_from_rows(result.rows, &metadata);
                let cass_result: CassResult_ = Arc::new(CassResult {
                    rows: cass_rows,
                    metadata,
                });

                Ok(CassResultValue::QueryResult(cass_result))
            }
            Err(err) => Ok(CassResultValue::QueryError(Arc::new(err))),
        }
    };

    match request_timeout_ms {
        Some(timeout_ms) => CassFuture::make_raw(async move {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), future).await {
                Ok(result) => result,
                Err(_timeout_err) => Ok(CassResultValue::QueryError(Arc::new(
                    QueryError::TimeoutError,
                ))),
            }
        }),
        None => CassFuture::make_raw(future),
    }
}

fn create_cass_rows_from_rows(
    rows: Option<Vec<Row>>,
    metadata: &Arc<CassResultData>,
) -> Option<Vec<CassRow>> {
    let rows = rows?;
    let cass_rows = rows
        .into_iter()
        .map(|r| CassRow {
            columns: create_cass_row_columns(r, metadata),
            result_metadata: metadata.clone(),
        })
        .collect();

    Some(cass_rows)
}

fn create_cass_row_columns(row: Row, metadata: &Arc<CassResultData>) -> Vec<CassValue> {
    row.columns
        .into_iter()
        .zip(metadata.col_specs.iter())
        .map(|(val, col)| {
            let column_type = Arc::new(get_column_type(&col.typ));
            CassValue {
                value: val.map(|col_val| get_column_value(col_val, &column_type)),
                value_type: column_type,
            }
        })
        .collect()
}

fn get_column_value(column: CqlValue, column_type: &CassDataTypeArc) -> Value {
    match (column, column_type.as_ref()) {
        (CqlValue::List(list), CassDataType::List(Some(list_type))) => {
            CollectionValue(Collection::List(
                list.into_iter()
                    .map(|val| CassValue {
                        value_type: list_type.clone(),
                        value: Some(get_column_value(val, list_type)),
                    })
                    .collect(),
            ))
        }
        (CqlValue::Map(map), CassDataType::Map(Some(key_type), Some(value_type))) => {
            CollectionValue(Collection::Map(
                map.into_iter()
                    .map(|(key, val)| {
                        (
                            CassValue {
                                value_type: key_type.clone(),
                                value: Some(get_column_value(key, key_type)),
                            },
                            CassValue {
                                value_type: value_type.clone(),
                                value: Some(get_column_value(val, value_type)),
                            },
                        )
                    })
                    .collect(),
            ))
        }
        (CqlValue::Set(set), CassDataType::Set(Some(set_type))) => {
            CollectionValue(Collection::Set(
                set.into_iter()
                    .map(|val| CassValue {
                        value_type: set_type.clone(),
                        value: Some(get_column_value(val, set_type)),
                    })
                    .collect(),
            ))
        }
        (
            CqlValue::UserDefinedType {
                keyspace,
                type_name,
                fields,
            },
            CassDataType::UDT(udt_type),
        ) => CollectionValue(Collection::UserDefinedType {
            keyspace,
            type_name,
            fields: fields
                .into_iter()
                .enumerate()
                .map(|(index, (name, val_opt))| {
                    let udt_field_type_opt = udt_type.get_field_by_index(index);
                    if let (Some(val), Some(udt_field_type)) = (val_opt, udt_field_type_opt) {
                        return (
                            name,
                            Some(CassValue {
                                value_type: udt_field_type.clone(),
                                value: Some(get_column_value(val, udt_field_type)),
                            }),
                        );
                    }
                    (name, None)
                })
                .collect(),
        }),
        (CqlValue::Tuple(tuple), CassDataType::Tuple(tuple_types)) => {
            CollectionValue(Collection::Tuple(
                tuple
                    .into_iter()
                    .enumerate()
                    .map(|(index, val_opt)| {
                        val_opt
                            .zip(tuple_types.get(index))
                            .map(|(val, tuple_field_type)| CassValue {
                                value_type: tuple_field_type.clone(),
                                value: Some(get_column_value(val, tuple_field_type)),
                            })
                    })
                    .collect(),
            ))
        }
        (regular_value, _) => RegularValue(regular_value),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_from_existing(
    cass_session: *mut CassSession,
    statement: *const CassStatement,
) -> *const CassFuture {
    let session = ptr_to_ref(cass_session);
    let cass_statement = ptr_to_ref(statement);
    let statement = cass_statement.statement.clone();

    CassFuture::make_raw(async move {
        let query = match &statement {
            Statement::Simple(q) => q,
            Statement::Prepared(ps) => {
                return Ok(CassResultValue::Prepared(ps.clone()));
            }
        };

        let session_guard = session.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }
        let session = session_guard.as_ref().unwrap();
        let prepared = session
            .prepare(query.clone())
            .await
            .map_err(|err| (CassError::from(&err), err.msg()))?;

        Ok(CassResultValue::Prepared(Arc::new(prepared)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare(
    session: *mut CassSession,
    query: *const c_char,
) -> *const CassFuture {
    cass_session_prepare_n(session, query, strlen(query))
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

        // Set Cpp Driver default configuration for queries:
        prepared.disable_paging();
        prepared.set_consistency(Consistency::One);

        Ok(CassResultValue::Prepared(Arc::new(prepared)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_free(session_raw: *mut CassSession) {
    free_arced(session_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_close(session: *mut CassSession) -> *const CassFuture {
    let session_opt = ptr_to_ref(session);

    CassFuture::make_raw(async move {
        let mut session_guard = session_opt.write().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_UNABLE_TO_CLOSE,
                "Already closing or closed".msg(),
            ));
        }

        *session_guard = None;

        Ok(CassResultValue::Empty)
    })
}
