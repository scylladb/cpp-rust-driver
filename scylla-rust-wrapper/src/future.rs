use crate::argconv::*;
use crate::cass_error::{self, CassError};
use crate::query_result::CassResult;
use crate::RUNTIME;
use scylla::QueryResult;
use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};

pub enum CassResultValue {
    Empty,
    QueryResult(Arc<QueryResult>),
}

pub type CassFutureResult = Result<CassResultValue, CassError>;

#[derive(Default)]
struct CassFutureState {
    value: Option<CassFutureResult>,
}

pub struct CassFuture {
    state: Mutex<CassFutureState>,
    wait_for_value: Condvar,
}

impl CassFuture {
    pub fn make_raw(
        fut: impl Future<Output = CassFutureResult> + Send + Sync + 'static,
    ) -> *const CassFuture {
        Self::new_from_future(fut).into_raw()
    }

    pub fn new_from_future(
        fut: impl Future<Output = CassFutureResult> + Send + Sync + 'static,
    ) -> Arc<CassFuture> {
        let cass_fut = Arc::new(CassFuture {
            state: Mutex::new(Default::default()),
            wait_for_value: Condvar::new(),
        });
        let cass_fut_clone = cass_fut.clone();
        RUNTIME.spawn(async move {
            let r = fut.await;
            cass_fut_clone.state.lock().unwrap().value = Some(r);
            cass_fut_clone.wait_for_value.notify_all();
        });
        cass_fut
    }

    pub fn new_ready(r: CassFutureResult) -> Arc<Self> {
        Arc::new(CassFuture {
            state: Mutex::new(CassFutureState {
                value: Some(r),
                ..Default::default()
            }),
            wait_for_value: Condvar::new(),
        })
    }

    pub fn with_waited_result<T>(&self, f: impl FnOnce(&mut CassFutureResult) -> T) -> T {
        let mut guard = self
            .wait_for_value
            .wait_while(self.state.lock().unwrap(), |s| s.value.is_none())
            .unwrap();
        f((*guard).value.as_mut().unwrap())
    }

    fn into_raw(self: Arc<Self>) -> *const Self {
        Arc::into_raw(self)
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_error_code(future_raw: *const CassFuture) -> CassError {
    ptr_to_ref(future_raw).with_waited_result(|r: &mut CassFutureResult| match r {
        Ok(_) => cass_error::OK,
        Err(err) => *err,
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_free(future_raw: *const CassFuture) {
    free_arced(future_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_result(
    future_raw: *const CassFuture,
) -> *const CassResult {
    ptr_to_ref(future_raw)
        .with_waited_result(|r: &mut CassFutureResult| -> Option<CassResult> {
            match r.as_ref().ok()? {
                CassResultValue::QueryResult(qr) => Some(qr.clone()),
                _ => None,
            }
        })
        .map_or(std::ptr::null(), |qr| Box::into_raw(Box::new(qr)))
}
