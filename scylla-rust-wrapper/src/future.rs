use crate::argconv::*;
use crate::cass_error::{self, CassError};
use crate::RUNTIME;
use oneshot::{channel, Receiver};
use std::future::Future;

pub enum CassResultValue {
    Empty,
}

pub type CassFutureResult = Result<CassResultValue, CassError>;

pub enum CassFuture {
    Pending(Receiver<CassFutureResult>),
    Done(CassFutureResult),
}

impl CassFuture {
    pub fn make_raw(
        fut: impl Future<Output = CassFutureResult> + Send + Sync + 'static,
    ) -> *mut CassFuture {
        Self::new_from_future(fut).box_and_make_raw()
    }

    pub fn new_from_future(
        fut: impl Future<Output = CassFutureResult> + Send + Sync + 'static,
    ) -> CassFuture {
        let (tx, rx) = channel::<CassFutureResult>();
        RUNTIME.spawn(async move {
            let _ = tx.send(fut.await);
        });
        CassFuture::Pending(rx)
    }

    pub fn new_pending(r: Receiver<CassFutureResult>) -> Self {
        CassFuture::Pending(r)
    }

    pub fn new_ready(r: CassFutureResult) -> Self {
        CassFuture::Done(r)
    }

    pub fn wait_for_result(&mut self) -> &CassFutureResult {
        match self {
            CassFuture::Pending(_) => {
                let mut dummy = CassFuture::Done(Err(cass_error::LIB_INTERNAL_ERROR));
                std::mem::swap(&mut dummy, self);

                let result = dummy
                    .consume_rx()
                    .recv()
                    .unwrap_or(Err(cass_error::LIB_INTERNAL_ERROR));
                *self = CassFuture::Done(result);
                self.get_ready_result()
            }
            CassFuture::Done(r) => r,
        }
    }

    pub fn box_and_make_raw(self) -> *mut Self {
        Box::into_raw(Box::new(self))
    }

    fn consume_rx(self) -> Receiver<CassFutureResult> {
        match self {
            CassFuture::Pending(rx) => rx,
            CassFuture::Done(_) => unreachable!(),
        }
    }

    fn get_ready_result(&self) -> &CassFutureResult {
        match self {
            CassFuture::Pending(_) => unreachable!(),
            CassFuture::Done(v) => v,
        }
    }
}

impl<F: Future<Output = CassFutureResult> + Send + Sync + 'static> From<F> for CassFuture {
    fn from(f: F) -> CassFuture {
        CassFuture::new_from_future(f)
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_error_code(future_raw: *mut CassFuture) -> CassError {
    let future = ptr_to_ref_mut(future_raw);
    match future.wait_for_result() {
        Ok(_) => cass_error::OK,
        Err(err) => *err,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_free(future_raw: *mut CassFuture) {
    free_boxed(future_raw);
}
