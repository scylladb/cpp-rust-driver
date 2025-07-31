use crate::RUNTIME;
use crate::argconv::*;
use crate::cass_error::{CassError, CassErrorMessage, CassErrorResult, ToCassError as _};
use crate::prepared::CassPrepared;
use crate::query_result::{CassNode, CassResult};
use crate::types::*;
use crate::uuid::CassUuid;
use futures::future;
use std::future::Future;
use std::mem;
use std::os::raw::c_void;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use tokio::task::JoinHandle;
use tokio::time::Duration;

#[derive(Debug)]
pub(crate) enum CassResultValue {
    Empty,
    QueryResult(Arc<CassResult>),
    QueryError(Arc<CassErrorResult>),
    Prepared(Arc<CassPrepared>),
}

type CassFutureError = (CassError, String);

pub(crate) type CassFutureResult = Result<CassResultValue, CassFutureError>;

pub type CassFutureCallback = Option<NonNullFutureCallback>;

type NonNullFutureCallback =
    unsafe extern "C" fn(future: CassBorrowedSharedPtr<CassFuture, CMut>, data: *mut c_void);

struct BoundCallback {
    cb: NonNullFutureCallback,
    data: *mut c_void,
}

// *mut c_void is not Send, so Rust will have to take our word
// that we won't screw something up
unsafe impl Send for BoundCallback {}

impl BoundCallback {
    fn invoke(self, fut_ptr: CassBorrowedSharedPtr<CassFuture, CMut>) {
        unsafe {
            (self.cb)(fut_ptr, self.data);
        }
    }
}

#[derive(Default)]
struct CassFutureState {
    callback: Option<BoundCallback>,
    join_handle: Option<JoinHandle<()>>,
}

pub struct CassFuture {
    state: Mutex<CassFutureState>,
    result: OnceLock<CassFutureResult>,
    err_string: OnceLock<String>,
    wait_for_value: Condvar,
    #[cfg(cpp_integration_testing)]
    recording_listener: Option<Arc<crate::integration_testing::RecordingHistoryListener>>,
}

impl FFI for CassFuture {
    type Origin = FromArc;
}

/// An error that can appear during `cass_future_wait_timed`.
enum FutureError {
    TimeoutError,
    InvalidDuration,
}

/// The timeout appeared when we tried to await `JoinHandle`.
/// This errors contains the original handle, so it can be awaited later again.
struct JoinHandleTimeout(JoinHandle<()>);

impl CassFuture {
    pub(crate) fn make_ready_raw(res: CassFutureResult) -> CassOwnedSharedPtr<CassFuture, CMut> {
        Self::new_ready(res).into_raw()
    }

    pub(crate) fn make_raw(
        fut: impl Future<Output = CassFutureResult> + Send + 'static,
        #[cfg(cpp_integration_testing)] recording_listener: Option<
            Arc<crate::integration_testing::RecordingHistoryListener>,
        >,
    ) -> CassOwnedSharedPtr<CassFuture, CMut> {
        Self::new_from_future(
            fut,
            #[cfg(cpp_integration_testing)]
            recording_listener,
        )
        .into_raw()
    }

    pub(crate) fn new_from_future(
        fut: impl Future<Output = CassFutureResult> + Send + 'static,
        #[cfg(cpp_integration_testing)] recording_listener: Option<
            Arc<crate::integration_testing::RecordingHistoryListener>,
        >,
    ) -> Arc<CassFuture> {
        let cass_fut = Arc::new(CassFuture {
            state: Mutex::new(Default::default()),
            result: OnceLock::new(),
            err_string: OnceLock::new(),
            wait_for_value: Condvar::new(),
            #[cfg(cpp_integration_testing)]
            recording_listener,
        });
        let cass_fut_clone = Arc::clone(&cass_fut);
        let join_handle = RUNTIME.spawn(async move {
            let r = fut.await;
            let maybe_cb = {
                let mut guard = cass_fut_clone.state.lock().unwrap();
                cass_fut_clone
                    .result
                    .set(r)
                    .expect("Tried to resolve future result twice!");
                // Take the callback and call it after releasing the lock
                guard.callback.take()
            };
            if let Some(bound_cb) = maybe_cb {
                let fut_ptr = ArcFFI::as_ptr::<CMut>(&cass_fut_clone);
                // Safety: pointer is valid, because we get it from arc allocation.
                bound_cb.invoke(fut_ptr);
            }

            cass_fut_clone.wait_for_value.notify_all();
        });
        {
            let mut lock = cass_fut.state.lock().unwrap();
            lock.join_handle = Some(join_handle);
        }
        cass_fut
    }

    pub(crate) fn new_ready(r: CassFutureResult) -> Arc<Self> {
        Arc::new(CassFuture {
            state: Mutex::new(CassFutureState::default()),
            result: OnceLock::from(r),
            err_string: OnceLock::new(),
            wait_for_value: Condvar::new(),
            #[cfg(cpp_integration_testing)]
            recording_listener: None,
        })
    }

    /// Awaits the future until completion and exposes the result.
    ///
    /// There are three possible cases:
    /// - result is already available -> we can return.
    /// - no one is currently working on the future -> we take the ownership
    ///   of JoinHandle (future) and poll it until completion.
    /// - some other thread is working on the future -> we wait on the condition
    ///   variable to get an access to the future's state. Once we are notified,
    ///   there are three cases:
    ///     - result is already available -> we can return.
    ///     - JoinHandle is consumed -> some other thread already resolved the future.
    ///       We can return.
    ///     - JoinHandle is Some -> some other thread was working on the future, but
    ///       timed out (see [CassFuture::waited_result_timed]). We need to
    ///       take the ownership of the handle, and complete the work.
    pub(crate) fn waited_result(&self) -> &CassFutureResult {
        let mut guard = self.state.lock().unwrap();
        loop {
            if let Some(result) = self.result.get() {
                // The result is already available, we can return it.
                return result;
            }
            let handle = guard.join_handle.take();
            if let Some(handle) = handle {
                mem::drop(guard);
                // unwrap: JoinError appears only when future either panic'ed or canceled.
                RUNTIME.block_on(handle).unwrap();
            } else {
                guard = self
                    .wait_for_value
                    .wait_while(guard, |state| {
                        self.result.get().is_none() && state.join_handle.is_none()
                    })
                    // unwrap: Error appears only when mutex is poisoned.
                    .unwrap();
                if self.result.get().is_none() && guard.join_handle.is_some() {
                    // join_handle was none, and now it isn't - some other thread must
                    // have timed out and returned the handle. We need to take over
                    // the work of completing the future, because the result is still not available.
                    // To do that, we go into another iteration so that we land in the branch
                    // with `block_on`.
                    continue;
                }
            }
            return self.result.get().unwrap(); // FIXME: refactor this to avoid unwrap
        }
    }

    /// Tries to await the future with a given timeout and exposes the result,
    /// if it is available.
    ///
    /// There are three possible cases:
    /// - result is already available -> we can return.
    /// - no one is currently working on the future -> we take the ownership
    ///   of JoinHandle (future) and we try to poll it with given timeout.
    ///   If we timed out, we need to return the unfinished JoinHandle, so
    ///   some other thread can complete the future later.
    /// - some other thread is working on the future -> we wait on the condition
    ///   variable to get an access to the future's state.
    ///   Once we are notified (before the timeout), there are three cases:
    ///     - result is already available -> we can return.
    ///     - JoinHandle is consumed -> some other thread already resolved the future.
    ///       We can return.
    ///     - JoinHandle is Some -> some other thread was working on the future, but
    ///       timed out (see [CassFuture::waited_result_timed]). We need to
    ///       take the ownership of the handle, and continue the work.
    fn waited_result_timed(
        &self,
        timeout_duration: Duration,
    ) -> Result<&CassFutureResult, FutureError> {
        let mut guard = self.state.lock().unwrap();
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout_duration)
            .ok_or(FutureError::InvalidDuration)?;

        loop {
            if let Some(result) = self.result.get() {
                // The result is already available, we can return it.
                return Ok(result);
            }
            let handle = guard.join_handle.take();
            if let Some(handle) = handle {
                mem::drop(guard);
                // Need to wrap it with async{} block, so the timeout is lazily executed inside the runtime.
                // See mention about panics: https://docs.rs/tokio/latest/tokio/time/fn.timeout.html.
                let timed = async {
                    let sleep_future = tokio::time::sleep_until(deadline);
                    tokio::pin!(sleep_future);
                    let value = future::select(handle, sleep_future).await;
                    match value {
                        future::Either::Left((result, _)) => Ok(result),
                        future::Either::Right((_, handle)) => Err(JoinHandleTimeout(handle)),
                    }
                };
                match RUNTIME.block_on(timed) {
                    Err(JoinHandleTimeout(returned_handle)) => {
                        // We timed out. so we can't finish waiting for the future.
                        // The problem is that if current thread executor is used,
                        // then no one will run this future - other threads will
                        // go into the branch with condvar and wait there.
                        // To fix that:
                        //  - Return the join handle, so that next thread can take it
                        //  - Signal one thread, so that if all other consumers are
                        //    already waiting on condvar, one of them wakes up and
                        //    picks up the work.
                        guard = self.state.lock().unwrap();
                        guard.join_handle = Some(returned_handle);
                        self.wait_for_value.notify_one();
                        return Err(FutureError::TimeoutError);
                    }
                    // unwrap: JoinError appears only when future either panic'ed or canceled.
                    Ok(result) => result.unwrap(),
                };
            } else {
                let remaining_timeout = deadline.duration_since(tokio::time::Instant::now());
                let (guard_result, timeout_result) = self
                    .wait_for_value
                    .wait_timeout_while(guard, remaining_timeout, |state| {
                        self.result.get().is_none() && state.join_handle.is_none()
                    })
                    // unwrap: Error appears only when mutex is poisoned.
                    .unwrap();
                if timeout_result.timed_out() {
                    return Err(FutureError::TimeoutError);
                }

                guard = guard_result;
                if self.result.get().is_none() && guard.join_handle.is_some() {
                    // join_handle was none, and now it isn't - some other thread must
                    // have timed out and returned the handle. We need to take over
                    // the work of completing the future. To do that, we go into
                    // another iteration so that we land in the branch with block_on.
                    continue;
                }
            }

            return Ok(self.result.get().unwrap()); // FIXME: refactor this to avoid unwrap
        }
    }

    pub(crate) unsafe fn set_callback(
        &self,
        self_ptr: CassBorrowedSharedPtr<CassFuture, CMut>,
        cb: NonNullFutureCallback,
        data: *mut c_void,
    ) -> CassError {
        let mut lock = self.state.lock().unwrap();
        if lock.callback.is_some() {
            // Another callback has been already set
            return CassError::CASS_ERROR_LIB_CALLBACK_ALREADY_SET;
        }
        let bound_cb = BoundCallback { cb, data };
        if self.result.get().is_some() {
            // The value is already available, we need to call the callback ourselves
            mem::drop(lock);
            bound_cb.invoke(self_ptr);
            return CassError::CASS_OK;
        }
        // Store the callback
        lock.callback = Some(bound_cb);
        CassError::CASS_OK
    }

    pub(crate) fn into_raw(self: Arc<Self>) -> CassOwnedSharedPtr<Self, CMut> {
        ArcFFI::into_ptr(self)
    }

    #[cfg(cpp_integration_testing)]
    pub(crate) fn attempted_hosts(&self) -> Vec<std::net::SocketAddr> {
        if let Some(listener) = &self.recording_listener {
            listener.get_attempted_hosts()
        } else {
            vec![]
        }
    }
}

// Do not remove; this asserts that `CassFuture` implements Send + Sync,
// which is required by the cpp-driver (saying that `CassFuture` is thread-safe).
#[allow(unused)]
trait CheckSendSync: Send + Sync {}
impl CheckSendSync for CassFuture {}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_set_callback(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
    callback: CassFutureCallback,
    data: *mut ::std::os::raw::c_void,
) -> CassError {
    let Some(future) = ArcFFI::as_ref(future_raw.borrow()) else {
        tracing::error!("Provided null future pointer to cass_future_set_callback!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    let Some(callback) = callback else {
        tracing::error!("Provided null callback pointer to cass_future_set_callback!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    unsafe { future.set_callback(future_raw.borrow(), callback, data) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_wait(future_raw: CassBorrowedSharedPtr<CassFuture, CMut>) {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_wait!");
        return;
    };

    future.waited_result();
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_wait_timed(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
    timeout_us: cass_duration_t,
) -> cass_bool_t {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_wait_timed!");
        return cass_false;
    };

    future
        .waited_result_timed(Duration::from_micros(timeout_us))
        .is_ok() as cass_bool_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_ready(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> cass_bool_t {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_ready!");
        return cass_false;
    };

    future.result.get().is_some() as cass_bool_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_error_code(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> CassError {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_error_code!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match future.waited_result() {
        Ok(CassResultValue::QueryError(err)) => err.to_cass_error(),
        Err((err, _)) => *err,
        _ => CassError::CASS_OK,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_error_message(
    future: CassBorrowedSharedPtr<CassFuture, CMut>,
    message: *mut *const ::std::os::raw::c_char,
    message_length: *mut size_t,
) {
    let Some(future) = ArcFFI::as_ref(future) else {
        tracing::error!("Provided null future pointer to cass_future_error_message!");
        return;
    };

    let value = future.waited_result();
    let msg = future.err_string.get_or_init(|| match value {
        Ok(CassResultValue::QueryError(err)) => err.msg(),
        Err((_, s)) => s.msg(),
        _ => "".to_string(),
    });

    unsafe { write_str_to_c(msg.as_str(), message, message_length) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_free(future_raw: CassOwnedSharedPtr<CassFuture, CMut>) {
    ArcFFI::free(future_raw);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_get_result(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> CassOwnedSharedPtr<CassResult, CConst> {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_get_result!");
        return ArcFFI::null();
    };

    future
        .waited_result()
        .as_ref()
        .ok()
        .and_then(|r| match r {
            CassResultValue::QueryResult(qr) => Some(Arc::clone(qr)),
            _ => None,
        })
        .map_or(ArcFFI::null(), ArcFFI::into_ptr)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_get_error_result(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> CassOwnedSharedPtr<CassErrorResult, CConst> {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_get_error_result!");
        return ArcFFI::null();
    };

    future
        .waited_result()
        .as_ref()
        .ok()
        .and_then(|r| match r {
            CassResultValue::QueryError(qr) => Some(Arc::clone(qr)),
            _ => None,
        })
        .map_or(ArcFFI::null(), ArcFFI::into_ptr)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_get_prepared(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> CassOwnedSharedPtr<CassPrepared, CConst> {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future pointer to cass_future_get_prepared!");
        return ArcFFI::null();
    };

    future
        .waited_result()
        .as_ref()
        .ok()
        .and_then(|r| match r {
            CassResultValue::Prepared(p) => Some(Arc::clone(p)),
            _ => None,
        })
        .map_or(ArcFFI::null(), ArcFFI::into_ptr)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_tracing_id(
    future: CassBorrowedSharedPtr<CassFuture, CMut>,
    tracing_id: *mut CassUuid,
) -> CassError {
    let Some(future) = ArcFFI::as_ref(future) else {
        tracing::error!("Provided null future pointer to cass_future_tracing_id!");
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    };

    match future.waited_result() {
        Ok(CassResultValue::QueryResult(result)) => match result.tracing_id {
            Some(id) => {
                unsafe { *tracing_id = CassUuid::from(id) };
                CassError::CASS_OK
            }
            None => CassError::CASS_ERROR_LIB_NO_TRACING_ID,
        },
        _ => CassError::CASS_ERROR_LIB_INVALID_FUTURE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_future_coordinator(
    future_raw: CassBorrowedSharedPtr<CassFuture, CMut>,
) -> CassBorrowedSharedPtr<CassNode, CConst> {
    let Some(future) = ArcFFI::as_ref(future_raw) else {
        tracing::error!("Provided null future to cass_future_coordinator!");
        return RefFFI::null();
    };

    match future.waited_result() {
        Ok(CassResultValue::QueryResult(result)) => {
            // unwrap: Coordinator is `None` only for tests.
            RefFFI::as_ptr(result.coordinator.as_ref().unwrap())
        }
        _ => RefFFI::null(),
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{assert_cass_error_eq, assert_cass_future_error_message_eq};

    use super::*;
    use std::{
        os::raw::c_char,
        thread::{self},
        time::Duration,
    };

    // This is not a particularly smart test, but if some thread is granted access the value
    // before it is truly computed, then weird things should happen, even a segfault.
    // In the incorrect implementation that inspired this test to be written, this test
    // results with unwrap on a PoisonError on the CassFuture's mutex.
    #[test]
    #[ntest::timeout(100)]
    fn cass_future_thread_safety() {
        const ERROR_MSG: &str = "NOBODY EXPECTED SPANISH INQUISITION";
        let fut = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err((CassError::CASS_OK, ERROR_MSG.into()))
        };
        let cass_fut = CassFuture::make_raw(
            fut,
            #[cfg(cpp_integration_testing)]
            None,
        );

        struct PtrWrapper(CassBorrowedSharedPtr<'static, CassFuture, CMut>);
        unsafe impl Send for PtrWrapper {}
        unsafe {
            // transmute to erase the lifetime to 'static, so the reference
            // can be passed to an async block.
            let static_cass_fut_ref = std::mem::transmute::<
                CassBorrowedSharedPtr<'_, CassFuture, CMut>,
                CassBorrowedSharedPtr<'static, CassFuture, CMut>,
            >(cass_fut.borrow());
            let wrapped_cass_fut = PtrWrapper(static_cass_fut_ref);
            let handle = thread::spawn(move || {
                let wrapper = &wrapped_cass_fut;
                let PtrWrapper(cass_fut) = wrapper;
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
            });

            assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));

            handle.join().unwrap();
            cass_future_free(cass_fut);
        }
    }

    // This test makes sure that the future resolves even if timeout happens.
    #[test]
    #[ntest::timeout(200)]
    fn cass_future_resolves_after_timeout() {
        const ERROR_MSG: &str = "NOBODY EXPECTED SPANISH INQUISITION";
        const HUNDRED_MILLIS_IN_MICROS: u64 = 100 * 1000;
        let fut = async move {
            tokio::time::sleep(Duration::from_micros(HUNDRED_MILLIS_IN_MICROS)).await;
            Err((CassError::CASS_OK, ERROR_MSG.into()))
        };
        let cass_fut = CassFuture::make_raw(
            fut,
            #[cfg(cpp_integration_testing)]
            None,
        );

        unsafe {
            // This should timeout on tokio::time::timeout.
            let timed_result =
                cass_future_wait_timed(cass_fut.borrow(), HUNDRED_MILLIS_IN_MICROS / 5);
            assert_eq!(0, timed_result);

            // This should timeout as well.
            let timed_result =
                cass_future_wait_timed(cass_fut.borrow(), HUNDRED_MILLIS_IN_MICROS / 5);
            assert_eq!(0, timed_result);

            // Verify that future eventually resolves, even though timeouts occurred before.
            assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));

            cass_future_free(cass_fut);
        }
    }

    // This test checks whether the future callback is executed correctly when:
    // - a future is awaited indefinitely
    // - a future is awaited, after the timeout appeared (_wait_timed)
    // - a future is not awaited. We simply sleep, and let the tokio runtime resolve
    //   the future, and execute its callback
    #[test]
    #[ntest::timeout(600)]
    #[allow(clippy::disallowed_methods)]
    fn test_cass_future_callback() {
        const ERROR_MSG: &str = "NOBODY EXPECTED SPANISH INQUISITION";
        const HUNDRED_MILLIS_IN_MICROS: u64 = 100 * 1000;

        let create_future_and_flag = || {
            unsafe extern "C" fn mark_flag_cb(
                _fut: CassBorrowedSharedPtr<CassFuture, CMut>,
                data: *mut c_void,
            ) {
                let flag = data as *mut bool;
                unsafe {
                    *flag = true;
                }
            }

            let fut = async move {
                tokio::time::sleep(Duration::from_micros(HUNDRED_MILLIS_IN_MICROS)).await;
                Err((CassError::CASS_OK, ERROR_MSG.into()))
            };
            let cass_fut = CassFuture::make_raw(
                fut,
                #[cfg(cpp_integration_testing)]
                None,
            );
            let flag = Box::new(false);
            let flag_ptr = Box::into_raw(flag);

            unsafe {
                assert_cass_error_eq!(
                    cass_future_set_callback(
                        cass_fut.borrow(),
                        Some(mark_flag_cb),
                        flag_ptr as *mut c_void,
                    ),
                    CassError::CASS_OK
                )
            };

            (cass_fut, flag_ptr)
        };

        // Callback executed after awaiting.
        {
            let (cass_fut, flag_ptr) = create_future_and_flag();
            unsafe { cass_future_wait(cass_fut.borrow()) };

            unsafe {
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
            }
            assert!(unsafe { *flag_ptr });

            unsafe { cass_future_free(cass_fut) };
            let _ = unsafe { Box::from_raw(flag_ptr) };
        }

        // Future awaited via `assert_cass_future_error_message_eq`.
        {
            let (cass_fut, flag_ptr) = create_future_and_flag();

            unsafe {
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
            }
            assert!(unsafe { *flag_ptr });

            unsafe { cass_future_free(cass_fut) };
            let _ = unsafe { Box::from_raw(flag_ptr) };
        }

        // Callback executed after timeouts.
        {
            let (cass_fut, flag_ptr) = create_future_and_flag();

            // This should timeout on tokio::time::timeout.
            let timed_result =
                unsafe { cass_future_wait_timed(cass_fut.borrow(), HUNDRED_MILLIS_IN_MICROS / 5) };
            assert_eq!(0, timed_result);
            // This should timeout as well.
            let timed_result =
                unsafe { cass_future_wait_timed(cass_fut.borrow(), HUNDRED_MILLIS_IN_MICROS / 5) };
            assert_eq!(0, timed_result);

            // Await and check result.
            unsafe {
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
            }
            assert!(unsafe { *flag_ptr });

            unsafe { cass_future_free(cass_fut) };
            let _ = unsafe { Box::from_raw(flag_ptr) };
        }

        // Don't await the future. Just sleep.
        {
            let (cass_fut, flag_ptr) = create_future_and_flag();

            RUNTIME.block_on(async {
                tokio::time::sleep(Duration::from_micros(HUNDRED_MILLIS_IN_MICROS + 10 * 1000))
                    .await
            });

            assert!(unsafe { *flag_ptr });

            unsafe { cass_future_free(cass_fut) };
            let _ = unsafe { Box::from_raw(flag_ptr) };
        }
    }
}
