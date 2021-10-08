use lazy_static::lazy_static;
use oneshot::{channel, Receiver};
use scylla::{Session, SessionBuilder};
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;

#[repr(C)]
#[derive(Copy, Clone)]
#[allow(non_camel_case_types)]
pub enum CassError {
    CASS_OK,
    CASS_ERROR_LIB_BAD_PARAMS,
    CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
    CASS_ERROR_LIB_INTERNAL_ERROR,
}

enum CassResultType {
    NoneResult,
}

struct CassFutureResult {
    error_code: CassError,
    result: CassResultType,
}

pub struct CassFuture {
    receiver: Option<Receiver<CassFutureResult>>,
    data: Option<CassFutureResult>,
}

type CassSession = Arc<RwLock<Option<Session>>>;

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

#[no_mangle]
pub extern "C" fn cass_cluster_new() -> *mut SessionBuilder {
    Box::into_raw(Box::new(SessionBuilder::new()))
}

#[no_mangle]
pub extern "C" fn cass_cluster_free(session_builder_raw: *mut SessionBuilder) {
    if !session_builder_raw.is_null() {
        let ptr = unsafe { Box::from_raw(session_builder_raw) };
        drop(ptr);
    }
}

#[no_mangle]
pub extern "C" fn cass_cluster_set_contact_points(
    session_builder_raw: *mut SessionBuilder,
    uri_raw: *const c_char,
) -> CassError {
    let session_builder: &mut SessionBuilder = unsafe { session_builder_raw.as_mut().unwrap() };
    let uri_cstr = unsafe { CStr::from_ptr(uri_raw) };

    if let Ok(uri) = uri_cstr.to_str() {
        session_builder.config.add_known_node(uri);
        CassError::CASS_OK
    } else {
        CassError::CASS_ERROR_LIB_BAD_PARAMS
    }
}

#[no_mangle]
pub extern "C" fn cass_session_new() -> *mut CassSession {
    Box::into_raw(Box::new(Arc::new(RwLock::new(None::<Session>))))
}

#[no_mangle]
pub extern "C" fn cass_session_connect(
    session_raw: *mut CassSession,
    session_builder_raw: *const SessionBuilder,
) -> *mut CassFuture {
    let session_opt: &CassSession = unsafe { session_raw.as_ref().unwrap() };
    let builder: &SessionBuilder = unsafe { session_builder_raw.as_ref().unwrap() }.clone();

    let (tx, rx) = channel::<CassFutureResult>();
    RUNTIME.spawn(async move {
        match builder.build().await {
            Ok(s) => {
                let mut modifiable_session = session_opt.write().unwrap();
                *modifiable_session = Some(s);
                // We ignore error from send - if receiver is dropped, the user is simply not interested in result
                let _ = tx.send(CassFutureResult {
                    error_code: CassError::CASS_OK,
                    result: CassResultType::NoneResult,
                });
            }
            Err(_e) => {
                // TODO: send proper error
                let _ = tx.send(CassFutureResult {
                    error_code: CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                    result: CassResultType::NoneResult,
                });
            }
        }
    });

    Box::into_raw(Box::new(CassFuture {
        receiver: Some(rx),
        data: None,
    }))
}

#[no_mangle]
pub extern "C" fn cass_session_free(session_raw: *mut CassSession) {
    if !session_raw.is_null() {
        let ptr = unsafe { Box::from_raw(session_raw) };
        drop(ptr); // Explicit drop, to make function clearer
    }
}

fn resolve_cass_future(future: &mut CassFuture) -> () {
    future
        .receiver
        .take()
        .map(|receiver| match receiver.recv() {
            Ok(res) => {
                future.data = Some(res);
            }
            Err(oneshot::RecvError) => {
                future.data = Some(CassFutureResult {
                    error_code: CassError::CASS_ERROR_LIB_INTERNAL_ERROR,
                    result: CassResultType::NoneResult,
                });
            }
        });

    ()
}

#[no_mangle]
pub extern "C" fn cass_future_error_code(future_raw: *mut CassFuture) -> CassError {
    let future: &mut CassFuture = unsafe { future_raw.as_mut().unwrap() };
    resolve_cass_future(future);

    future.data.as_ref().unwrap().error_code
}

#[no_mangle]
pub extern "C" fn cass_future_free(future_raw: *mut Option<Receiver<i32>>) {
    if !future_raw.is_null() {
        let ptr = unsafe { Box::from_raw(future_raw) };
        drop(ptr); // Explicit drop, to make function clearer
    }
}

// To send a Rust object to C:

// #[no_mangle]
// pub extern "C" fn create_foo() -> *mut Foo {
//     Box::into_raw(Box::new(Foo))
// }

// To borrow (and not free) from C:

// #[no_mangle]
// pub unsafe extern "C" fn do(foo: *mut Foo) -> *mut Foo {
//     let foo = foo.as_ref().unwrap(); // That's ptr::as_ref
// }

// To take over/destroy Rust object previously given to C:

// #[no_mangle]
// pub unsafe extern "C" fn free_foo(foo: *mut Foo) {
//     assert!(!foo.is_null());
//     Box::from_raw(foo); // Rust auto-drops it
// }
