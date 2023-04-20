#![allow(clippy::missing_safety_doc)]

use crate::logging::set_tracing_subscriber_with_level;
use crate::logging::stderr_log_callback;
use crate::logging::Logger;
use lazy_static::lazy_static;
use std::sync::RwLock;
use tokio::runtime::Runtime;
use tracing::dispatcher::DefaultGuard;

#[macro_use]
mod binding;
mod argconv;
pub mod batch;
pub mod cass_error;
pub mod cass_types;
pub mod cluster;
pub mod collection;
pub mod exec_profile;
mod external;
pub mod future;
pub mod inet;
mod logging;
pub mod metadata;
pub mod prepared;
pub mod query_error;
pub mod query_result;
pub mod retry_policy;
pub mod session;
pub mod ssl;
pub mod statement;
#[cfg(test)]
pub mod testing;
pub mod tuple;
pub mod types;
pub mod user_type;
pub mod uuid;

lazy_static! {
    pub static ref RUNTIME: Runtime = Runtime::new().unwrap();
    pub static ref LOGGER: RwLock<Logger> = RwLock::new(Logger {
        cb: Some(stderr_log_callback),
        data: std::ptr::null_mut(),
    });
    pub static ref LOG: RwLock<Option<DefaultGuard>> = RwLock::new(Some(
        set_tracing_subscriber_with_level(tracing::Level::WARN)
    ));
}

// To send a Rust object to C:

// #[no_mangle]
// pub extern "C" fn create_foo() -> *mut Foo {
//     Box::into_raw(Box::new(Foo))
// }

// To borrow (and not free) from C:

// #[no_mangle]
// pub unsafe extern "C" fn do(foo: *mut Foo) -> *mut Foo {
//     let foo = argconv::ptr_to_ref(foo);
// }

// To take over/destroy Rust object previously given to C:

// #[no_mangle]
// pub unsafe extern "C" fn free_foo(foo: *mut Foo) {
//     // Take the ownership of the value and it will be automatically dropped
//     argconv::ptr_to_opt_box(foo);
// }
