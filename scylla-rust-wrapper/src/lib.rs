use lazy_static::lazy_static;
use tokio::runtime::Runtime;

pub mod cass_error;
pub mod cluster;
pub mod future;
pub mod session;
pub mod statement;

#[allow(non_camel_case_types)]
pub type size_t = usize;

lazy_static! {
    pub static ref RUNTIME: Runtime = Runtime::new().unwrap();
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
