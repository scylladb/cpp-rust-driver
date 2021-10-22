use lazy_static::lazy_static;
use tokio::runtime::Runtime;

mod argconv;
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
//     let foo = argconv::ptr_to_ref(foo);
// }

// To take over/destroy Rust object previously given to C:

// #[no_mangle]
// pub unsafe extern "C" fn free_foo(foo: *mut Foo) {
//     // Take the ownership of the value and it will be automatically dropped
//     argconv::ptr_to_opt_box(foo);
// }
