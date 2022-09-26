use crate::types::size_t;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::Arc;

pub unsafe fn ptr_to_ref<T>(ptr: *const T) -> &'static T {
    ptr.as_ref().unwrap()
}

pub unsafe fn ptr_to_ref_mut<T>(ptr: *mut T) -> &'static mut T {
    ptr.as_mut().unwrap()
}

pub unsafe fn free_boxed<T>(ptr: *mut T) {
    if !ptr.is_null() {
        // This takes the ownership of the boxed value and drops it
        let _ = Box::from_raw(ptr);
    }
}

pub unsafe fn clone_arced<T>(ptr: *const T) -> Arc<T> {
    Arc::increment_strong_count(ptr);
    Arc::from_raw(ptr)
}

pub unsafe fn free_arced<T>(ptr: *const T) {
    if !ptr.is_null() {
        // This decrements the arc's internal counter and potentially drops it
        Arc::from_raw(ptr);
    }
}

pub unsafe fn ptr_to_cstr(ptr: *const c_char) -> Option<&'static str> {
    CStr::from_ptr(ptr).to_str().ok()
}

pub unsafe fn ptr_to_cstr_n(ptr: *const c_char, size: size_t) -> Option<&'static str> {
    std::str::from_utf8(std::slice::from_raw_parts(ptr as *const u8, size as usize)).ok()
}

pub unsafe fn write_str_to_c(s: &str, c_str: *mut *const c_char, c_strlen: *mut size_t) {
    *c_str = s.as_ptr() as *const i8;
    *c_strlen = s.len() as u64;
}

pub unsafe fn strlen(ptr: *const c_char) -> size_t {
    if ptr.is_null() {
        return 0;
    }
    libc::strlen(ptr) as size_t
}
