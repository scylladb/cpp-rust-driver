use crate::types::size_t;
use std::cmp::min;
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
    if ptr.is_null() {
        return None;
    }
    std::str::from_utf8(std::slice::from_raw_parts(ptr as *const u8, size as usize)).ok()
}

pub unsafe fn arr_to_cstr<const N: usize>(arr: &[c_char]) -> Option<&'static str> {
    let null_char = '\0' as c_char;
    let end_index = arr[..N].iter().position(|c| c == &null_char).unwrap_or(N);
    ptr_to_cstr_n(arr.as_ptr(), end_index as size_t)
}

pub fn str_to_arr<const N: usize>(s: &str) -> [c_char; N] {
    let mut result = ['\0' as c_char; N];

    // Max length must be null-terminated
    let mut max_len = min(N - 1, s.as_bytes().len());

    while !s.is_char_boundary(max_len) {
        max_len -= 1;
    }

    for (i, c) in s.as_bytes().iter().enumerate().take(max_len) {
        result[i] = *c as c_char;
    }

    result
}

pub unsafe fn write_str_to_c(s: &str, c_str: *mut *const c_char, c_strlen: *mut size_t) {
    *c_str = s.as_ptr() as *const c_char;
    *c_strlen = s.len() as u64;
}

pub unsafe fn strlen(ptr: *const c_char) -> size_t {
    if ptr.is_null() {
        return 0;
    }
    libc::strlen(ptr) as size_t
}

#[cfg(test)]
pub fn str_to_c_str_n(s: &str) -> (*const c_char, size_t) {
    let mut c_str = std::ptr::null();
    let mut c_strlen = size_t::default();

    // SAFETY: The pointers that are passed to `write_str_to_c` are compile-checked references.
    unsafe { write_str_to_c(s, &mut c_str, &mut c_strlen) };

    (c_str, c_strlen)
}

#[cfg(test)]
macro_rules! make_c_str {
    ($str:literal) => {
        concat!($str, "\0").as_ptr() as *const c_char
    };
}

#[cfg(test)]
pub(crate) use make_c_str;

/// Defines a pointer manipulation API for non-shared heap-allocated data.
///
/// Implement this trait for types that are allocated by the driver via [`Box::new`],
/// and then returned to the user as a pointer. The user is responsible for freeing
/// the memory associated with the pointer using corresponding driver's API function.
pub trait BoxFFI {
    fn into_ptr(self: Box<Self>) -> *mut Self {
        Box::into_raw(self)
    }
    unsafe fn from_ptr(ptr: *mut Self) -> Box<Self> {
        Box::from_raw(ptr)
    }
    unsafe fn as_maybe_ref<'a>(ptr: *const Self) -> Option<&'a Self> {
        ptr.as_ref()
    }
    unsafe fn as_ref<'a>(ptr: *const Self) -> &'a Self {
        ptr.as_ref().unwrap()
    }
    unsafe fn as_mut_ref<'a>(ptr: *mut Self) -> &'a mut Self {
        ptr.as_mut().unwrap()
    }
    unsafe fn free(ptr: *mut Self) {
        std::mem::drop(BoxFFI::from_ptr(ptr));
    }
}

/// Defines a pointer manipulation API for shared heap-allocated data.
///
/// Implement this trait for types that require a shared ownership of data.
/// The data should be allocated via [`Arc::new`], and then returned to the user as a pointer.
/// The user is responsible for freeing the memory associated
/// with the pointer using corresponding driver's API function.
pub trait ArcFFI {
    fn as_ptr(self: &Arc<Self>) -> *const Self {
        Arc::as_ptr(self)
    }
    fn into_ptr(self: Arc<Self>) -> *const Self {
        Arc::into_raw(self)
    }
    unsafe fn from_ptr(ptr: *const Self) -> Arc<Self> {
        Arc::from_raw(ptr)
    }
    unsafe fn cloned_from_ptr(ptr: *const Self) -> Arc<Self> {
        Arc::increment_strong_count(ptr);
        Arc::from_raw(ptr)
    }
    unsafe fn as_maybe_ref<'a>(ptr: *const Self) -> Option<&'a Self> {
        ptr.as_ref()
    }
    unsafe fn as_ref<'a>(ptr: *const Self) -> &'a Self {
        ptr.as_ref().unwrap()
    }
    unsafe fn free(ptr: *const Self) {
        std::mem::drop(ArcFFI::from_ptr(ptr));
    }
}

/// Defines a pointer manipulation API for data owned by some other object.
///
/// Implement this trait for the types that do not need to be freed (directly) by the user.
/// The lifetime of the data is bound to some other object owning it.
///
/// For example: lifetime of CassRow is bound by the lifetime of CassResult.
/// There is no API function that frees the CassRow. It should be automatically
/// freed when user calls cass_result_free.
pub trait RefFFI {
    fn as_ptr(&self) -> *const Self {
        self as *const Self
    }
    unsafe fn as_ref<'a>(ptr: *const Self) -> &'a Self {
        ptr.as_ref().unwrap()
    }
}
