use crate::types::size_t;
use std::cmp::min;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::ptr::NonNull;
use std::sync::Arc;

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

mod sealed {
    pub trait Sealed {}
}

/// A trait representing origin of the pointer.
///
/// We distinguish three origin kinds: [`FromBox`], [`FromArc`] and [`FromRef`]
///
/// ## FromBox pointers
/// FromBox pointer originates from a valid [`Box`] allocation. This is the only
/// way to obtain such pointer.
///
/// FromBox pointers are **always** [`Owned`] (see [`Ownership`] trait).
/// They can be either [`Const`] or [`Mut`] (see [`Mutability`] trait) based on the function needs.
///
/// The pointer can be converted to:
/// - [`Box`]
/// - valid immutable reference (&)
/// - valid mutable reference (&mut) - only if it's [`Mut`] at the same time
///
/// ## FromArc
/// FromArc pointer originates from a valid [`Arc`] allocation.
///
/// FromArc pointers are **always** [`Const`].
/// They can be either [`Owned`] or [`Borrowed`] (see [`Ownership`] trait), based on
/// how the pointer was obtained.
///
/// How to obtain such pointer:
/// - from an owned [`Arc`] - it returns an [`Owned`] pointer. User is then responsible for
///   decreasing reference count (and potential freeing of memory) by converting back to [`Arc`]
/// - from a reference to [`Arc`] - it returned a [`Borrowed`] pointer.
///   User is then the borrower. Cannot free the memory.
///
/// Thanks to the aforementioned assumptions, we are guaranteed that the pointer originated
/// from an Arc allocation. Because of that, we can safely increase/decrease the reference count
/// of the pointer.
///
/// The pointer can be converted to:
/// - [`Arc`] - only from an [`Owned`] pointer!!! It decreases the reference count, and potentialy frees the memory.
/// - cloned [`Arc`] - shares the ownership (increases reference count)
/// - valid immutable reference (&)
///
/// Above conversions are always valid and safe for the pointers obtained from Arc -> ptr conversions ([`Owned`]).
/// One need to be cautios of &Arc -> ptr conversions ([`Borrowed`]).
/// It's the C API and argconv API user's responsibility to guarantee that
/// the owner of the &Arc reference is still alive and wasn't freed before the conversion.
///
/// ## FromRef pointers
/// Borrowed pointer originates from a valid Rust immutable reference. This is the only way
/// to obtain such pointer.
///
/// FromRef pointers are **always** [`Const`] and [`Borrowed`].
///
/// The pointer can be converted back to the valid reference. It is assuming that the owner
/// wasn't freed before. It's responsibility of C API and argconv API users to guarantee this.
pub trait Origin: sealed::Sealed {}

/// For Box-allocated pointers.
pub struct FromBox;
impl sealed::Sealed for FromBox {}
impl Origin for FromBox {}

/// For Arc-allocated pointers.
pub struct FromArc;
impl sealed::Sealed for FromArc {}
impl Origin for FromArc {}

/// For pointers created from some valid reference.
pub struct FromRef;
impl sealed::Sealed for FromRef {}
impl Origin for FromRef {}

/// A trait representing an ownership the pointer.
///
/// We distinguish three ownership kinds: [`Owned`] and [`Borrowed`].
///
/// ## Owned pointers
/// The pointee has to be freed by the owner.
///
/// Notice that [`FromRef`] pointers are never Owned.
/// The logic of freeing memory differs between [`FromBox`] and [`FromRef`] pointers
/// and is explained in [`Origin`]'s documentation.
///
/// ## Borrowed pointers
/// User is the borrower. All of the conversions from such pointer, to some rust referential
/// type are unsafe, because we cannot guarantee that the owner of borrowed data is alive.
pub trait Ownership: sealed::Sealed {}

/// Represents an exclusive or shared ownership of the data.
pub struct Owned;
impl sealed::Sealed for Owned {}
impl Ownership for Owned {}

/// Represents a borrowed pointer.
pub struct Borrowed;
impl sealed::Sealed for Borrowed {}
impl Ownership for Borrowed {}

/// A trait representing mutability of the pointer.
///
/// Pointer can either be [`Const`] or [`Mut`].
///
/// ## Const pointers
/// Const pointers can only be converted to **immutable** Rust referential types.
/// There is no way to obtain a mutable reference from such pointer.
///
/// In some cases, we need to be able to mutate the data behind a shared pointer.
/// There is an example of such use case - namely [`crate::cass_types::CassDataType`].
/// argconv API does not provide a way to mutate such pointer - one can only convert the pointer
/// to [`Arc`] or &. It is the API user's responsibility to implement sound interior mutability
/// pattern in such case. This is what we currently do - CassDataType wraps CassDataTypeInner
/// inside an `UnsafeCell` to implement interior mutability for the type.
/// Other example is [`crate::future::CassFuture`] which uses Mutex.
///
/// ## Mut pointers
/// Mut pointers can be converted to both immutable and mutable Rust referential types.
/// Pointer can be [`Mut`] only if it's [`FromBox`].
pub trait Mutability: sealed::Sealed {}

/// Represents immutable pointer.
pub struct Const;
impl sealed::Sealed for Const {}
impl Mutability for Const {}

/// Represents mutable pointer.
pub struct Mut;
impl sealed::Sealed for Mut {}
impl Mutability for Mut {}

/// Represents pointer properties (origin + ownership + mutability).
pub trait Properties {
    type Origin: Origin;
    type Ownership: Ownership;
    type Mutability: Mutability;
}

impl<O: Origin, Own: Ownership, M: Mutability> Properties for (O, Own, M) {
    type Origin = O;
    type Ownership = Own;
    type Mutability = M;
}

// repr(transparent), so the struct has the same layout as underlying Option<NonNull<T>>.
// Thanks to https://doc.rust-lang.org/std/option/#representation optimization,
// we are guaranteed, that for T: Sized, our struct has the same layout
// and function call ABI as simply NonNull<T>.
#[repr(transparent)]
pub struct CassPtr<T: Sized, P: Properties> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<P>,
}

/// An exclusive, box-allocated pointer type, generic over mutability.
pub type CassExclusivePtr<T, M> = CassPtr<T, (FromBox, Owned, M)>;

/// An exclusive const pointer. Should be used for Box-allocated objects
/// that need to be read-only (C API functions that accept/return const *T).
pub type CassExclusiveConstPtr<T> = CassExclusivePtr<T, Const>;

/// An exclusive mutable pointer. Should be used for Box-allocated objects
/// that need to be mutable (C API functions that accept/return *T).
pub type CassExclusiveMutPtr<T> = CassExclusivePtr<T, Mut>;

/// A shared pointer. Should be used for Arc-allocated objects.
/// Generic over ownership. It can be either owned (shared ownership)
/// or borrowed. Borrowed pointers cannot be freed by the user.
///
/// Notice that this type does not provide interior mutability itself.
/// It is the responsiblity of the user of this API, to provide soundness
/// in this matter (aliasing ^ mutability).
/// Take for example [`CassDataType`](crate::cass_types::CassDataType). It
/// holds underlying data in [`UnsafeCell`](std::cell::UnsafeCell) to provide
/// interior mutability.
pub type CassSharedPtr<T, Own> = CassPtr<T, (FromArc, Own, Const)>;

/// A shared owned pointer. User receiving such pointer is responsible for freeing the data.
pub type CassSharedOwnedPtr<T> = CassSharedPtr<T, Owned>;

/// A shared borrowed pointer. User receiving such pointer is responsible for ensuring
/// that the owner of borrowed data is alive when pointer is being dereferenced.
pub type CassSharedBorrowedPtr<T> = CassSharedPtr<T, Borrowed>;

/// A borrowed const pointer. Should be used for objects that reference
/// some field of already allocated object.
///
/// When operating on the pointer of this type, we do not make any assumptions
/// about the origin of the pointer, apart from the fact that it is obtained
/// from some valid reference.
///
/// In particular, it may be a reference obtained from `Box/Arc::as_ref()`.
/// Notice, however, that we do not allow to convert such pointer back to `Box/Arc`,
/// since we do not have a guarantee that corresponding memory was allocated certain way.
/// OTOH, we can safely convert the pointer to a valid reference (assuming user did not
/// provide a pointer pointing to some garbage memory).
///
/// User of the pointer is responsible for ensuring that the owner of borrowed data is
/// alive when pointer is being dereferenced.
pub type CassBorrowedPtr<T> = CassPtr<T, (FromRef, Borrowed, Const)>;

/// Unsafe implementation of clone for tests.
///
/// In tests, we play a role as C API user. We need to be able
/// to reuse the pointer and to pass it to different API function calls.
/// API functions consume the pointer, thus we need some way to obtain
/// an owned version of the pointer again.
/// We introduce unsafe `clone` method for this reason.
#[cfg(test)]
impl<T: Sized, P: Properties> CassPtr<T, P> {
    /// ## Why is this unsafe?
    /// In the following implementations, we utilize the compile-time mechanism
    /// that borrow checker provides to us. For example, [`CassExclusiveMutPtr::into_box`] consumes
    /// the pointer. If clone wasn't unsafe, we could cause a use-after-free by:
    /// ```rust
    /// let b = Box::new(5);
    /// let ptr = CassExclusiveMutPtr::from_box(b);
    /// let ptr_cloned = ptr.clone();
    /// let _ = CassExclusiveMutPtr::into_box(ptr);
    /// // Use after free.
    /// let _ = CassExclusiveMutPtr::into_box(ptr_cloned);
    /// ```
    pub unsafe fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/// Methods for any pointer kind.
///
/// Notice that there are no constructors except from null(), which is trivial.
/// Thanks to that, we can make some assumptions and guarantees based on
/// the origin of the pointer. For these, see `SAFETY` comments.
impl<T: Sized, P: Properties> CassPtr<T, P> {
    fn null() -> Self {
        CassPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }

    fn is_null(&self) -> bool {
        self.ptr.is_none()
    }

    /// Converts self to raw pointer.
    fn as_raw(&self) -> *const T {
        self.ptr
            .map(|ptr| ptr.as_ptr() as *const _)
            .unwrap_or(std::ptr::null())
    }

    /// Converts a pointer to an optional valid reference, and consumes the pointer.
    /// It can be used to "erase" the lifetime of the reference. This is thus unsafe.
    unsafe fn into_ref<'a>(self) -> Option<&'a T> {
        self.ptr.map(|p| p.as_ref())
    }
}

/// Safe to-reference conversion for [`Owned`] pointers.
impl<T: Sized, O: Origin, M: Mutability> CassPtr<T, (O, Owned, M)> {
    /// Converts a pointer to an optional valid reference.
    fn as_ref(&self) -> Option<&T> {
        // SAFETY: We know that our pointers either come from a valid allocation (Box or Arc),
        // or were created based on the reference to the field of some allocated object.
        // Thus, if the pointer is non-null, we are guaranteed that it's valid.
        //
        // We also know, that pointers are Owned, thus we are guaranteed that pointee
        // is well-defined.
        unsafe { self.ptr.map(|p| p.as_ref()) }
    }
}

/// Unsafe to-reference conversion for [`Borrowed`] pointers.
impl<T: Sized, O: Origin, M: Mutability> CassPtr<T, (O, Borrowed, M)> {
    /// Converts a pointer to an optional valid reference, assuming that
    /// owner of the borrowed data is well-defined.
    unsafe fn as_ref(&self) -> Option<&T> {
        self.ptr.map(|p| p.as_ref())
    }
}

/// Methods for any exclusive pointers (no matter the mutability).
impl<T, M: Mutability> CassExclusivePtr<T, M> {
    /// Creates a pointer based on a VALID Box allocation.
    /// This is the only way to obtain such pointer.
    fn from_box(b: Box<T>) -> Self {
        #[allow(clippy::disallowed_methods)]
        let ptr = Box::into_raw(b);
        CassPtr {
            ptr: NonNull::new(ptr),
            _phantom: PhantomData,
        }
    }

    /// Converts the pointer back to the owned Box (if not null).
    fn into_box(self) -> Option<Box<T>> {
        // SAFETY: We are guaranteed that if ptr is Some, then it was obtained
        // from a valid Box allocation (see new() implementation).
        // We also know that the caller is the only owner of the pointer,
        // because `FromBox` (`CassExclusivePtr`) pointers are always `Owned`.
        unsafe {
            self.ptr.map(|p| {
                #[allow(clippy::disallowed_methods)]
                Box::from_raw(p.as_ptr())
            })
        }
    }
}

/// Methods for exclusive mutable pointers.
impl<T> CassExclusiveMutPtr<T> {
    fn null_mut() -> Self {
        CassPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }

    /// Converts a pointer to an optional valid, and mutable reference.
    fn as_mut_ref(&mut self) -> Option<&mut T> {
        // SAFETY: We are guaranteed that if ptr is Some, then it was obtained
        // from a valid Box allocation (see new() implementation).
        // We also know that the caller is the only owner of the pointer,
        // because `FromBox` (`CassExclusivePtr`) pointers are always `Owned`.
        unsafe { self.ptr.map(|mut p| p.as_mut()) }
    }
}

/// Define this conversion for test purposes.
/// User receives a mutable exclusive pointer from constructor,
/// but following function calls need a const exclusive pointer.
#[cfg(test)]
impl<T> CassExclusiveMutPtr<T> {
    pub fn into_const(self) -> CassExclusiveConstPtr<T> {
        CassPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/// Methods for Shared (Arc-allocated) owned pointers.
impl<T: Sized> CassSharedOwnedPtr<T> {
    /// Creates a pointer based on a VALID Arc allocation.
    fn from_arc(a: Arc<T>) -> Self {
        #[allow(clippy::disallowed_methods)]
        let ptr = Arc::into_raw(a);
        CassPtr {
            ptr: NonNull::new(ptr as *mut T),
            _phantom: PhantomData,
        }
    }

    /// Converts the pointer back to the Arc.
    fn into_arc(self) -> Option<Arc<T>> {
        // SAFETY: The pointer can only be obtained via new() or from_ref(),
        // both of which accept an Arc.
        // It means that the pointer comes from a valid allocation.
        unsafe {
            self.ptr.map(|p| {
                #[allow(clippy::disallowed_methods)]
                Arc::from_raw(p.as_ptr())
            })
        }
    }
}

/// Define this conversion for test purposes.
/// User receives a shared owned pointer,
/// but following function can accept a pointer with arbitrary
/// ownership. Borrowed ownership is more general, since it's the type
/// with a weaker guarantees. Such conversion is safe.
#[cfg(test)]
impl<T> CassSharedOwnedPtr<T> {
    pub fn into_borrowed(self) -> CassSharedBorrowedPtr<T> {
        CassPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/// Methods for Shared (Arc-allocated) borrowed pointers.
impl<T: Sized> CassSharedBorrowedPtr<T> {
    /// Creates a pointer which borrows from a VALID Arc allocation.
    fn from_ref(a: &Arc<T>) -> Self {
        #[allow(clippy::disallowed_methods)]
        let ptr = Arc::as_ptr(a);
        CassPtr {
            ptr: NonNull::new(ptr as *mut T),
            _phantom: PhantomData,
        }
    }

    /// Converts the pointer to an Arc, by increasing its reference count.
    /// We are not guaranteed that the caller shares the ownership of the pointer.
    /// We only know that the pointer originated from an Arc allocation. The pointer
    /// may be borrowed, though - we are not guaranteed that all of the owners
    /// of the borrowed data are alive. Thus, this method is unsafe.
    unsafe fn clone_arced(self) -> Option<Arc<T>> {
        // The pointer can only be obtained via new() or from_ref(),
        // both of which accept an Arc.
        // It means that the pointer comes from a valid allocation.
        self.ptr.map(|p| {
            let ptr = p.as_ptr();
            #[allow(clippy::disallowed_methods)]
            Arc::increment_strong_count(ptr);
            #[allow(clippy::disallowed_methods)]
            Arc::from_raw(ptr)
        })
    }
}

/// Methods for borrowed pointers.
impl<T: Sized> CassBorrowedPtr<T> {
    /// Creates a borrowed pointer from a valid reference.
    fn from_ref(r: &T) -> Self {
        Self {
            ptr: Some(NonNull::from(r)),
            _phantom: PhantomData,
        }
    }
}

mod own_sealed {
    pub trait ExclusiveSealed {}
    pub trait SharedSealed {}
    pub trait BorrowedSealed {}
}

/// Defines a pointer manipulation API for non-shared heap-allocated data.
///
/// Implement this trait for types that are allocated by the driver via [`Box::new`],
/// and then returned to the user as a pointer. The user is responsible for freeing
/// the memory associated with the pointer using corresponding driver's API function.
pub trait BoxFFI: Sized + own_sealed::ExclusiveSealed {
    /// Consumes the Box and returns a pointer with exclusive ownership.
    fn into_ptr<M: Mutability>(self: Box<Self>) -> CassExclusivePtr<Self, M> {
        CassExclusivePtr::from_box(self)
    }

    /// Consumes the pointer with exclusive ownership back to the Box.
    fn from_ptr<M: Mutability>(ptr: CassExclusivePtr<Self, M>) -> Option<Box<Self>> {
        ptr.into_box()
    }

    /// Creates a reference from an exclusive pointer.
    /// Reference inherits the lifetime of the pointer's borrow.
    fn as_ref<M: Mutability>(ptr: &CassExclusivePtr<Self, M>) -> Option<&Self> {
        ptr.as_ref()
    }

    /// Creates a lifetime-erased (thus, unsafe) reference from an exclusive pointer.
    unsafe fn into_ref<'a, M: Mutability>(ptr: CassExclusivePtr<Self, M>) -> Option<&'a Self> {
        ptr.into_ref()
    }

    /// Creates a mutable from an exlusive pointer.
    /// Reference inherits the lifetime of the pointer's mutable borrow.
    fn as_mut_ref(ptr: &mut CassExclusiveMutPtr<Self>) -> Option<&mut Self> {
        ptr.as_mut_ref()
    }

    /// Frees the pointee.
    fn free(ptr: CassExclusiveMutPtr<Self>) {
        std::mem::drop(BoxFFI::from_ptr(ptr));
    }

    #[cfg(test)]
    fn null() -> CassExclusiveConstPtr<Self> {
        CassExclusiveConstPtr::null()
    }

    fn null_mut() -> CassExclusiveMutPtr<Self> {
        CassExclusiveMutPtr::null_mut()
    }
}

/// Defines a pointer manipulation API for shared heap-allocated data.
///
/// Implement this trait for types that require a shared ownership of data.
/// The data should be allocated via [`Arc::new`], and then returned to the user as a pointer.
/// The user is responsible for freeing the memory associated
/// with the pointer using corresponding driver's API function.
pub trait ArcFFI: Sized + own_sealed::SharedSealed {
    /// Creates a pointer from a valid reference to Arc-allocated data.
    /// The pointer is [`Borrowed`].
    fn as_ptr(self: &Arc<Self>) -> CassSharedBorrowedPtr<Self> {
        CassSharedBorrowedPtr::from_ref(self)
    }

    /// Creates a pointer from a valid Arc allocation.
    /// The pointer is [`Owned`].
    fn into_ptr(self: Arc<Self>) -> CassSharedOwnedPtr<Self> {
        CassSharedOwnedPtr::from_arc(self)
    }

    /// Converts shared owned pointer back to owned Arc.
    fn from_ptr(ptr: CassSharedOwnedPtr<Self>) -> Option<Arc<Self>> {
        ptr.into_arc()
    }

    /// Increases the reference count of the pointer, and returns an owned Arc.
    /// User needs to ensure that owner of the borrowed pointee is alive, thus unsafe.
    unsafe fn cloned_from_ptr(ptr: CassSharedBorrowedPtr<Self>) -> Option<Arc<Self>> {
        ptr.clone_arced()
    }

    /// Converts a shared borrowed pointer to reference.
    /// The reference inherits the lifetime of pointer's borrow.
    /// User needs to ensure that owner of the borrowed pointee is alive, thus unsafe.
    unsafe fn as_ref(ptr: &CassSharedBorrowedPtr<Self>) -> Option<&Self> {
        ptr.as_ref()
    }

    /// Converts a shared borrowed pointer to reference.
    /// The reference is lifetime-erased and user needs to
    /// ensure that owner of the borrowed pointee is alive, thus unsafe.
    unsafe fn into_ref<'a>(ptr: CassSharedBorrowedPtr<Self>) -> Option<&'a Self> {
        ptr.into_ref()
    }

    /// Converts the shared owned pointer to raw pointer.
    fn to_raw<Own: Ownership>(ptr: &CassSharedPtr<Self, Own>) -> *const Self {
        ptr.as_raw()
    }

    /// Decreases the reference count (and potentially frees) of the
    /// [`Owned`] shared pointer.
    fn free(ptr: CassSharedOwnedPtr<Self>) {
        std::mem::drop(ArcFFI::from_ptr(ptr));
    }

    fn null<Own: Ownership>() -> CassSharedPtr<Self, Own> {
        CassSharedPtr::null()
    }

    fn is_null<Own: Ownership>(ptr: &CassSharedPtr<Self, Own>) -> bool {
        ptr.is_null()
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
pub trait RefFFI: Sized + own_sealed::BorrowedSealed {
    /// Creates a borrowed pointer from a valid reference.
    fn as_ptr(&self) -> CassBorrowedPtr<Self> {
        CassBorrowedPtr::from_ref(self)
    }

    /// Converts a borrowed pointer to reference.
    /// The reference inherits the lifetime of pointer's borrow.
    /// User needs to ensure that owner of the borrowed pointee is alive, thus unsafe.
    unsafe fn as_ref(ptr: &CassBorrowedPtr<Self>) -> Option<&Self> {
        ptr.as_ref()
    }

    /// Converts a borrowed pointer to reference.
    /// The reference is lifetime-erased and user needs to
    /// ensure that owner of the borrowed pointee is alive, thus unsafe.
    unsafe fn into_ref<'a>(ptr: CassBorrowedPtr<Self>) -> Option<&'a Self> {
        ptr.into_ref()
    }

    fn null() -> CassBorrowedPtr<Self> {
        CassBorrowedPtr::null()
    }

    fn is_null(ptr: &CassBorrowedPtr<Self>) -> bool {
        ptr.is_null()
    }
}

/// This trait should be implemented for types that are passed between
/// C and Rust API. We currently distinguish 3 kinds of implementors,
/// wrt. the ownership. The implementor should pick one of the 3 ownership
/// kinds as the associated type:
/// - [`OwnershipExclusive`]
/// - [`OwnershipShared`]
/// - [`OwnershipBorrowed`]
#[allow(clippy::upper_case_acronyms)]
pub trait FFI {
    type Ownership;
}

/// Represents types with an exclusive ownership.
///
/// Use this associated type for implementors that require:
/// - [`CassExclusivePtr`] manipulation via [`BoxFFI`]
/// - exclusive ownership of the corresponding object
/// - potential mutability of the corresponding object
/// - manual memory freeing
///
/// C API user should be responsible for freeing associated memory manually
/// via corresponding API call.
///
/// An example of such implementor would be [`CassCluster`](crate::cluster::CassCluster):
/// - it is allocated on the heap via [`Box::new`]
/// - user is the exclusive owner of the CassCluster object
/// - there is no API to increase a reference count of CassCluster object
/// - CassCluster is mutable via some API methods (`cass_cluster_set_*`)
/// - user is responsible for freeing the associated memory (`cass_cluster_free`)
pub struct OwnershipExclusive;
impl<T> own_sealed::ExclusiveSealed for T where T: FFI<Ownership = OwnershipExclusive> {}
impl<T> BoxFFI for T where T: FFI<Ownership = OwnershipExclusive> {}

/// Represents types with a shared ownership.
///
/// Use this associated type for implementors that require:
/// - [`CassSharedPtr`] manipulation via [`ArcFFI`]
/// - shared ownership of the corresponding object
/// - manual memory freeing
///
/// C API user should be responsible for freeing (decreasing reference count of)
/// associated memory manually via corresponding API call.
///
/// An example of such implementor would be [`CassDataType`](crate::cass_types::CassDataType):
/// - it is allocated on the heap via [`Arc::new`]
/// - there are multiple owners of the shared CassDataType object
/// - some API functions require to increase a reference count of the object
/// - user is responsible for freeing (decreasing RC of) the associated memory (`cass_data_type_free`)
pub struct OwnershipShared;
impl<T> own_sealed::SharedSealed for T where T: FFI<Ownership = OwnershipShared> {}
impl<T> ArcFFI for T where T: FFI<Ownership = OwnershipShared> {}

/// Represents borrowed types.
///
/// Use this associated type for implementors that do not require any assumptions
/// about the pointer type (apart from validity).
/// The implementation will enable [`CassBorrowedPtr`] manipulation via [`RefFFI`]
///
/// C API user is not responsible for freeing associated memory manually. The memory
/// should be freed automatically, when the owner is being dropped.
///
/// An example of such implementor would be [`CassRow`](crate::query_result::CassRow):
/// - its lifetime is tied to the lifetime of CassResult
/// - user only "borrows" the pointer - he is not responsible for freeing the memory
pub struct OwnershipBorrowed;
impl<T> own_sealed::BorrowedSealed for T where T: FFI<Ownership = OwnershipBorrowed> {}
impl<T> RefFFI for T where T: FFI<Ownership = OwnershipBorrowed> {}
