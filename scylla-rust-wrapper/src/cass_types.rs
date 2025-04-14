use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use scylla::cluster::metadata::{CollectionType, NativeType};
use scylla::frame::response::result::ColumnType;
use scylla::frame::types::{Consistency, SerialConsistency};
use scylla::statement::batch::BatchType;
use std::cell::UnsafeCell;
use std::convert::TryFrom;
use std::os::raw::c_char;
use std::sync::Arc;

pub(crate) use crate::cass_batch_types::CassBatchType;
pub(crate) use crate::cass_consistency_types::CassConsistency;
pub(crate) use crate::cass_data_types::CassValueType;

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct UDTDataType {
    // Vec to preserve the order of types
    pub field_types: Vec<(String, Arc<CassDataType>)>,

    pub keyspace: String,
    pub name: String,
    pub frozen: bool,
}

impl UDTDataType {
    pub fn new() -> UDTDataType {
        UDTDataType {
            field_types: Vec::new(),
            keyspace: "".to_string(),
            name: "".to_string(),
            frozen: false,
        }
    }

    pub fn with_capacity(capacity: usize) -> UDTDataType {
        UDTDataType {
            field_types: Vec::with_capacity(capacity),
            keyspace: "".to_string(),
            name: "".to_string(),
            frozen: false,
        }
    }

    pub fn add_field(&mut self, name: String, field_type: Arc<CassDataType>) {
        self.field_types.push((name, field_type));
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<&Arc<CassDataType>> {
        self.field_types
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, t)| t)
    }

    pub fn get_field_by_index(&self, index: usize) -> Option<&Arc<CassDataType>> {
        self.field_types.get(index).map(|(_, b)| b)
    }

    fn typecheck_equals(&self, other: &UDTDataType) -> bool {
        // See: https://github.com/scylladb/cpp-driver/blob/master/src/data_type.hpp#L354-L386

        if !any_string_empty_or_both_equal(&self.keyspace, &other.keyspace) {
            return false;
        }
        if !any_string_empty_or_both_equal(&self.name, &other.name) {
            return false;
        }

        // A comment from cpp-driver:
        //// UDT's can be considered equal as long as the mutual first fields shared
        //// between them are equal. UDT's are append only as far as fields go, so a
        //// newer 'version' of the UDT data type after a schema change event should be
        //// treated as equivalent in this scenario, by simply looking at the first N
        //// mutual fields they should share.
        //
        // Iterator returned from zip() is perfect for checking the first mutual fields.
        for (field, other_field) in self.field_types.iter().zip(other.field_types.iter()) {
            // Compare field names.
            if field.0 != other_field.0 {
                return false;
            }
            // Compare field types.
            if unsafe {
                !field
                    .1
                    .get_unchecked()
                    .typecheck_equals(other_field.1.get_unchecked())
            } {
                return false;
            }
        }

        true
    }
}

fn any_string_empty_or_both_equal(s1: &str, s2: &str) -> bool {
    s1.is_empty() || s2.is_empty() || s1 == s2
}

impl Default for UDTDataType {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum MapDataType {
    Untyped,
    Key(Arc<CassDataType>),
    KeyAndValue(Arc<CassDataType>, Arc<CassDataType>),
}

#[derive(Debug)]
pub struct CassColumnSpec {
    pub name: String,
    pub data_type: Arc<CassDataType>,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum CassDataTypeInner {
    Value(CassValueType),
    UDT(UDTDataType),
    List {
        // None stands for untyped list.
        typ: Option<Arc<CassDataType>>,
        frozen: bool,
    },
    Set {
        // None stands for untyped set.
        typ: Option<Arc<CassDataType>>,
        frozen: bool,
    },
    Map {
        typ: MapDataType,
        frozen: bool,
    },
    // Empty vector stands for untyped tuple.
    Tuple(Vec<Arc<CassDataType>>),
    Custom(String),
}

impl FFI for CassDataType {
    type Origin = FromArc;
}

impl CassDataTypeInner {
    /// Checks for equality during typechecks.
    ///
    /// This takes into account the fact that tuples/collections may be untyped.
    pub fn typecheck_equals(&self, other: &CassDataTypeInner) -> bool {
        match self {
            CassDataTypeInner::Value(t) => *t == other.get_value_type(),
            CassDataTypeInner::UDT(udt) => match other {
                CassDataTypeInner::UDT(other_udt) => udt.typecheck_equals(other_udt),
                _ => false,
            },
            CassDataTypeInner::List { typ, .. } | CassDataTypeInner::Set { typ, .. } => match other
            {
                CassDataTypeInner::List { typ: other_typ, .. }
                | CassDataTypeInner::Set { typ: other_typ, .. } => {
                    // If one of them is list, and the other is set, fail the typecheck.
                    if self.get_value_type() != other.get_value_type() {
                        return false;
                    }
                    match (typ, other_typ) {
                        // One of them is untyped, skip the typecheck for subtype.
                        (None, _) | (_, None) => true,
                        (Some(typ), Some(other_typ)) => unsafe {
                            typ.get_unchecked()
                                .typecheck_equals(other_typ.get_unchecked())
                        },
                    }
                }
                _ => false,
            },
            CassDataTypeInner::Map { typ: t, .. } => match other {
                CassDataTypeInner::Map { typ: t_other, .. } => match (t, t_other) {
                    // See https://github.com/scylladb/cpp-driver/blob/master/src/data_type.hpp#L218
                    // In cpp-driver the types are held in a vector.
                    // The logic is following:

                    // If either of vectors is empty, skip the typecheck.
                    (MapDataType::Untyped, _) => true,
                    (_, MapDataType::Untyped) => true,

                    // Otherwise, the vectors should have equal length and we perform the typecheck for subtypes.
                    (MapDataType::Key(k), MapDataType::Key(k_other)) => unsafe {
                        k.get_unchecked().typecheck_equals(k_other.get_unchecked())
                    },
                    (
                        MapDataType::KeyAndValue(k, v),
                        MapDataType::KeyAndValue(k_other, v_other),
                    ) => unsafe {
                        k.get_unchecked().typecheck_equals(k_other.get_unchecked())
                            && v.get_unchecked().typecheck_equals(v_other.get_unchecked())
                    },
                    _ => false,
                },
                _ => false,
            },
            CassDataTypeInner::Tuple(sub) => match other {
                CassDataTypeInner::Tuple(other_sub) => {
                    // If either of tuples is untyped, skip the typecheck for subtypes.
                    if sub.is_empty() || other_sub.is_empty() {
                        return true;
                    }

                    // If both are non-empty, check for subtypes equality.
                    if sub.len() != other_sub.len() {
                        return false;
                    }
                    sub.iter()
                        .zip(other_sub.iter())
                        .all(|(typ, other_typ)| unsafe {
                            typ.get_unchecked()
                                .typecheck_equals(other_typ.get_unchecked())
                        })
                }
                _ => false,
            },
            CassDataTypeInner::Custom(_) => {
                unimplemented!("Cpp-rust-driver does not support custom types!")
            }
        }
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct CassDataType(UnsafeCell<CassDataTypeInner>);

/// PartialEq and Eq for test purposes.
#[cfg(test)]
impl PartialEq for CassDataType {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.get_unchecked() == other.get_unchecked() }
    }
}
#[cfg(test)]
impl Eq for CassDataType {}

unsafe impl Sync for CassDataType {}

impl CassDataType {
    pub unsafe fn get_unchecked(&self) -> &CassDataTypeInner {
        unsafe { &*self.0.get() }
    }

    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_mut_unchecked(&self) -> &mut CassDataTypeInner {
        unsafe { &mut *self.0.get() }
    }

    pub const fn new(inner: CassDataTypeInner) -> CassDataType {
        CassDataType(UnsafeCell::new(inner))
    }

    pub fn new_arced(inner: CassDataTypeInner) -> Arc<CassDataType> {
        Arc::new(CassDataType(UnsafeCell::new(inner)))
    }
}

fn native_type_to_cass_value_type(native_type: &NativeType) -> CassValueType {
    use NativeType::*;
    match native_type {
        Ascii => CassValueType::CASS_VALUE_TYPE_ASCII,
        Boolean => CassValueType::CASS_VALUE_TYPE_BOOLEAN,
        Blob => CassValueType::CASS_VALUE_TYPE_BLOB,
        Counter => CassValueType::CASS_VALUE_TYPE_COUNTER,
        Date => CassValueType::CASS_VALUE_TYPE_DATE,
        Decimal => CassValueType::CASS_VALUE_TYPE_DECIMAL,
        Double => CassValueType::CASS_VALUE_TYPE_DOUBLE,
        Duration => CassValueType::CASS_VALUE_TYPE_DURATION,
        Float => CassValueType::CASS_VALUE_TYPE_FLOAT,
        Int => CassValueType::CASS_VALUE_TYPE_INT,
        BigInt => CassValueType::CASS_VALUE_TYPE_BIGINT,
        Text => CassValueType::CASS_VALUE_TYPE_TEXT,
        Timestamp => CassValueType::CASS_VALUE_TYPE_TIMESTAMP,
        Inet => CassValueType::CASS_VALUE_TYPE_INET,
        SmallInt => CassValueType::CASS_VALUE_TYPE_SMALL_INT,
        TinyInt => CassValueType::CASS_VALUE_TYPE_TINY_INT,
        Time => CassValueType::CASS_VALUE_TYPE_TIME,
        Timeuuid => CassValueType::CASS_VALUE_TYPE_TIMEUUID,
        Uuid => CassValueType::CASS_VALUE_TYPE_UUID,
        Varint => CassValueType::CASS_VALUE_TYPE_VARINT,

        // NativeType is non_exhaustive
        _ => CassValueType::CASS_VALUE_TYPE_UNKNOWN,
    }
}

impl CassDataTypeInner {
    fn get_sub_data_type(&self, index: usize) -> Option<&Arc<CassDataType>> {
        match self {
            CassDataTypeInner::UDT(udt_data_type) => {
                udt_data_type.field_types.get(index).map(|(_, b)| b)
            }
            CassDataTypeInner::List { typ, .. } | CassDataTypeInner::Set { typ, .. } => {
                if index > 0 { None } else { typ.as_ref() }
            }
            CassDataTypeInner::Map {
                typ: MapDataType::Untyped,
                ..
            } => None,
            CassDataTypeInner::Map {
                typ: MapDataType::Key(k),
                ..
            } => (index == 0).then_some(k),
            CassDataTypeInner::Map {
                typ: MapDataType::KeyAndValue(k, v),
                ..
            } => match index {
                0 => Some(k),
                1 => Some(v),
                _ => None,
            },
            CassDataTypeInner::Tuple(v) => v.get(index),
            _ => None,
        }
    }

    fn add_sub_data_type(&mut self, sub_type: Arc<CassDataType>) -> Result<(), CassError> {
        match self {
            CassDataTypeInner::List { typ, .. } | CassDataTypeInner::Set { typ, .. } => match typ {
                Some(_) => Err(CassError::CASS_ERROR_LIB_BAD_PARAMS),
                None => {
                    *typ = Some(sub_type);
                    Ok(())
                }
            },
            CassDataTypeInner::Map {
                typ: MapDataType::KeyAndValue(_, _),
                ..
            } => Err(CassError::CASS_ERROR_LIB_BAD_PARAMS),
            CassDataTypeInner::Map {
                typ: MapDataType::Key(k),
                frozen,
            } => {
                *self = CassDataTypeInner::Map {
                    typ: MapDataType::KeyAndValue(k.clone(), sub_type),
                    frozen: *frozen,
                };
                Ok(())
            }
            CassDataTypeInner::Map {
                typ: MapDataType::Untyped,
                frozen,
            } => {
                *self = CassDataTypeInner::Map {
                    typ: MapDataType::Key(sub_type),
                    frozen: *frozen,
                };
                Ok(())
            }
            CassDataTypeInner::Tuple(types) => {
                types.push(sub_type);
                Ok(())
            }
            _ => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    }

    pub fn get_udt_type(&self) -> &UDTDataType {
        match self {
            CassDataTypeInner::UDT(udt) => udt,
            _ => panic!("Can get UDT out of non-UDT data type"),
        }
    }

    pub fn get_value_type(&self) -> CassValueType {
        match &self {
            CassDataTypeInner::Value(value_data_type) => *value_data_type,
            CassDataTypeInner::UDT { .. } => CassValueType::CASS_VALUE_TYPE_UDT,
            CassDataTypeInner::List { .. } => CassValueType::CASS_VALUE_TYPE_LIST,
            CassDataTypeInner::Set { .. } => CassValueType::CASS_VALUE_TYPE_SET,
            CassDataTypeInner::Map { .. } => CassValueType::CASS_VALUE_TYPE_MAP,
            CassDataTypeInner::Tuple(..) => CassValueType::CASS_VALUE_TYPE_TUPLE,
            CassDataTypeInner::Custom(..) => CassValueType::CASS_VALUE_TYPE_CUSTOM,
        }
    }
}

pub fn get_column_type(column_type: &ColumnType) -> CassDataType {
    use CollectionType::*;
    use ColumnType::*;
    let inner = match column_type {
        Native(native) => CassDataTypeInner::Value(native_type_to_cass_value_type(native)),
        Collection {
            typ: List(boxed_type),
            frozen,
        } => CassDataTypeInner::List {
            typ: Some(Arc::new(get_column_type(boxed_type.as_ref()))),
            frozen: *frozen,
        },
        Collection {
            typ: Map(key, value),
            frozen,
        } => CassDataTypeInner::Map {
            typ: MapDataType::KeyAndValue(
                Arc::new(get_column_type(key.as_ref())),
                Arc::new(get_column_type(value.as_ref())),
            ),
            frozen: *frozen,
        },
        Collection {
            typ: Set(boxed_type),
            frozen,
        } => CassDataTypeInner::Set {
            typ: Some(Arc::new(get_column_type(boxed_type.as_ref()))),
            frozen: *frozen,
        },
        UserDefinedType { definition, frozen } => CassDataTypeInner::UDT(UDTDataType {
            field_types: definition
                .field_types
                .iter()
                .map(|(name, col_type)| {
                    (
                        name.clone().into_owned(),
                        Arc::new(get_column_type(col_type)),
                    )
                })
                .collect(),
            keyspace: definition.keyspace.clone().into_owned(),
            name: definition.name.clone().into_owned(),
            frozen: *frozen,
        }),
        Tuple(v) => CassDataTypeInner::Tuple(
            v.iter()
                .map(|col_type| Arc::new(get_column_type(col_type)))
                .collect(),
        ),

        // ColumnType is non_exhaustive.
        _ => CassDataTypeInner::Value(CassValueType::CASS_VALUE_TYPE_UNKNOWN),
    };

    CassDataType::new(inner)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_new(
    value_type: CassValueType,
) -> CassOwnedSharedPtr<CassDataType, CMut> {
    let inner = match value_type {
        CassValueType::CASS_VALUE_TYPE_LIST => CassDataTypeInner::List {
            typ: None,
            frozen: false,
        },
        CassValueType::CASS_VALUE_TYPE_SET => CassDataTypeInner::Set {
            typ: None,
            frozen: false,
        },
        CassValueType::CASS_VALUE_TYPE_TUPLE => CassDataTypeInner::Tuple(Vec::new()),
        CassValueType::CASS_VALUE_TYPE_MAP => CassDataTypeInner::Map {
            typ: MapDataType::Untyped,
            frozen: false,
        },
        CassValueType::CASS_VALUE_TYPE_UDT => CassDataTypeInner::UDT(UDTDataType::new()),
        CassValueType::CASS_VALUE_TYPE_CUSTOM => CassDataTypeInner::Custom("".to_string()),
        CassValueType::CASS_VALUE_TYPE_UNKNOWN => return ArcFFI::null(),
        t if t < CassValueType::CASS_VALUE_TYPE_LAST_ENTRY => CassDataTypeInner::Value(t),
        _ => return ArcFFI::null(),
    };
    ArcFFI::into_ptr(CassDataType::new_arced(inner))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_new_from_existing(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> CassOwnedSharedPtr<CassDataType, CMut> {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    ArcFFI::into_ptr(CassDataType::new_arced(
        unsafe { data_type.get_unchecked() }.clone(),
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_new_tuple(
    item_count: size_t,
) -> CassOwnedSharedPtr<CassDataType, CMut> {
    ArcFFI::into_ptr(CassDataType::new_arced(CassDataTypeInner::Tuple(
        Vec::with_capacity(item_count as usize),
    )))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_new_udt(
    field_count: size_t,
) -> CassOwnedSharedPtr<CassDataType, CMut> {
    ArcFFI::into_ptr(CassDataType::new_arced(CassDataTypeInner::UDT(
        UDTDataType::with_capacity(field_count as usize),
    )))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_free(data_type: CassOwnedSharedPtr<CassDataType, CMut>) {
    ArcFFI::free(data_type);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_type(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> CassValueType {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    unsafe { data_type.get_unchecked() }.get_value_type()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_is_frozen(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> cass_bool_t {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    let is_frozen = match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::UDT(udt) => udt.frozen,
        CassDataTypeInner::List { frozen, .. } => *frozen,
        CassDataTypeInner::Set { frozen, .. } => *frozen,
        CassDataTypeInner::Map { frozen, .. } => *frozen,
        _ => false,
    };

    is_frozen as cass_bool_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_type_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    type_name: *mut *const c_char,
    type_name_length: *mut size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::UDT(UDTDataType { name, .. }) => {
            unsafe { write_str_to_c(name, type_name, type_name_length) };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_set_type_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    type_name: *const c_char,
) -> CassError {
    unsafe { cass_data_type_set_type_name_n(data_type, type_name, strlen(type_name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_set_type_name_n(
    data_type_raw: CassBorrowedSharedPtr<CassDataType, CMut>,
    type_name: *const c_char,
    type_name_length: size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type_raw).unwrap();
    let type_name_string = unsafe { ptr_to_cstr_n(type_name, type_name_length) }
        .unwrap()
        .to_string();

    match unsafe { data_type.get_mut_unchecked() } {
        CassDataTypeInner::UDT(udt_data_type) => {
            udt_data_type.name = type_name_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_keyspace(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    keyspace: *mut *const c_char,
    keyspace_length: *mut size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::UDT(UDTDataType { name, .. }) => {
            unsafe { write_str_to_c(name, keyspace, keyspace_length) };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_set_keyspace(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    keyspace: *const c_char,
) -> CassError {
    unsafe { cass_data_type_set_keyspace_n(data_type, keyspace, strlen(keyspace)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_set_keyspace_n(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    keyspace: *const c_char,
    keyspace_length: size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    let keyspace_string = unsafe { ptr_to_cstr_n(keyspace, keyspace_length) }
        .unwrap()
        .to_string();

    match unsafe { data_type.get_mut_unchecked() } {
        CassDataTypeInner::UDT(udt_data_type) => {
            udt_data_type.keyspace = keyspace_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_class_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    class_name: *mut *const ::std::os::raw::c_char,
    class_name_length: *mut size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::Custom(name) => {
            unsafe { write_str_to_c(name, class_name, class_name_length) };
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_set_class_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    class_name: *const ::std::os::raw::c_char,
) -> CassError {
    unsafe { cass_data_type_set_class_name_n(data_type, class_name, strlen(class_name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_set_class_name_n(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    class_name: *const ::std::os::raw::c_char,
    class_name_length: size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    let class_string = unsafe { ptr_to_cstr_n(class_name, class_name_length) }
        .unwrap()
        .to_string();
    match unsafe { data_type.get_mut_unchecked() } {
        CassDataTypeInner::Custom(name) => {
            *name = class_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_sub_type_count(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> size_t {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::Value(..) => 0,
        CassDataTypeInner::UDT(udt_data_type) => udt_data_type.field_types.len() as size_t,
        CassDataTypeInner::List { typ, .. } | CassDataTypeInner::Set { typ, .. } => {
            typ.is_some() as size_t
        }
        CassDataTypeInner::Map { typ, .. } => match typ {
            MapDataType::Untyped => 0,
            MapDataType::Key(_) => 1,
            MapDataType::KeyAndValue(_, _) => 2,
        },
        CassDataTypeInner::Tuple(v) => v.len() as size_t,
        CassDataTypeInner::Custom(..) => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_sub_type_count(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> size_t {
    unsafe { cass_data_type_sub_type_count(data_type) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_sub_data_type(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    let sub_type: Option<&Arc<CassDataType>> =
        unsafe { data_type.get_unchecked() }.get_sub_data_type(index as usize);

    match sub_type {
        None => ArcFFI::null(),
        // Semantic from cppdriver which also returns non-owning pointer
        Some(arc) => ArcFFI::as_ptr(arc),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_sub_data_type_by_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    name: *const ::std::os::raw::c_char,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    unsafe { cass_data_type_sub_data_type_by_name_n(data_type, name, strlen(name)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_sub_data_type_by_name_n(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    name: *const ::std::os::raw::c_char,
    name_length: size_t,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    let name_str = unsafe { ptr_to_cstr_n(name, name_length) }.unwrap();
    match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::UDT(udt) => match udt.get_field_by_name(name_str) {
            None => ArcFFI::null(),
            Some(t) => ArcFFI::as_ptr(t),
        },
        _ => ArcFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_sub_type_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
    index: size_t,
    name: *mut *const ::std::os::raw::c_char,
    name_length: *mut size_t,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    match unsafe { data_type.get_unchecked() } {
        CassDataTypeInner::UDT(udt) => match udt.field_types.get(index as usize) {
            None => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            Some((field_name, _)) => {
                unsafe { write_str_to_c(field_name, name, name_length) };
                CassError::CASS_OK
            }
        },
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_add_sub_type(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    sub_data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> CassError {
    let data_type = ArcFFI::as_ref(data_type).unwrap();
    match unsafe { data_type.get_mut_unchecked() }
        .add_sub_data_type(ArcFFI::cloned_from_ptr(sub_data_type).unwrap())
    {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    name: *const c_char,
    sub_data_type: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> CassError {
    unsafe { cass_data_type_add_sub_type_by_name_n(data_type, name, strlen(name), sub_data_type) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name_n(
    data_type_raw: CassBorrowedSharedPtr<CassDataType, CMut>,
    name: *const c_char,
    name_length: size_t,
    sub_data_type_raw: CassBorrowedSharedPtr<CassDataType, CConst>,
) -> CassError {
    let name_string = unsafe { ptr_to_cstr_n(name, name_length) }
        .unwrap()
        .to_string();
    let sub_data_type = ArcFFI::cloned_from_ptr(sub_data_type_raw).unwrap();

    let data_type = ArcFFI::as_ref(data_type_raw).unwrap();
    match unsafe { data_type.get_mut_unchecked() } {
        CassDataTypeInner::UDT(udt_data_type) => {
            // The Cpp Driver does not check whether field_types size
            // exceeded field_count.
            udt_data_type.field_types.push((name_string, sub_data_type));
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = CassDataType::new_arced(CassDataTypeInner::Value(sub_value_type));
    unsafe { cass_data_type_add_sub_type(data_type, ArcFFI::as_ptr(&sub_data_type)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    name: *const c_char,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = CassDataType::new_arced(CassDataTypeInner::Value(sub_value_type));
    unsafe { cass_data_type_add_sub_type_by_name(data_type, name, ArcFFI::as_ptr(&sub_data_type)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name_n(
    data_type: CassBorrowedSharedPtr<CassDataType, CMut>,
    name: *const c_char,
    name_length: size_t,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = CassDataType::new_arced(CassDataTypeInner::Value(sub_value_type));
    unsafe {
        cass_data_type_add_sub_type_by_name_n(
            data_type,
            name,
            name_length,
            ArcFFI::as_ptr(&sub_data_type),
        )
    }
}

impl TryFrom<CassConsistency> for Consistency {
    type Error = ();

    fn try_from(c: CassConsistency) -> Result<Consistency, Self::Error> {
        match c {
            CassConsistency::CASS_CONSISTENCY_ANY => Ok(Consistency::Any),
            CassConsistency::CASS_CONSISTENCY_ONE => Ok(Consistency::One),
            CassConsistency::CASS_CONSISTENCY_TWO => Ok(Consistency::Two),
            CassConsistency::CASS_CONSISTENCY_THREE => Ok(Consistency::Three),
            CassConsistency::CASS_CONSISTENCY_QUORUM => Ok(Consistency::Quorum),
            CassConsistency::CASS_CONSISTENCY_ALL => Ok(Consistency::All),
            CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Ok(Consistency::LocalQuorum),
            CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Ok(Consistency::EachQuorum),
            CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Ok(Consistency::LocalOne),
            _ => Err(()),
        }
    }
}

impl TryFrom<CassConsistency> for SerialConsistency {
    type Error = ();

    fn try_from(serial: CassConsistency) -> Result<SerialConsistency, Self::Error> {
        match serial {
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(SerialConsistency::Serial),
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Ok(SerialConsistency::LocalSerial),
            _ => Err(()),
        }
    }
}

pub fn make_batch_type(type_: CassBatchType) -> Option<BatchType> {
    match type_ {
        CassBatchType::CASS_BATCH_TYPE_LOGGED => Some(BatchType::Logged),
        CassBatchType::CASS_BATCH_TYPE_UNLOGGED => Some(BatchType::Unlogged),
        CassBatchType::CASS_BATCH_TYPE_COUNTER => Some(BatchType::Counter),
        _ => None,
    }
}
