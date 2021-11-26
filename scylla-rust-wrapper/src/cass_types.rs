use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_types.rs"));

#[derive(Clone)]
pub struct UDTDataType {
    // Vec to preserve the order of types
    pub field_types: Vec<(String, CassDataTypeArc)>,

    pub keyspace: String,
    pub name: String,
}

impl UDTDataType {
    pub fn new() -> UDTDataType {
        UDTDataType {
            field_types: Vec::new(),
            keyspace: "".to_string(),
            name: "".to_string(),
        }
    }

    pub fn with_capacity(capacity: usize) -> UDTDataType {
        UDTDataType {
            field_types: Vec::with_capacity(capacity),
            keyspace: "".to_string(),
            name: "".to_string(),
        }
    }

    pub fn add_field(&mut self, name: String, field_type: CassDataTypeArc) {
        self.field_types.push((name, field_type));
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<&CassDataTypeArc> {
        for (field_name, field_type) in self.field_types.iter() {
            if field_name == name {
                return Some(field_type);
            }
        }
        None
    }

    pub fn get_field_by_index(&self, index: usize) -> Option<&CassDataTypeArc> {
        self.field_types.get(index).map(|(_, b)| b)
    }
}

#[derive(Clone)]
pub enum CassDataType {
    Value(CassValueType),
    UDT(UDTDataType),
    List(Option<CassDataTypeArc>),
    Set(Option<CassDataTypeArc>),
    Map(Option<CassDataTypeArc>, Option<CassDataTypeArc>),
    Tuple(Vec<CassDataTypeArc>),
    Custom(String),
}

pub type CassDataTypeArc = Arc<CassDataType>;

impl CassDataType {
    fn get_sub_data_type(&self, index: usize) -> Option<&CassDataTypeArc> {
        match self {
            CassDataType::UDT(udt_data_type) => udt_data_type
                .field_types
                .get(index as usize)
                .map(|(_, b)| b),
            CassDataType::List(t) | CassDataType::Set(t) => {
                if index > 0 {
                    None
                } else {
                    t.as_ref()
                }
            }
            CassDataType::Map(t1, t2) => match index {
                0 => t1.as_ref(),
                1 => t2.as_ref(),
                _ => None,
            },
            CassDataType::Tuple(v) => v.get(index as usize),
            _ => None,
        }
    }

    fn add_sub_data_type(&mut self, sub_type: CassDataTypeArc) -> Result<(), CassError> {
        match self {
            CassDataType::List(t) | CassDataType::Set(t) => match t {
                Some(_) => Err(CassError::CASS_ERROR_LIB_BAD_PARAMS),
                None => {
                    *t = Some(sub_type);
                    Ok(())
                }
            },
            CassDataType::Map(t1, t2) => {
                if t1.is_some() && t2.is_some() {
                    Err(CassError::CASS_ERROR_LIB_BAD_PARAMS)
                } else if t1.is_none() {
                    *t1 = Some(sub_type);
                    Ok(())
                } else {
                    *t2 = Some(sub_type);
                    Ok(())
                }
            }
            CassDataType::Tuple(types) => {
                types.push(sub_type);
                Ok(())
            }
            _ => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    }
}

// Changed return type to const ptr - Arc::into_raw is const.
// It's probably not a good idea - but cppdriver doesn't guarantee
// thread safety apart from CassSession and CassFuture.
// This comment also applies to other functions that create CassDataType.
#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new(value_type: CassValueType) -> *const CassDataType {
    let data_type = match value_type {
        CassValueType::CASS_VALUE_TYPE_LIST => CassDataType::List(None),
        CassValueType::CASS_VALUE_TYPE_SET => CassDataType::Set(None),
        CassValueType::CASS_VALUE_TYPE_TUPLE => CassDataType::Tuple(Vec::new()),
        CassValueType::CASS_VALUE_TYPE_MAP => CassDataType::Map(None, None),
        CassValueType::CASS_VALUE_TYPE_UDT => CassDataType::UDT(UDTDataType::new()),
        CassValueType::CASS_VALUE_TYPE_CUSTOM => CassDataType::Custom("".to_string()),
        CassValueType::CASS_VALUE_TYPE_UNKNOWN => return ptr::null_mut(),
        t if t < CassValueType::CASS_VALUE_TYPE_LAST_ENTRY => CassDataType::Value(t),
        _ => return ptr::null_mut(),
    };
    Arc::into_raw(Arc::new(data_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_from_existing(
    data_type: *const CassDataType,
) -> *const CassDataType {
    let data_type = ptr_to_ref(data_type);
    Arc::into_raw(Arc::new(data_type.clone()))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_tuple(item_count: size_t) -> *const CassDataType {
    Arc::into_raw(Arc::new(CassDataType::Tuple(Vec::with_capacity(
        item_count as usize,
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_udt(field_count: size_t) -> *const CassDataType {
    Arc::into_raw(Arc::new(CassDataType::UDT(UDTDataType::with_capacity(
        field_count as usize,
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_free(data_type: *mut CassDataType) {
    free_arced(data_type);
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_type(data_type: *const CassDataType) -> CassValueType {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::Value(value_data_type) => *value_data_type,
        CassDataType::UDT { .. } => CassValueType::CASS_VALUE_TYPE_UDT,
        CassDataType::List(..) => CassValueType::CASS_VALUE_TYPE_LIST,
        CassDataType::Set(..) => CassValueType::CASS_VALUE_TYPE_SET,
        CassDataType::Map(..) => CassValueType::CASS_VALUE_TYPE_MAP,
        CassDataType::Tuple(..) => CassValueType::CASS_VALUE_TYPE_TUPLE,
        CassDataType::Custom(..) => CassValueType::CASS_VALUE_TYPE_CUSTOM,
    }
}

// #[no_mangle]
// pub unsafe extern "C" fn cass_data_type_is_frozen(data_type: *const CassDataType) -> cass_bool_t {}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_type_name(
    data_type: *const CassDataType,
    type_name: *mut *const c_char,
    type_name_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::UDT(UDTDataType { name, .. }) => {
            write_str_to_c(name, type_name, type_name_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_type_name(
    data_type: *mut CassDataType,
    type_name: *const c_char,
) -> CassError {
    cass_data_type_set_type_name_n(data_type, type_name, strlen(type_name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_type_name_n(
    data_type_raw: *mut CassDataType,
    type_name: *const c_char,
    type_name_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type_raw);
    let type_name_string = ptr_to_cstr_n(type_name, type_name_length)
        .unwrap()
        .to_string();

    match data_type {
        CassDataType::UDT(udt_data_type) => {
            udt_data_type.name = type_name_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_keyspace(
    data_type: *const CassDataType,
    keyspace: *mut *const c_char,
    keyspace_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::UDT(UDTDataType { name, .. }) => {
            write_str_to_c(name, keyspace, keyspace_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_keyspace(
    data_type: *mut CassDataType,
    keyspace: *const c_char,
) -> CassError {
    cass_data_type_set_keyspace_n(data_type, keyspace, strlen(keyspace))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_keyspace_n(
    data_type: *mut CassDataType,
    keyspace: *const c_char,
    keyspace_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type);
    let keyspace_string = ptr_to_cstr_n(keyspace, keyspace_length)
        .unwrap()
        .to_string();

    match data_type {
        CassDataType::UDT(udt_data_type) => {
            udt_data_type.keyspace = keyspace_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_class_name(
    data_type: *const CassDataType,
    class_name: *mut *const ::std::os::raw::c_char,
    class_name_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::Custom(name) => {
            write_str_to_c(&name, class_name, class_name_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_class_name(
    data_type: *mut CassDataType,
    class_name: *const ::std::os::raw::c_char,
) -> CassError {
    cass_data_type_set_class_name_n(data_type, class_name, strlen(class_name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_class_name_n(
    data_type: *mut CassDataType,
    class_name: *const ::std::os::raw::c_char,
    class_name_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type);
    let class_string = ptr_to_cstr_n(class_name, class_name_length)
        .unwrap()
        .to_string();
    match data_type {
        CassDataType::Custom(name) => {
            *name = class_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_type_count(data_type: *const CassDataType) -> size_t {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::Value(..) => 0,
        CassDataType::UDT(udt_data_type) => udt_data_type.field_types.len() as size_t,
        CassDataType::List(t) | CassDataType::Set(t) => t.is_some() as size_t,
        CassDataType::Map(t1, t2) => t1.is_some() as size_t + t2.is_some() as size_t,
        CassDataType::Tuple(v) => v.len() as size_t,
        CassDataType::Custom(..) => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_sub_type_count(data_type: *const CassDataType) -> size_t {
    cass_data_type_sub_type_count(data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_data_type(
    data_type: *const CassDataType,
    index: size_t,
) -> *const CassDataType {
    let data_type = ptr_to_ref(data_type);
    let sub_type: Option<&CassDataTypeArc> = data_type.get_sub_data_type(index as usize);

    match sub_type {
        None => std::ptr::null(),
        // Semantic from cppdriver which also returns non-owning pointer
        Some(arc) => Arc::as_ptr(arc),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_data_type_by_name(
    data_type: *const CassDataType,
    name: *const ::std::os::raw::c_char,
) -> *const CassDataType {
    cass_data_type_sub_data_type_by_name_n(data_type, name, strlen(name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_data_type_by_name_n(
    data_type: *const CassDataType,
    name: *const ::std::os::raw::c_char,
    name_length: size_t,
) -> *const CassDataType {
    let data_type = ptr_to_ref(data_type);
    let name_str = ptr_to_cstr_n(name, name_length).unwrap();
    match data_type {
        CassDataType::UDT(udt) => match udt.get_field_by_name(name_str) {
            None => std::ptr::null(),
            Some(t) => Arc::as_ptr(t),
        },
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_type_name(
    data_type: *const CassDataType,
    index: size_t,
    name: *mut *const ::std::os::raw::c_char,
    name_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::UDT(udt) => match udt.field_types.get(index as usize) {
            None => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            Some((field_name, _)) => {
                write_str_to_c(field_name, name, name_length);
                CassError::CASS_OK
            }
        },
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type(
    data_type: *mut CassDataType,
    sub_data_type: *const CassDataType,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type);
    match data_type.add_sub_data_type(clone_arced(sub_data_type)) {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name(
    data_type: *mut CassDataType,
    name: *const c_char,
    sub_data_type: *const CassDataType,
) -> CassError {
    cass_data_type_add_sub_type_by_name_n(data_type, name, strlen(name), sub_data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name_n(
    data_type_raw: *mut CassDataType,
    name: *const c_char,
    name_length: size_t,
    sub_data_type_raw: *const CassDataType,
) -> CassError {
    let name_string = ptr_to_cstr_n(name, name_length).unwrap().to_string();
    let sub_data_type = clone_arced(sub_data_type_raw);

    let data_type = ptr_to_ref_mut(data_type_raw);
    match data_type {
        CassDataType::UDT(udt_data_type) => {
            // The Cpp Driver does not check whether field_types size
            // exceeded field_count.
            udt_data_type.field_types.push((name_string, sub_data_type));
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type(
    data_type: *mut CassDataType,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = Arc::new(CassDataType::Value(sub_value_type));
    cass_data_type_add_sub_type(data_type, Arc::as_ptr(&sub_data_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name(
    data_type: *mut CassDataType,
    name: *const c_char,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = Arc::new(CassDataType::Value(sub_value_type));
    cass_data_type_add_sub_type_by_name(data_type, name, Arc::as_ptr(&sub_data_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name_n(
    data_type: *mut CassDataType,
    name: *const c_char,
    name_length: size_t,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = Arc::new(CassDataType::Value(sub_value_type));
    cass_data_type_add_sub_type_by_name_n(data_type, name, name_length, Arc::as_ptr(&sub_data_type))
}
