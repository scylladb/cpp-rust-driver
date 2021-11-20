use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use std::os::raw::c_char;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_types.rs"));

#[derive(Clone)]
pub struct UDTDataType {
    pub field_count: usize,

    // Vec to preserve the order of types
    pub field_types: Vec<(String, CassDataType)>,

    pub keyspace: String,
    pub name: String,
}

#[derive(Clone)]
pub enum CassDataType {
    ValueDataType(CassValueType),
    UDTDataType(UDTDataType),
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new(value_type: CassValueType) -> *mut CassDataType {
    Box::into_raw(Box::new(CassDataType::ValueDataType(value_type)))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_from_existing(
    data_type_raw: *const CassDataType,
) -> *mut CassDataType {
    let data_type = ptr_to_ref(data_type_raw);
    Box::into_raw(Box::new(data_type.clone()))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_udt(field_count: size_t) -> *mut CassDataType {
    Box::into_raw(Box::new(CassDataType::UDTDataType(UDTDataType {
        // The defaults of Cpp Driver
        keyspace: "".to_string(),
        name: "".to_string(),

        field_count: field_count as usize,
        field_types: Vec::with_capacity(field_count as usize),
    })))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name(
    data_type: *mut CassDataType,
    name: *const c_char,
    sub_data_type: *const CassDataType,
) -> CassError {
    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_data_type_add_sub_type_by_name_n(data_type, name, name_length as size_t, sub_data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name_n(
    data_type_raw: *mut CassDataType,
    name: *const c_char,
    name_length: size_t,
    sub_data_type_raw: *const CassDataType,
) -> CassError {
    let name_string = ptr_to_cstr_n(name, name_length).unwrap().to_string();
    let sub_data_type = ptr_to_ref(sub_data_type_raw);

    let data_type = ptr_to_ref_mut(data_type_raw);
    match data_type {
        CassDataType::ValueDataType(_) => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        CassDataType::UDTDataType(udt_data_type) => {
            // The Cpp Driver does not check whether field_types size
            // exceeded field_count.
            udt_data_type
                .field_types
                .push((name_string, sub_data_type.clone()));
            CassError::CASS_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name(
    data_type: *mut CassDataType,
    name: *const c_char,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = CassDataType::ValueDataType(sub_value_type);
    cass_data_type_add_sub_type_by_name(data_type, name, &sub_data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name_n(
    data_type: *mut CassDataType,
    name: *const c_char,
    name_length: size_t,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = CassDataType::ValueDataType(sub_value_type);
    cass_data_type_add_sub_type_by_name_n(data_type, name, name_length, &sub_data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_type_count(
    data_type_raw: *const CassDataType,
) -> size_t {
    let data_type = ptr_to_ref(data_type_raw);
    match data_type {
        CassDataType::ValueDataType(_) => 0,
        CassDataType::UDTDataType(udt_data_type) => udt_data_type.field_count as size_t,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_sub_type_count(data_type: *const CassDataType) -> size_t {
    cass_data_type_sub_type_count(data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_type_name(
    data_type: *mut CassDataType,
    type_name: *const c_char,
) -> CassError {
    let type_name_str = ptr_to_cstr(type_name).unwrap();
    let type_name_length = type_name_str.len();

    cass_data_type_set_type_name_n(data_type, type_name, type_name_length as size_t)
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
        CassDataType::ValueDataType(_) => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        CassDataType::UDTDataType(udt_data_type) => {
            udt_data_type.name = type_name_string;
            CassError::CASS_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_keyspace(
    data_type: *mut CassDataType,
    keyspace: *const c_char,
) -> CassError {
    let keyspace_str = ptr_to_cstr(keyspace).unwrap();
    let keyspace_length = keyspace_str.len();

    cass_data_type_set_keyspace_n(data_type, keyspace, keyspace_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_keyspace_n(
    data_type_raw: *mut CassDataType,
    keyspace: *const c_char,
    keyspace_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type_raw);
    let keyspace_string = ptr_to_cstr_n(keyspace, keyspace_length)
        .unwrap()
        .to_string();

    match data_type {
        CassDataType::ValueDataType(_) => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
        CassDataType::UDTDataType(udt_data_type) => {
            udt_data_type.keyspace = keyspace_string;
            CassError::CASS_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_type(data_type_raw: *const CassDataType) -> CassValueType {
    let data_type = ptr_to_ref(data_type_raw);
    match data_type {
        CassDataType::ValueDataType(value_data_type) => *value_data_type,
        CassDataType::UDTDataType { .. } => CassValueType::CASS_VALUE_TYPE_UDT,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_free(data_type: *mut CassDataType) {
    free_boxed(data_type);
}
