use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::cass_types::UDTDataType;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::*;
use std::os::raw::c_char;

#[derive(Clone)]
pub struct CassUserType {
    pub udt_data_type: UDTDataType,

    // Vec to preserve the order of fields
    pub field_values: Vec<(String, Option<CqlValue>)>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_new_from_data_type(
    data_type_raw: *const CassDataType,
) -> *mut CassUserType {
    let data_type = ptr_to_ref(data_type_raw);

    match data_type {
        CassDataType::UDTDataType(udt_data_type) => {
            let field_values = udt_data_type
                .field_types
                .iter()
                .map(|(field_name, _)| (field_name.clone(), None))
                .collect();

            Box::into_raw(Box::new(CassUserType {
                udt_data_type: udt_data_type.clone(),
                field_values,
            }))
        }
        _ => std::ptr::null_mut(),
    }
}

unsafe fn cass_user_type_set_option_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: Option<CqlValue>,
) -> CassError {
    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_user_type_set_option_by_name_n(user_type, name, name_length as size_t, value)
}

unsafe fn cass_user_type_set_option_by_name_n(
    user_type_raw: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: Option<CqlValue>,
) -> CassError {
    let name_string = ptr_to_cstr_n(name, name_length).unwrap().to_string();

    let user_type = ptr_to_ref_mut(user_type_raw);
    for (field_name, field_value) in &mut user_type.field_values {
        if *field_name == name_string {
            // The Cpp driver does not validate whether value is
            // of the correct type as far as I checked.
            *field_value = value;
            return crate::cass_error::OK;
        }
    }

    crate::cass_error::LIB_NAME_DOES_NOT_EXIST
}

unsafe fn cass_user_type_set_cql_value_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: CqlValue,
) -> CassError {
    cass_user_type_set_option_by_name(user_type, name, Some(value))
}

unsafe fn cass_user_type_set_cql_value_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: CqlValue,
) -> CassError {
    cass_user_type_set_option_by_name_n(user_type, name, name_length, Some(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_null_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
) -> CassError {
    cass_user_type_set_option_by_name(user_type, name, None)
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_null_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    cass_user_type_set_option_by_name_n(user_type, name, name_length, None)
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int8_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_int8_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, TinyInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int8_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_int8_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, TinyInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int16_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_int16_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, SmallInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int16_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_int16_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, SmallInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int32_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_int32_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, Int(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int32_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_int32_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Int(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_uint32_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_uint32_t,
) -> CassError {
    // cass_user_type_set_uint32_by_name is only used to set a DATE.
    cass_user_type_set_cql_value_by_name(user_type, name, Date(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_uint32_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_uint32_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Date(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int64_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_int64_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, BigInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_int64_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_int64_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, BigInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_float_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_float_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, Float(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_float_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_float_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Float(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_double_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_double_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, Double(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_double_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_double_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Double(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_bool_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: cass_bool_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name(user_type, name, Boolean(value != 0))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_bool_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: cass_bool_t,
) -> CassError {
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Boolean(value != 0))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_string_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: *const c_char,
) -> CassError {
    let value_str = ptr_to_cstr(value).unwrap();
    let value_length = value_str.len();

    let name_str = ptr_to_cstr(name).unwrap();
    let name_length = name_str.len();

    cass_user_type_set_string_by_name_n(
        user_type,
        name,
        name_length as size_t,
        value,
        value_length as size_t,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_string_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: *const c_char,
    value_length: size_t,
) -> CassError {
    // TODO: Error handling
    let value_string = ptr_to_cstr_n(value, value_length).unwrap().to_string();
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Text(value_string))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_bytes_by_name(
    user_type: *mut CassUserType,
    name: *const c_char,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_user_type_set_cql_value_by_name(user_type, name, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_set_bytes_by_name_n(
    user_type: *mut CassUserType,
    name: *const c_char,
    name_length: size_t,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_user_type_set_cql_value_by_name_n(user_type, name, name_length, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_free(user_type: *mut CassUserType) {
    free_boxed(user_type);
}
