use crate::argconv::*;
use crate::binding::is_compatible_type;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::cass_types::CassDataTypeArc;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use std::os::raw::c_char;
use std::sync::Arc;

#[derive(Clone)]
pub struct CassUserType {
    pub data_type: CassDataTypeArc,

    // Vec to preserve the order of fields
    pub field_values: Vec<Option<CqlValue>>,
}

impl CassUserType {
    fn set_option_by_index(&mut self, index: usize, value: Option<CqlValue>) -> CassError {
        if index >= self.field_values.len() {
            return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
        }
        if !is_compatible_type(&self.data_type.get_udt_type().field_types[index].1, &value) {
            return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
        }
        self.field_values[index] = value;
        CassError::CASS_OK
    }

    //TODO: some hashtable for name lookup?
    fn set_option_by_name(&mut self, name: &str, value: Option<CqlValue>) -> CassError {
        let mut found_field: bool = false;
        for (index, (field_name, field_type)) in
            self.data_type.get_udt_type().field_types.iter().enumerate()
        {
            if *field_name == name {
                found_field = true;
                if index >= self.field_values.len() {
                    return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
                }
                if !is_compatible_type(field_type, &value) {
                    return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                }
                self.field_values[index] = value.clone();
            }
        }

        if found_field {
            CassError::CASS_OK
        } else {
            CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
        }
    }
}

impl From<&CassUserType> for CqlValue {
    fn from(user_type: &CassUserType) -> Self {
        CqlValue::UserDefinedType {
            keyspace: user_type.data_type.get_udt_type().keyspace.clone(),
            type_name: user_type.data_type.get_udt_type().name.clone(),
            fields: user_type
                .field_values
                .iter()
                .zip(user_type.data_type.get_udt_type().field_types.iter())
                .map(|(v, (name, _))| (name.clone(), v.clone()))
                .collect(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_new_from_data_type(
    data_type_raw: *const CassDataType,
) -> *mut CassUserType {
    let data_type = ptr_to_ref(data_type_raw);

    match data_type {
        CassDataType::UDT(udt_data_type) => {
            let field_values = vec![None; udt_data_type.field_types.len()];
            Box::into_raw(Box::new(CassUserType {
                data_type: Arc::new(CassDataType::UDT(udt_data_type.clone())),
                field_values,
            }))
        }
        _ => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_free(user_type: *mut CassUserType) {
    free_boxed(user_type);
}
#[no_mangle]
pub unsafe extern "C" fn cass_user_type_data_type(
    user_type: *const CassUserType,
) -> *const CassDataType {
    Arc::as_ptr(&ptr_to_ref(user_type).data_type)
}

prepare_binders_macro!(@index_and_name CassUserType,
    |udt: &mut CassUserType, index, v| udt.set_option_by_index(index, v),
    |udt: &mut CassUserType, name, v| udt.set_option_by_name(name, v));

make_binders!(
    null,
    cass_user_type_set_null,
    cass_user_type_set_null_by_name,
    cass_user_type_set_null_by_name_n
);
make_binders!(
    int8,
    cass_user_type_set_int8,
    cass_user_type_set_int8_by_name,
    cass_user_type_set_int8_by_name_n
);
make_binders!(
    int16,
    cass_user_type_set_int16,
    cass_user_type_set_int16_by_name,
    cass_user_type_set_int16_by_name_n
);
make_binders!(
    int32,
    cass_user_type_set_int32,
    cass_user_type_set_int32_by_name,
    cass_user_type_set_int32_by_name_n
);
make_binders!(
    uint32,
    cass_user_type_set_uint32,
    cass_user_type_set_uint32_by_name,
    cass_user_type_set_uint32_by_name_n
);
make_binders!(
    int64,
    cass_user_type_set_int64,
    cass_user_type_set_int64_by_name,
    cass_user_type_set_int64_by_name_n
);
make_binders!(
    float,
    cass_user_type_set_float,
    cass_user_type_set_float_by_name,
    cass_user_type_set_float_by_name_n
);
make_binders!(
    double,
    cass_user_type_set_double,
    cass_user_type_set_double_by_name,
    cass_user_type_set_double_by_name_n
);
make_binders!(
    bool,
    cass_user_type_set_bool,
    cass_user_type_set_bool_by_name,
    cass_user_type_set_bool_by_name_n
);
make_binders!(
    string,
    cass_user_type_set_string,
    string,
    cass_user_type_set_string_by_name,
    string_n,
    cass_user_type_set_string_by_name_n
);
make_binders!(@index string_n, cass_user_type_set_string_n);
make_binders!(
    bytes,
    cass_user_type_set_bytes,
    cass_user_type_set_bytes_by_name,
    cass_user_type_set_bytes_by_name_n
);
make_binders!(
    uuid,
    cass_user_type_set_uuid,
    cass_user_type_set_uuid_by_name,
    cass_user_type_set_uuid_by_name_n
);
make_binders!(
    inet,
    cass_user_type_set_inet,
    cass_user_type_set_inet_by_name,
    cass_user_type_set_inet_by_name_n
);
make_binders!(
    collection,
    cass_user_type_set_collection,
    cass_user_type_set_collection_by_name,
    cass_user_type_set_collection_by_name_n
);
make_binders!(
    tuple,
    cass_user_type_set_tuple,
    cass_user_type_set_tuple_by_name,
    cass_user_type_set_tuple_by_name_n
);
make_binders!(
    user_type,
    cass_user_type_set_user_type,
    cass_user_type_set_user_type_by_name,
    cass_user_type_set_user_type_by_name_n
);
