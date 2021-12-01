use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::cass_types::{CassDataTypeArc, UDTDataType};
use crate::collection::CassCollection;
use crate::inet::CassInet;
use crate::types::*;
use crate::uuid::CassUuid;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::*;
use std::convert::TryInto;
use std::os::raw::c_char;
use std::sync::Arc;

#[derive(Clone)]
pub struct CassUserType {
    pub data_type: CassDataTypeArc,

    // Vec to preserve the order of fields
    pub field_values: Vec<Option<CqlValue>>,
}

fn is_compatible_type(_data_type: &CassDataType, _value: &Option<CqlValue>) -> bool {
    // TODO: cppdriver actually checks types.
    // TODO: this function should probably somewhere else
    true
}

impl CassUserType {
    pub fn get_udt_type(&self) -> &UDTDataType {
        match &*self.data_type {
            CassDataType::UDT(udt) => udt,
            _ => unreachable!(),
        }
    }

    unsafe fn set_option_by_index(&mut self, index: usize, value: Option<CqlValue>) -> CassError {
        if index >= self.field_values.len() {
            return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
        }
        if !is_compatible_type(&*self.get_udt_type().field_types[index].1, &value) {
            return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
        }
        self.field_values[index] = value;
        CassError::CASS_OK
    }

    //TODO: some hashtable for name lookup?
    unsafe fn set_option_by_name(&mut self, name: &str, value: Option<CqlValue>) -> CassError {
        let mut indices = Vec::new();
        for (i, (field_name, _)) in self.get_udt_type().field_types.iter().enumerate() {
            if *field_name == name {
                indices.push(i);
            }
        }

        if indices.is_empty() {
            return CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
        }

        for i in indices.into_iter() {
            let rc = self.set_option_by_index(i, value.clone());
            if rc != CassError::CASS_OK {
                return rc;
            }
        }

        CassError::CASS_OK
    }
}

impl From<&CassUserType> for CqlValue {
    fn from(user_type: &CassUserType) -> Self {
        CqlValue::UserDefinedType {
            keyspace: user_type.get_udt_type().keyspace.clone(),
            type_name: user_type.get_udt_type().name.clone(),
            fields: user_type
                .field_values
                .iter()
                .zip(user_type.get_udt_type().field_types.iter())
                .map(|(v, (name, _))| (name.clone(), v.clone()))
                .collect(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_new_from_data_type(
    data_type_raw: *const CassDataType,
) -> *mut CassUserType {
    let data_type = clone_arced(data_type_raw);

    match &*data_type {
        CassDataType::UDT(udt_data_type) => {
            let field_values = vec![None; udt_data_type.field_types.len()];
            Box::into_raw(Box::new(CassUserType {
                data_type,
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

macro_rules! make_binders {
    (@index $fn_by_idx:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_idx(
            user_type: *mut CassUserType,
            index: size_t,
            $($arg: $t), *
        ) -> CassError {
            match ($e)($($arg), *) {
                Ok(v) => ptr_to_ref_mut(user_type).set_option_by_index(index as usize, v),
                Err(e) => e,
            }
        }
    };
    (@name $fn_by_name:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name(
            user_type: *mut CassUserType,
            name: *const c_char,
            $($arg: $t), *
        ) -> CassError {
            let name = ptr_to_cstr(name).unwrap();
            match ($e)($($arg), *) {
                Ok(v) => ptr_to_ref_mut(user_type).set_option_by_name(name, v),
                Err(e) => e,
            }
        }
    };
    (@name_n $fn_by_name_n:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name_n(
            user_type: *mut CassUserType,
            name: *const c_char,
            name_length: size_t,
            $($arg: $t), *
        ) -> CassError {
            let name = ptr_to_cstr_n(name, name_length).unwrap();
            match ($e)($($arg), *) {
                Ok(v) => ptr_to_ref_mut(user_type).set_option_by_name(name, v),
                Err(e) => e,
            }
        }
    };
    (@multi $m_fn_by_idx:ident, $m_fn_by_name:ident, $m_fn_by_name_n:ident,
        $e1:expr, [$($arg1:ident @ $t1:ty), *],
        $e2:expr, [$($arg2:ident @ $t2:ty), *],
        $e3:expr, [$($arg3:ident @ $t3:ty), *]) => {
            make_binders!(@index $m_fn_by_idx, $e1, [$($arg1 @ $t1), *]);
            make_binders!(@name $m_fn_by_name, $e2, [$($arg2 @ $t2), *]);
            make_binders!(@name_n $m_fn_by_name_n, $e3, [$($arg3 @ $t3), *]);
    };

    ($fn_by_idx:ident, $fn_by_name:ident, $fn_by_name_n:ident, $e:expr, $($arg:ident @ $t:ty), *) => {
        make_binders!(@multi $fn_by_idx, $fn_by_name, $fn_by_name_n, $e, [$($arg @ $t), *], $e, [$($arg @ $t), *], $e, [$($arg @ $t), *]);
    }
}

make_binders!(
    cass_user_type_set_null,
    cass_user_type_set_null_by_name,
    cass_user_type_set_null_by_name_n,
    || Ok(None),
);

make_binders!(
    cass_user_type_set_int8,
    cass_user_type_set_int8_by_name,
    cass_user_type_set_int8_by_name_n,
    |v| Ok(Some(TinyInt(v))),
    v @ cass_int8_t
);

make_binders!(
    cass_user_type_set_int16,
    cass_user_type_set_int16_by_name,
    cass_user_type_set_int16_by_name_n,
    |v| Ok(Some(SmallInt(v))),
    v @ cass_int16_t
);

make_binders!(
    cass_user_type_set_int32,
    cass_user_type_set_int32_by_name,
    cass_user_type_set_int32_by_name_n,
    |v| Ok(Some(Int(v))),
    v @ cass_int32_t
);

make_binders!(
    cass_user_type_set_uint32,
    cass_user_type_set_uint32_by_name,
    cass_user_type_set_uint32_by_name_n,
    |v| Ok(Some(Date(v))),
    v @ cass_uint32_t
);

make_binders!(
    cass_user_type_set_int64,
    cass_user_type_set_int64_by_name,
    cass_user_type_set_int64_by_name_n,
    |v| Ok(Some(BigInt(v))),
    v @ cass_int64_t
);

make_binders!(
    cass_user_type_set_float,
    cass_user_type_set_float_by_name,
    cass_user_type_set_float_by_name_n,
    |v| Ok(Some(Float(v))),
    v @ cass_float_t
);

make_binders!(
    cass_user_type_set_double,
    cass_user_type_set_double_by_name,
    cass_user_type_set_double_by_name_n,
    |v| Ok(Some(Double(v))),
    v @ cass_double_t
);

make_binders!(
    cass_user_type_set_bool,
    cass_user_type_set_bool_by_name,
    cass_user_type_set_bool_by_name_n,
    |v| Ok(Some(Boolean(v != 0))),
    v @ cass_bool_t
);

make_binders!(@multi
    cass_user_type_set_string,
    cass_user_type_set_string_by_name,
    cass_user_type_set_string_by_name_n,
    |v, n| Ok(Some(Text(ptr_to_cstr_n(v, n).unwrap().to_string()))),
    [v @ *const c_char, n @ size_t],
    |v| Ok(Some(Text(ptr_to_cstr(v).unwrap().to_string()))),
    [v @ *const c_char],
    |v, n| Ok(Some(Text(ptr_to_cstr_n(v, n).unwrap().to_string()))),
    [v @ *const c_char, n @ size_t]
);

make_binders!(
    cass_user_type_set_bytes,
    cass_user_type_set_bytes_by_name,
    cass_user_type_set_bytes_by_name_n,
    |v, v_size| {
        let v_vec = std::slice::from_raw_parts(v, v_size as usize).to_vec();
        Ok(Some(Blob(v_vec)))
    },
    v @ *const cass_byte_t,
    v_size @ size_t
);

make_binders!(
    cass_user_type_set_uuid,
    cass_user_type_set_uuid_by_name,
    cass_user_type_set_uuid_by_name_n,
    |v: CassUuid| Ok(Some(Uuid(v.into()))),
    v @ CassUuid
);

make_binders!(
    cass_user_type_set_inet,
    cass_user_type_set_inet_by_name,
    cass_user_type_set_inet_by_name_n,
    |v: CassInet| {
        // Err if length in struct is invalid.
        // cppdriver doesn't check this - it encodes any length given to it
        // but it doesn't seem like something we wanna do. Also, rust driver can't
        // really do it afaik.
        match v.try_into() {
            Ok(v) => Ok(Some(Inet(v))),
            Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    },
    v @ CassInet
);

make_binders!(
    cass_user_type_set_collection,
    cass_user_type_set_collection_by_name,
    cass_user_type_set_collection_by_name_n,
    |p: *const CassCollection| {
        match ptr_to_ref(p).clone().try_into() {
            Ok(v) => Ok(Some(v)),
            Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    },
    p @ *const CassCollection
);

make_binders!(
    cass_user_type_set_user_type,
    cass_user_type_set_user_type_by_name,
    cass_user_type_set_user_type_by_name_n,
    |p: *const CassUserType| Ok(Some(ptr_to_ref(p).into())),
    p @ *const CassUserType
);
