use crate::argconv::*;
use crate::binding;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::types::*;
use crate::value::CassCqlValue;
use std::sync::Arc;

static EMPTY_TUPLE_TYPE: CassDataType = CassDataType::Tuple(Vec::new());

#[derive(Clone)]
pub struct CassTuple {
    pub data_type: Option<Arc<CassDataType>>,
    pub items: Vec<Option<CassCqlValue>>,
}

impl CassTuple {
    fn get_types(&self) -> Option<&Vec<Arc<CassDataType>>> {
        match &self.data_type {
            Some(t) => match &**t {
                CassDataType::Tuple(v) => Some(v),
                _ => unreachable!(),
            },
            None => None,
        }
    }

    // Logic in this function is adapted from cppdriver.
    // https://github.com/scylladb/cpp-driver/blob/81ac12845bdb02f43bbf0384107334e37ae57cde/src/tuple.hpp#L87-L99
    // If the tuple was created without type (`cass_tuple_new`) we'll only make sure to
    // not bind items with too high index.
    // If it was created using `cass_tuple_new_from_data_type` we additionally check if
    // value has correct type.
    fn bind_value(&mut self, index: usize, v: Option<CassCqlValue>) -> CassError {
        if index >= self.items.len() {
            return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
        }

        if let Some(inner_types) = self.get_types() {
            if !binding::is_compatible_type(&inner_types[index], &v) {
                return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
            }
        }

        self.items[index] = v;

        CassError::CASS_OK
    }
}

impl From<&CassTuple> for CassCqlValue {
    fn from(tuple: &CassTuple) -> Self {
        CassCqlValue::Tuple(tuple.items.clone())
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_tuple_new(item_count: size_t) -> *mut CassTuple {
    Box::into_raw(Box::new(CassTuple {
        data_type: None,
        items: vec![None; item_count as usize],
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_new_from_data_type(
    data_type: *const CassDataType,
) -> *mut CassTuple {
    let data_type = clone_arced(data_type);
    let item_count = match &*data_type {
        CassDataType::Tuple(v) => v.len(),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(CassTuple {
        data_type: Some(data_type),
        items: vec![None; item_count],
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_free(tuple: *mut CassTuple) {
    free_boxed(tuple)
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_data_type(tuple: *const CassTuple) -> *const CassDataType {
    match &ptr_to_ref(tuple).data_type {
        Some(t) => Arc::as_ptr(t),
        None => &EMPTY_TUPLE_TYPE,
    }
}

prepare_binders_macro!(@only_index CassTuple, |tuple: &mut CassTuple, index, v| tuple.bind_value(index, v));
make_binders!(null, cass_tuple_set_null);
make_binders!(int8, cass_tuple_set_int8);
make_binders!(int16, cass_tuple_set_int16);
make_binders!(int32, cass_tuple_set_int32);
make_binders!(uint32, cass_tuple_set_uint32);
make_binders!(int64, cass_tuple_set_int64);
make_binders!(float, cass_tuple_set_float);
make_binders!(double, cass_tuple_set_double);
make_binders!(bool, cass_tuple_set_bool);
make_binders!(string, cass_tuple_set_string);
make_binders!(string_n, cass_tuple_set_string_n);
make_binders!(bytes, cass_tuple_set_bytes);
make_binders!(uuid, cass_tuple_set_uuid);
make_binders!(inet, cass_tuple_set_inet);
make_binders!(duration, cass_tuple_set_duration);
make_binders!(collection, cass_tuple_set_collection);
make_binders!(tuple, cass_tuple_set_tuple);
make_binders!(user_type, cass_tuple_set_user_type);
