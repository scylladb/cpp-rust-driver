use crate::argconv::*;
use crate::binding;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::cass_types::CassDataTypeArc;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use std::convert::TryFrom;
use std::sync::Arc;

#[derive(Clone)]
pub struct CassTuple {
    pub data_type: CassDataTypeArc,
    pub items: Vec<MaybeUnset<Option<CqlValue>>>,
}

impl CassTuple {
    fn get_types(&self) -> &Vec<CassDataTypeArc> {
        match *self.data_type {
            CassDataType::Tuple(ref v) => v,
            _ => unreachable!(),
        }
    }

    fn bind_value(&mut self, index: usize, v: Option<CqlValue>) -> CassError {
        if index >= self.items.len() {
            return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
        }

        let inner_types = self.get_types();
        if inner_types.len() > index && !binding::is_compatible_type(&inner_types[index], &v) {
            return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
        }

        self.items[index] = Set(v);

        CassError::CASS_OK
    }
}

impl TryFrom<&CassTuple> for CqlValue {
    type Error = CassError;
    fn try_from(tuple: &CassTuple) -> Result<Self, Self::Error> {
        let mut result: Vec<Option<CqlValue>> = Vec::with_capacity(tuple.items.len());
        for item in tuple.items.iter() {
            match item {
                Set(v) => result.push(v.clone()),
                Unset => return Err(CassError::CASS_ERROR_LIB_INTERNAL_ERROR),
            }
        }
        Ok(CqlValue::Tuple(result))
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_tuple_new(item_count: size_t) -> *mut CassTuple {
    Box::into_raw(Box::new(CassTuple {
        data_type: Arc::new(CassDataType::Tuple(Vec::new())),
        items: vec![Unset; item_count as usize],
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_new_from_data_type(
    data_type: *const CassDataType,
) -> *mut CassTuple {
    let data_type = clone_arced(data_type);
    let item_count = match &*data_type {
        CassDataType::Tuple(v) => v.len(),
        // TODO: I think cppdriver doesn't return error here, but maybe we should?
        _ => panic!("Invalid data type passed to tuple!"),
    };
    Box::into_raw(Box::new(CassTuple {
        data_type,
        items: vec![Unset; item_count],
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_free(tuple: *mut CassTuple) {
    free_boxed(tuple)
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_data_type(tuple: *const CassTuple) -> *const CassDataType {
    Arc::as_ptr(&ptr_to_ref(tuple).data_type)
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
make_binders!(collection, cass_tuple_set_collection);
make_binders!(tuple, cass_tuple_set_tuple);
make_binders!(user_type, cass_tuple_set_user_type);
