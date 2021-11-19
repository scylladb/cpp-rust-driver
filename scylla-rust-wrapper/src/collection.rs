use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::*;
use std::os::raw::c_char;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_collection.rs"));

#[derive(Clone)]
pub struct CassCollection {
    pub collection_type: CassCollectionType,
    pub capacity: usize,
    pub items: Vec<CqlValue>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_new(
    collection_type: CassCollectionType,
    item_count: size_t,
) -> *mut CassCollection {
    let capacity = match collection_type {
        // Maps consist of a key and a value, so twice
        // the number of CqlValue will be stored.
        CassCollectionType::CASS_COLLECTION_TYPE_MAP => item_count * 2,
        _ => item_count,
    } as usize;

    Box::into_raw(Box::new(CassCollection {
        collection_type,
        capacity,
        items: Vec::with_capacity(capacity),
    }))
}

unsafe fn cass_collection_append_cql_value(
    collection_raw: *mut CassCollection,
    value: CqlValue,
) -> CassError {
    // FIXME: Bounds check
    let collection = ptr_to_ref_mut(collection_raw);
    collection.items.push(value);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_int8(
    collection: *mut CassCollection,
    value: cass_int8_t,
) -> CassError {
    cass_collection_append_cql_value(collection, TinyInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_int16(
    collection: *mut CassCollection,
    value: cass_int16_t,
) -> CassError {
    cass_collection_append_cql_value(collection, SmallInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_int32(
    collection: *mut CassCollection,
    value: cass_int32_t,
) -> CassError {
    cass_collection_append_cql_value(collection, Int(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_uint32(
    collection: *mut CassCollection,
    value: cass_uint32_t,
) -> CassError {
    // cass_collection_append_uint32 is only used to set a DATE.
    cass_collection_append_cql_value(collection, Date(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_int64(
    collection: *mut CassCollection,
    value: cass_int64_t,
) -> CassError {
    cass_collection_append_cql_value(collection, BigInt(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_float(
    collection: *mut CassCollection,
    value: cass_float_t,
) -> CassError {
    cass_collection_append_cql_value(collection, Float(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_double(
    collection: *mut CassCollection,
    value: cass_double_t,
) -> CassError {
    cass_collection_append_cql_value(collection, Double(value))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_bool(
    collection: *mut CassCollection,
    value: cass_bool_t,
) -> CassError {
    cass_collection_append_cql_value(collection, Boolean(value != 0))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_string(
    collection: *mut CassCollection,
    value: *const c_char,
) -> CassError {
    let value_str = ptr_to_cstr(value).unwrap();
    let value_length = value_str.len();

    cass_collection_append_string_n(collection, value, value_length as size_t)
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_string_n(
    collection: *mut CassCollection,
    value: *const c_char,
    value_length: size_t,
) -> CassError {
    // TODO: Error handling
    let value_string = ptr_to_cstr_n(value, value_length).unwrap().to_string();
    cass_collection_append_cql_value(collection, Text(value_string))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_append_bytes(
    collection: *mut CassCollection,
    value: *const cass_byte_t,
    value_size: size_t,
) -> CassError {
    let value_vec = std::slice::from_raw_parts(value, value_size as usize).to_vec();
    cass_collection_append_cql_value(collection, Blob(value_vec))
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_free(collection: *mut CassCollection) {
    free_boxed(collection);
}

impl From<CassCollection> for CqlValue {
    fn from(collection: CassCollection) -> Self {
        // FIXME: validate that collection items are correct
        match collection.collection_type {
            CassCollectionType::CASS_COLLECTION_TYPE_LIST => List(collection.items),
            CassCollectionType::CASS_COLLECTION_TYPE_MAP => {
                let mut grouped_items = Vec::new();
                // FIXME: validate even number of items
                for i in (0..collection.items.len()).step_by(2) {
                    let key = collection.items[i].clone();
                    let value = collection.items[i + 1].clone();

                    grouped_items.push((key, value));
                }

                Map(grouped_items)
            }
            CassCollectionType::CASS_COLLECTION_TYPE_SET => CqlValue::Set(collection.items),
            _ => panic!("Collection with invalid type encountered!"),
        }
    }
}
