use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::types::*;
use crate::value::CassCqlValue;
use std::convert::TryFrom;
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_collection.rs"));

// These constants help us to save an allocation in case user calls `cass_collection_new` (untyped collection).
static UNTYPED_LIST_TYPE: CassDataType = CassDataType::List {
    typ: None,
    frozen: false,
};
static UNTYPED_SET_TYPE: CassDataType = CassDataType::Set {
    typ: None,
    frozen: false,
};
static UNTYPED_MAP_TYPE: CassDataType = CassDataType::Map {
    key_type: None,
    val_type: None,
    frozen: false,
};

#[derive(Clone)]
pub struct CassCollection {
    pub collection_type: CassCollectionType,
    pub data_type: Option<Arc<CassDataType>>,
    pub capacity: usize,
    pub items: Vec<CassCqlValue>,
}

impl CassCollection {
    pub fn append_cql_value(&mut self, value: Option<CassCqlValue>) -> CassError {
        // FIXME: Bounds check, type check
        // There is no API to append null, so unwrap is safe
        self.items.push(value.unwrap());
        CassError::CASS_OK
    }
}

impl TryFrom<&CassCollection> for CassCqlValue {
    type Error = ();
    fn try_from(collection: &CassCollection) -> Result<Self, Self::Error> {
        // FIXME: validate that collection items are correct
        let data_type = collection.data_type.clone();
        match collection.collection_type {
            CassCollectionType::CASS_COLLECTION_TYPE_LIST => Ok(CassCqlValue::List {
                data_type,
                values: collection.items.clone(),
            }),
            CassCollectionType::CASS_COLLECTION_TYPE_MAP => {
                let mut grouped_items = Vec::new();
                // FIXME: validate even number of items
                for i in (0..collection.items.len()).step_by(2) {
                    let key = collection.items[i].clone();
                    let value = collection.items[i + 1].clone();

                    grouped_items.push((key, value));
                }

                Ok(CassCqlValue::Map {
                    data_type,
                    values: grouped_items,
                })
            }
            CassCollectionType::CASS_COLLECTION_TYPE_SET => Ok(CassCqlValue::Set {
                data_type,
                values: collection.items.clone(),
            }),
            _ => Err(()),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_new(
    collection_type: CassCollectionType,
    item_count: size_t,
) -> *mut CassCollection {
    let capacity = match collection_type {
        // Maps consist of a key and a value, so twice
        // the number of CassCqlValue will be stored.
        CassCollectionType::CASS_COLLECTION_TYPE_MAP => item_count * 2,
        _ => item_count,
    } as usize;

    Box::into_raw(Box::new(CassCollection {
        collection_type,
        data_type: None,
        capacity,
        items: Vec::with_capacity(capacity),
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_collection_new_from_data_type(
    data_type: *const CassDataType,
    item_count: size_t,
) -> *mut CassCollection {
    let data_type = clone_arced(data_type);
    let (capacity, collection_type) = match data_type.as_ref() {
        CassDataType::List { .. } => (item_count, CassCollectionType::CASS_COLLECTION_TYPE_LIST),
        CassDataType::Set { .. } => (item_count, CassCollectionType::CASS_COLLECTION_TYPE_SET),
        // Maps consist of a key and a value, so twice
        // the number of CassCqlValue will be stored.
        CassDataType::Map { .. } => (item_count * 2, CassCollectionType::CASS_COLLECTION_TYPE_MAP),
        _ => return std::ptr::null_mut(),
    };
    let capacity = capacity as usize;

    Box::into_raw(Box::new(CassCollection {
        collection_type,
        data_type: Some(data_type),
        capacity,
        items: Vec::with_capacity(capacity),
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_collection_data_type(
    collection: *const CassCollection,
) -> *const CassDataType {
    let collection_ref = ptr_to_ref(collection);

    match &collection_ref.data_type {
        Some(dt) => Arc::as_ptr(dt),
        None => match collection_ref.collection_type {
            CassCollectionType::CASS_COLLECTION_TYPE_LIST => &UNTYPED_LIST_TYPE,
            CassCollectionType::CASS_COLLECTION_TYPE_SET => &UNTYPED_SET_TYPE,
            CassCollectionType::CASS_COLLECTION_TYPE_MAP => &UNTYPED_MAP_TYPE,
            // CassCollectionType is a C enum. Panic, if it's out of range.
            _ => panic!(
                "CassCollectionType enum value out of range: {}",
                collection_ref.collection_type.0
            ),
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_free(collection: *mut CassCollection) {
    free_boxed(collection);
}

prepare_binders_macro!(@append CassCollection, |collection: &mut CassCollection, v| collection.append_cql_value(v));
make_binders!(int8, cass_collection_append_int8);
make_binders!(int16, cass_collection_append_int16);
make_binders!(int32, cass_collection_append_int32);
make_binders!(uint32, cass_collection_append_uint32);
make_binders!(int64, cass_collection_append_int64);
make_binders!(float, cass_collection_append_float);
make_binders!(double, cass_collection_append_double);
make_binders!(bool, cass_collection_append_bool);
make_binders!(string, cass_collection_append_string);
make_binders!(string_n, cass_collection_append_string_n);
make_binders!(bytes, cass_collection_append_bytes);
make_binders!(uuid, cass_collection_append_uuid);
make_binders!(inet, cass_collection_append_inet);
make_binders!(duration, cass_collection_append_duration);
make_binders!(decimal, cass_collection_append_decimal);
make_binders!(collection, cass_collection_append_collection);
make_binders!(tuple, cass_collection_append_tuple);
make_binders!(user_type, cass_collection_append_user_type);
