use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::*;
use std::convert::TryFrom;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_collection.rs"));

#[derive(Clone)]
pub struct CassCollection {
    pub collection_type: CassCollectionType,
    pub capacity: usize,
    pub items: Vec<CqlValue>,
}

impl CassCollection {
    pub fn append_cql_value(&mut self, value: Option<CqlValue>) -> CassError {
        // FIXME: Bounds check, type check
        // There is no API to append null, so unwrap is safe
        self.items.push(value.unwrap());
        CassError::CASS_OK
    }
}

impl TryFrom<&CassCollection> for CqlValue {
    type Error = ();
    fn try_from(collection: &CassCollection) -> Result<Self, Self::Error> {
        // FIXME: validate that collection items are correct
        match collection.collection_type {
            CassCollectionType::CASS_COLLECTION_TYPE_LIST => Ok(List(collection.items.clone())),
            CassCollectionType::CASS_COLLECTION_TYPE_MAP => {
                let mut grouped_items = Vec::new();
                // FIXME: validate even number of items
                for i in (0..collection.items.len()).step_by(2) {
                    let key = collection.items[i].clone();
                    let value = collection.items[i + 1].clone();

                    grouped_items.push((key, value));
                }

                Ok(Map(grouped_items))
            }
            CassCollectionType::CASS_COLLECTION_TYPE_SET => {
                Ok(CqlValue::Set(collection.items.clone()))
            }
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
make_binders!(collection, cass_collection_append_collection);
make_binders!(tuple, cass_collection_append_tuple);
make_binders!(user_type, cass_collection_append_user_type);
