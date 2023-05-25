use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use scylla::batch::{BatchType, Consistency, SerialConsistency};
use scylla::frame::response::result::ColumnType;
use scylla::transport::topology::{CollectionType, CqlType, NativeType, UserDefinedType};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/cppdriver_data_types.rs"));
include!(concat!(env!("OUT_DIR"), "/cppdriver_data_query_error.rs"));
include!(concat!(env!("OUT_DIR"), "/cppdriver_batch_types.rs"));

#[derive(Clone, Debug)]
pub struct UDTDataType {
    // Vec to preserve the order of types
    pub field_types: Vec<(String, Arc<CassDataType>)>,

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

    pub fn create_with_params(
        user_defined_types: &HashMap<String, Arc<UserDefinedType>>,
        keyspace_name: &str,
        name: &str,
    ) -> UDTDataType {
        UDTDataType {
            field_types: user_defined_types
                .get(name)
                .map(|udf| &udf.field_types)
                .unwrap_or(&vec![])
                .iter()
                .map(|(udt_field_name, udt_field_type)| {
                    (
                        udt_field_name.clone(),
                        Arc::new(get_column_type_from_cql_type(
                            udt_field_type,
                            user_defined_types,
                            keyspace_name,
                        )),
                    )
                })
                .collect(),
            keyspace: keyspace_name.to_string(),
            name: name.to_owned(),
        }
    }

    pub fn with_capacity(capacity: usize) -> UDTDataType {
        UDTDataType {
            field_types: Vec::with_capacity(capacity),
            keyspace: "".to_string(),
            name: "".to_string(),
        }
    }

    pub fn add_field(&mut self, name: String, field_type: Arc<CassDataType>) {
        self.field_types.push((name, field_type));
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<&Arc<CassDataType>> {
        self.field_types
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, t)| t)
    }

    pub fn get_field_by_index(&self, index: usize) -> Option<&Arc<CassDataType>> {
        self.field_types.get(index).map(|(_, b)| b)
    }
}

impl Default for UDTDataType {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub enum CassDataType {
    Value(CassValueType),
    UDT(UDTDataType),
    List(Option<Arc<CassDataType>>),
    Set(Option<Arc<CassDataType>>),
    Map(Option<Arc<CassDataType>>, Option<Arc<CassDataType>>),
    Tuple(Vec<Arc<CassDataType>>),
    Custom(String),
}

impl From<NativeType> for CassValueType {
    fn from(native_type: NativeType) -> CassValueType {
        match native_type {
            NativeType::Ascii => CassValueType::CASS_VALUE_TYPE_ASCII,
            NativeType::Boolean => CassValueType::CASS_VALUE_TYPE_BOOLEAN,
            NativeType::Blob => CassValueType::CASS_VALUE_TYPE_BLOB,
            NativeType::Counter => CassValueType::CASS_VALUE_TYPE_COUNTER,
            NativeType::Date => CassValueType::CASS_VALUE_TYPE_DATE,
            NativeType::Decimal => CassValueType::CASS_VALUE_TYPE_DECIMAL,
            NativeType::Double => CassValueType::CASS_VALUE_TYPE_DOUBLE,
            NativeType::Duration => CassValueType::CASS_VALUE_TYPE_DURATION,
            NativeType::Float => CassValueType::CASS_VALUE_TYPE_FLOAT,
            NativeType::Int => CassValueType::CASS_VALUE_TYPE_INT,
            NativeType::BigInt => CassValueType::CASS_VALUE_TYPE_BIGINT,
            NativeType::Text => CassValueType::CASS_VALUE_TYPE_TEXT,
            NativeType::Timestamp => CassValueType::CASS_VALUE_TYPE_TIMESTAMP,
            NativeType::Inet => CassValueType::CASS_VALUE_TYPE_INET,
            NativeType::SmallInt => CassValueType::CASS_VALUE_TYPE_SMALL_INT,
            NativeType::TinyInt => CassValueType::CASS_VALUE_TYPE_TINY_INT,
            NativeType::Time => CassValueType::CASS_VALUE_TYPE_TIME,
            NativeType::Timeuuid => CassValueType::CASS_VALUE_TYPE_TIMEUUID,
            NativeType::Uuid => CassValueType::CASS_VALUE_TYPE_UUID,
            NativeType::Varint => CassValueType::CASS_VALUE_TYPE_VARINT,
        }
    }
}

pub fn get_column_type_from_cql_type(
    cql_type: &CqlType,
    user_defined_types: &HashMap<String, Arc<UserDefinedType>>,
    keyspace_name: &str,
) -> CassDataType {
    match cql_type {
        CqlType::Native(native) => CassDataType::Value(native.clone().into()),
        CqlType::Collection { type_, .. } => match type_ {
            CollectionType::List(list) => CassDataType::List(Some(Arc::new(
                get_column_type_from_cql_type(list, user_defined_types, keyspace_name),
            ))),
            CollectionType::Map(key, value) => CassDataType::Map(
                Some(Arc::new(get_column_type_from_cql_type(
                    key,
                    user_defined_types,
                    keyspace_name,
                ))),
                Some(Arc::new(get_column_type_from_cql_type(
                    value,
                    user_defined_types,
                    keyspace_name,
                ))),
            ),
            CollectionType::Set(set) => CassDataType::Set(Some(Arc::new(
                get_column_type_from_cql_type(set, user_defined_types, keyspace_name),
            ))),
        },
        CqlType::Tuple(tuple) => CassDataType::Tuple(
            tuple
                .iter()
                .map(|field_type| {
                    Arc::new(get_column_type_from_cql_type(
                        field_type,
                        user_defined_types,
                        keyspace_name,
                    ))
                })
                .collect(),
        ),
        CqlType::UserDefinedType { definition, .. } => {
            let name = match definition {
                Ok(resolved) => &resolved.name,
                Err(not_resolved) => &not_resolved.name,
            };
            CassDataType::UDT(UDTDataType::create_with_params(
                user_defined_types,
                keyspace_name,
                name,
            ))
        }
    }
}

impl CassDataType {
    fn get_sub_data_type(&self, index: usize) -> Option<&Arc<CassDataType>> {
        match self {
            CassDataType::UDT(udt_data_type) => {
                udt_data_type.field_types.get(index).map(|(_, b)| b)
            }
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
            CassDataType::Tuple(v) => v.get(index),
            _ => None,
        }
    }

    fn add_sub_data_type(&mut self, sub_type: Arc<CassDataType>) -> Result<(), CassError> {
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

    pub fn get_udt_type(&self) -> &UDTDataType {
        match self {
            CassDataType::UDT(udt) => udt,
            _ => panic!("Can get UDT out of non-UDT data type"),
        }
    }

    pub fn get_value_type(&self) -> CassValueType {
        match &self {
            CassDataType::Value(value_data_type) => *value_data_type,
            CassDataType::UDT { .. } => CassValueType::CASS_VALUE_TYPE_UDT,
            CassDataType::List(..) => CassValueType::CASS_VALUE_TYPE_LIST,
            CassDataType::Set(..) => CassValueType::CASS_VALUE_TYPE_SET,
            CassDataType::Map(..) => CassValueType::CASS_VALUE_TYPE_MAP,
            CassDataType::Tuple(..) => CassValueType::CASS_VALUE_TYPE_TUPLE,
            CassDataType::Custom(..) => CassValueType::CASS_VALUE_TYPE_CUSTOM,
        }
    }
}

pub fn get_column_type(column_type: &ColumnType) -> CassDataType {
    match column_type {
        ColumnType::Custom(s) => CassDataType::Custom((*s).clone()),
        ColumnType::Ascii => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_ASCII),
        ColumnType::Boolean => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BOOLEAN),
        ColumnType::Blob => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BLOB),
        ColumnType::Counter => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_COUNTER),
        ColumnType::Decimal => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DECIMAL),
        ColumnType::Date => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DATE),
        ColumnType::Double => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DOUBLE),
        ColumnType::Float => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_FLOAT),
        ColumnType::Int => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_INT),
        ColumnType::BigInt => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BIGINT),
        ColumnType::Text => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TEXT),
        ColumnType::Timestamp => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TIMESTAMP),
        ColumnType::Inet => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_INET),
        ColumnType::List(boxed_type) => {
            CassDataType::List(Some(Arc::new(get_column_type(boxed_type.as_ref()))))
        }
        ColumnType::Map(key, value) => CassDataType::Map(
            Some(Arc::new(get_column_type(key.as_ref()))),
            Some(Arc::new(get_column_type(value.as_ref()))),
        ),
        ColumnType::Set(boxed_type) => {
            CassDataType::Set(Some(Arc::new(get_column_type(boxed_type.as_ref()))))
        }
        ColumnType::UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => CassDataType::UDT(UDTDataType {
            field_types: field_types
                .iter()
                .map(|(name, col_type)| ((*name).clone(), Arc::new(get_column_type(col_type))))
                .collect(),
            keyspace: (*keyspace).clone(),
            name: (*type_name).clone(),
        }),
        ColumnType::SmallInt => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_SMALL_INT),
        ColumnType::TinyInt => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TINY_INT),
        ColumnType::Time => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TIME),
        ColumnType::Timeuuid => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TIMEUUID),
        ColumnType::Tuple(v) => CassDataType::Tuple(
            v.iter()
                .map(|col_type| Arc::new(get_column_type(col_type)))
                .collect(),
        ),
        ColumnType::Uuid => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_UUID),
        ColumnType::Varint => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_VARINT),
        _ => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_UNKNOWN),
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
    data_type.get_value_type()
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
            write_str_to_c(name, class_name, class_name_length);
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
    let sub_type: Option<&Arc<CassDataType>> = data_type.get_sub_data_type(index as usize);

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

impl TryFrom<CassConsistency> for Consistency {
    type Error = ();

    fn try_from(c: CassConsistency) -> Result<Consistency, Self::Error> {
        match c {
            CassConsistency::CASS_CONSISTENCY_ANY => Ok(Consistency::Any),
            CassConsistency::CASS_CONSISTENCY_ONE => Ok(Consistency::One),
            CassConsistency::CASS_CONSISTENCY_TWO => Ok(Consistency::Two),
            CassConsistency::CASS_CONSISTENCY_THREE => Ok(Consistency::Three),
            CassConsistency::CASS_CONSISTENCY_QUORUM => Ok(Consistency::Quorum),
            CassConsistency::CASS_CONSISTENCY_ALL => Ok(Consistency::All),
            CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Ok(Consistency::LocalQuorum),
            CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Ok(Consistency::EachQuorum),
            CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Ok(Consistency::LocalOne),
            _ => Err(()),
        }
    }
}

impl TryFrom<CassConsistency> for SerialConsistency {
    type Error = ();

    fn try_from(serial: CassConsistency) -> Result<SerialConsistency, Self::Error> {
        match serial {
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(SerialConsistency::Serial),
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Ok(SerialConsistency::LocalSerial),
            _ => Err(()),
        }
    }
}

pub fn make_batch_type(type_: CassBatchType) -> Option<BatchType> {
    match type_ {
        CassBatchType::CASS_BATCH_TYPE_LOGGED => Some(BatchType::Logged),
        CassBatchType::CASS_BATCH_TYPE_UNLOGGED => Some(BatchType::Unlogged),
        CassBatchType::CASS_BATCH_TYPE_COUNTER => Some(BatchType::Counter),
        _ => None,
    }
}
