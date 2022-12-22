use crate::argconv::*;
use crate::cass_types::get_column_type_from_cql_type;
use crate::cass_types::CassDataType;
use crate::types::*;
use scylla::transport::topology::{ColumnKind, CqlType, Table};
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::Weak;

include!(concat!(env!("OUT_DIR"), "/cppdriver_column_type.rs"));

pub type CassSchemaMeta_ = &'static CassSchemaMeta;

pub struct CassSchemaMeta {
    pub keyspaces: HashMap<String, CassKeyspaceMeta>,
}

pub type CassKeyspaceMeta_ = &'static CassKeyspaceMeta;

pub struct CassKeyspaceMeta {
    pub name: String,

    // User defined type name to type
    pub user_defined_type_data_type: HashMap<String, Arc<CassDataType>>,
    pub tables: HashMap<String, Arc<CassTableMeta>>,
    pub views: HashMap<String, Arc<CassMaterializedViewMeta>>,
}

pub type CassTableMeta_ = &'static CassTableMeta;

pub struct CassTableMeta {
    pub name: String,
    pub columns_metadata: HashMap<String, CassColumnMeta>,
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
    pub views: HashMap<String, Arc<CassMaterializedViewMeta>>,
}

pub type CassMaterializedViewMeta_ = &'static CassMaterializedViewMeta;

pub struct CassMaterializedViewMeta {
    pub name: String,
    pub view_metadata: CassTableMeta,
    pub base_table: Weak<CassTableMeta>,
}

pub struct CassColumnMeta {
    pub name: String,
    pub column_type: CassDataType,
    pub column_kind: CassColumnType,
}

pub unsafe fn create_table_metadata(
    keyspace_name: &str,
    table_name: &str,
    table_metadata: &Table,
    user_defined_types: &HashMap<String, Vec<(String, CqlType)>>,
) -> CassTableMeta {
    let mut columns_metadata = HashMap::new();
    table_metadata
        .columns
        .iter()
        .for_each(|(column_name, column_metadata)| {
            let cass_column_meta = CassColumnMeta {
                name: column_name.clone(),
                column_type: get_column_type_from_cql_type(
                    &column_metadata.type_,
                    user_defined_types,
                    keyspace_name,
                ),
                column_kind: match column_metadata.kind {
                    ColumnKind::Regular => CassColumnType::CASS_COLUMN_TYPE_REGULAR,
                    ColumnKind::Static => CassColumnType::CASS_COLUMN_TYPE_STATIC,
                    ColumnKind::Clustering => CassColumnType::CASS_COLUMN_TYPE_CLUSTERING_KEY,
                    ColumnKind::PartitionKey => CassColumnType::CASS_COLUMN_TYPE_PARTITION_KEY,
                },
            };

            columns_metadata.insert(column_name.clone(), cass_column_meta);
        });

    CassTableMeta {
        name: table_name.to_owned(),
        columns_metadata,
        partition_keys: table_metadata.partition_key.clone(),
        clustering_keys: table_metadata.clustering_key.clone(),
        views: HashMap::new(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_schema_meta_free(schema_meta: *mut CassSchemaMeta) {
    free_boxed(schema_meta)
}

#[no_mangle]
pub unsafe extern "C" fn cass_schema_meta_keyspace_by_name(
    schema_meta: *const CassSchemaMeta,
    keyspace_name: *const c_char,
) -> *const CassKeyspaceMeta {
    cass_schema_meta_keyspace_by_name_n(schema_meta, keyspace_name, strlen(keyspace_name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_schema_meta_keyspace_by_name_n(
    schema_meta: *const CassSchemaMeta,
    keyspace_name: *const c_char,
    keyspace_name_length: size_t,
) -> *const CassKeyspaceMeta {
    if keyspace_name.is_null() {
        return std::ptr::null();
    }

    let metadata = ptr_to_ref(schema_meta);
    let keyspace = ptr_to_cstr_n(keyspace_name, keyspace_name_length).unwrap();

    let keyspace_meta = metadata.keyspaces.get(keyspace);

    match keyspace_meta {
        Some(meta) => meta,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_name(
    keyspace_meta: *const CassKeyspaceMeta,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let keyspace_meta = ptr_to_ref(keyspace_meta);
    write_str_to_c(keyspace_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_user_type_by_name(
    keyspace_meta: *const CassKeyspaceMeta,
    type_: *const c_char,
) -> *const CassDataType {
    cass_keyspace_meta_user_type_by_name_n(keyspace_meta, type_, strlen(type_))
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_user_type_by_name_n(
    keyspace_meta: *const CassKeyspaceMeta,
    type_: *const c_char,
    type_length: size_t,
) -> *const CassDataType {
    if type_.is_null() {
        return std::ptr::null();
    }

    let keyspace_meta = ptr_to_ref(keyspace_meta);
    let user_type_name = ptr_to_cstr_n(type_, type_length).unwrap();

    match keyspace_meta
        .user_defined_type_data_type
        .get(user_type_name)
    {
        Some(udt) => Arc::as_ptr(udt),
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_table_by_name(
    keyspace_meta: *const CassKeyspaceMeta,
    table: *const c_char,
) -> *const CassTableMeta {
    cass_keyspace_meta_table_by_name_n(keyspace_meta, table, strlen(table))
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_table_by_name_n(
    keyspace_meta: *const CassKeyspaceMeta,
    table: *const c_char,
    table_length: size_t,
) -> *const CassTableMeta {
    if table.is_null() {
        return std::ptr::null();
    }

    let keyspace_meta = ptr_to_ref(keyspace_meta);
    let table_name = ptr_to_cstr_n(table, table_length).unwrap();

    let table_meta = keyspace_meta.tables.get(table_name);

    match table_meta {
        Some(meta) => Arc::as_ptr(meta) as *const CassTableMeta,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_name(
    table_meta: *const CassTableMeta,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let table_meta = ptr_to_ref(table_meta);
    write_str_to_c(table_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_column_count(table_meta: *const CassTableMeta) -> size_t {
    let table_meta = ptr_to_ref(table_meta);
    table_meta.columns_metadata.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_partition_key(
    table_meta: *const CassTableMeta,
    index: size_t,
) -> *const CassColumnMeta {
    let table_meta = ptr_to_ref(table_meta);

    match table_meta.partition_keys.get(index as usize) {
        Some(column_name) => match table_meta.columns_metadata.get(column_name) {
            Some(column_meta) => column_meta as *const CassColumnMeta,
            None => std::ptr::null(),
        },
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_partition_key_count(
    table_meta: *const CassTableMeta,
) -> size_t {
    let table_meta = ptr_to_ref(table_meta);
    table_meta.partition_keys.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_clustering_key(
    table_meta: *const CassTableMeta,
    index: size_t,
) -> *const CassColumnMeta {
    let table_meta = ptr_to_ref(table_meta);

    match table_meta.clustering_keys.get(index as usize) {
        Some(column_name) => match table_meta.columns_metadata.get(column_name) {
            Some(column_meta) => column_meta as *const CassColumnMeta,
            None => std::ptr::null(),
        },
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_clustering_key_count(
    table_meta: *const CassTableMeta,
) -> size_t {
    let table_meta = ptr_to_ref(table_meta);
    table_meta.clustering_keys.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_column_by_name(
    table_meta: *const CassTableMeta,
    column: *const c_char,
) -> *const CassColumnMeta {
    cass_table_meta_column_by_name_n(table_meta, column, strlen(column))
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_column_by_name_n(
    table_meta: *const CassTableMeta,
    column: *const c_char,
    column_length: size_t,
) -> *const CassColumnMeta {
    if column.is_null() {
        return std::ptr::null();
    }

    let table_meta = ptr_to_ref(table_meta);
    let column_name = ptr_to_cstr_n(column, column_length).unwrap();

    match table_meta.columns_metadata.get(column_name) {
        Some(column_meta) => column_meta as *const CassColumnMeta,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_column_meta_name(
    column_meta: *const CassColumnMeta,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let column_meta = ptr_to_ref(column_meta);
    write_str_to_c(column_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_column_meta_data_type(
    column_meta: *const CassColumnMeta,
) -> *const CassDataType {
    let column_meta = ptr_to_ref(column_meta);
    &column_meta.column_type as *const CassDataType
}

#[no_mangle]
pub unsafe extern "C" fn cass_column_meta_type(
    column_meta: *const CassColumnMeta,
) -> CassColumnType {
    let column_meta = ptr_to_ref(column_meta);
    column_meta.column_kind
}
