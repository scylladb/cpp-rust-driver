use crate::argconv::*;
use crate::cass_column_types::CassColumnType;
use crate::cass_types::CassDataType;
use crate::cass_types::get_column_type;
use crate::types::*;
use scylla::cluster::metadata::{ColumnKind, Table};
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::Weak;

pub struct CassSchemaMeta {
    pub(crate) keyspaces: HashMap<String, CassKeyspaceMeta>,
}

unsafe impl FFI for CassSchemaMeta {
    type Origin = FromBox;
}

pub struct CassKeyspaceMeta {
    pub(crate) name: String,

    // User defined type name to type
    pub(crate) user_defined_type_data_type: HashMap<String, Arc<CassDataType>>,
    pub(crate) tables: HashMap<String, Arc<CassTableMeta>>,
    pub(crate) views: HashMap<String, Arc<CassMaterializedViewMeta>>,
}

// Owned by CassSchemaMeta
unsafe impl FFI for CassKeyspaceMeta {
    type Origin = FromRef;
}

pub struct CassTableMeta {
    pub(crate) name: String,
    pub(crate) columns_metadata: HashMap<String, CassColumnMeta>,
    pub(crate) partition_keys: Vec<String>,
    pub(crate) clustering_keys: Vec<String>,
    /// Non-key columns sorted alphabetically by name.
    pub(crate) non_key_sorted_columns: Vec<String>,
    pub(crate) views: HashMap<String, Arc<CassMaterializedViewMeta>>,
}

// Either:
// - owned by CassMaterializedViewMeta - won't be given to user
// - Owned by CassKeyspaceMeta (in Arc), referenced (Weak) by CassMaterializedViewMeta
unsafe impl FFI for CassTableMeta {
    type Origin = FromRef;
}

pub struct CassMaterializedViewMeta {
    pub(crate) name: String,
    pub(crate) view_metadata: CassTableMeta,
    pub(crate) base_table: Weak<CassTableMeta>,
}

// Shared ownership by CassKeyspaceMeta and CassTableMeta
unsafe impl FFI for CassMaterializedViewMeta {
    type Origin = FromRef;
}

pub struct CassColumnMeta {
    pub(crate) name: String,
    pub(crate) column_type: Arc<CassDataType>,
    pub(crate) column_kind: CassColumnType,
}

// Owned by CassTableMeta
unsafe impl FFI for CassColumnMeta {
    type Origin = FromRef;
}

pub(crate) fn create_table_metadata(table_name: &str, table_metadata: &Table) -> CassTableMeta {
    let mut columns_metadata = HashMap::new();
    table_metadata
        .columns
        .iter()
        .for_each(|(column_name, column_metadata)| {
            let cass_column_meta = CassColumnMeta {
                name: column_name.clone(),
                column_type: Arc::new(get_column_type(&column_metadata.typ)),
                column_kind: match column_metadata.kind {
                    ColumnKind::Regular => CassColumnType::CASS_COLUMN_TYPE_REGULAR,
                    ColumnKind::Static => CassColumnType::CASS_COLUMN_TYPE_STATIC,
                    ColumnKind::Clustering => CassColumnType::CASS_COLUMN_TYPE_CLUSTERING_KEY,
                    ColumnKind::PartitionKey => CassColumnType::CASS_COLUMN_TYPE_PARTITION_KEY,

                    // ColumnKind is non_exhaustive.
                    _ => panic!("Unsupported column kind"),
                },
            };

            columns_metadata.insert(column_name.clone(), cass_column_meta);
        });

    let mut non_key_sorted_columns = columns_metadata
        .iter()
        .filter(|(_, column)| {
            !matches!(
                column.column_kind,
                CassColumnType::CASS_COLUMN_TYPE_PARTITION_KEY
                    | CassColumnType::CASS_COLUMN_TYPE_CLUSTERING_KEY,
            )
        })
        .map(|(name, _column)| name.to_owned())
        .collect::<Vec<_>>();
    non_key_sorted_columns.sort_unstable();

    CassTableMeta {
        name: table_name.to_owned(),
        columns_metadata,
        partition_keys: table_metadata.partition_key.clone(),
        clustering_keys: table_metadata.clustering_key.clone(),
        non_key_sorted_columns,
        views: HashMap::new(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_schema_meta_free(
    schema_meta: CassOwnedExclusivePtr<CassSchemaMeta, CConst>,
) {
    BoxFFI::free(schema_meta);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_schema_meta_keyspace_by_name(
    schema_meta: CassBorrowedSharedPtr<CassSchemaMeta, CConst>,
    keyspace_name: *const c_char,
) -> CassBorrowedSharedPtr<CassKeyspaceMeta, CConst> {
    unsafe {
        cass_schema_meta_keyspace_by_name_n(schema_meta, keyspace_name, strlen(keyspace_name))
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_schema_meta_keyspace_by_name_n(
    schema_meta: CassBorrowedSharedPtr<CassSchemaMeta, CConst>,
    keyspace_name: *const c_char,
    keyspace_name_length: size_t,
) -> CassBorrowedSharedPtr<CassKeyspaceMeta, CConst> {
    let Some(metadata) = BoxFFI::as_ref(schema_meta) else {
        tracing::error!(
            "Provided null schema metadata pointer to cass_schema_meta_keyspace_by_name_n!"
        );
        return RefFFI::null();
    };
    if keyspace_name.is_null() {
        return RefFFI::null();
    }

    let keyspace = unsafe { ptr_to_cstr_n(keyspace_name, keyspace_name_length) }.unwrap();

    let keyspace_meta = metadata.keyspaces.get(keyspace);

    match keyspace_meta {
        Some(meta) => RefFFI::as_ptr(meta),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_name(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let Some(keyspace_meta) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!("Provided null keyspace metadata pointer to cass_keyspace_meta_name!");
        return;
    };

    unsafe { write_str_to_c(keyspace_meta.name.as_str(), name, name_length) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_user_type_by_name(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    type_: *const c_char,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    unsafe { cass_keyspace_meta_user_type_by_name_n(keyspace_meta, type_, strlen(type_)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_user_type_by_name_n(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    type_: *const c_char,
    type_length: size_t,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let Some(keyspace_meta) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!(
            "Provided null keyspace metadata pointer to cass_keyspace_meta_user_type_by_name_n!"
        );
        return ArcFFI::null();
    };
    if type_.is_null() {
        return ArcFFI::null();
    }

    let user_type_name = unsafe { ptr_to_cstr_n(type_, type_length) }.unwrap();

    match keyspace_meta
        .user_defined_type_data_type
        .get(user_type_name)
    {
        Some(udt) => ArcFFI::as_ptr(udt),
        None => ArcFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_table_by_name(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    table: *const c_char,
) -> CassBorrowedSharedPtr<CassTableMeta, CConst> {
    unsafe { cass_keyspace_meta_table_by_name_n(keyspace_meta, table, strlen(table)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_table_by_name_n(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    table: *const c_char,
    table_length: size_t,
) -> CassBorrowedSharedPtr<CassTableMeta, CConst> {
    let Some(keyspace_meta) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!(
            "Provided null keyspace metadata pointer to cass_keyspace_meta_table_by_name_n!"
        );
        return RefFFI::null();
    };
    if table.is_null() {
        return RefFFI::null();
    }

    let table_name = unsafe { ptr_to_cstr_n(table, table_length) }.unwrap();

    let table_meta = keyspace_meta.tables.get(table_name);

    match table_meta {
        Some(meta) => RefFFI::as_ptr(meta),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_name(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!("Provided null table metadata pointer to cass_table_meta_name!");
        return;
    };

    unsafe { write_str_to_c(table_meta.name.as_str(), name, name_length) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_column_count(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
) -> size_t {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!("Provided null table metadata pointer to cass_table_meta_column_count!");
        return 0;
    };

    table_meta.columns_metadata.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_column(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    // The order of columns in cpp-driver (and in DESCRIBE TABLE in cqlsh):
    // 1. partition keys sorted by position <- this is guaranteed by rust-driver.
    //    Table::partition_keys is a Vector of pk names, sorted by position.
    // 2. clustering keys sorted by position <- this is guaranteed by rust-driver (same reasoning as above).
    // 3. remaining columns in alphabetical order <- this is something we need to guarantee.
    //
    // Example:
    // CREATE TABLE t
    // (
    //   i int, f int, g int STATIC, b int, c int STATIC, a int, d int, j int, h int,
    //   PRIMARY KEY( (d, a, j), h, i )
    // );
    //
    // The order should be: d, a, j, h, i, b, c, f, g
    // First pks by position: d, a, j
    // Then cks by position: h, i
    // Then remaining columns alphabetically: b, c, f, g

    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!("Provided null table metadata pointer to cass_table_meta_column!");
        return RefFFI::null();
    };
    let index = index as usize;

    // Check if the index lands in partition keys. If so, simply return the corresponding column.
    if let Some(pk_name) = table_meta.partition_keys.get(index) {
        // unwrap: partition key must exist in columns_metadata. This is ensured by rust-driver.
        return RefFFI::as_ptr(table_meta.columns_metadata.get(pk_name).unwrap());
    }

    // Update the index to search in clustering keys
    let index = index - table_meta.partition_keys.len();

    // Check if the index lands in clustering keys. If so, simply return the corresponding column.
    if let Some(ck_name) = table_meta.clustering_keys.get(index) {
        // unwrap: clustering key must exist in columns_metadata. This is ensured by rust-driver.
        return RefFFI::as_ptr(table_meta.columns_metadata.get(ck_name).unwrap());
    }

    // Update the index to search in remaining columns
    let index = index - table_meta.clustering_keys.len();

    table_meta
        .non_key_sorted_columns
        .get(index)
        .map_or(RefFFI::null(), |column_name| {
            // unwrap: We guarantee that column_name exists in columns_metadata. See `create_table_metadata`.
            RefFFI::as_ptr(table_meta.columns_metadata.get(column_name).unwrap())
        })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_partition_key(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!("Provided null table metadata pointer to cass_table_meta_partition_key!");
        return RefFFI::null();
    };

    match table_meta.partition_keys.get(index as usize) {
        Some(column_name) => match table_meta.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_partition_key_count(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
) -> size_t {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_table_meta_partition_key_count!"
        );
        return 0;
    };

    table_meta.partition_keys.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_clustering_key(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!("Provided null table metadata pointer to cass_table_meta_clustering_key!");
        return RefFFI::null();
    };

    match table_meta.clustering_keys.get(index as usize) {
        Some(column_name) => match table_meta.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_clustering_key_count(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
) -> size_t {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_table_meta_clustering_key_count!"
        );
        return 0;
    };

    table_meta.clustering_keys.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_column_by_name(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    column: *const c_char,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    unsafe { cass_table_meta_column_by_name_n(table_meta, column, strlen(column)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_column_by_name_n(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    column: *const c_char,
    column_length: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_table_meta_column_by_name_n!"
        );
        return RefFFI::null();
    };
    if column.is_null() {
        return RefFFI::null();
    }

    let column_name = unsafe { ptr_to_cstr_n(column, column_length) }.unwrap();

    match table_meta.columns_metadata.get(column_name) {
        Some(column_meta) => RefFFI::as_ptr(column_meta),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_column_meta_name(
    column_meta: CassBorrowedSharedPtr<CassColumnMeta, CConst>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let Some(column_meta) = RefFFI::as_ref(column_meta) else {
        tracing::error!("Provided null column metadata pointer to cass_column_meta_name!");
        return;
    };

    unsafe { write_str_to_c(column_meta.name.as_str(), name, name_length) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_column_meta_data_type(
    column_meta: CassBorrowedSharedPtr<CassColumnMeta, CConst>,
) -> CassBorrowedSharedPtr<CassDataType, CConst> {
    let Some(column_meta) = RefFFI::as_ref(column_meta) else {
        tracing::error!("Provided null column metadata pointer to cass_column_meta_data_type!");
        return ArcFFI::null();
    };

    ArcFFI::as_ptr(&column_meta.column_type)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_column_meta_type(
    column_meta: CassBorrowedSharedPtr<CassColumnMeta, CConst>,
) -> CassColumnType {
    let Some(column_meta) = RefFFI::as_ref(column_meta) else {
        tracing::error!("Provided null column metadata pointer to cass_column_meta_type!");
        return CassColumnType::CASS_COLUMN_TYPE_REGULAR;
    };

    column_meta.column_kind
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_materialized_view_by_name(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    view: *const c_char,
) -> CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst> {
    unsafe { cass_keyspace_meta_materialized_view_by_name_n(keyspace_meta, view, strlen(view)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_keyspace_meta_materialized_view_by_name_n(
    keyspace_meta: CassBorrowedSharedPtr<CassKeyspaceMeta, CConst>,
    view: *const c_char,
    view_length: size_t,
) -> CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst> {
    let Some(keyspace_meta) = RefFFI::as_ref(keyspace_meta) else {
        tracing::error!(
            "Provided null keyspace metadata pointer to cass_keyspace_meta_materialized_view_by_name_n!"
        );
        return RefFFI::null();
    };
    if view.is_null() {
        return RefFFI::null();
    }

    let view_name = unsafe { ptr_to_cstr_n(view, view_length).unwrap() };

    match keyspace_meta.views.get(view_name) {
        Some(view_meta) => RefFFI::as_ptr(view_meta.as_ref()),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_materialized_view_by_name(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    view: *const c_char,
) -> CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst> {
    unsafe { cass_table_meta_materialized_view_by_name_n(table_meta, view, strlen(view)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_materialized_view_by_name_n(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    view: *const c_char,
    view_length: size_t,
) -> CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst> {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_table_meta_materialized_view_by_name_n!"
        );
        return RefFFI::null();
    };
    if view.is_null() {
        return RefFFI::null();
    }

    let view_name = unsafe { ptr_to_cstr_n(view, view_length).unwrap() };

    match table_meta.views.get(view_name) {
        Some(view_meta) => RefFFI::as_ptr(view_meta.as_ref()),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_materialized_view_count(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
) -> size_t {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_table_meta_materialized_view_count!"
        );
        return 0;
    };

    table_meta.views.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_table_meta_materialized_view(
    table_meta: CassBorrowedSharedPtr<CassTableMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst> {
    let Some(table_meta) = RefFFI::as_ref(table_meta) else {
        tracing::error!(
            "Provided null table metadata pointer to cass_table_meta_materialized_view!"
        );
        return RefFFI::null();
    };

    match table_meta.views.iter().nth(index as usize) {
        Some(view_meta) => RefFFI::as_ptr(view_meta.1.as_ref()),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_column_by_name(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
    column: *const c_char,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    unsafe { cass_materialized_view_meta_column_by_name_n(view_meta, column, strlen(column)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_column_by_name_n(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
    column: *const c_char,
    column_length: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_column_by_name_n!"
        );
        return RefFFI::null();
    };

    if column.is_null() {
        return RefFFI::null();
    }

    let column_name = unsafe { ptr_to_cstr_n(column, column_length).unwrap() };

    match view_meta.view_metadata.columns_metadata.get(column_name) {
        Some(column_meta) => RefFFI::as_ptr(column_meta),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_name(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_name!"
        );
        return;
    };

    unsafe { write_str_to_c(view_meta.name.as_str(), name, name_length) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_base_table(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
) -> CassBorrowedSharedPtr<CassTableMeta, CConst> {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_base_table!"
        );
        return RefFFI::null();
    };

    let ptr = unsafe { RefFFI::weak_as_ptr(&view_meta.base_table) };
    if RefFFI::is_null(&ptr) {
        tracing::error!(
            "Failed to upgrade a weak reference to table metadata from materialized view metadata! This is a driver bug!"
        );
    }
    ptr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_column_count(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
) -> size_t {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();
    view_meta.view_metadata.columns_metadata.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_column(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_column!"
        );
        return RefFFI::null();
    };

    match view_meta
        .view_metadata
        .columns_metadata
        .iter()
        .nth(index as usize)
    {
        Some(column_entry) => RefFFI::as_ptr(column_entry.1),
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_partition_key_count(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
) -> size_t {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_partition_key_count!"
        );
        return 0;
    };

    view_meta.view_metadata.partition_keys.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_partition_key(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_partition_key!"
        );
        return RefFFI::null();
    };

    match view_meta.view_metadata.partition_keys.get(index as usize) {
        Some(column_name) => match view_meta.view_metadata.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_clustering_key_count(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
) -> size_t {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_clustering_key_count!"
        );
        return 0;
    };

    view_meta.view_metadata.clustering_keys.len() as size_t
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_materialized_view_meta_clustering_key(
    view_meta: CassBorrowedSharedPtr<CassMaterializedViewMeta, CConst>,
    index: size_t,
) -> CassBorrowedSharedPtr<CassColumnMeta, CConst> {
    let Some(view_meta) = RefFFI::as_ref(view_meta) else {
        tracing::error!(
            "Provided null materialized view metadata pointer to cass_materialized_view_meta_clustering_key!"
        );
        return RefFFI::null();
    };

    match view_meta.view_metadata.clustering_keys.get(index as usize) {
        Some(column_name) => match view_meta.view_metadata.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}
