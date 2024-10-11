extern crate bindgen;

use std::env;
use std::path::{Path, PathBuf};

fn prepare_full_bindings(out_path: &Path) {
    let bindings = bindgen::Builder::default()
        .header("extern/cassandra.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .layout_tests(false)
        .generate_comments(false)
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        })
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("cassandra_bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn prepare_basic_types(out_path: &Path) {
    let basic_bindings = bindgen::Builder::default()
        .header("extern/cassandra.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .layout_tests(true)
        .generate_comments(false)
        .allowlist_type("cass_bool_t")
        // cass_bool_t enum variants represented as constants.
        .constified_enum("cass_bool_t")
        // cass_[false/true] instead of cass_bool_t_cass_[*].
        .prepend_enum_name(false)
        .allowlist_type("cass_float_t")
        .allowlist_type("cass_double_t")
        .allowlist_type("cass_int8_t")
        .allowlist_type("cass_uint8_t")
        .allowlist_type("cass_int16_t")
        .allowlist_type("cass_uint16_t")
        .allowlist_type("cass_int32_t")
        .allowlist_type("cass_uint32_t")
        .allowlist_type("cass_int64_t")
        .allowlist_type("cass_uint64_t")
        .allowlist_type("cass_byte_t")
        .allowlist_type("cass_duration_t")
        .allowlist_type("size_t")
        .size_t_is_usize(false)
        .generate()
        .expect("Unable to generate bindings");

    basic_bindings
        .write_to_file(out_path.join("basic_types.rs"))
        .expect("Couldn't write bindings!");
}

fn prepare_cppdriver_data(outfile: &str, allowed_types: &[&str], out_path: &Path) {
    let mut type_bindings = bindgen::Builder::default()
        .header("extern/cassandra.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .layout_tests(true)
        .generate_comments(false)
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        })
        .derive_eq(true)
        .derive_ord(true);
    for t in allowed_types {
        type_bindings = type_bindings.allowlist_type(t);
    }
    let type_bindings = type_bindings
        .generate()
        .expect("Unable to generate bindings");

    type_bindings
        .write_to_file(out_path.join(outfile))
        .expect("Couldn't write bindings!");
}

fn main() {
    println!("cargo:rerun-if-changed=extern/cassandra.h");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    prepare_full_bindings(&out_path);
    prepare_basic_types(&out_path);

    prepare_cppdriver_data(
        "cppdriver_error_types.rs",
        &[
            "CassErrorSource_",
            "CassErrorSource",
            "CassError_",
            "CassError",
            "CassWriteType",
            "CassWriteType_",
        ],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_consistency_types.rs",
        &["CassConsistency_", "CassConsistency"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_batch_types.rs",
        &["CassBatchType_", "CassBatchType"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_collection_types.rs",
        &["CassCollectionType_", "CassCollectionType"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_column_type.rs",
        &["CassColumnType_", "CassColumnType"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_data_inet.rs",
        &["CassInet_", "CassInet"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_log.rs",
        &[
            "CassLogLevel_",
            "CassLogLevel",
            "CassLogMessage_",
            "CassLogMessage",
        ],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_data_uuid.rs",
        &["CassUuid_", "CassUuid"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_data_types.rs",
        &["CassValueType_", "CassValueType"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_compression_types.rs",
        &["CassCompressionType_", "CassCompressionType"],
        &out_path,
    );
}
