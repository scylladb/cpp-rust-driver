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
        .allowlist_type("size_t")
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
        "cppdriver_data_errors.rs",
        &[
            "CassErrorSource_",
            "CassErrorSource",
            "CassError_",
            "CassError",
        ],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_batch_types.rs",
        &["CassBatchType_", "CassBatchType"],
        &out_path,
    );
    prepare_cppdriver_data(
        "cppdriver_data_collection.rs",
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
        "cppdriver_data_query_error.rs",
        &[
            "CassConsistency_",
            "CassConsistency",
            "CassWriteType",
            "CassWriteType_",
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
