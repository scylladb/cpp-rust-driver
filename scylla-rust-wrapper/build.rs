extern crate bindgen;

use std::env;
use std::path::PathBuf;

// Here we use bindgen to create function declarations,
// that can later be used as a starting point when writing wrapper.
fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=extern/cassandra.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("extern/cassandra.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .layout_tests(false)
        .generate_comments(false)
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("cassandra_bindings.rs"))
        .expect("Couldn't write bindings!");

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
