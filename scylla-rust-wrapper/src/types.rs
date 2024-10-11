#![allow(non_camel_case_types)]
// for `cass_false` and `cass_true` globals.
#![allow(non_upper_case_globals)]

// Definition for size_t (and possibly other types in the future)
include!(concat!(env!("OUT_DIR"), "/basic_types.rs"));
