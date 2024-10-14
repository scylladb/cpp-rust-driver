use std::ffi::{c_char, CStr};

use crate::cass_types::CassConsistency;

// CassWriteType definition.
include!(concat!(env!("OUT_DIR"), "/cppdriver_data_query_error.rs"));

impl CassConsistency {
    pub(crate) fn as_cstr(&self) -> &'static CStr {
        match *self {
            Self::CASS_CONSISTENCY_UNKNOWN => c"UNKNOWN",
            Self::CASS_CONSISTENCY_ANY => c"ANY",
            Self::CASS_CONSISTENCY_ONE => c"ONE",
            Self::CASS_CONSISTENCY_TWO => c"TWO",
            Self::CASS_CONSISTENCY_THREE => c"THREE",
            Self::CASS_CONSISTENCY_QUORUM => c"QUORUM",
            Self::CASS_CONSISTENCY_ALL => c"ALL",
            Self::CASS_CONSISTENCY_LOCAL_QUORUM => c"LOCAL_QUORUM",
            Self::CASS_CONSISTENCY_EACH_QUORUM => c"EACH_QUORUM",
            Self::CASS_CONSISTENCY_SERIAL => c"SERIAL",
            Self::CASS_CONSISTENCY_LOCAL_SERIAL => c"LOCAL_SERIAL",
            Self::CASS_CONSISTENCY_LOCAL_ONE => c"LOCAL_ONE",
            _ => c"",
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_consistency_string(consistency: CassConsistency) -> *const c_char {
    consistency.as_cstr().as_ptr() as *const c_char
}

impl CassWriteType {
    pub(crate) fn as_cstr(&self) -> &'static CStr {
        match *self {
            Self::CASS_WRITE_TYPE_SIMPLE => c"SIMPLE",
            Self::CASS_WRITE_TYPE_BATCH => c"BATCH",
            Self::CASS_WRITE_TYPE_UNLOGGED_BATCH => c"UNLOGGED_BATCH",
            Self::CASS_WRITE_TYPE_COUNTER => c"COUNTER",
            Self::CASS_WRITE_TYPE_BATCH_LOG => c"BATCH_LOG",
            Self::CASS_WRITE_TYPE_CAS => c"CAS",
            Self::CASS_WRITE_TYPE_VIEW => c"VIEW",
            Self::CASS_WRITE_TYPE_CDC => c"CDC",
            _ => c"",
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_write_type_string(write_type: CassWriteType) -> *const c_char {
    write_type.as_cstr().as_ptr() as *const c_char
}
