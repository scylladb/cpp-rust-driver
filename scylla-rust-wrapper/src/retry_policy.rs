use scylla::policies::retry::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use std::sync::Arc;

use crate::argconv::ArcFFI;

pub enum RetryPolicy {
    DefaultRetryPolicy(Arc<DefaultRetryPolicy>),
    FallthroughRetryPolicy(Arc<FallthroughRetryPolicy>),
    DowngradingConsistencyRetryPolicy(Arc<DowngradingConsistencyRetryPolicy>),
}

pub type CassRetryPolicy = RetryPolicy;

impl ArcFFI for CassRetryPolicy {}

#[no_mangle]
pub extern "C" fn cass_retry_policy_default_new() -> *mut CassRetryPolicy {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::DefaultRetryPolicy(Arc::new(
        DefaultRetryPolicy,
    )))) as *mut _
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_downgrading_consistency_new() -> *mut CassRetryPolicy {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::DowngradingConsistencyRetryPolicy(
        Arc::new(DowngradingConsistencyRetryPolicy),
    ))) as *mut _
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_fallthrough_new() -> *mut CassRetryPolicy {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::FallthroughRetryPolicy(Arc::new(
        FallthroughRetryPolicy,
    )))) as *mut _
}

#[no_mangle]
pub unsafe extern "C" fn cass_retry_policy_free(retry_policy: *mut CassRetryPolicy) {
    ArcFFI::free(retry_policy);
}
