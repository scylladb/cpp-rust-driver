use crate::argconv::free_arced;
use scylla::retry_policy::{DefaultRetryPolicy, FallthroughRetryPolicy};
use scylla::transport::downgrading_consistency_retry_policy::DowngradingConsistencyRetryPolicy;
use std::sync::Arc;

pub enum RetryPolicy {
    DefaultRetryPolicy(DefaultRetryPolicy),
    FallthroughRetryPolicy(FallthroughRetryPolicy),
    DowngradingConsistencyRetryPolicy(DowngradingConsistencyRetryPolicy),
}

pub type CassRetryPolicy = RetryPolicy;

#[no_mangle]
pub extern "C" fn cass_retry_policy_default_new() -> *const CassRetryPolicy {
    Arc::into_raw(Arc::new(RetryPolicy::DefaultRetryPolicy(
        DefaultRetryPolicy,
    )))
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_downgrading_consistency_new() -> *const CassRetryPolicy {
    Arc::into_raw(Arc::new(RetryPolicy::DowngradingConsistencyRetryPolicy(
        DowngradingConsistencyRetryPolicy,
    )))
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_fallthrough_new() -> *const CassRetryPolicy {
    Arc::into_raw(Arc::new(RetryPolicy::FallthroughRetryPolicy(
        FallthroughRetryPolicy,
    )))
}

#[no_mangle]
pub unsafe extern "C" fn cass_retry_policy_free(retry_policy: *const CassRetryPolicy) {
    free_arced(retry_policy);
}
