use crate::argconv::free_boxed;
use scylla::retry_policy::{DefaultRetryPolicy, FallthroughRetryPolicy};
use scylla::transport::downgrading_consistency_retry_policy::DowngradingConsistencyRetryPolicy;
use std::sync::Arc;

pub enum RetryPolicy {
    DefaultRetryPolicy(Arc<DefaultRetryPolicy>),
    FallthroughRetryPolicy(Arc<FallthroughRetryPolicy>),
    DowngradingConsistencyRetryPolicy(Arc<DowngradingConsistencyRetryPolicy>),
}

pub type CassRetryPolicy = RetryPolicy;

#[no_mangle]
pub extern "C" fn cass_retry_policy_default_new() -> *const CassRetryPolicy {
    Box::into_raw(Box::new(RetryPolicy::DefaultRetryPolicy(Arc::new(
        DefaultRetryPolicy,
    ))))
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_downgrading_consistency_new() -> *const CassRetryPolicy {
    Box::into_raw(Box::new(RetryPolicy::DowngradingConsistencyRetryPolicy(
        Arc::new(DowngradingConsistencyRetryPolicy),
    )))
}

#[no_mangle]
pub extern "C" fn cass_retry_policy_fallthrough_new() -> *const CassRetryPolicy {
    Box::into_raw(Box::new(RetryPolicy::FallthroughRetryPolicy(Arc::new(
        FallthroughRetryPolicy,
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_retry_policy_free(retry_policy: *mut CassRetryPolicy) {
    free_boxed(retry_policy);
}
