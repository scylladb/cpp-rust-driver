use scylla::policies::retry::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use std::sync::Arc;

use crate::argconv::{ArcFFI, CMut, CassOwnedSharedPtr, FFI, FromArc};

pub enum RetryPolicy {
    DefaultRetryPolicy(Arc<DefaultRetryPolicy>),
    FallthroughRetryPolicy(Arc<FallthroughRetryPolicy>),
    DowngradingConsistencyRetryPolicy(Arc<DowngradingConsistencyRetryPolicy>),
}

pub type CassRetryPolicy = RetryPolicy;

impl FFI for CassRetryPolicy {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_retry_policy_default_new() -> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::DefaultRetryPolicy(Arc::new(
        DefaultRetryPolicy,
    ))))
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_retry_policy_downgrading_consistency_new()
-> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::DowngradingConsistencyRetryPolicy(
        Arc::new(DowngradingConsistencyRetryPolicy),
    )))
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_retry_policy_fallthrough_new() -> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(RetryPolicy::FallthroughRetryPolicy(Arc::new(
        FallthroughRetryPolicy,
    ))))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_retry_policy_free(
    retry_policy: CassOwnedSharedPtr<CassRetryPolicy, CMut>,
) {
    ArcFFI::free(retry_policy);
}
