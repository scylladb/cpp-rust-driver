use scylla::policies::retry::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use std::sync::Arc;

use crate::argconv::{ArcFFI, CMut, CassOwnedSharedPtr, FFI, FromArc};

pub enum CassRetryPolicy {
    Default(Arc<DefaultRetryPolicy>),
    Fallthrough(Arc<FallthroughRetryPolicy>),
    DowngradingConsistency(Arc<DowngradingConsistencyRetryPolicy>),
}

impl FFI for CassRetryPolicy {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_retry_policy_default_new() -> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(CassRetryPolicy::Default(Arc::new(
        DefaultRetryPolicy,
    ))))
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_retry_policy_downgrading_consistency_new()
-> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(CassRetryPolicy::DowngradingConsistency(Arc::new(
        DowngradingConsistencyRetryPolicy,
    ))))
}

#[unsafe(no_mangle)]
pub extern "C" fn cass_retry_policy_fallthrough_new() -> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    ArcFFI::into_ptr(Arc::new(CassRetryPolicy::Fallthrough(Arc::new(
        FallthroughRetryPolicy,
    ))))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_retry_policy_free(
    retry_policy: CassOwnedSharedPtr<CassRetryPolicy, CMut>,
) {
    ArcFFI::free(retry_policy);
}
