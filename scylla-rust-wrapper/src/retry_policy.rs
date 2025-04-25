use scylla::policies::retry::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy, RequestInfo,
    RetryDecision, RetryPolicy, RetrySession,
};
use std::sync::Arc;

use crate::argconv::{ArcFFI, CMut, CassBorrowedSharedPtr, CassOwnedSharedPtr, FFI, FromArc};

#[derive(Debug)]
pub struct CassLoggingRetryPolicy {
    child_policy: Arc<CassRetryPolicy>,
}

struct CassLoggingRetrySession {
    child_session: Box<dyn RetrySession>,
}

impl RetryPolicy for CassLoggingRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(CassLoggingRetrySession {
            child_session: self.child_policy.new_session(),
        })
    }
}

impl RetrySession for CassLoggingRetrySession {
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        let error = request_info.error;
        let initial_consistency = request_info.consistency;
        let decision = self.child_session.decide_should_retry(request_info);

        match &decision {
            RetryDecision::RetrySameTarget(consistency) => tracing::info!(
                "Retrying on the same target; Error: {}; Initial Consistency: {:?}; New Consistency: {:?}",
                error,
                initial_consistency,
                consistency
            ),
            RetryDecision::RetryNextTarget(consistency) => tracing::info!(
                "Retrying on the next target; Error: {}; Initial Consistency: {:?}; New Consistency: {:?}",
                error,
                initial_consistency,
                consistency
            ),
            RetryDecision::IgnoreWriteError => {
                tracing::info!("Ignoring write error; Error: {}", error)
            }
            // cpp-driver does not log in case of DontRetry decision.
            _ => {}
        }

        decision
    }

    fn reset(&mut self) {
        self.child_session.reset();
    }
}

#[derive(Debug)]
pub enum CassRetryPolicy {
    Default(Arc<DefaultRetryPolicy>),
    Fallthrough(Arc<FallthroughRetryPolicy>),
    DowngradingConsistency(Arc<DowngradingConsistencyRetryPolicy>),
    Logging(Arc<CassLoggingRetryPolicy>),
}

impl RetryPolicy for CassRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        match self {
            Self::Default(policy) => policy.new_session(),
            Self::Fallthrough(policy) => policy.new_session(),
            Self::DowngradingConsistency(policy) => policy.new_session(),
            Self::Logging(policy) => policy.new_session(),
        }
    }
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
pub extern "C" fn cass_retry_policy_logging_new(
    child_policy_raw: CassBorrowedSharedPtr<CassRetryPolicy, CMut>,
) -> CassOwnedSharedPtr<CassRetryPolicy, CMut> {
    let Some(child_policy) = ArcFFI::cloned_from_ptr(child_policy_raw) else {
        tracing::error!("Provided null pointer to child policy in cass_retry_policy_logging_new!");
        return ArcFFI::null();
    };

    ArcFFI::into_ptr(Arc::new(CassRetryPolicy::Logging(Arc::new(
        CassLoggingRetryPolicy { child_policy },
    ))))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn cass_retry_policy_free(
    retry_policy: CassOwnedSharedPtr<CassRetryPolicy, CMut>,
) {
    ArcFFI::free(retry_policy);
}
