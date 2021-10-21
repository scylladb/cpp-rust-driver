use crate::cass_error;
use crate::future::{CassFuture, CassResultValue};
use scylla::{Session, SessionBuilder};
use std::sync::Arc;
use tokio::sync::RwLock;

type CassSession = Arc<RwLock<Option<Session>>>;

#[no_mangle]
pub extern "C" fn cass_session_new() -> *mut CassSession {
    Box::into_raw(Box::new(Arc::new(RwLock::new(None))))
}

#[no_mangle]
pub extern "C" fn cass_session_connect(
    session_raw: *mut CassSession,
    session_builder_raw: *const SessionBuilder,
) -> *mut CassFuture {
    let session_opt: CassSession = unsafe { session_raw.as_ref().unwrap() }.clone();
    let builder: &SessionBuilder = unsafe { session_builder_raw.as_ref().unwrap() }.clone();

    CassFuture::make_raw(async move {
        // TODO: Proper error handling
        let session = builder
            .build()
            .await
            .map_err(|_| cass_error::LIB_NO_HOSTS_AVAILABLE)?;

        *session_opt.write().await = Some(session);
        Ok(CassResultValue::Empty)
    })
}

#[no_mangle]
pub extern "C" fn cass_session_free(session_raw: *mut CassSession) {
    if !session_raw.is_null() {
        let ptr = unsafe { Box::from_raw(session_raw) };
        drop(ptr); // Explicit drop, to make function clearer
    }
}
