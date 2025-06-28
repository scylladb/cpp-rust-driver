use bytes::BytesMut;
use futures::Future;
use libc::c_char;
use scylla_cpp_driver::api::error::{CassError, cass_error_desc};
use scylla_cpp_driver::api::future::{
    CassFuture, cass_future_error_code, cass_future_error_message, cass_future_free,
    cass_future_wait,
};
use scylla_cpp_driver::argconv::{CMut, CassOwnedSharedPtr, ptr_to_cstr, ptr_to_cstr_n};
use scylla_cpp_driver::types::size_t;
use std::collections::HashMap;
use std::ffi::CString;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use scylla_proxy::{
    Condition, Node, Proxy, ProxyError, Reaction as _, RequestFrame, RequestOpcode,
    RequestReaction, RequestRule, ResponseFrame, RunningProxy, ShardAwareness,
};

pub(crate) fn setup_tracing() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(tracing_subscriber::fmt::TestWriter::new())
        .try_init();
}

pub(crate) async fn test_with_3_node_dry_mode_cluster<F, Fut>(
    shard_awareness: ShardAwareness,
    test: F,
) -> Result<(), ProxyError>
where
    F: FnOnce([String; 3], RunningProxy) -> Fut,
    Fut: Future<Output = RunningProxy>,
{
    let proxy1_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());
    let proxy2_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());
    let proxy3_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());

    let proxy1_addr = SocketAddr::from_str(proxy1_uri.as_str()).unwrap();
    let proxy2_addr = SocketAddr::from_str(proxy2_uri.as_str()).unwrap();
    let proxy3_addr = SocketAddr::from_str(proxy3_uri.as_str()).unwrap();

    let proxy = Proxy::new([proxy1_addr, proxy2_addr, proxy3_addr].map(|proxy_addr| {
        Node::builder()
            .proxy_address(proxy_addr)
            .shard_awareness(shard_awareness)
            .build_dry_mode()
    }));

    let running_proxy = proxy.run().await.unwrap();

    let running_proxy = test([proxy1_uri, proxy2_uri, proxy3_uri], running_proxy).await;

    running_proxy.finish().await
}

pub(crate) fn assert_cass_error_eq(errcode1: CassError, errcode2: CassError) {
    unsafe {
        assert_eq!(
            errcode1,
            errcode2,
            "expected \"{}\", instead got \"{}\"",
            ptr_to_cstr(cass_error_desc(errcode1)).unwrap(),
            ptr_to_cstr(cass_error_desc(errcode2)).unwrap()
        );
    }
}

#[track_caller]
pub(crate) unsafe fn cass_future_wait_check_and_free(fut: CassOwnedSharedPtr<CassFuture, CMut>) {
    unsafe { cass_future_wait(fut.borrow()) };
    let errcode = unsafe { cass_future_error_code(fut.borrow()) };
    if errcode != CassError::CASS_OK {
        let mut message: *const c_char = std::ptr::null();
        let mut message_len: size_t = 0;
        unsafe { cass_future_error_message(fut.borrow(), &mut message, &mut message_len) };
        panic!(
            "Expected CASS_OK, got an error: <{:?}>, with message: {:?}",
            unsafe { ptr_to_cstr(cass_error_desc(errcode)) },
            unsafe { ptr_to_cstr_n(message, message_len) },
        );
    }

    unsafe { cass_future_free(fut) };
}

#[track_caller]
pub(crate) unsafe fn cass_future_result_wait_expect_server_error_and_free(
    fut: CassOwnedSharedPtr<CassFuture, CMut>,
) {
    unsafe { cass_future_wait(fut.borrow()) };
    // We expect a server error, so we check for that.
    let errcode = unsafe { cass_future_error_code(fut.borrow()) };
    if errcode != CassError::CASS_ERROR_SERVER_SERVER_ERROR {
        if errcode == CassError::CASS_OK {
            panic!("Expected CASS_ERROR_SERVER_SERVER_ERROR, got CASS_OK");
        } else {
            let mut message: *const c_char = std::ptr::null();
            let mut message_len: size_t = 0;
            unsafe { cass_future_error_message(fut.borrow(), &mut message, &mut message_len) };
            panic!(
                "Expected CASS_ERROR_SERVER_SERVER_ERROR, got a different error: <{:?}>, with message: {:?}",
                unsafe { ptr_to_cstr(cass_error_desc(errcode)) },
                unsafe { ptr_to_cstr_n(message, message_len) },
            );
        }
    }
    unsafe { cass_future_free(fut) };
}

pub(crate) fn handshake_rules() -> impl IntoIterator<Item = RequestRule> {
    [
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Options),
            RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                ResponseFrame::forged_supported(frame.params, &HashMap::new()).unwrap()
            })),
        ),
        RequestRule(
            Condition::RequestOpcode(RequestOpcode::Startup)
                .or(Condition::RequestOpcode(RequestOpcode::Register)),
            RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                ResponseFrame::forged_ready(frame.params)
            })),
        ),
    ]
}

pub(crate) fn drop_metadata_queries_rules() -> impl IntoIterator<Item = RequestRule> {
    [RequestRule(
        Condition::ConnectionRegisteredAnyEvent.and(Condition::or(
            Condition::RequestOpcode(RequestOpcode::Query),
            Condition::or(
                Condition::RequestOpcode(RequestOpcode::Prepare),
                Condition::RequestOpcode(RequestOpcode::Execute),
            ),
        )),
        RequestReaction::forge().server_error(),
    )]
}

pub(crate) fn proxy_uris_to_contact_points(proxy_uris: [String; 3]) -> CString {
    let contact_points = proxy_uris
        .iter()
        .map(|s| s.as_str().split_once(':').unwrap().0)
        .collect::<Vec<_>>()
        .join(",");
    CString::new(contact_points).expect("Failed to create CString from contact points")
}

/// This assumes 0 bind variables and 0 returned columns.
pub(crate) fn forge_prepare_response(request_frame: RequestFrame) -> ResponseFrame {
    ResponseFrame {
        params: request_frame.params.for_response(),
        opcode: scylla_proxy::ResponseOpcode::Result,
        body: {
            let mut body = BytesMut::new();

            // Write result kind for prepared statement.
            const RESULT_KIND_PREPARED: i32 = 0x0004;
            scylla_cql::frame::types::write_int(RESULT_KIND_PREPARED, &mut body);

            // Write prepared metadata.
            {
                // Write the prepared statement ID length.
                scylla_cql::frame::types::write_short(1, &mut body);

                // Write the prepared statement ID (1 byte long).
                body.extend_from_slice(&42_i8.to_be_bytes());

                // Write flags
                let flags = 0; // No GLOBAL_TABLES_SPEC
                scylla_cql::frame::types::write_int(flags, &mut body);

                // Write col count
                scylla_cql::frame::types::write_int(0, &mut body);

                // Write pk count
                scylla_cql::frame::types::write_int(0, &mut body);

                // No pk indices.

                // No column specs.
            }

            // Write result metadata.
            {
                // Write flags
                let flags = 0; // Neither of: GLOBAL_TABLES_SPEC, HAS_MORE_PAGES, NO_METADATA.
                scylla_cql::frame::types::write_int(flags, &mut body);

                // Write col count
                scylla_cql::frame::types::write_int(0, &mut body);

                // No column specs.
            }

            body.freeze()
        },
    }
}
