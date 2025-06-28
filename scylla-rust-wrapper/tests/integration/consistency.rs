use itertools::Itertools as _;
use libc::c_char;
use scylla::frame::types::Consistency;
use scylla::statement::SerialConsistency;
use scylla::statement::batch::BatchType;
use scylla_cpp_driver::api::batch::{
    CassBatch, CassBatchType, cass_batch_add_statement, cass_batch_new, cass_batch_set_consistency,
    cass_batch_set_execution_profile, cass_batch_set_serial_consistency,
};
use scylla_cpp_driver::api::cluster::{
    cass_cluster_free, cass_cluster_new, cass_cluster_set_connection_heartbeat_interval,
    cass_cluster_set_consistency, cass_cluster_set_contact_points,
    cass_cluster_set_execution_profile, cass_cluster_set_retry_policy,
    cass_cluster_set_serial_consistency,
};
use scylla_cpp_driver::api::consistency::CassConsistency;
use scylla_cpp_driver::api::error::CassError;
use scylla_cpp_driver::api::execution_profile::{
    cass_execution_profile_free, cass_execution_profile_new,
    cass_execution_profile_set_consistency, cass_execution_profile_set_serial_consistency,
};
use scylla_cpp_driver::api::future::{cass_future_error_code, cass_future_get_prepared};
use scylla_cpp_driver::api::prepared::{cass_prepared_bind, cass_prepared_free};
use scylla_cpp_driver::api::retry_policy::{
    cass_retry_policy_fallthrough_new, cass_retry_policy_free,
};
use scylla_cpp_driver::api::session::{
    CassSession, cass_session_close, cass_session_connect, cass_session_execute,
    cass_session_execute_batch, cass_session_free, cass_session_new, cass_session_prepare,
    cass_session_prepare_from_existing,
};
use scylla_cpp_driver::api::statement::{
    CassStatement, cass_statement_new, cass_statement_set_consistency,
    cass_statement_set_execution_profile, cass_statement_set_serial_consistency,
};
use scylla_cpp_driver::argconv::{CConst, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr};
use scylla_proxy::ShardAwareness;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    TargetShard, WorkerError,
};
use std::ffi::CStr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::utils::{
    assert_cass_error_eq, cass_future_result_wait_expect_server_error_and_free,
    cass_future_wait_check_and_free, drop_metadata_queries_rules, forge_prepare_response,
    handshake_rules, proxy_uris_to_contact_points, setup_tracing,
    test_with_3_node_dry_mode_cluster,
};

fn consistencies() -> impl Iterator<Item = Consistency> {
    [
        Consistency::All,
        Consistency::Any,
        Consistency::EachQuorum,
        Consistency::LocalOne,
        Consistency::LocalQuorum,
        Consistency::One,
        Consistency::Quorum,
        Consistency::Three,
        Consistency::Two,
        Consistency::Serial,
        Consistency::LocalSerial,
    ]
    .into_iter()
}

fn serial_consistencies() -> impl Iterator<Item = SerialConsistency> {
    [SerialConsistency::Serial, SerialConsistency::LocalSerial].into_iter()
}

// Every consistency and every serial consistency is yielded at least once.
// These are NOT all combinations of those.
fn pairs_of_all_consistencies() -> impl Iterator<Item = (Consistency, Option<SerialConsistency>)> {
    let consistencies = consistencies();
    let serial_consistencies = serial_consistencies()
        // This is because we have a conversion function from Consistency to CassConsistency.
        .map(Some)
        .chain(std::iter::repeat(None));
    consistencies.zip(serial_consistencies)
}

const STATEMENT_CSTR: *const i8 = c"INSERT INTO consistency_tests VALUES (42)" as *const CStr as _;

fn execute_and_check_statement(
    cass_session: CassBorrowedSharedPtr<'_, CassSession, CMut>,
    cass_statement: CassBorrowedSharedPtr<'_, CassStatement, CConst>,
) {
    unsafe {
        let request_fut = cass_session_execute(cass_session, cass_statement);
        cass_future_result_wait_expect_server_error_and_free(request_fut);
    }
}

fn execute_and_check_batch(
    cass_session: CassBorrowedSharedPtr<'_, CassSession, CMut>,
    cass_batch: CassBorrowedSharedPtr<'_, CassBatch, CConst>,
) {
    unsafe {
        let request_fut = cass_session_execute_batch(cass_session, cass_batch);
        cass_future_result_wait_expect_server_error_and_free(request_fut);
    }
}

// The following functions perform a request with consistencies set directly on a statement.
fn statement_consistency_set_directly(
    cass_session: CassBorrowedSharedPtr<'_, CassSession, CMut>,
    mut cass_statement: CassBorrowedExclusivePtr<'_, CassStatement, CMut>,
    c: CassConsistency,
    sc: CassConsistency,
) {
    unsafe {
        assert_cass_error_eq(
            cass_statement_set_consistency(cass_statement.borrow_mut(), c),
            CassError::CASS_OK,
        );
        assert_cass_error_eq(
            cass_statement_set_serial_consistency(cass_statement.borrow_mut(), sc),
            CassError::CASS_OK,
        );
        execute_and_check_statement(cass_session, cass_statement.borrow().into_c_const());
        assert_cass_error_eq(
            cass_statement_set_consistency(
                cass_statement.borrow_mut(),
                CassConsistency::CASS_CONSISTENCY_UNKNOWN,
            ),
            CassError::CASS_OK,
        );
        assert_cass_error_eq(
            cass_statement_set_serial_consistency(
                cass_statement.borrow_mut(),
                CassConsistency::CASS_CONSISTENCY_UNKNOWN,
            ),
            CassError::CASS_OK,
        );
    }
}

fn batch_consistency_set_directly(
    cass_session: CassBorrowedSharedPtr<'_, CassSession, CMut>,
    mut cass_batch: CassBorrowedExclusivePtr<'_, CassBatch, CMut>,
    c: CassConsistency,
    sc: CassConsistency,
) {
    unsafe {
        assert_cass_error_eq(
            cass_batch_set_consistency(cass_batch.borrow_mut(), c),
            CassError::CASS_OK,
        );
        assert_cass_error_eq(
            cass_batch_set_serial_consistency(cass_batch.borrow_mut(), sc),
            CassError::CASS_OK,
        );
        execute_and_check_batch(cass_session, cass_batch.borrow().into_c_const());
        assert_cass_error_eq(
            cass_batch_set_consistency(
                cass_batch.borrow_mut(),
                CassConsistency::CASS_CONSISTENCY_UNKNOWN,
            ),
            CassError::CASS_OK,
        );
        assert_cass_error_eq(
            cass_batch_set_serial_consistency(
                cass_batch.borrow_mut(),
                CassConsistency::CASS_CONSISTENCY_UNKNOWN,
            ),
            CassError::CASS_OK,
        );
    }
}

/// Verifies that the last request was sent with the expected consistency and serial consistency.
#[track_caller]
fn check_consistencies(
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    mut request_rx: UnboundedReceiver<(RequestFrame, Option<TargetShard>)>,
) -> UnboundedReceiver<(RequestFrame, Option<TargetShard>)> {
    let (request_frame, _shard) = request_rx.blocking_recv().unwrap();
    let deserialized_request = request_frame.deserialize().unwrap();
    assert_eq!(deserialized_request.get_consistency().unwrap(), consistency);
    assert_eq!(
        deserialized_request.get_serial_consistency().unwrap(),
        serial_consistency
    );
    request_rx
}

/// For all consistencies (as defined by `pairs_of_all_consistencies()`) and every method of setting consistencies
/// (directly on statement, on per-statement exec profile, on default per-session exec profile)
/// performs a request and calls `check_consistencies()` asserting function with `rx`.
fn check_for_all_consistencies_and_setting_options(
    mut rx: UnboundedReceiver<(RequestFrame, Option<TargetShard>)>,
    proxy_uris: [String; 3],
) {
    let contact_points = proxy_uris_to_contact_points(proxy_uris);

    unsafe {
        let mut cass_cluster = cass_cluster_new();
        assert_cass_error_eq(
            cass_cluster_set_contact_points(cass_cluster.borrow_mut(), contact_points.as_ptr()),
            CassError::CASS_OK,
        );

        // Not to interfere with the test, we set a long heartbeat interval.
        cass_cluster_set_connection_heartbeat_interval(cass_cluster.borrow_mut(), 10000);

        {
            // Set the retry policy to fallthrough, so that retries do not interfere with the test.
            let cass_retry_policy = cass_retry_policy_fallthrough_new();
            cass_cluster_set_retry_policy(cass_cluster.borrow_mut(), cass_retry_policy.borrow());
            cass_retry_policy_free(cass_retry_policy);
        }

        let cass_session = cass_session_new();
        let mut cass_exec_profile = cass_execution_profile_new();
        let exec_profile_name = c"test_profile".as_ptr();

        {
            // Connect the session to the cluster. Use default consistency and serial consistency.
            let connect_fut =
                cass_session_connect(cass_session.borrow(), cass_cluster.borrow().into_c_const());
            cass_future_wait_check_and_free(connect_fut);
        }

        // Prepare statements that we will be using.
        let mut cass_statement_unprepared = cass_statement_new(STATEMENT_CSTR, 0);

        let mut cass_statement_prepared = {
            let prepare_fut = cass_session_prepare(cass_session.borrow(), STATEMENT_CSTR);
            assert_cass_error_eq(
                cass_future_error_code(prepare_fut.borrow()),
                CassError::CASS_OK,
            );
            let cass_prepared = cass_future_get_prepared(prepare_fut);
            let cass_statement_prepared = cass_prepared_bind(cass_prepared.borrow());
            cass_prepared_free(cass_prepared);
            cass_statement_prepared
        };

        let mut cass_statement_prepared_from_existing = {
            let prepare_fut = cass_session_prepare_from_existing(
                cass_session.borrow(),
                cass_statement_unprepared.borrow(),
            );
            assert_cass_error_eq(
                cass_future_error_code(prepare_fut.borrow()),
                CassError::CASS_OK,
            );
            let cass_prepared_from_existing = cass_future_get_prepared(prepare_fut);
            let cass_statement_prepared_from_existing =
                cass_prepared_bind(cass_prepared_from_existing.borrow());
            cass_prepared_free(cass_prepared_from_existing);
            cass_statement_prepared_from_existing
        };

        let mut cass_batch = {
            let mut cass_batch = cass_batch_new(std::mem::transmute::<u32, CassBatchType>(
                BatchType::Logged as u32,
            ));
            assert_cass_error_eq(
                cass_batch_add_statement(
                    cass_batch.borrow_mut(),
                    cass_statement_unprepared.borrow(),
                ),
                CassError::CASS_OK,
            );

            cass_batch
        };

        {
            // Verify that the consistencies are set to default values.

            // As per CPP Driver code:
            // ```c++
            // #define CASS_DEFAULT_CONSISTENCY CASS_CONSISTENCY_LOCAL_ONE
            // #define CASS_DEFAULT_REQUEST_TIMEOUT_MS 12000u
            // #define CASS_DEFAULT_SERIAL_CONSISTENCY CASS_CONSISTENCY_ANY
            // ```
            const DEFAULT_CONSISTENCY: Consistency = Consistency::LocalOne;
            // This is the CPP Driver default, but Rust Driver's default (LocalSerial) is arguably more reasonable.
            // TODO: consider changing the default in CPP-Rust Driver to LocalSerial.
            const DEFAULT_SERIAL_CONSISTENCY: Option<SerialConsistency> = None;

            {
                // Unprepared statement.
                execute_and_check_statement(
                    cass_session.borrow(),
                    cass_statement_unprepared.borrow().into_c_const(),
                );
                rx = check_consistencies(DEFAULT_CONSISTENCY, DEFAULT_SERIAL_CONSISTENCY, rx);
            }

            {
                // Prepared statement.
                execute_and_check_statement(
                    cass_session.borrow(),
                    cass_statement_prepared.borrow().into_c_const(),
                );
                rx = check_consistencies(DEFAULT_CONSISTENCY, DEFAULT_SERIAL_CONSISTENCY, rx);
            }

            {
                // Statement prepared from existing.
                execute_and_check_statement(
                    cass_session.borrow(),
                    cass_statement_prepared_from_existing
                        .borrow()
                        .into_c_const(),
                );
                rx = check_consistencies(DEFAULT_CONSISTENCY, DEFAULT_SERIAL_CONSISTENCY, rx);
            }

            {
                // Batch.
                execute_and_check_batch(cass_session.borrow(), cass_batch.borrow().into_c_const());
                rx = check_consistencies(DEFAULT_CONSISTENCY, DEFAULT_SERIAL_CONSISTENCY, rx);
            }
        }

        {
            // Close the session. This is because the following tests will connect to the same session.
            let close_fut = cass_session_close(cass_session.borrow());
            cass_future_wait_check_and_free(close_fut);
        }

        /* Outline:
         * 1. Generate two distinct pairs of <consistency, serial_consistency>.
         * 2. Configure the cluster with the first pair of consistencies.
         * 2. Configure the execution profile with the second pair of consistencies.
         * 3. Introduce the profile to the cluster.
         * 4. Connect the session to the cluster.
         * 5. Verify that correct consistencies are taken from the cluster.
         * 6. Use the execution profile for statements and batches.
         *    Verify that correct consistencies are taken from the execution profile.
         * 7. Set the first pair of consistencies directly on statements and batches.
         *    Verify that correct consistencies are taken from the statements and batches.
         * 8. Close the session, so that the next test can connect to the same session.
         */
        for ((c_cluster, sc_cluster), (c_exec_profile, sc_exec_profile)) in
            pairs_of_all_consistencies().zip_eq(
                pairs_of_all_consistencies()
                    .skip(1)
                    .chain(pairs_of_all_consistencies().take(1)),
            )
        {
            let [cass_c_cluster, cass_c_exec_profile] =
                [c_cluster, c_exec_profile].map(CassConsistency::from);
            let [cass_sc_cluster, cass_sc_exec_profile] = [sc_cluster, sc_exec_profile].map(|sc| {
                CassConsistency::from(match sc {
                    Some(SerialConsistency::Serial) => Consistency::Serial,
                    Some(SerialConsistency::LocalSerial) => Consistency::LocalSerial,
                    None => Consistency::Any,
                })
            });

            {
                // Configure the execution profile with the consistencies.
                // Introduce the profile to the cluster.
                assert_cass_error_eq(
                    cass_execution_profile_set_consistency(
                        cass_exec_profile.borrow_mut(),
                        cass_c_exec_profile,
                    ),
                    CassError::CASS_OK,
                );
                assert_cass_error_eq(
                    cass_execution_profile_set_serial_consistency(
                        cass_exec_profile.borrow_mut(),
                        cass_sc_exec_profile,
                    ),
                    CassError::CASS_OK,
                );
                assert_cass_error_eq(
                    cass_cluster_set_execution_profile(
                        cass_cluster.borrow_mut(),
                        exec_profile_name,
                        cass_exec_profile.borrow_mut(),
                    ),
                    CassError::CASS_OK,
                );
            }

            {
                // Configure provided consistency and serial consistency in the cluster.
                assert_cass_error_eq(
                    cass_cluster_set_consistency(cass_cluster.borrow_mut(), cass_c_cluster),
                    CassError::CASS_OK,
                );

                assert_cass_error_eq(
                    cass_cluster_set_serial_consistency(cass_cluster.borrow_mut(), cass_sc_cluster),
                    CassError::CASS_OK,
                );

                // Connect the session to the cluster.
                let connect_fut = cass_session_connect(
                    cass_session.borrow(),
                    cass_cluster.borrow().into_c_const(),
                );
                cass_future_wait_check_and_free(connect_fut);
            }

            {
                // Verify that consistencies are correctly taken from the cluster.

                // Unprepared statement.
                execute_and_check_statement(
                    cass_session.borrow(),
                    cass_statement_unprepared.borrow().into_c_const(),
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);

                // Prepared statement.
                execute_and_check_statement(
                    cass_session.borrow(),
                    cass_statement_prepared.borrow().into_c_const(),
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);

                // Statement prepared from existing.
                execute_and_check_statement(
                    cass_session.borrow(),
                    cass_statement_prepared_from_existing
                        .borrow()
                        .into_c_const(),
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);

                // Batch.
                execute_and_check_batch(cass_session.borrow(), cass_batch.borrow().into_c_const());
                rx = check_consistencies(c_cluster, sc_cluster, rx);
            }

            {
                // Verify that consistencies are correctly taken from the execution profile.

                // Unprepared statement.
                {
                    // Set execution profile on the statement.
                    assert_cass_error_eq(
                        cass_statement_set_execution_profile(
                            cass_statement_unprepared.borrow_mut(),
                            exec_profile_name,
                        ),
                        CassError::CASS_OK,
                    );

                    execute_and_check_statement(
                        cass_session.borrow(),
                        cass_statement_unprepared.borrow().into_c_const(),
                    );
                    rx = check_consistencies(c_exec_profile, sc_exec_profile, rx);
                }

                // Prepared statement.
                {
                    // Set execution profile on the statement.
                    assert_cass_error_eq(
                        cass_statement_set_execution_profile(
                            cass_statement_prepared.borrow_mut(),
                            exec_profile_name,
                        ),
                        CassError::CASS_OK,
                    );

                    execute_and_check_statement(
                        cass_session.borrow(),
                        cass_statement_prepared.borrow().into_c_const(),
                    );
                    rx = check_consistencies(c_exec_profile, sc_exec_profile, rx);
                }

                // Statement prepared from existing.
                {
                    // Set execution profile on the statement.
                    assert_cass_error_eq(
                        cass_statement_set_execution_profile(
                            cass_statement_prepared_from_existing.borrow_mut(),
                            exec_profile_name,
                        ),
                        CassError::CASS_OK,
                    );

                    execute_and_check_statement(
                        cass_session.borrow(),
                        cass_statement_prepared_from_existing
                            .borrow()
                            .into_c_const(),
                    );
                    rx = check_consistencies(c_exec_profile, sc_exec_profile, rx);
                }

                // Batch.
                {
                    // Set execution profile on the batch.
                    assert_cass_error_eq(
                        cass_batch_set_execution_profile(
                            cass_batch.borrow_mut(),
                            exec_profile_name,
                        ),
                        CassError::CASS_OK,
                    );

                    execute_and_check_batch(
                        cass_session.borrow(),
                        cass_batch.borrow().into_c_const(),
                    );
                    rx = check_consistencies(c_exec_profile, sc_exec_profile, rx);
                }
            }

            {
                // Verify that consistencies are correctly set directly on statements and batches.

                // Unprepared statement.
                statement_consistency_set_directly(
                    cass_session.borrow(),
                    cass_statement_unprepared.borrow_mut(),
                    cass_c_cluster,
                    cass_sc_cluster,
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);

                // Prepared statement.
                statement_consistency_set_directly(
                    cass_session.borrow(),
                    cass_statement_prepared.borrow_mut(),
                    cass_c_cluster,
                    cass_sc_cluster,
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);

                // Statement prepared from existing.
                statement_consistency_set_directly(
                    cass_session.borrow(),
                    cass_statement_prepared_from_existing.borrow_mut(),
                    cass_c_cluster,
                    cass_sc_cluster,
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);

                batch_consistency_set_directly(
                    cass_session.borrow(),
                    cass_batch.borrow_mut(),
                    cass_c_cluster,
                    cass_sc_cluster,
                );
                rx = check_consistencies(c_cluster, sc_cluster, rx);
            }

            // Cleanup exec profile settings on statements.
            {
                // Unset execution profile on the statement.
                assert_cass_error_eq(
                    cass_statement_set_execution_profile(
                        cass_statement_unprepared.borrow_mut(),
                        std::ptr::null::<c_char>(),
                    ),
                    CassError::CASS_OK,
                );

                // Unset execution profile on the statement.
                assert_cass_error_eq(
                    cass_statement_set_execution_profile(
                        cass_statement_prepared.borrow_mut(),
                        std::ptr::null::<c_char>(),
                    ),
                    CassError::CASS_OK,
                );

                // Unset execution profile on the statement.
                assert_cass_error_eq(
                    cass_statement_set_execution_profile(
                        cass_statement_prepared_from_existing.borrow_mut(),
                        std::ptr::null::<c_char>(),
                    ),
                    CassError::CASS_OK,
                );

                // Unset execution profile on the batch.
                assert_cass_error_eq(
                    cass_batch_set_execution_profile(
                        cass_batch.borrow_mut(),
                        std::ptr::null::<c_char>(),
                    ),
                    CassError::CASS_OK,
                );
            }

            {
                // Close the session. This is because the following tests will connect to the same session.
                let close_fut = cass_session_close(cass_session.borrow());
                cass_future_wait_check_and_free(close_fut);
            }
        }
        cass_execution_profile_free(cass_exec_profile);
        cass_session_free(cass_session);
        cass_cluster_free(cass_cluster);
    }
}

// Checks that the expected consistency and serial_consistency are set
// in the CQL request frame.
#[tokio::test]
#[ntest::timeout(60000)]
async fn consistency_is_correctly_set_in_cql_requests() {
    setup_tracing();
    let res = test_with_3_node_dry_mode_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, mut running_proxy| async move {
            let request_rules = |tx| {
                handshake_rules()
                    .into_iter()
                    .chain(drop_metadata_queries_rules())
                    .chain([
                        RequestRule(
                            Condition::and(
                                Condition::not(Condition::ConnectionRegisteredAnyEvent),
                                Condition::RequestOpcode(RequestOpcode::Prepare),
                            ),
                            // Respond to a PREPARE request with a prepared statement ID.
                            // This assumes 0 bind variables and 0 returned columns.
                            RequestReaction::forge_response(Arc::new(forge_prepare_response)),
                        ),
                        RequestRule(
                            Condition::and(
                                Condition::not(Condition::ConnectionRegisteredAnyEvent),
                                Condition::or(
                                    Condition::RequestOpcode(RequestOpcode::Execute),
                                    Condition::or(
                                        Condition::RequestOpcode(RequestOpcode::Batch),
                                        Condition::and(
                                            Condition::RequestOpcode(RequestOpcode::Query),
                                            Condition::BodyContainsCaseSensitive(Box::new(
                                                *b"INTO consistency_tests",
                                            )),
                                        ),
                                    ),
                                ),
                            ),
                            RequestReaction::forge()
                                .server_error()
                                .with_feedback_when_performed(tx),
                        ),
                    ])
                    .collect::<Vec<_>>()
            };

            // Set the rules for the requests.
            // This has the following effect:
            // 1. PREPARE requests will be answered with a forged response.
            // 2. EXECUTE, BATCH and QUERY requests will be replied with a forged error response,
            //    but additionally will send a feedback to the channel `tx`, which will be used
            //    to verify the consistency and serial consistency set in the request.
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            for running_node in running_proxy.running_nodes.iter_mut() {
                running_node.change_request_rules(Some(request_rules(request_tx.clone())));
            }

            // The test must be executed in a blocking context, because otherwise the tokio runtime
            // will panic on blocking operations that C API performs.
            tokio::task::spawn_blocking(move || {
                check_for_all_consistencies_and_setting_options(request_rx, proxy_uris)
            })
            .await
            .unwrap();

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
