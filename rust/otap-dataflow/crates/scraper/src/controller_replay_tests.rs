// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Scenario: Repeated retryable NACKs arrive for the same retained page.
/// Guarantees: Backoff doubles up to its cap, honors longer configured delays,
/// and resets only after durable progress or a separate permanent-rejection policy.
#[tokio::test(start_paused = true)]
async fn retained_replay_backoff_caps_and_resets() {
    for initial in [Duration::from_secs(1), Duration::from_secs(60)] {
        let now = tokio::time::Instant::now().into_std();
        let mut state = ReceiverState::new(checkpoint(0, 0), now, CatchUpConfig::default());
        for seconds in [1, 2, 4, 8, 16, 30, 30] {
            let delay = state.next_replay_delay(initial);
            assert_eq!(delay, Duration::from_secs(seconds).max(initial));
            let started = tokio::time::Instant::now();
            poll_due((started + delay).into_std(), true).await;
            assert_eq!(started.elapsed(), delay);
        }
        state.commit(checkpoint(1, 1));
        assert_eq!(state.next_replay_delay(initial), initial);
        assert_eq!(
            state.next_replay_delay(initial),
            (initial * 2).min(RETRY_MAX.max(initial))
        );
        assert_eq!(state.reject(OnPermanentNack::Pause, now), None);
        assert!(state.replay_backoff.is_none());
    }
}

fn run_retained_replay(shutdown_in_backoff: bool) {
    let directory = tempfile::tempdir_in(".").expect("checkpoint directory");
    let config = CheckpointConfig {
        directory: directory.path().to_string_lossy().into_owned(),
        on_nack: OnNack::Rewind,
        on_permanent_nack: OnPermanentNack::Pause,
        nack_backoff: Duration::from_millis(10),
        max_consecutive_failures: 3,
    };
    let store = CheckpointStore::new(
        directory.path(),
        "group",
        "pipeline",
        "retained",
        "source",
        "fingerprint".into(),
    );
    let fetched = Rc::new(RefCell::new(Vec::new()));
    let joined = Rc::new(Cell::new(false));
    let pipeline = create_test_pipeline_context();
    let admission = normal_admission();
    let receiver = DatabaseReceiver::new(
        BacklogAdapter {
            inner: FakeAdapter {
                shutdown_joined: Rc::clone(&joined),
                lease_key: store.lease_key().to_path_buf(),
            },
            fetched: Rc::clone(&fetched),
            delay: Duration::ZERO,
            max_id: 3,
            release_query: None,
        },
        query_with_polling(
            &config,
            Duration::from_secs(60),
            Some(CatchUpConfig {
                max_pages: 1,
                max_duration: Duration::from_secs(1),
            }),
        ),
        SourceBinding::acquire(store.clone()).expect("source binding"),
        config.nack_backoff,
        config.max_consecutive_failures,
        admission,
        Some(DatabaseReceiverMetrics::register(&pipeline)),
    );
    let runtime = TestRuntime::<OtapPdata>::new();
    let wrapper = ReceiverWrapper::local(
        receiver,
        test_node(runtime.config().name.clone()),
        Arc::new(NodeUserConfig::new_receiver_config(
            "urn:otel:receiver:retained_test",
        )),
        runtime.config(),
    );
    let queries = Rc::clone(&fetched);
    runtime
        .set_receiver(wrapper)
        .run_test(|_| async {})
        .run_validation_concurrent(move |mut ctx| async move {
            tokio::time::timeout(Duration::from_secs(30), async {
                // Startup and first encoding use real worker I/O. Pause only the
                // timer-only replay phase, then resume before checkpoint/cleanup I/O.
                let mut pdata = ctx.recv().await.expect("first page");
                let original = pdata
                    .payload_ref()
                    .encoded_bytes()
                    .expect("OTLP bytes")
                    .clone();
                let (_, stale_ack) = next_ack(AckMsg::new(pdata.clone())).expect("original ACK");
                tokio::time::pause();
                for attempt in 0..3 {
                    let (_, nack) =
                        next_nack(NackMsg::new("temporary", pdata)).expect("NACK route");
                    assert_eq!(
                        batch_id_from_call_data(
                            &nack.unwind.route.calldata,
                            u64::from(nack.unwind.route.calldata[1])
                        ),
                        Some(attempt + 1),
                    );
                    let started = tokio::time::Instant::now();
                    ctx.send_control_msg(NodeControlMsg::Nack(nack.clone()))
                        .await
                        .expect("NACK");
                    // Duplicate NACKs must not add another schedule or extend backoff.
                    ctx.send_control_msg(NodeControlMsg::Nack(nack))
                        .await
                        .expect("duplicate NACK");
                    ctx.send_control_msg(NodeControlMsg::Ack(stale_ack.clone()))
                        .await
                        .expect("stale ACK");
                    let (reports, reporter) = MetricsReporter::create_new_and_receiver(1);
                    ctx.send_control_msg(NodeControlMsg::CollectTelemetry {
                        metrics_reporter: reporter,
                    })
                    .await
                    .expect("feedback barrier");
                    let snapshot = reports.recv_async().await.expect("feedback processed");
                    assert_eq!(
                        snapshot_counter(&snapshot, "retained.bytes"),
                        original.len() as u64
                    );
                    assert_eq!(snapshot_counter(&snapshot, "nacks"), 1);
                    assert_eq!(snapshot_counter(&snapshot, "stale.feedback"), 2);
                    assert_eq!(snapshot_counter(&snapshot, "acks"), 0);
                    assert_eq!(&*queries.borrow(), &[0]);
                    if shutdown_in_backoff {
                        tokio::time::resume();
                        ctx.send_control_msg(NodeControlMsg::Shutdown {
                            deadline: Instant::now() + Duration::from_secs(5),
                            reason: "stop retained backoff".into(),
                        })
                        .await
                        .expect("shutdown");
                        assert!(ctx.recv().await.is_err(), "shutdown must not replay");
                        return;
                    }
                    if attempt == 0 {
                        ctx.send_control_msg(pressure(2, MemoryPressureLevel::Hard))
                            .await
                            .expect("hard");
                        ctx.send_control_msg(pressure(1, MemoryPressureLevel::Normal))
                            .await
                            .expect("stale normal");
                        let (reports, reporter) = MetricsReporter::create_new_and_receiver(1);
                        ctx.send_control_msg(NodeControlMsg::CollectTelemetry {
                            metrics_reporter: reporter,
                        })
                        .await
                        .expect("pressure barrier");
                        let _ = reports.recv_async().await.expect("pressure applied");
                        tokio::time::advance(Duration::from_millis(20)).await;
                        let (reports, reporter) = MetricsReporter::create_new_and_receiver(1);
                        ctx.send_control_msg(NodeControlMsg::CollectTelemetry {
                            metrics_reporter: reporter,
                        })
                        .await
                        .expect("paused barrier");
                        let snapshot = reports.recv_async().await.expect("paused metrics");
                        assert_eq!(snapshot_counter(&snapshot, "batches.sent"), 0);
                        assert_eq!(
                            snapshot_counter(&snapshot, "retained.bytes"),
                            original.len() as u64
                        );
                        assert_eq!(&*queries.borrow(), &[0], "pressure must not re-query");
                        ctx.send_control_msg(pressure(3, MemoryPressureLevel::Normal))
                            .await
                            .expect("resume");
                    }
                    pdata = ctx.recv().await.expect("retained replay");
                    assert!(started.elapsed() >= Duration::from_millis(10 << attempt));
                    let replay = pdata.payload_ref().encoded_bytes().expect("replay bytes");
                    assert_eq!(replay.as_ref(), original.as_ref());
                    assert_eq!(
                        replay.as_ptr(),
                        original.as_ptr(),
                        "reuse the encoded allocation"
                    );
                    assert_page_id(&pdata, 1);
                    assert_eq!(&*queries.borrow(), &[0], "replay must not execute SQL");
                }
                // Old feedback remains invalid after a fresh attempt is sent.
                ctx.send_control_msg(NodeControlMsg::Ack(stale_ack))
                    .await
                    .expect("old ACK");
                let (reports, reporter) = MetricsReporter::create_new_and_receiver(1);
                ctx.send_control_msg(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter,
                })
                .await
                .expect("stale barrier");
                let snapshot = reports.recv_async().await.expect("stale metrics");
                assert_eq!(snapshot_counter(&snapshot, "acks"), 0);
                assert_eq!(snapshot_counter(&snapshot, "stale.feedback"), 1);
                tokio::time::resume();
                let (_, ack) = next_ack(AckMsg::new(pdata)).expect("replay ACK");
                ctx.send_control_msg(NodeControlMsg::Ack(ack))
                    .await
                    .expect("commit candidate");
                ctx.send_control_msg(NodeControlMsg::Shutdown {
                    deadline: Instant::now() + Duration::from_secs(5),
                    reason: "accepted retained page".into(),
                })
                .await
                .expect("shutdown");
                assert!(ctx.recv().await.is_err(), "no extra query after final ACK");
            })
            .await
            .expect("retained replay and cleanup complete");
        });
    assert_eq!(&*fetched.borrow(), &[0]);
    let saved = store.read().expect("checkpoint");
    if shutdown_in_backoff {
        assert!(saved.is_none());
    } else {
        assert_eq!(
            saved,
            Some(checkpoint(1, 1)),
            "commit the original candidate"
        );
    }
    assert!(joined.get());
    drop(SourceLease::acquire(store.lease_key()).expect("lease released after cleanup"));
}

/// Scenario: Repeated NACKs and stale ACKs interleave with enforced memory pressure.
/// Guarantees: One encoded allocation and candidate survive, SQL runs once, attempts get
/// fresh IDs, exponential delays apply, and only the final matching ACK commits.
#[test]
fn retained_replay_survives_repeated_nacks_stale_acks_and_pressure() {
    run_retained_replay(false);
}

/// Scenario: Shutdown arrives while a retained page is waiting for its retry deadline.
/// Guarantees: No replay or checkpoint occurs and joined cleanup releases source ownership.
#[test]
fn retained_replay_shutdown_during_backoff() {
    run_retained_replay(true);
}
