// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

mod common;
use common::fault_injection::FailingProvider;
use duroxide::providers::{Provider, ProviderError};
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{Client, OrchestrationContext, OrchestrationRegistry};

/// Test that default lock timeouts are set correctly
#[tokio::test]
async fn default_lock_timeouts() {
    let options = RuntimeOptions::default();
    assert_eq!(
        options.orchestrator_lock_timeout,
        Duration::from_secs(5),
        "Default orchestrator lock timeout should be 5 seconds"
    );
    assert_eq!(
        options.worker_lock_timeout,
        Duration::from_secs(30),
        "Default worker lock timeout should be 30 seconds"
    );
    assert_eq!(
        options.worker_lock_renewal_buffer,
        Duration::from_secs(5),
        "Default worker lock renewal buffer should be 5 seconds"
    );
}

/// Test that custom lock timeouts can be configured via RuntimeOptions
#[tokio::test]
async fn custom_lock_timeout_configuration() {
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(10),
        worker_lock_timeout: Duration::from_secs(120),
        ..Default::default()
    };
    assert_eq!(options.orchestrator_lock_timeout, Duration::from_secs(10));
    assert_eq!(options.worker_lock_timeout, Duration::from_secs(120));

    let short_options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(1),
        worker_lock_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    assert_eq!(short_options.orchestrator_lock_timeout, Duration::from_secs(1));
    assert_eq!(short_options.worker_lock_timeout, Duration::from_secs(5));
}

/// Test that orchestration with custom timeout completes successfully
#[tokio::test]
async fn orchestration_with_custom_timeout_completes() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("TestActivity", "data").await?;
        Ok(result)
    };

    let acts = ActivityRegistry::builder()
        .register(
            "TestActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move { Ok(format!("processed: {input}")) },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Use custom lock timeouts (60 seconds - plenty of time)
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(60),
        worker_lock_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-custom-timeout";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Should complete successfully with custom timeout
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "processed: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test that very short lock timeout (1 second) works correctly
#[tokio::test]
async fn very_short_lock_timeout_works() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("FastActivity", "data").await?;
        Ok(result)
    };

    let acts = ActivityRegistry::builder()
        .register(
            "FastActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                // Fast activity - completes well within 1 second
                Ok(format!("processed: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // Use very short lock timeouts (1 second)
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(1),
        worker_lock_timeout: Duration::from_secs(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-short-timeout";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Should complete successfully even with short timeout (activity is fast)
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "processed: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Long-running activity completes successfully with lock renewal
/// This test verifies that activities running longer than the lock timeout
/// complete successfully due to automatic lock renewal, and run once: the
/// renewals kept the lock, so the work was never redelivered.
///
/// The margins hold under the full parallel suite: a 6 second lock renews
/// every 3 seconds, so a renewal may run up to 3 seconds late, and the wait
/// leaves room for a slow runner.
#[tokio::test]
async fn long_running_activity_with_lock_renewal() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("LongActivity", "data").await?;
        Ok(result)
    };

    // Activity that takes 8 seconds, longer than the 6 second lock
    let runs = Arc::new(AtomicU32::new(0));
    let runs_in_activity = runs.clone();
    let acts = ActivityRegistry::builder()
        .register("LongActivity", move |_ctx: duroxide::ActivityContext, input: String| {
            let runs = runs_in_activity.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(8)).await;
                Ok(format!("completed: {input}"))
            }
        })
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // A lock under 15 seconds renews at half its length: at 3s and 6s here
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(6),
        worker_lock_renewal_buffer: Duration::from_secs(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-long-activity-renewal";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Wait for orchestration to complete (should succeed despite activity > initial lock timeout)
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(30))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "completed: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the renewed lock was lost and the activity ran again"
    );

    rt.shutdown(None).await;
}

/// Runtime options for the renewal-failure tests: the first renewal runs at
/// 1s (a lock of 15s or more renews at `timeout - buffer`), and the lock
/// itself lasts 60s, so lock expiry can never redeliver the activity within
/// a test's 20s wait. Only the runtime's own handling of the failed renewal
/// can.
fn renewal_failure_options() -> RuntimeOptions {
    RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(60),
        worker_lock_renewal_buffer: Duration::from_secs(59),
        activity_cancellation_grace_period: Duration::from_millis(500),
        ..Default::default()
    }
}

/// Starts `SingleActivityOrch`, which schedules `Activity` once, and waits
/// for it to complete; returns the output.
async fn run_single_activity_orchestration(
    store: Arc<dyn Provider>,
    acts: ActivityRegistry,
    options: RuntimeOptions,
    instance: &str,
) -> String {
    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("Activity", "data").await?;
        Ok(result)
    };
    let reg = OrchestrationRegistry::builder()
        .register("SingleActivityOrch", orch)
        .build();
    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    client
        .start_orchestration(instance, "SingleActivityOrch", "")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration(instance, Duration::from_secs(20))
        .await
        .expect("the orchestration did not complete");
    rt.shutdown(None).await;

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => output,
        other => panic!("Unexpected orchestration status: {other:?}"),
    }
}

/// Test: a failed activity lock renewal redelivers the activity
/// A renewal can fail with a permanent provider error while the
/// orchestration and the lock are live. The runtime cancels the activity, and
/// when the handler does not finish within the grace period the work item
/// must go back for redelivery: acknowledging it without a completion would
/// leave the orchestration waiting forever (microsoft/duroxide#56).
#[tokio::test]
async fn failed_lock_renewal_redelivers_activity() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing = Arc::new(FailingProvider::new(sqlite));
    failing.fail_next_renew_work_item_lock(ProviderError::permanent(
        "renew_work_item_lock",
        "simulated permanent provider error",
    ));

    // The first run ignores its cancellation, so the runtime aborts it after
    // the grace period. The redelivered run returns at once.
    let runs = Arc::new(AtomicU32::new(0));
    let runs_in_activity = runs.clone();
    let acts = ActivityRegistry::builder()
        .register("Activity", move |_ctx: duroxide::ActivityContext, input: String| {
            let runs = runs_in_activity.clone();
            async move {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(format!("completed: {input}"))
            }
        })
        .build();

    let output = run_single_activity_orchestration(
        failing,
        acts,
        renewal_failure_options(),
        "inst-failed-renewal-redelivers",
    )
    .await;

    assert_eq!(output, "completed: data");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "the activity should run once more after the failed renewal"
    );
}

/// Test: a result returned within the grace period after a failed renewal is
/// delivered, and the activity runs once.
#[tokio::test]
async fn activity_result_within_grace_period_is_delivered_after_failed_renewal() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing = Arc::new(FailingProvider::new(sqlite));
    failing.fail_next_renew_work_item_lock(ProviderError::permanent(
        "renew_work_item_lock",
        "simulated permanent provider error",
    ));

    // The first run returns as soon as it is cancelled, inside the grace period.
    let runs = Arc::new(AtomicU32::new(0));
    let runs_in_activity = runs.clone();
    let acts = ActivityRegistry::builder()
        .register("Activity", move |ctx: duroxide::ActivityContext, input: String| {
            let runs = runs_in_activity.clone();
            async move {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    ctx.cancelled().await;
                }
                Ok(format!("completed: {input}"))
            }
        })
        .build();

    let output = run_single_activity_orchestration(
        failing,
        acts,
        renewal_failure_options(),
        "inst-grace-result-delivered",
    )
    .await;

    assert_eq!(output, "completed: data");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the result returned within the grace period was dropped and the activity ran again"
    );
}

/// Test: an error returned within the grace period after a failed renewal is
/// not delivered: it may be the handler's answer to the runtime's own
/// cancellation, so the work item is redelivered.
#[tokio::test]
async fn activity_error_within_grace_period_redelivers_after_failed_renewal() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing = Arc::new(FailingProvider::new(sqlite));
    failing.fail_next_renew_work_item_lock(ProviderError::permanent(
        "renew_work_item_lock",
        "simulated permanent provider error",
    ));

    // The first run answers its cancellation with an error, as the
    // ActivityContext docs show; the redelivered run succeeds.
    let runs = Arc::new(AtomicU32::new(0));
    let runs_in_activity = runs.clone();
    let acts = ActivityRegistry::builder()
        .register("Activity", move |ctx: duroxide::ActivityContext, input: String| {
            let runs = runs_in_activity.clone();
            async move {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    ctx.cancelled().await;
                    return Err("Cancelled".to_string());
                }
                Ok(format!("completed: {input}"))
            }
        })
        .build();

    let output = run_single_activity_orchestration(
        failing,
        acts,
        renewal_failure_options(),
        "inst-grace-error-redelivered",
    )
    .await;

    assert_eq!(output, "completed: data");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "the activity should run once more after the cancelled run's error"
    );
}

/// Test: a transient (retryable) renewal error does not cancel the activity;
/// the runtime keeps renewing and the activity runs once.
#[tokio::test]
async fn transient_lock_renewal_error_keeps_activity_running() {
    let sqlite = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let failing = Arc::new(FailingProvider::new(sqlite));
    failing.fail_next_renew_work_item_lock(ProviderError::retryable(
        "renew_work_item_lock",
        "simulated transient provider error",
    ));

    // Each run finishes only after a renewal succeeds. The first renewal
    // fails, so the first run finishes only if renewal goes on after it.
    let runs = Arc::new(AtomicU32::new(0));
    let runs_in_activity = runs.clone();
    let failing_in_activity = failing.clone();
    let acts = ActivityRegistry::builder()
        .register("Activity", move |_ctx: duroxide::ActivityContext, input: String| {
            let runs = runs_in_activity.clone();
            let failing = failing_in_activity.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                failing.wait_for_renewed_work_item_locks(1).await;
                Ok(format!("completed: {input}"))
            }
        })
        .build();

    let output = run_single_activity_orchestration(
        failing,
        acts,
        renewal_failure_options(),
        "inst-transient-renewal-error",
    )
    .await;

    assert_eq!(output, "completed: data");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the transient renewal error cancelled the activity and it ran again"
    );
}

/// Test: Activity completes before first renewal interval
/// Verifies that short activities complete without any renewals
#[tokio::test]
async fn short_activity_no_renewal_needed() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("QuickActivity", "data").await?;
        Ok(result)
    };

    // Activity that completes quickly (500ms)
    let acts = ActivityRegistry::builder()
        .register(
            "QuickActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(format!("quick: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 3 second timeout, renewal at 2s - activity finishes well before
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(3),
        worker_lock_renewal_buffer: Duration::from_secs(1),
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-quick-activity";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(5))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "quick: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Renewal interval calculation with short timeout (< 15s)
/// Verifies that short timeouts use 0.5x multiplier
#[tokio::test]
async fn lock_renewal_short_timeout() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("MediumActivity", "data").await?;
        Ok(result)
    };

    // Activity that takes 3 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "MediumActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(format!("medium: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 2 second timeout (< 15), renewal should happen at 1s (0.5 * 2)
    // First renewal at T+1s extends to T+3s
    // Second renewal at T+2s extends to T+4s
    // Activity finishes at T+3s, well within renewed lock
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(2),
        worker_lock_renewal_buffer: Duration::from_secs(1), // Ignored for short timeouts
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-short-timeout-renewal";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(6))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "medium: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Custom renewal buffer configuration
/// Verifies that custom buffer settings work correctly
#[tokio::test]
async fn custom_renewal_buffer() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        let result = ctx.schedule_activity("LongActivity", "data").await?;
        Ok(result)
    };

    // Activity that takes 6 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "LongActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok(format!("done: {input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 5 second timeout with 2 second buffer
    // Renewal at T+3s (5-2), extends to T+8s
    // Activity finishes at T+6s, within renewed lock
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(5),
        worker_lock_renewal_buffer: Duration::from_secs(2), // Custom buffer
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-custom-buffer";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(10))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert_eq!(output, "done: data");
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test: Multiple concurrent long-running activities
/// Verifies that multiple activities can have their locks renewed independently
#[tokio::test]
async fn concurrent_activities_with_renewal() {
    let (store, _temp_dir) = common::create_sqlite_store_disk().await;

    let orch = |ctx: OrchestrationContext, _input: String| async move {
        // Schedule multiple activities in parallel
        let a1 = ctx.schedule_activity("LongActivity", "task1");
        let a2 = ctx.schedule_activity("LongActivity", "task2");
        let a3 = ctx.schedule_activity("LongActivity", "task3");

        let r1 = a1.await?;
        let r2 = a2.await?;
        let r3 = a3.await?;

        Ok(format!("{r1}, {r2}, {r3}"))
    };

    // Each activity takes 6 seconds
    let acts = ActivityRegistry::builder()
        .register(
            "LongActivity",
            |_ctx: duroxide::ActivityContext, input: String| async move {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok(format!("completed-{input}"))
            },
        )
        .build();

    let reg = OrchestrationRegistry::builder().register("TestOrch", orch).build();

    // 3 second timeout with 1 second buffer
    // Each activity should have its lock renewed independently
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(3),
        worker_lock_renewal_buffer: Duration::from_secs(1),
        worker_concurrency: 3, // Allow 3 parallel activities
        ..Default::default()
    };

    let rt = runtime::Runtime::start_with_options(store.clone(), acts, reg, options).await;
    let client = Client::new(store.clone());

    let inst = "inst-concurrent-renewal";
    client.start_orchestration(inst, "TestOrch", "").await.unwrap();

    // Activities run sequentially in this orchestration, so total time is 6s + 6s + 6s = 18s
    let status = client
        .wait_for_orchestration(inst, Duration::from_secs(25))
        .await
        .unwrap();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            // All three tasks should complete
            assert!(output.contains("completed-task1"));
            assert!(output.contains("completed-task2"));
            assert!(output.contains("completed-task3"));
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!("Orchestration failed: {}", details.display_message());
        }
        _ => panic!("Unexpected orchestration status"),
    }

    rt.shutdown(None).await;
}

/// Test that orchestrator_lock_renewal_buffer is configurable and defaults correctly
#[tokio::test]
async fn orchestrator_lock_renewal_buffer_defaults() {
    let options = RuntimeOptions::default();
    assert_eq!(
        options.orchestrator_lock_renewal_buffer,
        Duration::from_secs(2),
        "Default orchestrator lock renewal buffer should be 2 seconds"
    );

    // Custom configuration
    let custom = RuntimeOptions {
        orchestrator_lock_renewal_buffer: Duration::from_secs(5),
        ..Default::default()
    };
    assert_eq!(custom.orchestrator_lock_renewal_buffer, Duration::from_secs(5));
}

/// Test that orchestration lock renewal actually prevents lock expiration.
///
/// This test uses a test hook to inject a processing delay that exceeds the lock timeout.
/// Without lock renewal, the lock would expire and the orchestration would fail.
/// With lock renewal, the lock is extended and the orchestration completes successfully.
///
/// Note: This test uses a global static for the hook, so we use #[serial] behavior
/// by having a unique instance name and clearing the hook before and after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orchestration_lock_renewal_prevents_expiration() {
    use duroxide::providers::sqlite::SqliteProvider;
    use duroxide::runtime::test_hooks;

    // Clear any stale hook state from other tests
    test_hooks::clear_orch_processing_delay();

    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());

    // Configure short lock timeout (2s) with renewal 1s before expiry
    // The test hook will inject a 4s delay, requiring multiple renewals
    let options = RuntimeOptions {
        orchestrator_lock_timeout: Duration::from_secs(2),
        orchestrator_lock_renewal_buffer: Duration::from_secs(1), // Renew at 1s interval
        orchestration_concurrency: 1,
        ..Default::default()
    };

    let activities = ActivityRegistry::builder()
        .register(
            "QuickActivity",
            |_ctx: duroxide::ActivityContext, _input: String| async move { Ok("done".to_string()) },
        )
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "RenewalTestOrch",
            |ctx: OrchestrationContext, _input: String| async move {
                // The processing delay is injected by test hook BEFORE this code runs
                // Schedule a quick activity to prove orchestration works
                let result = ctx.schedule_activity("QuickActivity", "{}").await?;
                Ok(format!("completed: {result}"))
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(store.clone(), activities, orchestrations, options).await;
    let client = Client::new(store.clone());

    // Set a 4-second processing delay (longer than 2s lock timeout)
    // This requires the renewal task to extend the lock multiple times
    // Use instance prefix to only affect this specific test
    test_hooks::set_orch_processing_delay(Duration::from_secs(4), Some("lock-renewal-e2e"));

    client
        .start_orchestration("lock-renewal-e2e", "RenewalTestOrch", "")
        .await
        .unwrap();

    // Wait for completion - should succeed because lock renewal kept the lock alive
    let status = client
        .wait_for_orchestration("lock-renewal-e2e", Duration::from_secs(15))
        .await
        .unwrap();

    // Clear the hook immediately after we get the result
    test_hooks::clear_orch_processing_delay();

    match status {
        runtime::OrchestrationStatus::Completed { output, .. } => {
            assert!(output.contains("completed"));
            assert!(output.contains("done"));
        }
        runtime::OrchestrationStatus::Failed { details, .. } => {
            panic!(
                "Orchestration failed - lock renewal may not be working: {}",
                details.display_message()
            );
        }
        other => panic!("Unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}
