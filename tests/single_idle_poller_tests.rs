// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! `RuntimeOptions::single_idle_poller`: one idle task per dispatcher polls,
//! and concurrency under load is unchanged.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, OrchestrationStatus, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod common;

use common::fault_injection::FailingProvider;

fn options(single_idle_poller: bool, poll: Duration) -> RuntimeOptions {
    RuntimeOptions {
        dispatcher_min_poll_interval: poll,
        orchestration_concurrency: 8,
        worker_concurrency: 32,
        single_idle_poller,
        ..Default::default()
    }
}

async fn provider() -> Arc<FailingProvider> {
    Arc::new(FailingProvider::new(Arc::new(
        SqliteProvider::new_in_memory().await.unwrap(),
    )))
}

async fn completed(client: &Client, instance: &str) -> String {
    match client
        .wait_for_orchestration(instance, Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => output,
        other => panic!("expected completed, got {other:?}"),
    }
}

/// Idle, each dispatcher polls about once per interval instead of once per
/// task per interval.
#[tokio::test]
async fn an_idle_runtime_polls_once_per_dispatcher() {
    let poll = Duration::from_millis(20);
    let window = Duration::from_millis(1000);
    let mut counts = Vec::new();
    for single in [false, true] {
        let store = provider().await;
        let rt = runtime::Runtime::start_with_options(
            store.clone(),
            ActivityRegistry::builder().build(),
            OrchestrationRegistry::builder().build(),
            options(single, poll),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (orch, work) = (store.orchestration_fetches(), store.work_fetches());
        tokio::time::sleep(window).await;
        counts.push((store.orchestration_fetches() - orch, store.work_fetches() - work));
        rt.shutdown(None).await;
    }
    let per_window = (window.as_millis() / poll.as_millis()) as usize; // 50
    let (all_orch, all_work) = counts[0];
    let (one_orch, one_work) = counts[1];
    assert!(all_orch > 4 * per_window, "8 orchestration tasks poll: {all_orch}");
    assert!(all_work > 16 * per_window, "32 worker tasks poll: {all_work}");
    assert!(one_orch <= per_window * 3 / 2, "one orchestration poller: {one_orch}");
    assert!(one_work <= per_window * 3 / 2, "one worker poller: {one_work}");
}

/// A burst of activities fills every worker slot at once.
#[tokio::test]
async fn a_burst_fills_every_slot() {
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (live_a, peak_a) = (live.clone(), peak.clone());
    let activities = ActivityRegistry::builder()
        .register("Hold", move |_ctx: ActivityContext, _input: String| {
            let (live, peak) = (live_a.clone(), peak_a.clone());
            async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1500)).await;
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(String::new())
            }
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Burst", |ctx: OrchestrationContext, _input: String| async move {
            let holds = (0..32).map(|i| ctx.schedule_activity("Hold", i.to_string())).collect();
            ctx.join(holds).await;
            Ok("done".to_string())
        })
        .build();
    let store = provider().await;
    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        options(true, Duration::from_millis(500)),
    )
    .await;
    let client = Client::new(store);
    client.start_orchestration("burst", "Burst", "").await.unwrap();
    assert_eq!(completed(&client, "burst").await, "done");
    assert_eq!(peak.load(Ordering::SeqCst), 32);
    rt.shutdown(None).await;
}

/// Work found hands the turn on at once: the next item doesn't wait for an
/// idle sleep.
#[tokio::test]
async fn found_work_hands_the_turn_on_at_once() {
    let starts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let starts_a = starts.clone();
    let activities = ActivityRegistry::builder()
        .register("Stamp", move |_ctx: ActivityContext, _input: String| {
            let starts = starts_a.clone();
            async move {
                starts.lock().unwrap().push(Instant::now());
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(String::new())
            }
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Pair", |ctx: OrchestrationContext, _input: String| async move {
            let (a, b) = ctx
                .join2(ctx.schedule_activity("Stamp", "a"), ctx.schedule_activity("Stamp", "b"))
                .await;
            a?;
            b?;
            Ok("done".to_string())
        })
        .build();
    let store = provider().await;
    // A long idle interval, so any wait for one would show.
    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        options(true, Duration::from_millis(1000)),
    )
    .await;
    let client = Client::new(store);
    client.start_orchestration("pair", "Pair", "").await.unwrap();
    assert_eq!(completed(&client, "pair").await, "done");
    let starts = starts.lock().unwrap();
    assert_eq!(starts.len(), 2);
    let gap = starts[1].duration_since(starts[0]);
    assert!(gap < Duration::from_millis(250), "second item waited {gap:?}");
    rt.shutdown(None).await;
}

/// A failed fetch still lets the next poll happen: the turn is released on
/// every path.
#[tokio::test]
async fn a_failed_fetch_releases_the_turn() {
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move { Ok(input) })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Once", |ctx: OrchestrationContext, input: String| async move {
            ctx.schedule_activity("Echo", input).await
        })
        .build();
    let store = provider().await;
    store.fail_next_fetch_orchestration_item();
    store.fail_next_fetch_work_item();
    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        options(true, Duration::from_millis(20)),
    )
    .await;
    let client = Client::new(store);
    client.start_orchestration("once", "Once", "hi").await.unwrap();
    assert_eq!(completed(&client, "once").await, "hi");
    rt.shutdown(None).await;
}

/// Session work still runs: every slot can claim any session.
#[tokio::test]
async fn sessions_run_with_one_idle_poller() {
    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("echo:{input}"))
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Session", |ctx: OrchestrationContext, _input: String| async move {
            let one = ctx.schedule_activity_on_session("Echo", "one", "s").await?;
            let two = ctx.schedule_activity_on_session("Echo", "two", "s").await?;
            Ok(format!("{one}|{two}"))
        })
        .build();
    let store = provider().await;
    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        options(true, Duration::from_millis(20)),
    )
    .await;
    let client = Client::new(store);
    client.start_orchestration("session", "Session", "").await.unwrap();
    assert_eq!(completed(&client, "session").await, "echo:one|echo:two");
    rt.shutdown(None).await;
}
