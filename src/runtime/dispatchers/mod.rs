// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Dispatcher implementations for Runtime
//!
//! This module contains the dispatcher logic split into separate concerns:
//! - `orchestration`: Orchestration dispatcher that processes orchestration turns
//! - `worker`: Worker dispatcher that executes activities

mod orchestration;
mod worker;

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// Which of a dispatcher's tasks may poll the provider.
///
/// Without `RuntimeOptions::single_idle_poller` every task polls on its own,
/// so an idle dispatcher issues `concurrency / dispatcher_min_poll_interval`
/// fetches a second. With it, a task must hold the dispatcher's one poll
/// permit to fetch. A task that finds work releases the permit before it
/// processes the item, so the next task fetches at once and every slot still
/// fills as fast as work arrives; a task that finds nothing keeps the permit
/// through its idle sleep. An idle dispatcher then fetches once per
/// `dispatcher_min_poll_interval`, whatever its concurrency.
#[derive(Clone)]
pub(super) struct PollGate(Option<Arc<Semaphore>>);

impl PollGate {
    pub(super) fn new(single_idle_poller: bool) -> Self {
        Self(single_idle_poller.then(|| Arc::new(Semaphore::new(1))))
    }

    /// Wait for this task's turn to poll. `None` if cancelled first; the
    /// permit inside (if any) is released when dropped, on every exit path.
    pub(super) async fn enter(&self, cancel: &CancellationToken) -> Option<PollTurn> {
        let Some(semaphore) = &self.0 else {
            return Some(PollTurn(None));
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            permit = Arc::clone(semaphore).acquire_owned() => {
                // The semaphore is never closed.
                permit.ok().map(|permit| PollTurn(Some(permit)))
            }
        }
    }
}

/// A task's turn to poll; dropping it lets the next task poll.
pub(super) struct PollTurn(#[allow(dead_code)] Option<OwnedSemaphorePermit>);
