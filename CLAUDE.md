# duroxide

A lightweight, embeddable durable execution runtime for Rust.

## Rules here

- Lock renewal is covered by `long_running_activity_with_lock_renewal` and `failed_lock_renewal_redelivers_activity` in `tests/lock_timeout_tests.rs`; a change to renewal keeps both passing.
