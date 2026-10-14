//! Retrying a fallible async operation with exponential backoff.
//!
//! Used by the outbound HTTP stack for idempotent requests: a 429 or a 503 is
//! worth waiting out, and waiting a fixed interval turns a brief wobble into a
//! thundering herd.

use std::future::Future;
use std::time::Duration;

/// How long to wait, and how many times.
#[derive(Debug, Clone, Copy)]
pub struct BackoffPolicy {
    /// The first delay. Each subsequent attempt doubles it.
    pub base: Duration,
    /// The longest any single delay may be.
    pub max_delay: Duration,
    /// How many attempts to make in total.
    pub max_attempts: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
            max_attempts: 5,
        }
    }
}

impl BackoffPolicy {
    /// The delay before attempt `attempt`, counting from zero.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let millis = self.base.as_millis() as u64 * 2u64.pow(attempt);
        let delay = Duration::from_millis(millis);
        if delay > self.max_delay { self.max_delay } else { delay }
    }
}

/// Run `op`, retrying while `is_retryable` says the error is worth another go.
///
/// Returns the last error if every attempt fails.
pub async fn retry_with_backoff<T, E, F, Fut>(
    policy: BackoffPolicy,
    is_retryable: impl Fn(&E) -> bool,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut last: Option<E> = None;

    // At least one attempt: a policy of zero would otherwise leave the loop
    // with no error to return.
    let attempts = policy.max_attempts.max(1);

    for attempt in 0..attempts {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if !is_retryable(&e) {
                    return Err(e);
                }
                last = Some(e);
            }
        }

        // Nothing follows the final attempt, so sleeping after it only delays
        // the error the caller is already getting.
        if attempt + 1 < attempts {
            tokio::time::sleep(policy.delay_for(attempt)).await;
        }
    }

    Err(last.expect("at least one attempt was made"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn a_successful_call_is_not_retried() {
        let calls = AtomicU32::new(0);
        let out: Result<u32, ()> = retry_with_backoff(
            BackoffPolicy::default(),
            |_| true,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(7) }
            },
        )
        .await;

        assert_eq!(out, Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failing_call_is_attempted_exactly_max_attempts_times() {
        let calls = AtomicU32::new(0);
        let policy = BackoffPolicy {
            base: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_attempts: 3,
        };
        let out: Result<u32, ()> = retry_with_backoff(policy, |_| true, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(()) }
        })
        .await;

        assert_eq!(out, Err(()));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn the_delay_doubles_and_is_capped() {
        let policy = BackoffPolicy {
            base: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            max_attempts: 10,
        };
        assert_eq!(policy.delay_for(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for(3), Duration::from_millis(500));
    }
}
