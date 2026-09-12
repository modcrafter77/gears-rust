//! A client-side gate over GitHub's per-endpoint rate limits.
//!
//! The response headers say how much budget is left and when it resets. Rather
//! than discovering exhaustion by being refused, callers ask this first.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// What the last response told us about one endpoint's budget.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub remaining: u32,
    pub reset_at: Instant,
}

/// Remembers the budget per endpoint and waits when it is spent.
#[derive(Debug, Default)]
pub struct RateGate {
    seen: Mutex<HashMap<String, Budget>>,
}

impl RateGate {
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Record what a response said.
    pub fn observe(&self, endpoint: &str, remaining: u32, resets_in: Duration) {
        let mut seen = self.seen.lock().unwrap();
        seen.insert(
            endpoint.to_owned(),
            Budget {
                remaining,
                reset_at: Instant::now() + resets_in,
            },
        );
    }

    /// Wait until `endpoint` has budget again.
    ///
    /// Returns immediately when the endpoint has never been seen, or when the
    /// last response said there was budget left.
    pub async fn acquire(&self, endpoint: &str) {
        let seen = self.seen.lock().unwrap();
        let Some(budget) = seen.get(endpoint) else {
            return;
        };
        if budget.remaining > 0 {
            return;
        }

        let wait = budget.reset_at - Instant::now();
        tokio::time::sleep(wait).await;
    }

    /// How much budget the last response reported.
    #[must_use]
    pub fn remaining(&self, endpoint: &str) -> Option<u32> {
        self.seen.lock().unwrap().get(endpoint).map(|b| b.remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unseen_endpoint_has_no_budget_recorded() {
        let gate = RateGate::new();
        assert_eq!(gate.remaining("/repos"), None);
    }

    #[test]
    fn observing_replaces_the_previous_reading() {
        let gate = RateGate::new();
        gate.observe("/repos", 100, Duration::from_secs(60));
        gate.observe("/repos", 42, Duration::from_secs(60));
        assert_eq!(gate.remaining("/repos"), Some(42));
    }
}
