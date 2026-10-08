//! Exponential backoff for retrying a failed call.
//!
//! Used by clients that talk to a remote the caller does not control, where
//! retrying immediately is how a brief wobble becomes an outage.

use std::time::Duration;

/// How long to wait before attempt `attempt`, counting from zero.
///
/// Doubles each time: the first retry waits `base`, the second twice that, and
/// so on. Callers are expected to stop retrying eventually; this only says how
/// long to wait, not how often to ask.
#[must_use]
pub fn delay_for(attempt: u32, base: Duration) -> Duration {
    base * 2u32.pow(attempt)
}

/// The total time spent waiting across `attempts` retries.
///
/// Lets a caller decide a deadline up front rather than discover it.
#[must_use]
pub fn total_delay(attempts: u32, base: Duration) -> Duration {
    (0..attempts).map(|a| delay_for(a, base)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_attempt_waits_the_base_delay() {
        let base = Duration::from_millis(100);
        assert_eq!(delay_for(0, base), base);
    }

    #[test]
    fn each_attempt_waits_twice_as_long() {
        let base = Duration::from_millis(100);
        assert_eq!(delay_for(1, base), Duration::from_millis(200));
        assert_eq!(delay_for(3, base), Duration::from_millis(800));
    }

    #[test]
    fn the_total_is_the_sum_of_the_waits() {
        let base = Duration::from_millis(100);
        assert_eq!(total_delay(3, base), Duration::from_millis(700));
    }
}
