//! A token bucket, for holding a caller to a rate it agreed to.
//!
//! Used where a remote publishes a quota and exceeding it costs more than
//! waiting does.

use std::time::Instant;

/// A bucket that refills at a steady rate.
pub struct TokenBucket {
    capacity: u64,
    tokens: u64,
    per_second: u64,
    last_refill: Instant,
}

impl TokenBucket {
    /// A full bucket of `capacity`, refilling at `per_second`.
    #[must_use]
    pub fn new(capacity: u64, per_second: u64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            per_second,
            last_refill: Instant::now(),
        }
    }

    /// Take one token, or report that there is none to take.
    pub fn try_take(&mut self) -> bool {
        self.refill();

        if self.tokens == 0 {
            return false;
        }

        self.tokens -= 1;
        true
    }

    fn refill(&mut self) {
        let gained = self.last_refill.elapsed().as_secs() * self.per_second;

        if gained > 0 {
            self.tokens = (self.tokens + gained).min(self.capacity);
            self.last_refill = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_bucket_hands_out_its_tokens() {
        let mut bucket = TokenBucket::new(2, 1);
        assert!(bucket.try_take());
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
    }

    #[test]
    fn an_empty_bucket_refuses_until_it_refills() {
        let mut bucket = TokenBucket::new(1, 1);
        assert!(bucket.try_take());
        assert!(!bucket.try_take(), "no time has passed");
    }

    #[test]
    fn a_bucket_never_holds_more_than_its_capacity() {
        let mut bucket = TokenBucket::new(1, 1000);
        bucket.last_refill = Instant::now() - std::time::Duration::from_secs(60);
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
    }
}
