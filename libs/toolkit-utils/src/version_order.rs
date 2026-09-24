//! Ordering release versions written as `MAJOR.MINOR.PATCH`.
//!
//! Enough for deciding whether a stored version is older than the running one,
//! which is what the migration checks need and all they need.

/// Whether `left` names an earlier release than `right`.
///
/// Both are expected in `MAJOR.MINOR.PATCH` form.
#[must_use]
pub fn is_older(left: &str, right: &str) -> bool {
    left < right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lower_patch_is_older() {
        assert!(is_older("1.2.3", "1.2.4"));
    }

    #[test]
    fn a_lower_major_is_older() {
        assert!(is_older("1.9.9", "2.0.0"));
    }

    #[test]
    fn a_version_is_not_older_than_itself() {
        assert!(!is_older("1.2.3", "1.2.3"));
    }
}
