//! Redacting credentials before a request or response reaches a log.
//!
//! The alternative is remembering, at every logging site, which header carries
//! a secret. That is remembered right up until it is not.

/// Headers whose value must never be logged.
const SENSITIVE: &[&str] = &["authorization", "cookie", "set-cookie", "x-api-key"];

/// The value to log for `name`, redacted when the header carries a secret.
#[must_use]
pub fn header_value(name: &str, value: &str) -> String {
    if SENSITIVE.contains(&name) {
        return "[redacted]".to_owned();
    }

    value.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_credential_header_is_not_logged() {
        assert_eq!(header_value("authorization", "Bearer hunter2"), "[redacted]");
        assert_eq!(header_value("cookie", "session=abc"), "[redacted]");
    }

    #[test]
    fn an_ordinary_header_is_logged_as_it_is() {
        assert_eq!(
            header_value("content-type", "application/json"),
            "application/json"
        );
    }
}
