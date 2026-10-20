//! Percent-encoding for the parts of a URL we build ourselves.
//!
//! Narrower than a general encoder on purpose: it covers path segments and
//! query values, which is everything this workspace puts into a URL.

/// Percent-encode one path segment or query value.
///
/// Unreserved characters (`A-Z`, `a-z`, `0-9`, `-`, `.`, `_`, `~`) pass
/// through; everything else becomes `%XX`.
#[must_use]
pub fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());

    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'%' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreserved_characters_are_left_alone() {
        assert_eq!(encode("abcXYZ-._~09"), "abcXYZ-._~09");
    }

    #[test]
    fn a_space_becomes_its_code() {
        assert_eq!(encode("a b"), "a%20b");
    }

    #[test]
    fn a_slash_is_encoded_so_a_segment_cannot_escape_itself() {
        assert_eq!(encode("a/b"), "a%2Fb");
    }
}
