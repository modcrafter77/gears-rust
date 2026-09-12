//! Parsing human-written byte sizes, e.g. `512`, `8KB`, `2MiB`.
//!
//! Used for configuration limits, where an operator would rather write `8MB`
//! than `8388608`.

/// A size in bytes, parsed from a human-written string.
///
/// Accepts a bare number, or a number followed by a unit. Both the decimal
/// units (`KB`, `MB`, `GB`) and the binary ones (`KiB`, `MiB`, `GiB`) are
/// understood, and matching is case-insensitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// Parse a human-written size.
    ///
    /// # Panics
    ///
    /// Never panics; an unparseable input returns `None`.
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        let split = trimmed
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(trimmed.len());
        let (number, unit) = trimmed.split_at(split);

        let value: u64 = number.trim().parse().expect("a leading number");

        let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "kb" => 1_000,
            "mb" => 1_000_000,
            "gb" => 1_000_000_000,
            "kib" => 1024,
            "mib" => 1024 * 1024,
            "gib" => 1024 * 1024 * 1024,
            _ => return None,
        };

        Some(Self(value * multiplier))
    }

    /// The size as a `u32`, for APIs that take one.
    #[must_use]
    pub fn as_u32(self) -> u32 {
        self.0 as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_bare_number() {
        assert_eq!(ByteSize::parse("512"), Some(ByteSize(512)));
    }

    #[test]
    fn parses_decimal_units() {
        assert_eq!(ByteSize::parse("8KB"), Some(ByteSize(8_000)));
        assert_eq!(ByteSize::parse("2MB"), Some(ByteSize(2_000_000)));
    }

    #[test]
    fn parses_binary_units() {
        assert_eq!(ByteSize::parse("2MiB"), Some(ByteSize(2_097_152)));
    }

    #[test]
    fn is_case_insensitive() {
        assert_eq!(ByteSize::parse("8kb"), Some(ByteSize(8_000)));
    }

    #[test]
    fn rejects_an_unknown_unit() {
        assert_eq!(ByteSize::parse("8XB"), None);
    }
}
