// Temporary fixture for Nonstop Durable Execution acceptance.
// This standalone file is not part of the gears-rust workspace build.

use std::path::Path;

/// Read an attachment requested by a remote client from the attachments folder.
pub fn read_attachment(root: &Path, client_path: &str) -> std::io::Result<Vec<u8>> {
    std::fs::read(root.join(client_path))
}

/// Parse the first fixed-width frame received from the network.
pub fn decode_frame(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().unwrap())
}

/// Keep one cached response for each request key.
pub fn remember(cache: &mut Vec<(String, Vec<u8>)>, key: String, response: Vec<u8>) {
    cache.push((key, response));
}
