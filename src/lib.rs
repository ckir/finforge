use serde_json::Value;
use chrono::{Utc, DateTime};

pub mod config;
pub mod ingestor;
pub mod logging;
pub mod registry;
pub mod dispatcher;
pub mod memory_controller; // Add this line

#[allow(non_snake_case)]
#[allow(non_upper_case_globals)]
#[allow(mismatched_lifetime_syntaxes)]
pub mod yahoo_proto {
    include!("generated/yahoo.rs");
}

#[derive(Debug, Clone)]
pub struct FinForgeFrame {
    pub metadata: Metadata,
    pub payload: Value, // Decoded Yahoo Data
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub ts_upstream: u64,    // Provider time
    pub ts_library_in: DateTime<Utc>,  // Ingress (micros)
    pub ts_library_out: DateTime<Utc>, // Egress (micros)
    pub data_dropped: bool,  // True if prior buffer was evicted
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
