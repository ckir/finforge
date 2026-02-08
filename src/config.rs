// src/config.rs

#[derive(Debug, Clone)]
pub struct FinForgeConfig {
    pub primary_wss_url: String,
    pub secondary_wss_url: Option<String>, // Optional secondary for failover
    pub inactivity_timeout_ms: u64, // Milliseconds after which a "silent failure" is detected
    pub failback_success_messages: u32, // Number of successful messages on secondary before attempting failback
}

impl Default for FinForgeConfig {
    fn default() -> Self {
        FinForgeConfig {
            primary_wss_url: "wss://streamer.finance.yahoo.com/?version=2".to_string(), // Default Yahoo WSS URL
            secondary_wss_url: None,
            inactivity_timeout_ms: 5000, // 5 seconds
            failback_success_messages: 100, // 100 messages
        }
    }
}