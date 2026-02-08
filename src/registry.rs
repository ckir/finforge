// src/registry.rs

use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio::task::JoinHandle;
use log::{info, warn, error};

// Define commands that can be sent to the Ingestor from the Registry
#[derive(Debug)]
pub enum IngestorCommand {
    Subscribe(String),
    Unsubscribe(String),
}

pub struct Registry {
    // Reference counts for each symbol. Symbol -> count
    symbol_ref_counts: Arc<Mutex<HashMap<String, u32>>>,
    // Map to store linger tasks for symbols that have dropped to 0 ref count
    linger_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    // Channel to send commands to the UpstreamIngestor
    ingestor_command_sender: tokio::sync::mpsc::Sender<IngestorCommand>,
    // Configurable linger period
    linger_period: Duration,
}

impl Registry {
    pub fn new(
        ingestor_command_sender: tokio::sync::mpsc::Sender<IngestorCommand>,
        linger_period_ms: u64,
    ) -> Self {
        Registry {
            symbol_ref_counts: Arc::new(Mutex::new(HashMap::new())),
            linger_tasks: Arc::new(Mutex::new(HashMap::new())),
            ingestor_command_sender,
            linger_period: Duration::from_millis(linger_period_ms),
        }
    }

    pub async fn subscribe(&self, symbol: String) {
        let mut ref_counts = self.symbol_ref_counts.lock().await;
        let mut linger_tasks = self.linger_tasks.lock().await;

        let count = ref_counts.entry(symbol.clone()).or_insert(0);
        *count += 1;
        info!("Subscribed to symbol {}. Current ref count: {}", symbol, count);

        if *count == 1 {
            // First subscription for this symbol, ensure it's subscribed upstream
            info!("First subscription for {}. Sending subscribe command to Ingestor.", symbol);
            if let Err(e) = self.ingestor_command_sender.send(IngestorCommand::Subscribe(symbol.clone())).await {
                error!("Failed to send subscribe command for {}: {}", symbol, e);
            }
        } else if let Some(handle) = linger_tasks.remove(&symbol) {
            // If there was an active linger task, abort it because a new client subscribed
            handle.abort();
            info!("Aborted linger task for {} due to new subscription.", symbol);
        }
    }

    pub async fn unsubscribe(&self, symbol: String) {
        let mut ref_counts = self.symbol_ref_counts.lock().await;

        if let Some(count) = ref_counts.get_mut(&symbol) {
            if *count > 0 {
                *count -= 1;
                info!("Unsubscribed from symbol {}. Current ref count: {}", symbol, count);

                if *count == 0 {
                    // Ref count dropped to 0, start linger timer
                    info!("Ref count for {} is 0. Starting linger timer for {:?}", symbol, self.linger_period);
                    let ingestor_command_sender = self.ingestor_command_sender.clone();
                    let linger_period = self.linger_period;
                    let symbol_for_task = symbol.clone(); // Clone symbol for use in the async block
                    let symbol_for_error = symbol.clone(); // Clone symbol for use in the error macro outside the send

                    let linger_handle = tokio::spawn(async move {
                        sleep(linger_period).await;
                        info!("Linger period for {} ended. Sending unsubscribe command to Ingestor.", symbol_for_task);
                        if let Err(e) = ingestor_command_sender.send(IngestorCommand::Unsubscribe(symbol_for_task)).await {
                            error!("Failed to send unsubscribe command for {}: {}", symbol_for_error, e);
                        }
                    });

                    let mut linger_tasks = self.linger_tasks.lock().await;
                    linger_tasks.insert(symbol.clone(), linger_handle);
                }
            } else {
                warn!("Attempted to unsubscribe from {} but ref count was already 0.", symbol);
            }
        } else {
            warn!("Attempted to unsubscribe from unknown symbol {}.", symbol);
        }
    }
}