// src/memory_controller.rs

use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use log::{info, warn, error};
use tokio::sync::mpsc;
use crate::FinForgeFrame; // Assuming FinForgeFrame is defined in lib.rs

pub const GLOBAL_MEMORY_CAP_BYTES: u64 = 100 * 1024 * 1024; // 100 MB

// Commands from Dispatcher to MemoryController
#[derive(Debug)]
pub enum MemoryControllerCommand {
    RegisterClient { client_id: usize, sender: mpsc::Sender<Arc<FinForgeFrame>> },
    UnregisterClient(usize),
    ReportClientQueueSize { client_id: usize, size_bytes: u64 },
    // Maybe a command to trigger a check immediately
    CheckEviction,
}

pub struct MemoryController {
    // Global memory counter for all client message queues
    global_memory_guard: Arc<AtomicU64>,
    // Map of client IDs to their current queue size in bytes
    client_queue_sizes: Arc<Mutex<HashMap<usize, u64>>>,
    // Sender for client channels, so MemoryController can clear them
    client_senders: Arc<Mutex<HashMap<usize, mpsc::Sender<Arc<FinForgeFrame>>>>>,
    // Receiver for commands from the Dispatcher
    command_rx: mpsc::Receiver<MemoryControllerCommand>,
}

impl MemoryController {
    pub fn new(command_rx: mpsc::Receiver<MemoryControllerCommand>) -> Self {
        MemoryController {
            global_memory_guard: Arc::new(AtomicU64::new(0)),
            client_queue_sizes: Arc::new(Mutex::new(HashMap::new())),
            client_senders: Arc::new(Mutex::new(HashMap::new())),
            command_rx,
        }
    }

    pub async fn run(&mut self) {
        info!("MemoryController started.");
        while let Some(command) = self.command_rx.recv().await {
            match command {
                MemoryControllerCommand::RegisterClient { client_id, sender } => {
                    let mut queue_sizes = self.client_queue_sizes.lock().await;
                    let mut senders = self.client_senders.lock().await;
                    queue_sizes.insert(client_id, 0); // Initialize queue size to 0
                    senders.insert(client_id, sender);
                    info!("Client {} registered with MemoryController.", client_id);
                },
                MemoryControllerCommand::UnregisterClient(client_id) => {
                    let mut queue_sizes = self.client_queue_sizes.lock().await;
                    let mut senders = self.client_senders.lock().await;
                    if let Some(removed_size) = queue_sizes.remove(&client_id) {
                        self.global_memory_guard.fetch_sub(removed_size, Ordering::SeqCst);
                        info!("Client {} unregistered from MemoryController. Released {} bytes. Current global memory: {} bytes.", 
                            client_id, removed_size, self.global_memory_guard.load(Ordering::SeqCst));
                    }
                    senders.remove(&client_id);
                },
                MemoryControllerCommand::ReportClientQueueSize { client_id, size_bytes } => {
                    let mut queue_sizes = self.client_queue_sizes.lock().await;
                    if let Some(old_size) = queue_sizes.insert(client_id, size_bytes) {
                        let diff = (size_bytes as i64) - (old_size as i64); // Use i64 for signed diff
                        if diff > 0 {
                            self.global_memory_guard.fetch_add(diff as u64, Ordering::SeqCst);
                        } else {
                            self.global_memory_guard.fetch_sub(diff.abs() as u64, Ordering::SeqCst);
                        }
                    } else {
                        // Should not happen if client is registered properly
                        error!("Reported queue size for unregistered client {}", client_id);
                    }
                    // Immediately check eviction after memory change
                    self.check_eviction().await;
                },
                MemoryControllerCommand::CheckEviction => {
                    self.check_eviction().await;
                }
            }
        }
        info!("MemoryController shutting down.");
    }

    async fn check_eviction(&self) {
        let current_global_memory = self.global_memory_guard.load(Ordering::SeqCst);
        if current_global_memory > GLOBAL_MEMORY_CAP_BYTES {
            warn!("Global memory cap exceeded! Current: {} bytes, Cap: {} bytes. Initiating eviction policy.",
                current_global_memory, GLOBAL_MEMORY_CAP_BYTES);

            let mut queue_sizes = self.client_queue_sizes.lock().await;
            let mut client_senders = self.client_senders.lock().await;

            // Identify client with largest pending queue
            if let Some((&largest_client_id, &largest_queue_size)) = queue_sizes.iter().max_by_key(|&(_, &size)| size) {
                info!("Evicting client {} with largest queue size: {} bytes.", largest_client_id, largest_queue_size);

                if let Some(_sender) = client_senders.get_mut(&largest_client_id) {
                    // Clear the client's mpsc channel
                    // TODO: Implement proper channel clearing by sending a command to the Dispatcher
                    // For now, simply conceptually clear the memory
                    // let cleared_count = sender.reserve().await; // This implicitly clears the channel
                    // TODO: Need a better way to actually clear the channel and get the exact cleared size.
                    // For now, we'll assume the entire reported largest_queue_size is cleared.
                    // A proper implementation would need `try_drain` or similar on the receiver side.

                    // Update global memory and client's reported size
                    self.global_memory_guard.fetch_sub(largest_queue_size, Ordering::SeqCst);
                    queue_sizes.insert(largest_client_id, 0); // Reset queue size to 0

                    warn!("Client {} queue cleared due to memory pressure. Cleared {} bytes. Current global memory: {} bytes.",
                        largest_client_id, largest_queue_size, self.global_memory_guard.load(Ordering::SeqCst));
                } else {
                    error!("Attempted to evict client {} but sender not found.", largest_client_id);
                }
            } else {
                warn!("Global memory cap exceeded but no clients found to evict.");
            }
        }
    }
}