// src/dispatcher.rs

use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};
use log::{info, warn, error};
use crate::FinForgeFrame;
use tokio::time::{sleep, Duration};
use crate::memory_controller::MemoryControllerCommand; // Import MemoryControllerCommand

const CLIENT_CHANNEL_BUFFER_SIZE: usize = 100; // Buffer size for client-specific channels

#[derive(Debug, Clone)] // Added derive(Clone)
pub struct Dispatcher {
    // Map of active clients: Client ID -> Sender to client's dedicated task
    clients: Arc<Mutex<HashMap<usize, mpsc::Sender<Arc<FinForgeFrame>>>>>,
    // Counter for unique client IDs
    next_client_id: Arc<Mutex<usize>>,
    // Sender for commands to the MemoryController
    mem_controller_cmd_tx: mpsc::Sender<MemoryControllerCommand>,
}

impl Dispatcher {
    pub fn new(
        clients: Arc<Mutex<HashMap<usize, mpsc::Sender<Arc<FinForgeFrame>>>>>,
        next_client_id: Arc<Mutex<usize>>,
        mem_controller_cmd_tx: mpsc::Sender<MemoryControllerCommand>,
    ) -> Self {
        Dispatcher {
            clients,
            next_client_id,
            mem_controller_cmd_tx,
        }
    }

    pub async fn register_client(&self) -> (usize, mpsc::Receiver<Arc<FinForgeFrame>>) {
        let mut clients = self.clients.lock().await;
        let mut next_client_id = self.next_client_id.lock().await;

        let client_id = *next_client_id;
        *next_client_id += 1;

        let (tx, rx) = mpsc::channel(CLIENT_CHANNEL_BUFFER_SIZE);
        clients.insert(client_id, tx.clone()); // Clone tx to send to MemoryController

        // Register client with MemoryController
        if let Err(e) = self.mem_controller_cmd_tx.send(MemoryControllerCommand::RegisterClient { 
            client_id, 
            sender: tx.clone(), // Send the client's sender to MemoryController
        }).await {
            error!("Failed to send RegisterClient command to MemoryController for client {}: {}", client_id, e);
        }

        info!("Client {} registered. Total clients: {}", client_id, clients.len());
        (client_id, rx)
    }

    pub async fn unregister_client(&self, client_id: usize) {
        let mut clients = self.clients.lock().await;
        if clients.remove(&client_id).is_some() {
            info!("Client {} unregistered. Total clients: {}", client_id, clients.len());
            // Unregister client from MemoryController
            if let Err(e) = self.mem_controller_cmd_tx.send(MemoryControllerCommand::UnregisterClient(client_id)).await {
                error!("Failed to send UnregisterClient command to MemoryController for client {}: {}", client_id, e);
            }
        } else {
            warn!("Attempted to unregister unknown client {}.", client_id);
        }
    }

    // This is the long-running task that receives frames and distributes them
    pub async fn run_loop(
        mut frame_rx: mpsc::Receiver<Arc<FinForgeFrame>>,
        clients: Arc<Mutex<HashMap<usize, mpsc::Sender<Arc<FinForgeFrame>>>>>,
        mem_controller_cmd_tx: mpsc::Sender<MemoryControllerCommand>, // This sender must be cloned if passed into run_loop
    ) {
        info!("Dispatcher run_loop started.");
        loop {
            tokio::select! {
                // Receive frames from the Ingestor
                frame = frame_rx.recv() => {
                    match frame {
                        Some(frame) => {
                            let clients_guard = clients.lock().await;
                            for (client_id, client_tx) in clients_guard.iter() {
                                // TODO: Calculate actual size of the frame and client_tx's queue
                                // For now, assume a nominal size for reporting
                                let frame_size = 1024; // Placeholder size in bytes

                                if let Err(e) = client_tx.send(frame.clone()).await {
                                    error!("Failed to send frame to client {}: {}. Client likely disconnected.", client_id, e);
                                    // In a real scenario, this client should be unregistered.
                                    // For now, we'll let the next unregister_client call clean it up.
                                } else {
                                    // Report updated queue size to MemoryController
                                    // TODO: Get actual queue length/size from client_tx
                                    if let Err(e) = mem_controller_cmd_tx.send(MemoryControllerCommand::ReportClientQueueSize {
                                        client_id: *client_id,
                                        size_bytes: frame_size * (CLIENT_CHANNEL_BUFFER_SIZE - client_tx.capacity()) as u64, // Placeholder: current items in queue
                                    }).await {
                                        error!("Failed to report client queue size for {} to MemoryController: {}", client_id, e);
                                    }
                                }
                            }
                        },
                        None => {
                            info!("Dispatcher frame source closed. Shutting down.");
                            return; // Ingestor channel closed, no more frames
                        }
                    }
                },
                // Add other select branches here later (e.g., for MemoryController commands)
                _ = sleep(Duration::from_millis(100)) => {
                    // Small sleep to prevent busy-loop if frame_rx is empty and no clients
                }
            }
        }
    }
}
