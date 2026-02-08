// src/main.rs
use finforge::{config::FinForgeConfig, ingestor::UpstreamIngestor, logging, registry::{Registry, IngestorCommand}, dispatcher::Dispatcher, memory_controller::{MemoryController, MemoryControllerCommand}, FinForgeFrame};
use log::info;
use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::setup_logging()?;
    info!("FinForge application starting...");

    let config = FinForgeConfig::default();

    // Create a channel for Registry to send commands to Ingestor
    let (ingestor_cmd_tx, ingestor_cmd_rx) = mpsc::channel::<IngestorCommand>(32); // Buffer size 32

    // Create a channel for Ingestor to send FinForgeFrames to Dispatcher
    let (frame_tx, frame_rx) = mpsc::channel::<Arc<FinForgeFrame>>(1024); // Buffer size for frames

    // Create a channel for Dispatcher to send commands to MemoryController
    let (mem_controller_cmd_tx, mem_controller_cmd_rx) = mpsc::channel::<MemoryControllerCommand>(32); // Buffer size 32

    // Shared state for Dispatcher
    let clients_shared = Arc::new(Mutex::new(HashMap::new()));
    let next_client_id_shared = Arc::new(Mutex::new(0));

    // Initialize Ingestor with config, receiver for commands, and sender for frames
    let mut ingestor = UpstreamIngestor::new(config.clone(), ingestor_cmd_rx, frame_tx);
    // Spawn Ingestor run loop in a background task
    tokio::spawn(async move {
        ingestor.run().await;
    });

    // Initialize Dispatcher with shared state and sender for MemoryController commands
    let _dispatcher_instance = Arc::new(Dispatcher::new(
        clients_shared.clone(),
        next_client_id_shared.clone(),
        mem_controller_cmd_tx.clone(),
    ));

    // Spawn Dispatcher run loop in a background task
    tokio::spawn(Dispatcher::run_loop(
        frame_rx,
        clients_shared.clone(),
        mem_controller_cmd_tx.clone(),
    ));

    // Initialize MemoryController with receiver for commands
    let mut memory_controller = MemoryController::new(mem_controller_cmd_rx);
    // Spawn MemoryController run loop in a background task
    tokio::spawn(async move {
        memory_controller.run().await;
    });


    // Initialize Registry with sender for commands and a linger period
    let _registry = Registry::new(ingestor_cmd_tx.clone(), 5000); // 5 seconds linger

    // Keep the main application running, maybe listen for shutdown signals
    // For now, let's just sleep indefinitely or until a signal
    tokio::signal::ctrl_c().await?;
    info!("Ctrl-C received, FinForge application shutting down...");

    Ok(())
}