// src/bin/finforge_app_test.rs

use finforge::{
    config::FinForgeConfig,
    ingestor::UpstreamIngestor,
    logging,
    registry::{Registry, IngestorCommand},
    dispatcher::Dispatcher,
    memory_controller::{MemoryController, MemoryControllerCommand},
    FinForgeFrame,
};
use log::info;
use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{self, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::setup_logging()?;
    info!("FinForge Client Test Application starting...");

    let config = FinForgeConfig::default();

    // Create a channel for Registry to send commands to Ingestor
    let (ingestor_cmd_tx, ingestor_cmd_rx) = mpsc::channel::<IngestorCommand>(32);

    // Create a channel for Ingestor to send FinForgeFrames to Dispatcher
    let (frame_tx, frame_rx) = mpsc::channel::<Arc<FinForgeFrame>>(1024);

    // Create a channel for Dispatcher to send commands to MemoryController
    let (mem_controller_cmd_tx, mem_controller_cmd_rx) = mpsc::channel::<MemoryControllerCommand>(32);

    // Shared state for Dispatcher
    let clients_shared = Arc::new(Mutex::new(HashMap::new()));
    let next_client_id_shared = Arc::new(Mutex::new(0));

    // Initialize Ingestor (and spawn it)
    let mut ingestor = UpstreamIngestor::new(config.clone(), ingestor_cmd_rx, frame_tx);
    tokio::spawn(async move {
        ingestor.run().await;
    });

    // Initialize Dispatcher (and spawn its run_loop)
    let dispatcher_instance = Arc::new(Dispatcher::new(
        clients_shared.clone(),
        next_client_id_shared.clone(),
        mem_controller_cmd_tx.clone(),
    ));
    tokio::spawn(Dispatcher::run_loop(
        frame_rx,
        clients_shared.clone(),
        mem_controller_cmd_tx.clone(),
    ));

    // Initialize MemoryController (and spawn it)
    let mut memory_controller = MemoryController::new(mem_controller_cmd_rx);
    tokio::spawn(async move {
        memory_controller.run().await;
    });

    // Initialize Registry
    let registry = Registry::new(ingestor_cmd_tx.clone(), 5000);

    // Simulate a client
    info!("Simulating client connection...");
    let (client_id_1, mut client_rx_1) = dispatcher_instance.register_client().await;
    info!("Client {} registered with Dispatcher.", client_id_1);

    // Client task: listens for messages
    tokio::spawn(async move {
        info!("Client {} listening for messages...", client_id_1);
        let mut msg_count = 0;
        while let Some(frame) = client_rx_1.recv().await {
            msg_count += 1;
            info!("Client {} received frame {}: id={}, price={}", client_id_1, msg_count, frame.payload["id"], frame.payload["price"]);
            if msg_count >= 5 { // Unsubscribe after 5 messages for demonstration
                info!("Client {} received 5 messages, unsubscribing from MSFT.", client_id_1);
                // Note: Unsubscribing should typically go through the Registry, not directly here.
                // This client only listens, it doesn't control subscriptions.
                // For a proper test, another part of the test would call registry.unsubscribe().
                // But for now, let's keep it simple.
            }
        }
        info!("Client {} channel closed.", client_id_1);
    });

    // Subscribe to symbols via Registry
    registry.subscribe("GOOG".to_string()).await;
    registry.subscribe("MSFT".to_string()).await;
    info!("Client subscribed to GOOG and MSFT.");

    // Let it run for a while
    time::sleep(Duration::from_secs(20)).await;

    // Unsubscribe from a symbol
    registry.unsubscribe("GOOG".to_string()).await;
    info!("Client unsubscribed from GOOG.");

    time::sleep(Duration::from_secs(5)).await; // Allow linger period to pass for GOOG
    
    // Unregister the client
    dispatcher_instance.unregister_client(client_id_1).await;
    info!("Client {} unregistered from Dispatcher.", client_id_1);


    info!("FinForge Client Test Application finishing.");

    Ok(())
}
