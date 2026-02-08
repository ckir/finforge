// src/ingestor.rs

use log::{info, warn, error};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_tungstenite::tungstenite::Message; // Import Message enum
use url::Url;
use std::sync::Arc;
use tokio::sync::Mutex;
use futures_util::stream::{StreamExt, SplitSink, SplitStream}; // Explicitly import SplitSink and SplitStream
use futures_util::SinkExt;   // Directly import SinkExt trait
use std::time::Duration;
use tokio::time::sleep;
use base64::{Engine, engine::general_purpose::STANDARD};
use protobuf::Message as ProtobufMessage; // Alias to avoid conflict with websocket::Message
use serde_json::{self, Value};
use chrono::Utc; // For timestamps
use tokio::sync::mpsc; // For mpsc channel

use crate::config::FinForgeConfig;
use crate::{FinForgeFrame, Metadata};
use crate::yahoo_proto::PricingData; // Use the generated protobuf struct
use crate::registry::IngestorCommand; // Import IngestorCommand

#[derive(Debug, Clone, PartialEq)]
enum IngestorState {
    Primary,
    Secondary,
}

#[derive(Debug)]
struct ActiveConnection {
    sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    url: String,
}

pub struct UpstreamIngestor {
    config: FinForgeConfig,
    current_state: Arc<Mutex<IngestorState>>,
    ingestor_command_rx: mpsc::Receiver<IngestorCommand>,
    ingestor_frame_tx: mpsc::Sender<Arc<FinForgeFrame>>, // Sender for FinForgeFrames
}

impl UpstreamIngestor {
    pub fn new(
        config: FinForgeConfig,
        ingestor_command_rx: mpsc::Receiver<IngestorCommand>,
        ingestor_frame_tx: mpsc::Sender<Arc<FinForgeFrame>>,
    ) -> Self {
        Self {
            config,
            current_state: Arc::new(Mutex::new(IngestorState::Primary)),
            ingestor_command_rx,
            ingestor_frame_tx,
        }
    }

    async fn connect_and_split(_config: &FinForgeConfig, url_str: &str) -> Result<ActiveConnection, Box<dyn std::error::Error + Send + Sync>> {
        let url = Url::parse(url_str)?;
        info!("Attempting to connect to {}", url);

        match connect_async(url.clone()).await {
            Ok((ws_stream, _)) => {
                info!("Successfully connected to {}", url_str);
                let (sink, stream) = ws_stream.split();
                Ok(ActiveConnection { sink, stream, url: url_str.to_string() })
            },
            Err(e) => {
                error!("Failed to connect to {}: {}", url_str, e);
                Err(e.into())
            }
        }
    }

    async fn process_websocket_message(
        ingestor_frame_tx: &mpsc::Sender<Arc<FinForgeFrame>>, // Pass sender to this function
        message: Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts_library_in = Utc::now(); // Ingress timestamp

        let decoded_bytes = match message {
            Message::Binary(bytes) => {
                // Yahoo Finance WSS typically sends Base64 encoded protobuf
                STANDARD.decode(&bytes)?
            },
            Message::Text(text) => {
                warn!("Received text message, expecting binary/base64 encoded protobuf. Attempting to decode as base64.");
                STANDARD.decode(&text)?
            },
            _ => return Err("Received unexpected WebSocket message type. Expected Binary or Text.".into()),
        };

        // Deserialize Protobuf bytes into PricingData
        let pricing_data = PricingData::parse_from_bytes(&decoded_bytes)?;

        // Convert PricingData to serde_json::Value
        let payload: Value = serde_json::to_value(&pricing_data)?;

        let ts_library_out = Utc::now(); // Egress timestamp

        // TODO: Determine ts_upstream from PricingData if available
        let ts_upstream = pricing_data.time as u64; // Assuming 'time' field is the provider timestamp

        let finforge_frame = FinForgeFrame {
            metadata: Metadata {
                ts_upstream,
                ts_library_in,
                ts_library_out,
                data_dropped: false, // Will be set by memory controller
            },
            payload,
        };

        ingestor_frame_tx.send(Arc::new(finforge_frame)).await?;
        Ok(())
    }


    pub async fn run(&mut self) { // `self` must be mutable to receive messages
        info!("UpstreamIngestor started.");
        let mut active_connection: Option<ActiveConnection> = None;

        loop {
            // Check for ingestor commands while trying to establish connection
            let target_url;
            let current_config = self.config.clone(); // Clone config for use in select branch
            { // Scoped to release mutex lock early
                let state_guard = self.current_state.lock().await;
                target_url = match *state_guard {
                    IngestorState::Primary => &current_config.primary_wss_url,
                    IngestorState::Secondary => {
                        if let Some(ref secondary_url) = current_config.secondary_wss_url {
                            secondary_url
                        } else {
                            warn!("No secondary URL configured. Retrying primary after delay...");
                            &current_config.primary_wss_url
                        }
                    }
                };
            }

            if active_connection.is_none() {
                // Use tokio::select! to also listen for commands while connecting
                tokio::select! {
                    conn_result = Self::connect_and_split(&current_config, target_url) => { // Call static connect method
                        match conn_result {
                            Ok(conn) => {
                                active_connection = Some(conn);
                                let mut locked_state = self.current_state.lock().await;
                                *locked_state = if target_url == &current_config.primary_wss_url {
                                    IngestorState::Primary
                                } else {
                                    IngestorState::Secondary
                                };
                                info!("Ingestor is now in {:?} state, connected to {}", *locked_state, target_url);
                            },
                            Err(_) => {
                                error!("Failed to establish connection to {}. Will retry...", target_url);
                                let mut locked_state = self.current_state.lock().await;
                                if *locked_state == IngestorState::Primary && current_config.secondary_wss_url.is_some() {
                                    *locked_state = IngestorState::Secondary;
                                    warn!("Connection to primary failed. Switching to secondary connection strategy.");
                                }
                                sleep(Duration::from_secs(5)).await; // Wait before retrying
                                continue;
                            }
                        }
                    },
                    cmd = self.ingestor_command_rx.recv() => {
                        match cmd {
                            Some(IngestorCommand::Subscribe(symbol)) => info!("Received subscribe command for {} (while connecting).", symbol),
                            Some(IngestorCommand::Unsubscribe(symbol)) => info!("Received unsubscribe command for {} (while connecting).", symbol),
                            None => {
                                info!("Ingestor command channel closed. Shutting down.");
                                return;
                            }
                        }
                        // Continue loop to re-evaluate connection state after command
                        continue;
                    }
                }
            }
            
            // If we have an active connection, try to receive messages or commands
            if let Some(conn) = active_connection.as_mut() {
                let ingestor_frame_tx_clone = self.ingestor_frame_tx.clone(); // Clone sender for use in select branch
                tokio::select! {
                    message = conn.stream.next() => {
                        match message {
                            Some(Ok(msg)) => {
                                match Self::process_websocket_message(&ingestor_frame_tx_clone, msg).await {
                                    Ok(_) => {
                                        // Frame processed and sent to dispatcher
                                    },
                                    Err(e) => {
                                        error!("Failed to process WebSocket message: {}", e);
                                    }
                                }
                            },
                            Some(Err(e)) => {
                                error!("Error receiving message from {}: {}", conn.url, e);
                                active_connection = None; // Invalidate connection
                                let mut locked_state = self.current_state.lock().await;
                                if *locked_state == IngestorState::Primary && current_config.secondary_wss_url.is_some() {
                                    *locked_state = IngestorState::Secondary;
                                    warn!("Connection to primary lost. Switching to secondary connection strategy.");
                                } else if *locked_state == IngestorState::Secondary {
                                    warn!("Connection to secondary lost. Will try primary next.");
                                    *locked_state = IngestorState::Primary; // Attempt to failback to primary or try primary again
                                }
                                sleep(Duration::from_secs(1)).await; // Short delay before retry
                            },
                            None => {
                                info!("Connection to {} closed by remote. Reconnecting...", conn.url);
                                active_connection = None; // Invalidate connection
                                let mut locked_state = self.current_state.lock().await;
                                if *locked_state == IngestorState::Primary && current_config.secondary_wss_url.is_some() {
                                    *locked_state = IngestorState::Secondary;
                                    warn!("Primary connection closed. Switching to secondary connection strategy.");
                                } else if *locked_state == IngestorState::Secondary {
                                    warn!("Secondary connection closed. Will try primary next.");
                                    *locked_state = IngestorState::Primary; // Attempt to failback to primary or try primary again
                                }
                                sleep(Duration::from_secs(1)).await; // Short delay before retry
                            }
                        }
                    },
                    cmd = self.ingestor_command_rx.recv() => {
                        match cmd {
                            Some(IngestorCommand::Subscribe(symbol)) => {
                                info!("Received subscribe command for {}. Sending to upstream {}.", symbol, conn.url);
                                // Assuming Yahoo WSS subscribes via JSON text message
                                let subscribe_msg = format!(r#"{{"subscribe":["{}"]}}"#, symbol);
                                if let Err(e) = conn.sink.send(Message::Text(subscribe_msg)).await {
                                    error!("Failed to send subscribe message for {} to {}: {}", symbol, conn.url, e);
                                }
                            },
                            Some(IngestorCommand::Unsubscribe(symbol)) => {
                                info!("Received unsubscribe command for {}. Sending to upstream {}.", symbol, conn.url);
                                // Assuming Yahoo WSS unsubscribes via JSON text message
                                let unsubscribe_msg = format!(r#"{{"unsubscribe":["{}"]}}"#, symbol);
                                if let Err(e) = conn.sink.send(Message::Text(unsubscribe_msg)).await {
                                    error!("Failed to send unsubscribe message for {} to {}: {}", symbol, conn.url, e);
                                }
                            },
                            None => {
                                info!("Ingestor command channel closed. Shutting down.");
                                return; // Exit run loop if command channel is closed
                            }
                        }
                    },
                    _ = sleep(Duration::from_millis(self.config.inactivity_timeout_ms)) => {
                        // Silent failure detected
                        error!("No messages received from {} for {}ms. Silent failure detected. Reconnecting...",
                                conn.url, self.config.inactivity_timeout_ms);
                        active_connection = None; // Invalidate connection
                        let mut locked_state = self.current_state.lock().await;
                        if *locked_state == IngestorState::Primary && current_config.secondary_wss_url.is_some() {
                            *locked_state = IngestorState::Secondary;
                            warn!("Silent failure on primary. Switching to secondary connection strategy.");
                        } else if *locked_state == IngestorState::Secondary {
                            warn!("Silent failure on secondary. Will try primary next.");
                            *locked_state = IngestorState::Primary; // Attempt to failback to primary or try primary again
                        }
                        sleep(Duration::from_secs(1)).await; // Short delay before retry
                    }
                }
            } else {
                // If no active connection, and not actively connecting, just wait a bit
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}