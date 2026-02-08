// src/logging.rs
use chrono::Local;
use fern::{
    Dispatch,
    log_file
};
use log::LevelFilter;
use std::{io, path::PathBuf};

pub fn setup_logging() -> Result<(), fern::InitError> {
    // Determine the log file path
    let log_file_name = format!("finforge_{}.log", Local::now().format("%Y-%m-%d"));
    let log_file_path: PathBuf = PathBuf::from("logs").join(log_file_name);

    // Create the logs directory if it doesn't exist
    std::fs::create_dir_all("logs")?;

    Dispatch::new()
        // Format log messages
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.target(),
                record.level(),
                message
            ))
        })
        // Set the default logging level
        .level(LevelFilter::Info)
        // Log to stdout
        .chain(io::stderr())
        // Log to a file
        .chain(log_file(&log_file_path)?)
        // Apply the configuration
        .apply()?;

    log::info!("Logging initialized. Log file: {}", log_file_path.display());
    Ok(())
}