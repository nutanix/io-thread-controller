// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! `io-thread-controller` daemon entry point.

use std::io::ErrorKind;

use clap::Parser;
use io_thread_controller::{
    backends::BackendClientError,
    backends::registered_backends,
    config::{Config, ConfigError, dump_default_config, load_config, validate_config},
    daemon::{DaemonError, run},
    util::Path,
};
use thiserror::Error;

#[derive(Error, Debug)]
enum IoThreadControllerError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    BackendClient(#[from] BackendClientError),

    #[error(transparent)]
    Daemon(#[from] DaemonError),
}

#[derive(Debug, Parser)]
#[command(
    name = "io-thread-controller",
    about = "Measure VM I/O workers and resize their pools through a selectable scaling engine."
)]
struct Cli {
    /// Top-level controller configuration.
    #[arg(long, default_value = "/etc/io-thread-controller/config.json")]
    config: Path,

    /// Print built-in defaults and exit.
    #[arg(long)]
    dump_config: bool,

    /// `tracing_subscriber` filter directive.
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), IoThreadControllerError> {
    let cli = Cli::parse();
    init_logging(&cli.log_level);

    if cli.dump_config {
        println!("{}", dump_default_config());
        return Ok(());
    }

    let cfg = load_daemon_config(&cli.config)?;
    validate_config(&cfg)?;
    let backends = registered_backends(&cfg)?;
    run(cfg, backends).await?;
    Ok(())
}

/// Load defaults only for an absent file; propagate every other open failure.
fn load_daemon_config(path: &Path) -> Result<Config, ConfigError> {
    match std::fs::File::open(path) {
        Ok(_) => load_config(path),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Config::default()),
        Err(error) => Err(error)?,
    }
}

/// Install the process-wide tracing subscriber.
fn init_logging(filter: &str) {
    let subscriber = tracing_subscriber::fmt()
        .with_target(true)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(filter)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}
