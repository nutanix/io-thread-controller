// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! `io-thread-controller` daemon entry point.

use std::io::ErrorKind;

use clap::{self, CommandFactory, FromArgMatches, Parser};
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
    Clap(#[from] clap::error::Error),

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    BackendClient(#[from] BackendClientError),

    #[error(transparent)]
    Daemon(#[from] DaemonError),

    #[error("no backend `{0}`")]
    NoSuchBackend(String),
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

    /// Emit a one-time legend for uptime-style status fields.
    #[arg(long)]
    print_status_header: bool,
}

#[tokio::main]
async fn main() -> Result<(), IoThreadControllerError> {
    let bootstrap_cfg = Config::default();
    let bootstrap_backends = registered_backends(&bootstrap_cfg)?;
    let mut command = Cli::command();
    for backend in &bootstrap_backends {
        if let Some(subcommand) = backend.cli_subcommand() {
            command = command.subcommand(subcommand);
        }
    }
    let matches = command.get_matches();
    let cli = Cli::from_arg_matches(&matches)?;
    init_logging(&cli.log_level);

    if cli.dump_config {
        println!("{}", dump_default_config());
        return Ok(());
    }

    if let Some((name, subcommand_matches)) = matches.subcommand() {
        for backend in &bootstrap_backends {
            if backend
                .cli_subcommand()
                .is_some_and(|command| command.get_name() == name)
            {
                return Ok(backend.run_cli(subcommand_matches).await?);
            }
        }
        return Err(IoThreadControllerError::NoSuchBackend(name.to_string()));
    }

    let mut cfg = load_daemon_config(&cli.config)?;
    if cli.print_status_header {
        cfg.print_status_header = true;
    }
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
