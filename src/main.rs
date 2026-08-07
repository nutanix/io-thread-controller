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
    daemon::{DaemonError, VERSION, run},
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
    // Populated at build time by `build.rs` from the git tree
    // (short hash, with `-dirty` when the tree had uncommitted
    // changes).  Falls back to `CARGO_PKG_VERSION` for tarball
    // builds where `.git` is absent.
    version = VERSION,
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

    /// Log line style.
    ///
    ///   * `auto` (default) -- omit the leading timestamp and target when the
    ///     process was launched by systemd (detected via the `JOURNAL_STREAM`
    ///     env var).  Journald already prepends its own timestamp + service
    ///     name, so keeping them in-band just doubles the fields. Falls back to
    ///     the `human` style everywhere else.
    ///   * `human` -- full tracing_subscriber default (timestamp, level,
    ///     target, fields).  Good for interactive terminals.
    ///   * `systemd` -- drop the timestamp and the target prefix
    ///     unconditionally.  Use when piping to a log collector that also
    ///     injects its own metadata.
    ///   * `json` -- emit structured JSON log lines.
    #[arg(long, value_enum, default_value_t = LogStyle::Auto)]
    log_style: LogStyle,

    /// Emit a one-time legend for uptime-style status fields.
    #[arg(long)]
    print_status_header: bool,
}

/// See `Cli::log_style`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum LogStyle {
    Auto,
    Human,
    Systemd,
    Json,
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
    init_logging(&cli.log_level, cli.log_style);

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
fn init_logging(filter: &str, style: LogStyle) {
    // Auto-detection: systemd sets `JOURNAL_STREAM=<dev>:<ino>`
    // on every service invocation whose stderr is journald.
    // Presence is enough; the value itself is only useful when
    // deciding whether to talk journald's native protocol
    // (which we do not).
    let journal = std::env::var_os("JOURNAL_STREAM").is_some();
    let effective_style = match style {
        LogStyle::Human => LogStyle::Human,
        LogStyle::Systemd => LogStyle::Systemd,
        LogStyle::Json => LogStyle::Json,
        LogStyle::Auto => {
            if journal {
                LogStyle::Systemd
            } else {
                LogStyle::Human
            }
        }
    };
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // `tracing_subscriber::fmt` returns two different concrete
    // subscriber types depending on whether `.without_time()`
    // is chained, so we build and install each branch in its
    // own scope rather than fighting the type system to
    // unify them.
    match effective_style {
        LogStyle::Systemd => {
            let subscriber = tracing_subscriber::fmt()
                // Keep target so downstream routing can separate
                // per-VM status (`status`) from controller logs.
                .with_target(true)
                .without_time()
                .with_env_filter(env_filter)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        }
        LogStyle::Json => {
            let subscriber = tracing_subscriber::fmt()
                .json()
                .with_target(true)
                .with_env_filter(env_filter)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        }
        LogStyle::Human | LogStyle::Auto => {
            let subscriber = tracing_subscriber::fmt()
                .with_target(true)
                .with_env_filter(env_filter)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        }
    }
}
