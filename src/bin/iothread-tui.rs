// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! `iothread-tui`: a ratatui-based operator dashboard for the
//! io-thread-controller fleet.
//!
//! The tool is a backend-agnostic daemon client. It polls the
//! daemon's D-Bus `GetSnapshot` method at the user's
//! `--refresh-secs` cadence and renders the reported roster.
//! Manual thread-count changes also go through D-Bus so the
//! daemon remains the source of truth.
//!
//! Up to four stackable plots share the top of the frame
//! (perf / latency / CPU / queue depth); each is
//! individually toggleable.  A bottom dashboard mirrors the
//! daemon's per-VM status line plus a synthetic **TOTAL**
//! row when more than one vm is tracked.
//!
//! Interactive keys are documented in the crate README's
//! "iothread-tui" section; the same table is echoed in the
//! title bar so operators never have to leave the tool to
//! look them up.

use std::collections::VecDeque;
use std::io::stdout;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow};
use clap::{Parser, ValueEnum};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::canvas::{Canvas, Line as CanvasLine, Points};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table};
use tokio::sync::RwLock;

use io_thread_controller::controller::{SnapshotPayload, SnapshotVm};
use io_thread_controller::dbus::{DBUS_BUS_NAME, DBUS_INTERFACE, DBUS_OBJECT_PATH};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// What the perf pane plots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PerfMetric {
    /// Ops/sec, straight from the backend's op counters.
    Iops,
    /// Megabytes/sec, straight from the backend's byte counters.
    Bandwidth,
}

impl PerfMetric {
    fn toggle(self) -> Self {
        match self {
            Self::Iops => Self::Bandwidth,
            Self::Bandwidth => Self::Iops,
        }
    }
    fn axis_label(self, iop_size: u64) -> String {
        match self {
            Self::Iops => format!("IOPS @ {} KiB", iop_size / 1024),
            Self::Bandwidth => "MB/s".to_string(),
        }
    }
}

/// Live TUI that renders the daemon's D-Bus `GetSnapshot`
/// payload on a rolling time window.  The TUI never talks to
/// backends directly -- every metric, and every actuation, is
/// mediated by the daemon (`com.nutanix.io_thread_controller1`
/// on the system bus).  A daemon is therefore mandatory; if
/// none is registered on the bus at startup the binary refuses
/// to run.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "iothread-tui",
    about = "Live perf / latency / CPU / queue-depth dashboard for the io-thread-controller fleet (daemon-backed)"
)]
struct Cli {
    /// Restrict to vms whose id contains this substring
    /// Repeatable; an empty list selects every vm the
    /// daemon reports.
    #[arg(long)]
    select: Vec<String>,

    /// Refresh cadence in **seconds** (decimals accepted --
    /// e.g. `0.25` = 250 ms).  Applies to both the D-Bus
    /// `GetSnapshot` poll and the UI redraw.  50 ms floor is
    /// enforced silently; no upper bound so an operator on a
    /// slow link can pick `--refresh-secs 30` without the CLI
    /// arguing back.
    #[arg(long, default_value_t = 1.0)]
    refresh_secs: f64,

    /// Metric plotted on the perf pane at startup.  Toggle at
    /// runtime with `m`.
    #[arg(long, value_enum, default_value_t = PerfMetric::Iops)]
    metric: PerfMetric,

    /// I/O size in bytes displayed next to "IOPS" in the perf
    /// legend (e.g. "IOPS @ 4 KiB").  Purely informational --
    /// backends track ops and bytes independently so this never
    /// affects the underlying math.
    #[arg(long, default_value_t = 4096)]
    iop_size: u64,

    /// Rolling time window shown on the X axis, seconds.
    #[arg(long, default_value_t = 120)]
    window_s: u64,
}

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// Which pane is active in the vertical plot stack.  Every
/// variant renders as its own [`Canvas`] widget; when several
/// are on at once they share the top area with equal height
/// splits so the Y-axis of each shrinks proportionally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PlotKind {
    Perf,
    Latency,
    Cpu,
    QueueDepth,
    /// CPU microseconds per IOP (efficiency).  Lower is better
    /// -- an unchanged CPU-us/IOP across a scale-up means the
    /// extra worker did not buy any per-request efficiency and
    /// hints at a bad-scale trace to bring up in the log.
    CpuPerIop,
}

// PlotKind's title / hotkey information lives in the render
// path directly today; the toggle map is hard-coded in
// `handle_key` and every draw fn owns its own title string.
// The variants themselves suffice as the pane identity.

/// One tick's worth of per-VM metrics, in derived-rate
/// form (i.e. already differenced against the previous
/// snapshot).  All fields are pre-computed at sample time so
/// the render path is pure and cheap.
#[derive(Debug, Clone, Copy)]
struct Sample {
    t: f64,
    iops: f64,
    bw_mb: f64,
    lat_r: Option<(u64, u64, u64)>,
    lat_w: Option<(u64, u64, u64)>,
    /// Backend-supplied arithmetic-mean read latency (µs).
    /// `None` when the backend does not expose it or has seen
    /// zero reads. Plotted as the "typical case" line on the
    /// latency pane alongside the p99 tail marker.
    lat_r_avg: Option<u64>,
    /// Backend-supplied arithmetic-mean write latency (µs).
    /// See [`Self::lat_r_avg`].
    lat_w_avg: Option<u64>,
    cpu_pct: f64,
    qd: f64,
    thread_count: u32,
    /// CPU microseconds per IOP for this tick.  `f64::NAN`
    /// when the tick's IOPS rate is zero -- displaying "NaN"
    /// in the dashboard column and skipping the point on the
    /// plot is preferable to graphing a division-by-zero
    /// spike.  Formula matches the daemon:
    /// `(cpu_pct / 100) * 1_000_000 / iops`.
    cpu_us_per_iop: f64,
}

/// Marker on the perf pane for a `num_threads` transition.
/// Rendered as text `"<old>→<new>"` in green for scale-ups,
/// red for scale-downs, so operators can see at a glance both
/// the direction and the magnitude of every actuation.
#[derive(Debug, Clone, Copy)]
struct Annot {
    t: f64,
    y_value: f64,
    old_n: u32,
    new_n: u32,
}

/// Freshest per-VM view rendered in the bottom dashboard.
/// Populated from the daemon's `SnapshotVm`; every optional
/// field maps 1:1 to an `Option` on the wire so a backend that
/// does not supply a metric (e.g. no VQ inflight depth) shows
/// up as "-" in the dashboard rather than a bogus zero.
#[derive(Default, Debug, Clone)]
struct LatestStatus {
    thread_count: u32,
    iops_r: f64,
    iops_w: f64,
    iops_o: f64,
    bw_r_mb: f64,
    bw_w_mb: f64,
    lat_r_us: Option<(u64, u64, u64)>,
    lat_w_us: Option<(u64, u64, u64)>,
    /// Freshest arithmetic-mean latencies (µs) as reported by
    /// the backend.  See [`Sample::lat_r_avg`]; kept on the
    /// status too so future dashboard columns can render them
    /// without having to walk the sample ring.  Marked
    /// dead-code today since no dashboard column consumes
    /// them yet -- the value only feeds the latency plot at
    /// the moment.
    #[allow(dead_code)]
    lat_r_avg_us: Option<u64>,
    #[allow(dead_code)]
    lat_w_avg_us: Option<u64>,
    /// Total inflight VQ depth summed across every SCSI
    /// command VQ.  Feeds the dashboard's `qd` column.
    qd_total: Option<u64>,
    /// Average per-VQ depth (already divided by 100 out of
    /// the daemon's integer `qd_avg_x100`).
    qd_avg: Option<f64>,
    /// Median per-VQ depth.
    qd_median: Option<u64>,
    /// Number of SCSI-command VQs the backend advertised on
    /// the freshest snapshot.
    num_queues: Option<u32>,
    /// "Total" CPU percent -- sum of every worker thread's
    /// per-tick util times 100.  Retained for the plot's
    /// autoscale + backwards-compat with older daemons that
    /// did not publish avg/median/total explicitly (in that
    /// case `cpu_pct` = the estimated total and `cpu_pct_avg`
    /// / `cpu_pct_median` fall back to `cpu_pct / thread_count`
    /// -- accurate on backends where every worker was pegged
    /// identically, less useful with a skewed load).
    cpu_pct: f64,
    /// Freshest CPU-avg / CPU-median / CPU-total triple
    /// (percent, integer-rounded on the daemon side).  See the
    /// `cpu_pct_*` fields on [`SnapshotVm`] for
    /// provenance.  `None` when running against an older
    /// daemon that did not populate the fields.
    cpu_pct_avg: Option<u64>,
    cpu_pct_median: Option<u64>,
    cpu_pct_total: Option<u64>,
    /// CPU microseconds per IOP for the most recent tick.
    /// `f64::NAN` when the tick had no IOPS.  See
    /// [`Sample::cpu_us_per_iop`] for the derivation.
    cpu_us_per_iop: f64,
    /// True when the daemon has marked this instance's manual
    /// override sticky (from a prior
    /// `SetThreadCount(sticky=true)`).  Rendered as a `*`
    /// suffix on the `thr` column so operators can see which
    /// vms the engine is currently ignoring.
    sticky: bool,
    /// True when the daemon's last refresh for this instance
    /// succeeded.  A run of "alive=false" ticks is what causes
    /// the reconciler to drop the vm from the roster.
    alive: bool,
}

/// Per-VM history + last-known state.  Wrapped in an
/// `Arc<RwLock<_>>` on the shared roster so the sampler and
/// the UI can read/write independently.  Populated purely
/// from D-Bus `GetSnapshot` payloads -- no /proc walker, no
/// direct backend socket touching.
struct VmState {
    id: String,
    /// Slot in the currently-active theme's palette that this
    /// vm's line uses.  Concrete `Color` is derived at
    /// render time (`theme.palette[color_idx % len]`) so
    /// switching themes with `t` recolours every vm
    /// instantly without rewriting per-VM state.
    color_idx: usize,
    /// Guest vCPU count as reported by the daemon.  `None`
    /// when the backend does not model this; used only for the
    /// dashboard's `vc=N` badge next to the vm id.
    vcpu_count: Option<u32>,
    samples: VecDeque<Sample>,
    annots: VecDeque<Annot>,
    status: LatestStatus,
    /// The last `thread_count` we saw for this vm.  When
    /// the next snapshot reports a different one we emit a
    /// scale annotation (green up-arrow, red down-arrow) on
    /// the perf plot.
    last_thread_count: Option<u32>,
    /// Wall-clock timestamp of the freshest snapshot we
    /// received for this vm.  Feeds "last seen ...s ago"
    /// on the dashboard when the daemon stops publishing an
    /// entry (typical when a vm vanishes).
    last_snapshot_at: Instant,
}

impl VmState {
    fn new(id: String, color_idx: usize) -> Self {
        Self {
            id,
            color_idx,
            vcpu_count: None,
            samples: VecDeque::new(),
            annots: VecDeque::new(),
            status: LatestStatus::default(),
            last_thread_count: None,
            last_snapshot_at: Instant::now(),
        }
    }
}

/// Palette / accent bundle for one operator-selectable
/// theme.  Themes are picked at runtime with the `t` hotkey
/// which walks [`THEMES`]; every render path resolves its
/// colours via [`UiState::theme`] so a fresh theme takes effect
/// on the next redraw without touching per-VM state.
struct Theme {
    /// Display name shown on the title bar and in the help
    /// overlay.
    name: &'static str,
    /// Colours cycled across per-VM traces (index into
    /// `VmState::color_idx`).
    palette: &'static [Color],
    /// Grey-ish colour used for the aggregate `TOTAL` line
    /// and dashboard row.
    aggregate: Color,
    /// Y-axis / min-mid-max label colour on every plot pane
    /// (was a hard-coded `Color::DarkGray` before themes
    /// existed).
    axis: Color,
    /// Colour of the sticky mode indicator on the title bar
    /// (`[STICKY]`) and other one-off accents.
    accent: Color,
    /// Terminal background colour.  Painted as a fill rect
    /// under every widget on every frame so a light-mode
    /// theme actually looks light-mode and a dark-mode theme
    /// keeps the whole surface consistently dark (some
    /// terminals default to a semi-transparent background
    /// otherwise).  `Color::Reset` means "leave the terminal
    /// alone" -- pick that for themes that should just paint
    /// text on whatever the operator's terminal already
    /// configured.
    background: Color,
}

/// Built-in themes.  Kept small on purpose: an operator who
/// wants more can add an entry to the array without touching
/// anything else.  Order matters -- `THEMES[0]` is the
/// startup default and matches the pre-theme look of the TUI.
const THEMES: &[Theme] = &[
    Theme {
        name: "default-dark",
        palette: &[
            Color::Cyan,
            Color::LightMagenta,
            Color::LightGreen,
            Color::Yellow,
            Color::LightBlue,
            Color::LightRed,
            Color::LightCyan,
            Color::White,
        ],
        aggregate: Color::Gray,
        axis: Color::DarkGray,
        accent: Color::Magenta,
        background: Color::Black,
    },
    Theme {
        name: "default-light",
        palette: &[
            Color::Blue,
            Color::Magenta,
            Color::Green,
            Color::Red,
            Color::Cyan,
            Color::DarkGray,
            Color::Yellow,
            Color::Black,
        ],
        aggregate: Color::DarkGray,
        axis: Color::Gray,
        accent: Color::Blue,
        background: Color::White,
    },
    // Vim-desert-ish: warm amber + green on dark bg.
    Theme {
        name: "vim-desert",
        palette: &[
            Color::Yellow,
            Color::LightRed,
            Color::LightGreen,
            Color::LightCyan,
            Color::LightMagenta,
            Color::Cyan,
            Color::White,
            Color::LightBlue,
        ],
        aggregate: Color::Gray,
        axis: Color::DarkGray,
        accent: Color::Yellow,
        // #333 charcoal so amber traces pop.
        background: Color::Rgb(0x33, 0x2d, 0x29),
    },
    // Solarized-dark-ish: base colours picked from a
    // dark-friendly rotation.
    Theme {
        name: "solarized-dark",
        palette: &[
            Color::LightBlue,
            Color::LightGreen,
            Color::LightMagenta,
            Color::Yellow,
            Color::LightCyan,
            Color::LightRed,
            Color::Cyan,
            Color::White,
        ],
        aggregate: Color::DarkGray,
        axis: Color::DarkGray,
        accent: Color::LightCyan,
        // Canonical solarized base03.
        background: Color::Rgb(0x00, 0x2b, 0x36),
    },
    // Gruvbox-ish: earthy tones that read well on both light
    // and dark terminals.
    Theme {
        name: "gruvbox",
        palette: &[
            Color::LightRed,
            Color::Yellow,
            Color::LightGreen,
            Color::Cyan,
            Color::LightBlue,
            Color::LightMagenta,
            Color::White,
            Color::Gray,
        ],
        aggregate: Color::DarkGray,
        axis: Color::DarkGray,
        accent: Color::LightRed,
        // Canonical gruvbox dark0_hard.
        background: Color::Rgb(0x1d, 0x20, 0x21),
    },
];

/// `(t, old_thread_count, new_thread_count, y_value)` -- the
/// tuple the Canvas paint fn consumes per scale-event
/// annotation.  Aliased so the fn signatures stay readable.
type AnnotPoint = (f64, u32, u32, f64);
type AnnotSeries = (Color, Vec<AnnotPoint>);

/// Cap on stored history per vm, and on the aggregate.
/// Bounds memory when `--refresh-ms` is unusually small.
const MAX_HISTORY: usize = 4096;

/// Drop a vm after this many consecutive snapshots that
/// omit it (typical: the daemon dropped the instance because
/// its process vanished).  Kept low so a shutdown VM disappears
/// from the dashboard within one refresh window.
const MISSING_STREAK_DROP: u32 = 3;

/// Consecutive `GetSnapshot` failures that must accumulate
/// before the TUI's daemon-lost modal pops.  Picked to be a
/// couple of refresh ticks, so a single dropped call caused
/// by a slow bus does not scare the operator with a red
/// modal.  Once the streak passes this threshold the plot
/// stops being trustworthy anyway (samples are frozen), so
/// there is no reason not to make the failure obvious.
const DAEMON_LOST_STREAK: u32 = 3;

/// Backing store shared between the sampler and the UI event
/// loop.  There is no rediscovery loop any more: every D-Bus
/// `GetSnapshot` reply already carries the full roster, so
/// reconciliation happens as a natural side-effect of the poll.
struct Shared {
    started: Instant,
    vms: RwLock<Vec<Arc<RwLock<VmState>>>>,
    /// Per-tick aggregate sample series (sum across every live
    /// vm).  Only populated when there are >=2 vms.
    aggregate: RwLock<VecDeque<Sample>>,
    /// True when the operator hit `p` / Space.  The sampler
    /// checks this at the top of every tick and skips its work
    /// while set; the UI freezes its notion of `t_now` at
    /// pause time so the plot's X axis stops advancing too.
    paused: AtomicBool,
    /// Current sampler cadence, stored as nanoseconds so the
    /// full `f64` precision of `--refresh-secs` round-trips
    /// through an `AtomicU64` without needing a `Mutex`.
    /// Written by the modal `r` input prompt and re-read at
    /// the top of every sampler iteration, so cadence changes
    /// take effect on the next tick without restarting the
    /// task.  The 50 ms floor is honoured at every read site;
    /// there is no upper bound.
    refresh_ns: AtomicU64,
    /// Consecutive `GetSnapshot` failures observed by the
    /// sampler.  Once this passes [`DAEMON_LOST_STREAK`] the
    /// UI paints an error modal instead of the (now stale)
    /// plot -- the D-Bus call was silently retrying at DEBUG
    /// before this counter existed, which made a downed
    /// daemon look like "the plot froze" from the operator's
    /// side.
    daemon_failed_streak: AtomicU32,
    /// Freshest `GetSnapshot` error text.  Populated on every
    /// failure and cleared on the first success.  Rendered
    /// verbatim in the daemon-lost overlay so an operator can
    /// tell "daemon stopped" from "D-Bus name kicked us" at a
    /// glance.
    daemon_last_error: RwLock<Option<String>>,
    /// Proxy to the daemon's D-Bus interface.  Mandatory --
    /// the TUI is a pure daemon client and refuses to start
    /// without one.  Used both for `GetSnapshot` polls and for
    /// routing manual `+`/`-` nudges through the daemon's
    /// `SetThreadCount(sticky)` verb so the daemon owns the
    /// source of truth for the sticky flag.
    daemon: zbus::Proxy<'static>,
}

// ---------------------------------------------------------------------------
// D-Bus polling + roster reconciliation
// ---------------------------------------------------------------------------

/// Ask the daemon for its freshest snapshot and parse it.
/// Returns `Err` on any wire / JSON error so the caller can
/// decide whether to retry, warn, or bail; a `Failed` reply
/// from the daemon (unknown verb, engine channel dead) bubbles
/// up here as an error too.
async fn poll_daemon(daemon: &zbus::Proxy<'static>) -> Result<SnapshotPayload> {
    let json: String = daemon
        .call("GetSnapshot", &())
        .await
        .context("D-Bus GetSnapshot")?;
    let payload: SnapshotPayload =
        serde_json::from_str(&json).context("parse SnapshotPayload from daemon")?;
    Ok(payload)
}

/// Roster reconciliation.  Every `GetSnapshot` reply carries
/// the *complete* live inventory as the daemon sees it, so
/// discovery collapses into a single pass:
///
///   * VMs in `payload.vms` that we already track stay put and get their
///     [`VmState`] refreshed.
///   * VMs in `payload.vms` we do not track get a fresh [`VmState`] appended
///     (colour picked from `roster.len()` in the theme-independent palette
///     slot).
///   * VMs in our roster but *not* in the payload accrue a "missing" streak;
///     once it crosses [`MISSING_STREAK_DROP`] they are removed.  The streak
///     keeps a briefly-flapping backend from wiping every ring buffer on the
///     TUI's side.
///
/// The daemon's own reconciliation logic (in the controller
/// tick loop) already smooths over transient wire hiccups, so
/// this second layer of tolerance is really only there to
/// cover the rare "daemon reboot with the TUI still open"
/// case where we would otherwise flush the roster and lose
/// history.
async fn reconcile_with_snapshot(
    shared: &Shared,
    payload: &SnapshotPayload,
    select: &[String],
    missing_streak: &mut std::collections::HashMap<String, u32>,
) {
    use std::collections::HashSet;
    let wanted: HashSet<String> = payload
        .vms
        .iter()
        .filter(|d| select.is_empty() || select.iter().any(|pat| d.id.contains(pat)))
        .map(|d| d.id.clone())
        .collect();

    // Phase 1: index current roster.
    let existing: HashSet<String> = {
        let r = shared.vms.read().await;
        let mut out = HashSet::with_capacity(r.len());
        for d in r.iter() {
            let g = d.read().await;
            out.insert(g.id.clone());
        }
        out
    };

    // Phase 2: append new vms.
    if wanted.iter().any(|id| !existing.contains(id)) {
        let mut roster = shared.vms.write().await;
        for d in &payload.vms {
            if !wanted.contains(&d.id) {
                continue;
            }
            if existing.contains(&d.id) {
                continue;
            }
            let color_idx = roster.len();
            roster.push(Arc::new(RwLock::new(VmState::new(d.id.clone(), color_idx))));
        }
    }

    // Phase 3: drop vms that have been missing from the
    // daemon's snapshot for too long.
    for id in existing.iter() {
        if wanted.contains(id) {
            missing_streak.remove(id);
        } else {
            let c = missing_streak.entry(id.clone()).or_insert(0);
            *c = c.saturating_add(1);
        }
    }
    let to_drop: HashSet<String> = missing_streak
        .iter()
        .filter(|&(_, &c)| c > MISSING_STREAK_DROP)
        .map(|(id, _)| id.clone())
        .collect();
    if !to_drop.is_empty() {
        let mut roster = shared.vms.write().await;
        let mut i = 0;
        while i < roster.len() {
            let drop_it = {
                let g = roster[i].read().await;
                to_drop.contains(&g.id)
            };
            if drop_it {
                roster.remove(i);
            } else {
                i += 1;
            }
        }
        for id in &to_drop {
            missing_streak.remove(id);
        }
    }
}

// ---------------------------------------------------------------------------
// Sampler loop
// ---------------------------------------------------------------------------

/// Turn one entry of the daemon's snapshot into the
/// derived-rate [`Sample`] the plots consume.
///
/// The daemon publishes per-tick rates directly
/// (`read_iops`, `write_iops`, `read_bw_bps`, ...), so unlike
/// the pre-D-Bus code path there is no delta math on the TUI
/// side any more -- the wall-clock spacing between two
/// snapshots is not even required to convert them. Latency digests are
/// optional because a backend can have no samples for one direction.
fn snapshot_to_sample(d: &SnapshotVm, t: f64) -> Sample {
    let iops = d
        .read_iops
        .saturating_add(d.write_iops)
        .saturating_add(d.other_iops) as f64;
    let bw_mb = (d.read_bw_bps.saturating_add(d.write_bw_bps) as f64) / 1.0e6;
    let cpu_pct = d.per_thread_util.clamp(0.0, 1.0) * (d.thread_count as f64) * 100.0;
    let cpu_us_per_iop = if iops > 0.0 {
        (cpu_pct / 100.0) * 1_000_000.0 / iops
    } else {
        f64::NAN
    };
    Sample {
        t,
        iops,
        bw_mb,
        lat_r: d
            .read_latency_us
            .map(|latency| (latency.p50, latency.p95, latency.p99)),
        lat_w: d
            .write_latency_us
            .map(|latency| (latency.p50, latency.p95, latency.p99)),
        lat_r_avg: d.read_latency_us.map(|latency| latency.avg),
        lat_w_avg: d.write_latency_us.map(|latency| latency.avg),
        cpu_pct,
        qd: d.qd_total.unwrap_or(0) as f64,
        thread_count: d.thread_count,
        cpu_us_per_iop,
    }
}

/// Turn one entry of the daemon's snapshot into the
/// [`LatestStatus`] the dashboard consumes.  Kept separate
/// from [`snapshot_to_sample`] because the two views want
/// different unit conventions -- the sample is derived-rate
/// only, the status carries the raw per-direction breakdown
/// plus the freshest sticky / alive flags.
fn snapshot_to_status(d: &SnapshotVm) -> LatestStatus {
    LatestStatus {
        thread_count: d.thread_count,
        iops_r: d.read_iops as f64,
        iops_w: d.write_iops as f64,
        iops_o: d.other_iops as f64,
        bw_r_mb: (d.read_bw_bps as f64) / 1.0e6,
        bw_w_mb: (d.write_bw_bps as f64) / 1.0e6,
        lat_r_us: d
            .read_latency_us
            .map(|latency| (latency.p50, latency.p95, latency.p99)),
        lat_w_us: d
            .write_latency_us
            .map(|latency| (latency.p50, latency.p95, latency.p99)),
        lat_r_avg_us: d.read_latency_us.map(|latency| latency.avg),
        lat_w_avg_us: d.write_latency_us.map(|latency| latency.avg),
        qd_total: d.qd_total,
        qd_avg: d.qd_avg_x100.map(|depth| depth as f64 / 100.0),
        qd_median: d.qd_median,
        num_queues: d.num_queues,
        cpu_pct: d.per_thread_util.clamp(0.0, 1.0) * (d.thread_count as f64) * 100.0,
        cpu_pct_avg: d.cpu_pct_avg,
        cpu_pct_median: d.cpu_pct_median,
        cpu_pct_total: d.cpu_pct_total,
        cpu_us_per_iop: {
            let iops = d
                .read_iops
                .saturating_add(d.write_iops)
                .saturating_add(d.other_iops) as f64;
            let cpu_pct = d.per_thread_util.clamp(0.0, 1.0) * (d.thread_count as f64) * 100.0;
            if iops > 0.0 {
                (cpu_pct / 100.0) * 1_000_000.0 / iops
            } else {
                f64::NAN
            }
        },
        sticky: d.manual_scaling_sticky,
        alive: d.alive,
    }
}

/// Read the current refresh cadence from the shared atomic
/// and clamp it to a 50 ms floor.  There is no upper bound --
/// an operator on a slow link can dial the cadence up to
/// minutes without the sampler arguing back -- but the floor
/// exists so a `0` typo cannot spin the sampler.
fn refresh_period(shared: &Shared) -> Duration {
    let ns = shared.refresh_ns.load(Ordering::Relaxed);
    Duration::from_nanos(ns).max(Duration::from_millis(50))
}

/// Single background task: every `refresh_period(shared)`
/// calls `GetSnapshot` on the daemon and folds the payload
/// into the shared roster + ring buffers.  A dropped D-Bus
/// call is logged at DEBUG (we assume the daemon is
/// intermittently slow, not gone -- if it stays down long
/// enough, `reconcile_with_snapshot` will time the tracked
/// instances out on its "missing streak" counter).
async fn sampler_loop(shared: Arc<Shared>, cli: Cli) {
    let mut missing_streak: std::collections::HashMap<String, u32> =
        std::collections::HashMap::new();
    loop {
        tokio::time::sleep(refresh_period(&shared)).await;
        if shared.paused.load(Ordering::Relaxed) {
            continue;
        }
        let now = Instant::now();
        let t = now.duration_since(shared.started).as_secs_f64();

        let payload = match poll_daemon(&shared.daemon).await {
            Ok(p) => {
                // Restore the "healthy daemon" state on the
                // first success after any failure streak.  The
                // UI's error modal keys off both the counter
                // and the message so we clear both together.
                if shared.daemon_failed_streak.swap(0, Ordering::Relaxed) > 0 {
                    *shared.daemon_last_error.write().await = None;
                    tracing::info!(target: "iothread-tui", "daemon reachable again");
                }
                p
            }
            Err(e) => {
                let n = shared.daemon_failed_streak.fetch_add(1, Ordering::Relaxed) + 1;
                *shared.daemon_last_error.write().await = Some(e.to_string());
                tracing::debug!(
                    target: "iothread-tui",
                    error = ?e,
                    streak = n,
                    "GetSnapshot failed"
                );
                continue;
            }
        };

        // Add / drop rows to match the daemon's view (subject
        // to the `--select` filter).
        reconcile_with_snapshot(&shared, &payload, &cli.select, &mut missing_streak).await;

        // Take a lock-free snapshot of the current roster; the
        // sampler is the only writer for the per-VM rings
        // so we do not need the write lock held across the
        // whole loop.
        let roster: Vec<Arc<RwLock<VmState>>> = {
            let r = shared.vms.read().await;
            r.iter().cloned().collect()
        };

        // Build id -> snapshot map so we can O(1) lookup while
        // iterating our roster (which may be shorter than the
        // payload if `--select` is in use).
        let by_id: std::collections::HashMap<&str, &SnapshotVm> =
            payload.vms.iter().map(|d| (d.id.as_str(), d)).collect();

        // Accumulator for the fleet-wide aggregate line.
        let mut agg = Sample {
            t,
            iops: 0.0,
            bw_mb: 0.0,
            lat_r: None,
            lat_w: None,
            // Fleet-wide latency aggregation is not
            // well-defined (Simpson's paradox on the
            // percentiles), so leave both slots empty; the
            // TUI's aggregate line does not appear on the
            // latency pane.
            lat_r_avg: None,
            lat_w_avg: None,
            cpu_pct: 0.0,
            qd: 0.0,
            thread_count: 0,
            cpu_us_per_iop: f64::NAN,
        };
        let mut live_count = 0usize;

        for d in &roster {
            let (id, last_thr) = {
                let g = d.read().await;
                (g.id.clone(), g.last_thread_count)
            };
            let Some(dom) = by_id.get(id.as_str()) else {
                // Vm no longer in payload; reconciliation
                // will drop it once the missing streak hits.
                continue;
            };
            let sample = snapshot_to_sample(dom, t);
            let status = snapshot_to_status(dom);
            let annot = last_thr.and_then(|prev| {
                if prev != dom.thread_count {
                    Some(Annot {
                        t,
                        y_value: sample.iops.max(sample.bw_mb),
                        old_n: prev,
                        new_n: dom.thread_count,
                    })
                } else {
                    None
                }
            });

            {
                let mut g = d.write().await;
                g.vcpu_count = Some(dom.vcpu_count);
                g.last_thread_count = Some(dom.thread_count);
                g.last_snapshot_at = now;
                g.samples.push_back(sample);
                while g.samples.len() > MAX_HISTORY {
                    g.samples.pop_front();
                }
                if let Some(a) = annot {
                    g.annots.push_back(a);
                    while g.annots.len() > MAX_HISTORY {
                        g.annots.pop_front();
                    }
                }
                g.status = status;
            }

            live_count += 1;
            agg.iops += sample.iops;
            agg.bw_mb += sample.bw_mb;
            agg.cpu_pct += sample.cpu_pct;
            agg.qd += sample.qd;
            agg.thread_count = agg.thread_count.saturating_add(sample.thread_count);
        }

        if live_count >= 2 {
            agg.cpu_us_per_iop = if agg.iops > 0.0 {
                (agg.cpu_pct / 100.0) * 1_000_000.0 / agg.iops
            } else {
                f64::NAN
            };
            let mut a = shared.aggregate.write().await;
            a.push_back(agg);
            while a.len() > MAX_HISTORY {
                a.pop_front();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// UI event loop
// ---------------------------------------------------------------------------

/// UI focus: which row (if any) the operator has selected.
/// `Total` is a pseudo-row that appears in the focus cycle
/// only when there are >=2 vms -- selecting it does not
/// enable `+`/`-` (broadcasting to every VM is almost always
/// a mistake) but it does highlight the grey aggregate line
/// in every plot pane, which is the operationally interesting
/// bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Vm(usize),
    Total,
}

/// A modal prompt asking the operator to type a value.  While
/// `UiState::input_prompt` is `Some`, every regular hotkey is
/// disabled; only digits, backspace, Enter (commit) and Esc
/// (cancel) reach the state machine.  Kept intentionally tiny
/// -- we only need it for numeric parameters today (refresh
/// cadence).
struct InputPrompt {
    kind: InputPromptKind,
    /// Live keystroke buffer.  Rendered on the modal overlay
    /// with a trailing cursor so operators can see what
    /// they have typed.
    buffer: String,
}

#[derive(Debug, Clone)]
enum InputPromptKind {
    /// Prompt for a refresh cadence in **seconds** (decimals
    /// accepted).  Committed to [`Shared::refresh_ns`] on
    /// Enter; a 50 ms floor is enforced silently.  No upper
    /// bound.
    RefreshSecs,
    /// Prompt for a per-plot Y-axis maximum.  Applied to
    /// [`UiState::y_max_override`] for the given
    /// [`PlotKind`].  A blank / zero value clears the
    /// override and returns the plot to autoscale.
    YMax(PlotKind),
    /// Prompt for a manual thread count for the focused
    /// vm.  The prompt renders a sticky checkbox line
    /// that Toggle-Space flips; on commit the value goes
    /// through the daemon's `SetThreadCount` verb with the
    /// current `sticky` flag.
    SetThreads {
        /// Snapshot of the vm the prompt was opened
        /// against, so a live roster shuffle between key
        /// press and Enter cannot silently retarget the
        /// scale.
        vm_id: String,
        /// Freshest thread count at prompt-open time.
        /// Threaded through solely so the daemon
        /// `SetThreadCount` log line shows the correct
        /// "from -> to" transition; commit does not read
        /// from the shared roster (which would need an
        /// async lock the sync commit fn cannot easily
        /// take).
        current: u32,
        /// Sticky checkbox state inside the modal.  Toggled
        /// by Tab.  Starts at [`UiState::sticky_mode`] so
        /// operators who already armed sticky do not have to
        /// tick it a second time.
        sticky: bool,
    },
}

impl InputPromptKind {
    /// Human-readable prompt shown at the top of the modal.
    fn title(&self) -> String {
        match self {
            Self::RefreshSecs => "refresh cadence (seconds)".to_string(),
            Self::YMax(k) => format!("y-max for {} (0 clears)", plot_kind_label(*k)),
            Self::SetThreads { vm_id, .. } => {
                format!("set thread count for {vm_id}")
            }
        }
    }
}

/// Short label for a plot pane, used in prompt titles + help.
fn plot_kind_label(k: PlotKind) -> &'static str {
    match k {
        PlotKind::Perf => "perf",
        PlotKind::Latency => "latency",
        PlotKind::Cpu => "cpu",
        PlotKind::QueueDepth => "qd",
        PlotKind::CpuPerIop => "cpu/iop",
    }
}

/// Every column the dashboard knows how to render.  Order
/// here is the *display* order: the dashboard iterates the
/// enum in the same sequence [`DashColumn::ALL`] lists and
/// emits the enabled ones only.  Adding a new column is a
/// three-liner: add a variant, extend `ALL`, add its `label`
/// / `width` / render arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DashColumn {
    Id,
    Threads,
    Iops,
    Bandwidth,
    Cpu,
    UsPerIop,
    LatRead,
    LatWrite,
    QueueDepth,
    NumQueues,
}

impl DashColumn {
    /// Every column in canonical display order.  The
    /// column-toggle modal iterates this slice too so its
    /// list matches what the dashboard will render.
    const ALL: &'static [Self] = &[
        Self::Id,
        Self::Threads,
        Self::Iops,
        Self::Bandwidth,
        Self::Cpu,
        Self::UsPerIop,
        Self::LatRead,
        Self::LatWrite,
        Self::QueueDepth,
        Self::NumQueues,
    ];

    /// Header text shown on the dashboard row.
    fn label(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Threads => "thr",
            Self::Iops => "iops r/w/o",
            // Unit rides on each value now (see `fmt_bandwidth_mbps`),
            // so the header stays unit-less.
            Self::Bandwidth => "bw r/w",
            Self::Cpu => "cpu avg/med/total",
            Self::UsPerIop => "us/iop",
            Self::LatRead => "lat_r p50/95/99",
            Self::LatWrite => "lat_w p50/95/99",
            Self::QueueDepth => "qd avg/med/tot",
            Self::NumQueues => "queues",
        }
    }

    /// Fixed cell width in characters.  Chosen so the header
    /// fits without wrapping under the widest plausible
    /// value.
    fn width(self) -> u16 {
        match self {
            Self::Id => 28,
            Self::Threads => 5,
            Self::Iops => 22,
            // Widened for the longest plausible pair with
            // the space-separated slash ("4.2GB/s / 500KB/s"
            // = 17 chars) plus a small margin.
            Self::Bandwidth => 20,
            // Widened for the avg/med/total triple, e.g.
            // "99/99/999" = 9 chars + a couple of slack cells.
            Self::Cpu => 18,
            Self::UsPerIop => 8,
            Self::LatRead => 20,
            Self::LatWrite => 20,
            Self::QueueDepth => 16,
            Self::NumQueues => 8,
        }
    }

    /// True for columns that must remain visible.  Only
    /// [`Self::Id`] carries the flag today -- everything else
    /// is operator-toggleable.
    fn required(self) -> bool {
        matches!(self, Self::Id)
    }
}

/// Modal picker for [`DashColumn`] visibility.  Opened with
/// `c`; navigated with up/down / j / k; toggled with Space or
/// Enter; dismissed with `c`, `q`, or Esc.
struct ColumnPicker {
    /// Cursor position within [`DashColumn::ALL`].
    cursor: usize,
}

/// UI-local state: the toggles the operator drives from the
/// keyboard.  Never crosses a `.await` boundary so no
/// synchronisation is needed.
struct UiState {
    active_plots: Vec<PlotKind>,
    perf_metric: PerfMetric,
    focused: Option<Focus>,
    paused: bool,
    /// Freezes the plot's X-axis when `paused` is set.  Cleared
    /// on unpause so redraws snap back to real time.
    paused_t_frozen: Option<f64>,
    /// True when the operator wants their next `+` / `-` to
    /// carry the sticky bit.  Toggled by `s`; echoed on the
    /// title bar so the operator never has to guess what mode
    /// they are in.
    sticky_mode: bool,
    /// True while the operator is looking at the help overlay.
    /// Any key press dismisses it.
    show_help: bool,
    /// Active modal input prompt, if any.  See
    /// [`InputPrompt`].
    input_prompt: Option<InputPrompt>,
    /// Currently selected UI theme.  Index into
    /// [`THEMES`]; wraps.
    theme_idx: usize,
    /// Set of dashboard columns currently visible.  The
    /// dashboard iterates [`DashColumn::ALL`] and renders the
    /// ones that appear here, in canonical display order.
    /// Required columns (see [`DashColumn::required`]) can
    /// never be removed from this set.
    columns: std::collections::HashSet<DashColumn>,
    /// Column-toggle modal, when open.  `None` = closed.
    column_picker: Option<ColumnPicker>,
    /// Log-scale flag for every plot pane.  A single toggle
    /// keeps the ergonomics simple ("everything gets the same
    /// treatment"); per-plot log scaling would need a
    /// per-plot hotkey and there is no obvious use case for
    /// "IOPS in log but latency in linear" today.
    log_y_scale: bool,
    /// Per-plot manual Y-max override.  When set for a plot
    /// the canvas draws with `[0, override]` as the Y range
    /// regardless of what samples land -- so an operator who
    /// wants a fixed comparison scale can lock it via `y`.
    /// Cleared by `Z` (autoscale reset) or by re-entering `0`
    /// / a blank value in the prompt.
    y_max_override: std::collections::HashMap<PlotKind, f64>,
    /// When true, the X axis labels render as wall-clock time
    /// (HH:MM:SS) rather than "seconds since TUI start".
    /// Purely a display toggle -- the underlying sample times
    /// stay monotonic to keep the plot arithmetic simple.
    wall_clock_x: bool,
    /// UNIX-epoch seconds when the TUI came up (`SystemTime`
    /// converted at startup).  Feeds the wall-clock X-axis
    /// labels: `wall_seconds = started_unix + t`; the
    /// formatter turns that into `HH:MM:SS` on the local
    /// timezone.  Captured once so a live theme swap or plot
    /// swap does not cause the labels to drift.
    started_unix_s: u64,
}

impl UiState {
    fn new(perf_metric: PerfMetric) -> Self {
        // Default to a broad-but-not-cluttered set: id, thr,
        // iops, bw, cpu, us/iop, qd (avg/med/tot), queues.
        // Latency columns are noisy by default (three numbers
        // each) so they stay off until the operator opts in
        // via `c`.
        let mut columns = std::collections::HashSet::new();
        for &c in [
            DashColumn::Id,
            DashColumn::Threads,
            DashColumn::Iops,
            DashColumn::Bandwidth,
            DashColumn::Cpu,
            DashColumn::UsPerIop,
            DashColumn::QueueDepth,
            DashColumn::NumQueues,
        ]
        .iter()
        {
            columns.insert(c);
        }
        Self {
            active_plots: vec![PlotKind::Perf],
            perf_metric,
            focused: None,
            paused: false,
            paused_t_frozen: None,
            sticky_mode: false,
            show_help: false,
            input_prompt: None,
            theme_idx: 0,
            columns,
            column_picker: None,
            log_y_scale: false,
            y_max_override: std::collections::HashMap::new(),
            wall_clock_x: false,
            started_unix_s: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }

    /// Ordered iterator over the columns that should render
    /// this frame.  Iterates [`DashColumn::ALL`] so display
    /// order stays canonical regardless of how the operator
    /// toggled them.
    fn visible_columns(&self) -> impl Iterator<Item = DashColumn> + '_ {
        DashColumn::ALL
            .iter()
            .copied()
            .filter(|c| self.columns.contains(c))
    }

    /// Currently-selected theme (see [`THEMES`]).  Cheap
    /// enough to look up every render pass -- it is a slice
    /// index.
    fn theme(&self) -> &'static Theme {
        &THEMES[self.theme_idx % THEMES.len()]
    }

    /// Concrete colour for a vm's palette slot under the
    /// active theme.
    fn vm_color(&self, color_idx: usize) -> Color {
        let p = self.theme().palette;
        p[color_idx % p.len()]
    }

    fn toggle_plot(&mut self, k: PlotKind) {
        if let Some(i) = self.active_plots.iter().position(|p| *p == k) {
            // Keep at least one plot on -- an empty top pane
            // leaves the operator staring at an empty box.
            if self.active_plots.len() > 1 {
                self.active_plots.remove(i);
            }
        } else {
            self.active_plots.push(k);
        }
    }
}

/// Reason the UI loop returned; only used so `main` can
/// distinguish "user quit cleanly" from "an unhandled error
/// bubbled up".
enum UiExit {
    Quit,
}

async fn run_ui(shared: Arc<Shared>, cli: Cli) -> Result<UiExit> {
    enable_raw_mode()?;
    let mut out = stdout();
    execute!(out, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(out);
    let mut terminal = Terminal::new(backend)?;
    // Clear the alternate screen before the first draw.  Some
    // terminals -- notably reconnected SSH sessions coming
    // back from a timeout -- inherit whatever glyphs were on
    // the primary screen at the moment `EnterAlternateScreen`
    // ran, so without this explicit clear the operator's
    // shell scrollback bleeds through the plot canvas until
    // the first full redraw covers it.  Cheap either way.
    terminal.clear()?;

    let mut ui = UiState::new(cli.metric);

    let result = loop {
        // Redraw cadence follows the sampler cadence.  We rebuild
        // the sleep on every iteration so the `r` hotkey takes
        // effect on the next redraw without restarting the task
        // (same trick as `sampler_loop`).
        let redraw = tokio::time::sleep(refresh_period(&shared));
        tokio::pin!(redraw);
        tokio::select! {
            _ = &mut redraw => {}
            key = read_key_event() => {
                match key? {
                    None => {}
                    Some((k, m)) if handle_key(k, m, &mut ui, &shared).await => {
                        break UiExit::Quit;
                    }
                    Some(_) => {}
                }
            }
        }

        // Snapshot every vm + the aggregate under short
        // read locks so drawing sees a consistent world.
        let snap = snapshot_shared(&shared).await;
        // Pause: freeze `t_now` at the value we captured when
        // the operator hit `p`.  Combined with the sampler /
        // rediscovery pause gates that stops the sample stream,
        // this leaves the plot genuinely still (both axes and
        // data) rather than just "not accepting new points but
        // still scrolling right".
        // Plot-scroll rate == refresh rate: the sampler and
        // the UI redraw share the same tokio timer, so the
        // right edge of the plot naturally advances one
        // `refresh_secs` interval per frame.  No separate
        // `scroll_secs` knob -- coupling the two on purpose
        // matches every operator expectation ("one setting,
        // one cadence").
        let t_now = if ui.paused {
            ui.paused_t_frozen
                .unwrap_or_else(|| Instant::now().duration_since(shared.started).as_secs_f64())
        } else {
            Instant::now().duration_since(shared.started).as_secs_f64()
        };
        let x_lo = (t_now - cli.window_s as f64).max(0.0);
        let x_hi = t_now.max(cli.window_s as f64);

        let refresh = refresh_period(&shared).as_secs_f64();
        terminal.draw(|f| {
            draw_frame(f, &cli, &ui, &snap, x_lo, x_hi, refresh);
        })?;
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(result)
}

/// Pulled-out clone of every piece of state the render path
/// needs, taken under short read locks so drawing is
/// snapshot-consistent.
struct RenderSnapshot {
    vms: Vec<VmRender>,
    aggregate: Vec<Sample>,
    /// `Some(msg)` when the sampler has seen at least
    /// [`DAEMON_LOST_STREAK`] consecutive `GetSnapshot`
    /// failures.  Rendered as a red modal overlay on the
    /// current frame -- the plot is left in place underneath
    /// so operators can see the last-known state, and the
    /// modal explains why nothing is updating.
    daemon_error: Option<String>,
}

struct VmRender {
    id: String,
    /// Palette slot; resolved to a concrete [`Color`] in each
    /// draw fn via `ui.theme().palette[color_idx % len]`.
    color_idx: usize,
    vcpu_count: Option<u32>,
    samples: Vec<Sample>,
    annots: Vec<Annot>,
    status: LatestStatus,
}

async fn snapshot_shared(shared: &Shared) -> RenderSnapshot {
    let mut out = Vec::new();
    let roster: Vec<Arc<RwLock<VmState>>> = {
        let r = shared.vms.read().await;
        r.iter().cloned().collect()
    };
    for d in &roster {
        let g = d.read().await;
        out.push(VmRender {
            id: g.id.clone(),
            color_idx: g.color_idx,
            vcpu_count: g.vcpu_count,
            samples: g.samples.iter().copied().collect(),
            annots: g.annots.iter().copied().collect(),
            status: g.status.clone(),
        });
    }
    let aggregate: Vec<Sample> = shared.aggregate.read().await.iter().copied().collect();
    // Only surface the daemon-lost overlay once the failure
    // streak has cleared the tolerance threshold -- a single
    // dropped call caused by a slow bus should not scare the
    // operator with a full-screen red modal.
    let daemon_error = if shared.daemon_failed_streak.load(Ordering::Relaxed) >= DAEMON_LOST_STREAK
    {
        shared.daemon_last_error.read().await.clone()
    } else {
        None
    };
    RenderSnapshot {
        vms: out,
        aggregate,
        daemon_error,
    }
}

// ---------------------------------------------------------------------------
// Keyboard handling
// ---------------------------------------------------------------------------

/// Handle a single key press.  Returns `true` when the caller
/// should exit the event loop.
async fn handle_key(
    k: KeyCode,
    // Kept in the signature (and threaded from `read_key_event`)
    // so future modifier-sensitive verbs can pick it up
    // without another plumbing pass; currently unused because
    // Ctrl-+/- were dropped over tmux passthrough issues.
    _m: KeyModifiers,
    ui: &mut UiState,
    shared: &Shared,
) -> bool {
    // The help overlay swallows every key; the first press
    // dismisses it and does nothing else so operators can
    // "peek" without accidentally scaling something.
    if ui.show_help {
        ui.show_help = false;
        return false;
    }
    // Modal input prompt (currently: `r` opens one for the
    // refresh cadence).  Digits are appended, Backspace pops,
    // Enter commits, Esc cancels.  Every other key is
    // swallowed so operators cannot accidentally scale a VM
    // while typing.
    if ui.input_prompt.is_some() {
        handle_input_prompt_key(k, ui, shared);
        return false;
    }
    // Column-toggle modal is exclusive too: while it is open,
    // arrow keys move the cursor and Space/Enter toggles
    // rather than reaching the regular hotkey table.
    if ui.column_picker.is_some() {
        handle_column_picker_key(k, ui);
        return false;
    }
    match k {
        // Layered exit: Esc first tries to *undo* the current
        // UI mode (help panel, focused vm, sticky arm)
        // before quitting.  This matches every terminal UI an
        // operator likely uses (less, htop, k9s, ...) and
        // stops a stray Esc from tearing the TUI down while
        // trying to just clear a highlight.  `q` is the
        // dedicated hard-quit hotkey.
        KeyCode::Char('q') => return true,
        KeyCode::Esc => {
            if ui.show_help {
                ui.show_help = false;
            } else if ui.focused.is_some() {
                ui.focused = None;
            } else if ui.sticky_mode {
                ui.sticky_mode = false;
            } else {
                return true;
            }
        }
        KeyCode::Char('h') | KeyCode::Char('?') => ui.show_help = true,
        KeyCode::Char('m') => ui.perf_metric = ui.perf_metric.toggle(),
        KeyCode::Char('p') | KeyCode::Char(' ') => toggle_pause(ui, shared),
        KeyCode::Char('r') => {
            let secs = refresh_period(shared).as_secs_f64();
            ui.input_prompt = Some(InputPrompt {
                kind: InputPromptKind::RefreshSecs,
                buffer: fmt_prompt_secs(secs),
            });
        }
        KeyCode::Char('c') => {
            ui.column_picker = Some(ColumnPicker { cursor: 0 });
        }
        KeyCode::Char('t') => cycle_theme(ui),
        KeyCode::Char('s') => ui.sticky_mode = !ui.sticky_mode,
        KeyCode::Char('1') => ui.toggle_plot(PlotKind::Perf),
        KeyCode::Char('2') => ui.toggle_plot(PlotKind::Latency),
        KeyCode::Char('3') => ui.toggle_plot(PlotKind::Cpu),
        KeyCode::Char('4') => ui.toggle_plot(PlotKind::QueueDepth),
        KeyCode::Char('5') => ui.toggle_plot(PlotKind::CpuPerIop),
        KeyCode::Down | KeyCode::Char('j') => move_focus(ui, shared, 1).await,
        KeyCode::Up | KeyCode::Char('k') => move_focus(ui, shared, -1).await,
        // `+`/`-` always route through `sticky_mode`.  We used
        // to also honour `Ctrl-+` / `Ctrl--` as a one-shot
        // sticky variant, but tmux swallows / rewrites those
        // key events depending on its own bindings and the
        // event never reaches the TUI; the `s` toggle is
        // strictly more reliable and only two extra keystrokes,
        // so it is now the sole way to arm sticky.
        KeyCode::Char('+') | KeyCode::Char('=') => {
            nudge_thread_count(ui, shared, 1, ui.sticky_mode).await;
        }
        KeyCode::Char('-') | KeyCode::Char('_') => {
            nudge_thread_count(ui, shared, -1, ui.sticky_mode).await;
        }
        // Y-axis controls.  `l` = toggle log/lin (all plots);
        // `z` = reset autoscale (drop old samples so the Y
        // range shrinks back to whatever the current window
        // needs); `y` = manual Y-max prompt on the first
        // active plot.
        KeyCode::Char('l') => ui.log_y_scale = !ui.log_y_scale,
        KeyCode::Char('z') => reset_autoscale(shared).await,
        KeyCode::Char('y') => open_ymax_prompt(ui),
        // `x` toggles wall-clock time labels on the X axis
        // (the underlying `t` axis stays "seconds since start"
        // so the plot arithmetic does not need to know).
        KeyCode::Char('x') => ui.wall_clock_x = !ui.wall_clock_x,
        // `n` (for "number of threads") opens the numeric
        // set-threads modal for the focused vm.  Silently
        // no-ops when the focus is on Total / nothing.
        KeyCode::Char('n') => open_set_threads_prompt(ui, shared).await,
        _ => {}
    }
    false
}

/// Discard sample history older than "now" for every vm
/// (and the aggregate).  Called on `z`: the Y axis of every
/// plot re-derives its upper bound from freshest samples only,
/// so an old peak that has aged out of the visible X-window no
/// longer keeps the axis inflated.
async fn reset_autoscale(shared: &Shared) {
    let vms = shared.vms.read().await;
    for d in vms.iter() {
        let mut g = d.write().await;
        g.samples.clear();
        g.annots.clear();
    }
    let mut agg = shared.aggregate.write().await;
    agg.clear();
}

/// Open the `YMax` prompt targeting the first plot in
/// [`UiState::active_plots`].  When multiple plots are on the
/// first one wins (the operator can always press `y` again
/// with a different plot as the primary; we do not surface a
/// selector today).
fn open_ymax_prompt(ui: &mut UiState) {
    let Some(&plot) = ui.active_plots.first() else {
        return;
    };
    let buffer = ui
        .y_max_override
        .get(&plot)
        .map(|v| format!("{v:.0}"))
        .unwrap_or_default();
    ui.input_prompt = Some(InputPrompt {
        kind: InputPromptKind::YMax(plot),
        buffer,
    });
}

/// Open the numeric SetThreads prompt for the focused
/// vm.  Silently no-ops (returns without opening a modal)
/// when there is no focused vm or when the focus points
/// at the pseudo-`Total` slot, mirroring the `+`/`-` nudge
/// semantics.
async fn open_set_threads_prompt(ui: &mut UiState, shared: &Shared) {
    let idx = match ui.focused {
        Some(Focus::Vm(i)) => i,
        _ => return,
    };
    let target_arc = {
        let r = shared.vms.read().await;
        r.get(idx).cloned()
    };
    let Some(target) = target_arc else {
        return;
    };
    let (id, cur) = {
        let g = target.read().await;
        (g.id.clone(), g.status.thread_count)
    };
    ui.input_prompt = Some(InputPrompt {
        kind: InputPromptKind::SetThreads {
            vm_id: id,
            current: cur,
            sticky: ui.sticky_mode,
        },
        buffer: cur.to_string(),
    });
}

/// Advance to the next theme in [`THEMES`] (wraps).  Kept
/// separate from the input-prompt path because theme changes
/// are a single-keystroke cycle -- no numeric entry required.
fn cycle_theme(ui: &mut UiState) {
    ui.theme_idx = (ui.theme_idx + 1) % THEMES.len();
}

/// Route a key press to the column-toggle modal.  The picker
/// walks [`DashColumn::ALL`] in order; the cursor wraps at
/// both ends.  Toggling a required column (see
/// [`DashColumn::required`]) is silently refused so the
/// operator cannot accidentally hide `id` and end up with an
/// anonymous row-per-VM dashboard.
fn handle_column_picker_key(k: KeyCode, ui: &mut UiState) {
    let Some(picker) = ui.column_picker.as_mut() else {
        return;
    };
    let total = DashColumn::ALL.len();
    match k {
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('c') => {
            ui.column_picker = None;
        }
        KeyCode::Down | KeyCode::Char('j') => {
            picker.cursor = (picker.cursor + 1) % total;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            picker.cursor = (picker.cursor + total - 1) % total;
        }
        KeyCode::Char(' ') | KeyCode::Enter => {
            let col = DashColumn::ALL[picker.cursor];
            if col.required() {
                // Nothing to toggle; a silent no-op is friendlier
                // than a bell.
                return;
            }
            if ui.columns.contains(&col) {
                ui.columns.remove(&col);
            } else {
                ui.columns.insert(col);
            }
        }
        _ => {}
    }
}

/// Centred modal listing every [`DashColumn`] with a
/// checkbox indicator (`[x]` on, `[ ]` off, `[·]` for
/// required columns that cannot be toggled).  Painted last
/// (on top of the frame) so it wins the paint stack over the
/// dashboard beneath.
fn draw_column_picker(f: &mut ratatui::Frame, ui: &UiState) {
    let Some(picker) = ui.column_picker.as_ref() else {
        return;
    };
    let theme = ui.theme();
    let full = f.area();
    let width = 44_u16.min(full.width);
    // Header + blank + rows + blank + footer.
    let height = (DashColumn::ALL.len() as u16 + 5).min(full.height);
    let x = full.x + (full.width.saturating_sub(width)) / 2;
    let y = full.y + (full.height.saturating_sub(height)) / 2;
    let rect = Rect {
        x,
        y,
        width,
        height,
    };
    let mut lines: Vec<Line> = Vec::with_capacity(DashColumn::ALL.len() + 4);
    lines.push(Line::from(Span::styled(
        "dashboard columns",
        Style::default()
            .fg(theme.accent)
            .add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(""));
    for (i, &col) in DashColumn::ALL.iter().enumerate() {
        let marker = if col.required() {
            "[·]"
        } else if ui.columns.contains(&col) {
            "[x]"
        } else {
            "[ ]"
        };
        let text = format!(" {} {}", marker, col.label());
        let style = if i == picker.cursor {
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::REVERSED)
        } else if col.required() {
            Style::default().fg(theme.axis)
        } else {
            Style::default()
        };
        lines.push(Line::from(Span::styled(text, style)));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Space/Enter: toggle    j/k or ↑/↓: move    c/Esc: close",
        Style::default().fg(theme.axis),
    )));

    f.render_widget(Clear, rect);
    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title("columns")
                .style(Style::default().fg(theme.accent)),
        ),
        rect,
    );
}

/// Route a key press to the active modal input prompt.  Only
/// digits (numeric prompts are the only kind today), backspace,
/// Enter (commit) and Esc (cancel) are honoured; everything
/// else is dropped on the floor so a stray hotkey cannot slip
/// through and re-arm a plot toggle while the operator is
/// typing.
fn handle_input_prompt_key(k: KeyCode, ui: &mut UiState, shared: &Shared) {
    let Some(prompt) = ui.input_prompt.as_mut() else {
        return;
    };
    match k {
        KeyCode::Esc => {
            ui.input_prompt = None;
        }
        KeyCode::Enter => {
            commit_input_prompt(ui, shared);
        }
        KeyCode::Backspace => {
            prompt.buffer.pop();
        }
        KeyCode::Tab | KeyCode::Char(' ') => {
            // Tab (and Space) toggle the sticky checkbox on
            // the SetThreads prompt.  Ignored on the other
            // prompt kinds so a stray Tab does nothing
            // dangerous.
            if let InputPromptKind::SetThreads { sticky, .. } = &mut prompt.kind {
                *sticky = !*sticky;
            }
        }
        KeyCode::Char(c) => {
            // Digit is always accepted.  YMax additionally
            // accepts a single decimal point (so "0.5" lands
            // on the plot).  Cap the buffer at 12 chars so an
            // operator hammering the key row cannot type a
            // 30-digit monster that parse::<f64>() would
            // silently round to infinity.
            let is_digit = c.is_ascii_digit();
            let is_dot_ok = matches!(
                &prompt.kind,
                InputPromptKind::YMax(_) | InputPromptKind::RefreshSecs
            ) && c == '.'
                && !prompt.buffer.contains('.');
            if (is_digit || is_dot_ok) && prompt.buffer.len() < 12 {
                prompt.buffer.push(c);
            }
        }
        _ => {}
    }
}

/// Parse the operator's typed buffer and apply it to the
/// prompt's target.  Failures (empty, out of range) leave the
/// prompt open so the operator can correct their input instead
/// of the change silently being dropped.
fn commit_input_prompt(ui: &mut UiState, shared: &Shared) {
    // Read the prompt state without holding a &mut so we can
    // mutate `ui.y_max_override` etc below without aliasing.
    let (kind, buffer) = match ui.input_prompt.as_ref() {
        Some(p) => (p.kind.clone(), p.buffer.clone()),
        None => return,
    };
    match kind {
        InputPromptKind::RefreshSecs => {
            let Ok(v) = buffer.trim().parse::<f64>() else {
                return;
            };
            // 50 ms floor: below that the sampler saturates
            // one CPU with no visible benefit on any real
            // terminal.  There is deliberately no upper bound
            // -- an operator on a slow link can dial the
            // cadence to minutes without the modal arguing
            // back.
            if !v.is_finite() || v < 0.050 {
                return;
            }
            let ns = (v * 1.0e9) as u64;
            shared.refresh_ns.store(ns, Ordering::Relaxed);
            ui.input_prompt = None;
        }
        InputPromptKind::YMax(kind) => {
            let trimmed = buffer.trim();
            if trimmed.is_empty() || trimmed == "0" {
                ui.y_max_override.remove(&kind);
                ui.input_prompt = None;
                return;
            }
            let Ok(v) = trimmed.parse::<f64>() else {
                return;
            };
            if !v.is_finite() || v <= 0.0 {
                return;
            }
            ui.y_max_override.insert(kind, v);
            ui.input_prompt = None;
        }
        InputPromptKind::SetThreads {
            vm_id,
            current,
            sticky,
        } => {
            let Ok(v) = buffer.trim().parse::<u32>() else {
                return;
            };
            if v == 0 {
                // Sanity: never accept "0 threads".  The
                // daemon would reject it too but bouncing
                // here keeps the operator inside the modal.
                return;
            }
            request_set_thread_count(shared, vm_id, current, v, sticky);
            ui.input_prompt = None;
        }
    }
}

/// Render the modal input prompt centred on the frame.
/// Mirrors the help overlay's centring math so the two
/// modals feel visually consistent.  A blinking-ish "_"
/// cursor is appended to the buffer so operators can see
/// where the next keystroke will land.
fn draw_input_prompt(f: &mut ratatui::Frame, ui: &UiState) {
    let Some(prompt) = ui.input_prompt.as_ref() else {
        return;
    };
    let full = f.area();
    let width = 60_u16.min(full.width);
    // 7 rows fit the default prompt (title/blank/input/blank
    // + Enter-line + top/bottom border).  SetThreads adds a
    // sticky-checkbox row plus its own blank spacer, so bump
    // the modal by 2 rows only for that variant.
    let height = match prompt.kind {
        InputPromptKind::SetThreads { .. } => 9_u16.min(full.height),
        _ => 7_u16.min(full.height),
    };
    let x = full.x + full.width.saturating_sub(width) / 2;
    let y = full.y + full.height.saturating_sub(height) / 2;
    let area = Rect {
        x,
        y,
        width,
        height,
    };
    f.render_widget(Clear, area);
    let theme = ui.theme();
    // The set-threads prompt gets an extra sticky-checkbox
    // row.  Every other prompt renders the compact 4-row body
    // it always had.
    let (extra_body_row, footer_hint) = match &prompt.kind {
        InputPromptKind::SetThreads { sticky, .. } => {
            let box_char = if *sticky { "[x]" } else { "[ ]" };
            (
                Some(Line::from(Span::styled(
                    format!("  {box_char} sticky   [Tab] toggle"),
                    Style::default().fg(theme.axis),
                ))),
                "  [Enter] commit    [Tab] sticky    [Esc] cancel",
            )
        }
        _ => (None, "  [Enter] commit    [Esc] cancel"),
    };
    let mut lines: Vec<Line> = vec![
        Line::from(""),
        Line::from(Span::styled(
            format!("  {}", prompt.kind.title()),
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            format!("  > {}_", prompt.buffer),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];
    if let Some(row) = extra_body_row {
        lines.push(row);
        lines.push(Line::from(""));
    }
    lines.push(Line::from(Span::styled(
        footer_hint,
        Style::default().fg(theme.axis),
    )));
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" input ")
        .style(Style::default().bg(theme.background));
    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Flip both the UI-local pause flag and the shared
/// AtomicBool that gates the sampler / rediscovery loops.
/// Capturing the freeze-time timestamp on the pause edge
/// keeps the X-axis exactly where it was when the operator
/// hit `p` instead of snapping back to `t=0`.
fn toggle_pause(ui: &mut UiState, shared: &Shared) {
    ui.paused = !ui.paused;
    shared.paused.store(ui.paused, Ordering::Relaxed);
    if ui.paused {
        ui.paused_t_frozen = Some(Instant::now().duration_since(shared.started).as_secs_f64());
    } else {
        ui.paused_t_frozen = None;
    }
}

/// Move the dashboard cursor within the currently-tracked
/// vm roster.  Wraps around at both ends; `focused = None`
/// (no cursor) latches to `Some(Vm(0))` on first
/// navigation.  When there are >=2 vms a virtual
/// `Focus::Total` slot appears at the end of the cycle so the
/// aggregate line can be highlighted the same way any real
/// vm can.
async fn move_focus(ui: &mut UiState, shared: &Shared, delta: i32) {
    let n = shared.vms.read().await.len();
    if n == 0 {
        ui.focused = None;
        return;
    }
    // Total is a slot only when there is more than one
    // vm -- with a single tracked vm the aggregate
    // and the vm trace are literally identical.
    let slots = if n >= 2 { n + 1 } else { n } as i32;
    let cur = match ui.focused {
        Some(Focus::Vm(i)) => i as i32,
        Some(Focus::Total) => n as i32,
        None => 0,
    };
    let next = (cur + delta).rem_euclid(slots);
    ui.focused = if n >= 2 && next as usize == n {
        Some(Focus::Total)
    } else {
        Some(Focus::Vm(next as usize))
    };
}

/// `+`/`-` hotkey: shift the focused vm's I/O thread count
/// by `delta` through the daemon's D-Bus interface. Refusals
/// are logged at WARN on the `iothread-tui` tracing target.
///
/// If no vm is focused the operation is a no-op; we also
/// refuse to actuate on `Focus::Total` because a broadcast
/// scale-up with a room-full of VMs is almost always a
/// mistake.  `sticky` mirrors the D-Bus `SetThreadCount`
/// verb's sticky flag: when true it pins the manual count on
/// the backend and disables auto-scaling until the operator
/// clears it (today by nudging again without sticky, which
/// the daemon interprets as "engine, take it back").
async fn nudge_thread_count(ui: &mut UiState, shared: &Shared, delta: i32, sticky: bool) {
    let idx = match ui.focused {
        Some(Focus::Vm(i)) => i,
        Some(Focus::Total) | None => return,
    };
    let target_arc = {
        let r = shared.vms.read().await;
        r.get(idx).cloned()
    };
    let Some(target) = target_arc else {
        return;
    };
    let (cur, id) = {
        let g = target.read().await;
        (g.status.thread_count, g.id.clone())
    };
    let next = cur as i32 + delta;
    if next < 1 {
        return;
    }
    request_set_thread_count(shared, id, cur, next as u32, sticky);
}

/// Fire-and-forget the daemon `SetThreadCount` call.  Shared
/// by `+`/`-` nudges and the numeric-input "set to N" modal
/// so both routes go through the same code path (and both
/// respect `sticky`).  A wire error is logged at WARN on the
/// `iothread-tui` target so operators see it in their own
/// shell (and in the daemon journal, which mirrors it).
fn request_set_thread_count(shared: &Shared, id: String, cur: u32, next: u32, sticky: bool) {
    let daemon = shared.daemon.clone();
    tokio::spawn(async move {
        if let Err(e) = daemon
            .call::<_, _, (u32, String)>("SetThreadCount", &(id.as_str(), next, sticky))
            .await
        {
            tracing::warn!(
                target: "iothread-tui",
                id = %id,
                from = cur,
                to = next,
                sticky,
                error = ?e,
                "D-Bus SetThreadCount refused"
            );
        }
    });
}

/// Connect to the daemon on the system bus.  Returns an error
/// (not `Option::None`) so `main` can log a clear diagnostic
/// and exit -- the TUI has no direct-backend fallback any
/// more, so an unreachable daemon is a fatal condition rather
/// than a soft degradation.  Uses the daemon's exported
/// constants so if the bus / interface / object path ever
/// moves, we get a compile error rather than a runtime
/// "verb not found".
async fn connect_daemon_proxy() -> Result<zbus::Proxy<'static>> {
    let conn = zbus::Connection::system()
        .await
        .context("connect to system D-Bus")?;
    let proxy = zbus::Proxy::new_owned(
        conn,
        DBUS_BUS_NAME.to_string(),
        DBUS_OBJECT_PATH.to_string(),
        DBUS_INTERFACE.to_string(),
    )
    .await
    .with_context(|| format!("attach to {} on {}", DBUS_INTERFACE, DBUS_OBJECT_PATH))?;
    // Cheapest liveness probe: ask for a snapshot up front.
    // A pending daemon (not-yet-registered service name) will
    // fail here, giving `main` a chance to bail loudly instead
    // of silently blocking every subsequent poll.  We also
    // special-case `UnknownMethod`, which is the tell-tale
    // sign that only the TUI got upgraded and the daemon on
    // this host is still on a build that predates the
    // `GetSnapshot` verb -- the July 2026 rollout the operator
    // most commonly runs into.
    if let Err(e) = proxy.call::<_, _, String>("GetSnapshot", &()).await {
        let msg = e.to_string();
        if msg.contains("UnknownMethod") || msg.contains("Unknown method") {
            return Err(anyhow!(
                "the io-thread-controller daemon on this host does not implement \
                 GetSnapshot -- please redeploy the daemon (io-thread-controller \
                 binary + restart the systemd unit); the TUI expects the July \
                 2026 D-Bus surface.  Original error: {e}"
            ));
        }
        return Err(anyhow!(
            "initial GetSnapshot failed ({e}); is the daemon running?"
        ));
    }
    Ok(proxy)
}

/// Non-blocking crossterm key poll.  100 ms poll granularity
/// keeps the loop responsive without pegging a CPU.  Returns
/// both the key and its modifiers so the caller can
/// distinguish `+` from `Ctrl-+` (sticky-nudge hotkey).
async fn read_key_event() -> Result<Option<(KeyCode, KeyModifiers)>> {
    tokio::task::spawn_blocking(|| -> Result<Option<(KeyCode, KeyModifiers)>> {
        if !event::poll(Duration::from_millis(100))? {
            return Ok(None);
        }
        match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => {
                // Ctrl-C is our secondary quit trigger.
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    return Ok(Some((KeyCode::Esc, KeyModifiers::NONE)));
                }
                Ok(Some((k.code, k.modifiers)))
            }
            _ => Ok(None),
        }
    })
    .await?
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw a single frame: title bar, N stacked plots (equal
/// height splits), then the dashboard.
fn draw_frame(
    f: &mut ratatui::Frame,
    cli: &Cli,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
    refresh_secs: f64,
) {
    // Paint the theme background under everything else so a
    // light-mode theme actually looks light.  A `Reset` in the
    // theme means "leave the terminal alone" -- we still paint
    // the block (cheap; kept the code path uniform) but with
    // the fully transparent default fg/bg pair.
    let bg_style = Style::default().bg(ui.theme().background);
    f.render_widget(Block::default().style(bg_style), f.area());

    let dashboard_height =
        (snap.vms.len() as u16).saturating_add(if snap.vms.len() >= 2 { 5 } else { 4 });
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(6),
            Constraint::Length(dashboard_height),
        ])
        .split(f.area());

    draw_title(f, vertical[0], cli, ui, snap.vms.len(), refresh_secs);
    draw_plots(f, vertical[1], cli, ui, snap, x_lo, x_hi);
    draw_dashboard(f, vertical[2], ui, snap);

    // Modal help overlay: paints last so it lands on top of
    // everything else.  Kept intentionally sparse -- the point
    // is a hotkey crib sheet, not a manual.
    if ui.show_help {
        draw_help_overlay(f, ui);
    }
    // Modal numeric input (currently: refresh cadence).
    // Rendered *after* the help overlay so an operator who
    // opens the refresh prompt while help is on still sees
    // the prompt on top.
    if ui.input_prompt.is_some() {
        draw_input_prompt(f, ui);
    }
    // Column-toggle modal.  Rendered last so it always wins
    // the paint stack over the input prompt / help / plots
    // beneath.
    if ui.column_picker.is_some() {
        draw_column_picker(f, ui);
    }
    // Daemon-lost overlay.  Rendered dead-last so it always
    // sits on top of everything else -- the plot underneath
    // is stale by definition once we reach this branch, so
    // there is no case where hiding it behind another modal
    // would actually be useful.
    if let Some(err) = &snap.daemon_error {
        draw_daemon_lost_overlay(f, ui, err);
    }
}

/// Full-width modal painted when [`Shared::daemon_failed_streak`]
/// passes [`DAEMON_LOST_STREAK`].  The overlay quotes the last
/// D-Bus error verbatim so an operator can tell "systemd
/// stopped it" from "the bus kicked us" without leaving the
/// TUI to check `journalctl`.  Kept read-only for now (the
/// old plot data is still visible underneath) so the operator
/// can hit `q` to leave once they have seen it.
fn draw_daemon_lost_overlay(f: &mut ratatui::Frame, ui: &UiState, err: &str) {
    let full = f.area();
    let width = 74_u16.min(full.width);
    let height = 8_u16.min(full.height);
    let area = Rect {
        x: full.x + full.width.saturating_sub(width) / 2,
        y: full.y + full.height.saturating_sub(height) / 2,
        width,
        height,
    };
    let theme = ui.theme();
    // Repaint so the message stays legible over whatever plot
    // slice happens to be underneath.
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" daemon lost ")
        .style(
            Style::default()
                .bg(theme.background)
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
        );
    // Truncate over-long D-Bus error strings so a stack trace
    // pasted into the JSON body does not blow the modal past
    // the visible width.
    let mut trimmed = err.to_string();
    let max = (width as usize).saturating_sub(4);
    if trimmed.len() > max {
        trimmed.truncate(max.saturating_sub(1));
        trimmed.push('…');
    }
    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "the io-thread-controller daemon stopped responding to GetSnapshot",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!("last error: {trimmed}")),
        Line::from(""),
        Line::from("press q to quit; the TUI keeps retrying in the background"),
    ];
    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Centred modal listing every hotkey the TUI understands.
/// Rendered on top of the main frame; any key press dismisses
/// it (see [`handle_key`]).
fn draw_help_overlay(f: &mut ratatui::Frame, ui: &UiState) {
    let full = f.area();
    let width = 72_u16.min(full.width);
    let theme = ui.theme();
    let lines: Vec<Line> = vec![
        Line::from(Span::styled(
            "hotkeys",
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("  q / Esc / Ctrl-C   quit"),
        Line::from("  h / ?              show/hide this help"),
        Line::from("  p / Space          pause (freezes plot + sampler)"),
        Line::from("  m                  toggle IOPS <-> bandwidth"),
        Line::from("  r                  prompt for refresh cadence in seconds"),
        Line::from("  c                  toggle dashboard columns"),
        Line::from("  t                  cycle theme (light/dark, vim, solarized, gruvbox)"),
        Line::from("  1 2 3 4 5          toggle perf / latency / cpu / qd / us-per-iop panes"),
        Line::from("  ↑ / k              focus previous vm (hides others)"),
        Line::from("  ↓ / j              focus next vm (last slot = TOTAL)"),
        Line::from("  Esc                clear focus (show every vm again)"),
        Line::from("  + / =              scale focused vm +1 thread"),
        Line::from("  -                  scale focused vm -1 thread"),
        Line::from("  n                  set focused vm thread count (with sticky toggle)"),
        Line::from("  s                  toggle sticky mode for +/- (title bar shows [STICKY])"),
        Line::from(""),
        Line::from("  l                  toggle log/linear Y-axis for every plot"),
        Line::from("  y                  set Y-max for the first active plot (blank = clear)"),
        Line::from("  z                  reset autoscale (drop old samples so Y shrinks)"),
        Line::from(
            "  x                  toggle X-axis labels: elapsed seconds vs wall-clock (UTC)",
        ),
        Line::from(""),
        Line::from(Span::styled(
            "sticky: when a daemon is on D-Bus, +/- go through",
            Style::default().fg(theme.axis),
        )),
        Line::from(Span::styled(
            "SetThreadCount(sticky=true) so the engine leaves the",
            Style::default().fg(theme.axis),
        )),
        Line::from(Span::styled(
            "instance alone until you clear sticky.",
            Style::default().fg(theme.axis),
        )),
        Line::from(""),
        Line::from(Span::styled(
            format!("theme: {}", THEMES[ui.theme_idx].name),
            Style::default().fg(theme.axis),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "press any key to dismiss",
            Style::default().fg(theme.axis),
        )),
    ];
    let height = (lines.len() as u16 + 2).min(full.height);
    let x = full.x + full.width.saturating_sub(width) / 2;
    let y = full.y + full.height.saturating_sub(height) / 2;
    let area = Rect {
        x,
        y,
        width,
        height,
    };
    // Wipe the cells first so plot dots do not leak through
    // the modal.
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" help ")
        .style(Style::default().bg(theme.background));
    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// One-line title bar: tool name, current perf metric,
/// refresh cadence, and the hotkey cheat-sheet.
fn draw_title(
    f: &mut ratatui::Frame,
    area: Rect,
    cli: &Cli,
    ui: &UiState,
    n_vms: usize,
    refresh_secs: f64,
) {
    let paused = if ui.paused { " [PAUSED]" } else { "" };
    let title_line = Line::from(vec![
        Span::styled(
            "iothread-tui",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {}", ui.perf_metric.axis_label(cli.iop_size))),
        Span::raw(format!("  refresh {}", fmt_duration_secs(refresh_secs))),
        Span::raw(format!("  vms={n_vms}")),
        // The theme name used to appear here as
        // "theme=default-dark".  Operators reading the title
        // bar top-to-bottom during a live incident do not need
        // to see the palette; if they want to check which theme
        // they are on, the help overlay (`h`) still lists it.
        Span::styled(paused, Style::default().fg(Color::Yellow)),
        Span::styled(
            if ui.sticky_mode { " [STICKY]" } else { "" },
            Style::default().fg(ui.theme().accent),
        ),
        Span::raw("   "),
        Span::styled(
            "[h]elp [q]uit [m]etric [p]ause [r]efresh [t]heme 12345 plots ↑↓ focus s+/- threads",
            Style::default().fg(ui.theme().axis),
        ),
    ]);
    f.render_widget(title_line, area);
}

/// Stack every active plot into the given rectangle with
/// equal-height splits.
fn draw_plots(
    f: &mut ratatui::Frame,
    area: Rect,
    cli: &Cli,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
) {
    let plots = &ui.active_plots;
    if plots.is_empty() {
        return;
    }
    let constraints: Vec<Constraint> = plots
        .iter()
        .map(|_| Constraint::Ratio(1, plots.len() as u32))
        .collect();
    let panes = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);
    for (i, kind) in plots.iter().enumerate() {
        match kind {
            PlotKind::Perf => draw_perf_plot(f, panes[i], cli, ui, snap, x_lo, x_hi),
            PlotKind::Latency => draw_latency_plot(f, panes[i], ui, snap, x_lo, x_hi),
            PlotKind::Cpu => draw_cpu_plot(f, panes[i], ui, snap, x_lo, x_hi),
            PlotKind::QueueDepth => draw_qd_plot(f, panes[i], ui, snap, x_lo, x_hi),
            PlotKind::CpuPerIop => draw_cpu_per_iop_plot(f, panes[i], ui, snap, x_lo, x_hi),
        }
    }
}

/// Perf pane: one line per vm in the perf metric, a grey
/// aggregate line when >=2 vms, and text annotations
/// `"<old>→<new>"` at every scale event (green up, red down).
fn draw_perf_plot(
    f: &mut ratatui::Frame,
    area: Rect,
    cli: &Cli,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
) {
    // Collect line series + annotation events + Y-max.
    //
    // Draw order matters: series pushed *first* render *first*,
    // so subsequent series overpaint them.  We push the
    // aggregate line first (grey) so per-VM traces render
    // on top -- otherwise a two-vm fleet with only one
    // active VM ends up with a grey aggregate line covering the
    // busy vm's colour, and the operator loses the
    // per-VM hue in the plot.
    let mut series: Vec<(Color, Vec<(f64, f64)>)> = Vec::new();
    let mut y_max = 1.0_f64;
    let mut annots_by_vm: Vec<AnnotSeries> = Vec::new();

    if snap.vms.len() >= 2 && !snap.aggregate.is_empty() {
        let mut line = Vec::with_capacity(snap.aggregate.len());
        for s in &snap.aggregate {
            if s.t < x_lo {
                continue;
            }
            let y = match ui.perf_metric {
                PerfMetric::Iops => s.iops,
                PerfMetric::Bandwidth => s.bw_mb,
            };
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.theme().aggregate, line));
    }

    for d in &snap.vms {
        let mut line = Vec::with_capacity(d.samples.len());
        for s in &d.samples {
            if s.t < x_lo {
                continue;
            }
            let y = match ui.perf_metric {
                PerfMetric::Iops => s.iops,
                PerfMetric::Bandwidth => s.bw_mb,
            };
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        let color = ui.vm_color(d.color_idx);
        series.push((color, line));

        let ann: Vec<AnnotPoint> = d
            .annots
            .iter()
            .filter(|a| a.t >= x_lo)
            .map(|a| {
                let y = match ui.perf_metric {
                    PerfMetric::Iops => a.y_value,
                    // In bandwidth mode we captured the "larger of
                    // the two rates" so clamp to the visible Y so
                    // the annotation stays on-screen.
                    PerfMetric::Bandwidth => a.y_value.min(y_max),
                };
                (a.t, a.old_n, a.new_n, y)
            })
            .collect();
        annots_by_vm.push((color, ann));
    }

    let y_max = y_max * 1.15;
    let y_label = ui.perf_metric.axis_label(cli.iop_size);
    let hi = highlight_indices_perf(ui, snap);
    // When focused on a single vm, hide every other
    // vm's annotations so the plot only shows the arrows
    // the operator cares about.  `annots_by_vm[i]` maps to
    // vm `i` (no aggregate slot, aggregate has no annots),
    // so filter by vm index rather than the per-plot
    // series index.
    if let Some(Focus::Vm(fi)) = ui.focused {
        for (i, (_, list)) in annots_by_vm.iter_mut().enumerate() {
            if i != fi {
                list.clear();
            }
        }
    } else if matches!(ui.focused, Some(Focus::Total)) {
        for (_, list) in annots_by_vm.iter_mut() {
            list.clear();
        }
    }
    render_line_canvas(
        f,
        area,
        &format!("perf ({y_label})"),
        x_lo,
        x_hi,
        0.0,
        y_max,
        &series,
        &annots_by_vm,
        /* y_humanize= */ ui.perf_metric == PerfMetric::Iops,
        &hi,
        ui.theme().axis,
        ui.log_y_scale,
        ui.y_max_override.get(&PlotKind::Perf).copied(),
        ui.wall_clock_x,
        ui.started_unix_s,
    );
}

/// Map the operator's current `Focus` to the series index (or
/// indices) the perf / CPU / QD panes should render with the
/// "thick" stroke.  Series order in each of those panes is:
///
///   * `[0]`        = aggregate (grey), when there are >=2 vms.  Drawn first so
///     per-VM lines overpaint it, keeping the vm hue visible even when the
///     aggregate trace overlaps.
///   * `[base..]`   = per-VM lines, in `snap.vms` order, where `base = 1` when
///     the aggregate is present and `0` otherwise.
///
/// `Focus::Total` therefore highlights index 0 iff the
/// aggregate is being plotted; a plain `Focus::Vm(i)`
/// highlights `base + i`.
fn highlight_indices_perf(ui: &UiState, snap: &RenderSnapshot) -> Vec<usize> {
    let agg = snap.vms.len() >= 2;
    let base = if agg { 1 } else { 0 };
    match ui.focused {
        Some(Focus::Vm(i)) if i < snap.vms.len() => vec![base + i],
        Some(Focus::Total) if agg => vec![0],
        _ => Vec::new(),
    }
}

/// Latency pane variant: each vm contributes *two* series
/// (read = index 2i, write = 2i+1), so a focused vm
/// highlights both rows.  `Focus::Total` is a no-op here
/// because the latency pane does not render an aggregate.
fn highlight_indices_latency(ui: &UiState, snap: &RenderSnapshot) -> Vec<usize> {
    // Latency plot pushes 4 series per vm (read avg,
    // read p99, write avg, write p99); focus therefore lights
    // up a full run of 4.
    match ui.focused {
        Some(Focus::Vm(i)) if i < snap.vms.len() => {
            let base = 4 * i;
            vec![base, base + 1, base + 2, base + 3]
        }
        _ => Vec::new(),
    }
}

/// Latency pane: four lines per vm --
///   * read avg  (bright)
///   * read p99  (dim)
///   * write avg (dim)
///   * write p99 (dim-of-dim)
///
/// The `avg` line is the histogram-derived arithmetic mean
/// (see [`SnapshotLatency::avg`]); the `p99` line remains as
/// the tail marker.  Together they give operators both the
/// "typical case" and the "worst common case" for each
/// direction on one pane without needing to scan the
/// dashboard columns for the percentile triple.
///
/// Simpler aggregation than the perf plot: pass-through per
/// vm, no fleet-wide sum -- fleet-wide latency
/// aggregation is not well-defined (Simpson's paradox and
/// all).
fn draw_latency_plot(
    f: &mut ratatui::Frame,
    area: Rect,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
) {
    let mut series: Vec<(Color, Vec<(f64, f64)>)> = Vec::new();
    let mut y_max = 1.0_f64;
    for d in &snap.vms {
        let mut r_avg = Vec::new();
        let mut r_p99 = Vec::new();
        let mut w_avg = Vec::new();
        let mut w_p99 = Vec::new();
        for s in &d.samples {
            if s.t < x_lo {
                continue;
            }
            if let Some(avg) = s.lat_r_avg {
                let y = avg as f64;
                if y > y_max {
                    y_max = y;
                }
                r_avg.push((s.t, y));
            }
            if let Some((_, _, p99)) = s.lat_r {
                let y = p99 as f64;
                if y > y_max {
                    y_max = y;
                }
                r_p99.push((s.t, y));
            }
            if let Some(avg) = s.lat_w_avg {
                let y = avg as f64;
                if y > y_max {
                    y_max = y;
                }
                w_avg.push((s.t, y));
            }
            if let Some((_, _, p99)) = s.lat_w {
                let y = p99 as f64;
                if y > y_max {
                    y_max = y;
                }
                w_p99.push((s.t, y));
            }
        }
        let color = ui.vm_color(d.color_idx);
        // Order matters: the `highlight_indices_latency`
        // helper below expects the same 4-in-a-row layout to
        // pick out the currently-focused vm.
        series.push((color, r_avg));
        series.push((dim(color), r_p99));
        series.push((dim(color), w_avg));
        series.push((dim(dim(color)), w_p99));
    }
    let y_max = y_max * 1.15;
    let hi = highlight_indices_latency(ui, snap);
    render_line_canvas(
        f,
        area,
        "latency (µs) — bright=read avg, dim=read p99 / write avg / write p99",
        x_lo,
        x_hi,
        0.0,
        y_max,
        &series,
        &[],
        /* y_humanize= */ true,
        &hi,
        ui.theme().axis,
        ui.log_y_scale,
        ui.y_max_override.get(&PlotKind::Latency).copied(),
        ui.wall_clock_x,
        ui.started_unix_s,
    );
}

/// CPU pane: `per_thread_util * num_threads * 100` per
/// vm (i.e. total pool CPU %, so a 4-thread pool
/// pegged at 100% reads as 400%).  Aggregate line sums the
/// per-VM figures.
fn draw_cpu_plot(
    f: &mut ratatui::Frame,
    area: Rect,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
) {
    // Aggregate first (see `draw_perf_plot` for the rationale).
    let mut series: Vec<(Color, Vec<(f64, f64)>)> = Vec::new();
    let mut y_max = 100.0_f64;
    if snap.vms.len() >= 2 {
        let mut line = Vec::with_capacity(snap.aggregate.len());
        for s in &snap.aggregate {
            if s.t < x_lo {
                continue;
            }
            let y = s.cpu_pct;
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.theme().aggregate, line));
    }
    for d in &snap.vms {
        let mut line = Vec::with_capacity(d.samples.len());
        for s in &d.samples {
            if s.t < x_lo {
                continue;
            }
            let y = s.cpu_pct;
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.vm_color(d.color_idx), line));
    }
    let hi = highlight_indices_perf(ui, snap);
    render_line_canvas(
        f,
        area,
        "cpu (%)",
        x_lo,
        x_hi,
        0.0,
        y_max * 1.15,
        &series,
        &[],
        /* y_humanize= */ false,
        &hi,
        ui.theme().axis,
        ui.log_y_scale,
        ui.y_max_override.get(&PlotKind::Cpu).copied(),
        ui.wall_clock_x,
        ui.started_unix_s,
    );
}

/// Queue-depth pane: per-VM `vq_depth_total`, plus an
/// aggregate when >=2.
fn draw_qd_plot(
    f: &mut ratatui::Frame,
    area: Rect,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
) {
    // Aggregate first (see `draw_perf_plot` for the rationale).
    let mut series: Vec<(Color, Vec<(f64, f64)>)> = Vec::new();
    let mut y_max = 1.0_f64;
    if snap.vms.len() >= 2 {
        let mut line = Vec::with_capacity(snap.aggregate.len());
        for s in &snap.aggregate {
            if s.t < x_lo {
                continue;
            }
            let y = s.qd;
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.theme().aggregate, line));
    }
    for d in &snap.vms {
        let mut line = Vec::with_capacity(d.samples.len());
        for s in &d.samples {
            if s.t < x_lo {
                continue;
            }
            let y = s.qd;
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.vm_color(d.color_idx), line));
    }
    let hi = highlight_indices_perf(ui, snap);
    render_line_canvas(
        f,
        area,
        "queue depth",
        x_lo,
        x_hi,
        0.0,
        y_max * 1.15,
        &series,
        &[],
        /* y_humanize= */ true,
        &hi,
        ui.theme().axis,
        ui.log_y_scale,
        ui.y_max_override.get(&PlotKind::QueueDepth).copied(),
        ui.wall_clock_x,
        ui.started_unix_s,
    );
}

/// Efficiency pane: CPU microseconds per IOP.  Lower is
/// better -- an unchanged CPU-us/IOP across a scale-up means
/// the extra worker cost the same CPU per request, i.e. did
/// not buy any per-request efficiency.  `NaN` samples (idle
/// ticks) are dropped from the line rather than dragging the
/// plot to infinity.
fn draw_cpu_per_iop_plot(
    f: &mut ratatui::Frame,
    area: Rect,
    ui: &UiState,
    snap: &RenderSnapshot,
    x_lo: f64,
    x_hi: f64,
) {
    // Aggregate first (see `draw_perf_plot` for the rationale).
    let mut series: Vec<(Color, Vec<(f64, f64)>)> = Vec::new();
    let mut y_max = 1.0_f64;
    if snap.vms.len() >= 2 {
        let mut line = Vec::with_capacity(snap.aggregate.len());
        for s in &snap.aggregate {
            if s.t < x_lo {
                continue;
            }
            if !s.cpu_us_per_iop.is_finite() {
                continue;
            }
            let y = s.cpu_us_per_iop;
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.theme().aggregate, line));
    }
    for d in &snap.vms {
        let mut line = Vec::with_capacity(d.samples.len());
        for s in &d.samples {
            if s.t < x_lo || !s.cpu_us_per_iop.is_finite() {
                continue;
            }
            let y = s.cpu_us_per_iop;
            if y > y_max {
                y_max = y;
            }
            line.push((s.t, y));
        }
        series.push((ui.vm_color(d.color_idx), line));
    }
    let hi = highlight_indices_perf(ui, snap);
    render_line_canvas(
        f,
        area,
        "cpu (µs / iop) — lower is better",
        x_lo,
        x_hi,
        0.0,
        y_max * 1.15,
        &series,
        &[],
        /* y_humanize= */ false,
        &hi,
        ui.theme().axis,
        ui.log_y_scale,
        ui.y_max_override.get(&PlotKind::CpuPerIop).copied(),
        ui.wall_clock_x,
        ui.started_unix_s,
    );
}

/// Format the qd column value as `avg/median/total`.  Falls
/// back to `-` on backends that do not expose per-VQ data.
fn fmt_qd(s: &LatestStatus) -> String {
    match (s.qd_avg, s.qd_median, s.qd_total) {
        (Some(a), Some(m), Some(t)) => format!("{:.1}/{}/{}", a, m, t),
        _ => "-".into(),
    }
}

/// Format the aggregate qd cell.  We can only meaningfully
/// sum totals across vms; avg / median across the whole
/// fleet needs per-VQ visibility which the daemon does not
/// currently aggregate, so we render those two slots as `-`.
fn fmt_qd_total(qd_total: Option<u64>) -> String {
    match qd_total {
        Some(t) => format!("-/-/{}", t),
        None => "-".into(),
    }
}

/// Render one dashboard cell for a given `(column, status)`
/// pair.  Kept as a free function (rather than a method on
/// [`DashColumn`]) because both the per-VM row and the
/// aggregate TOTAL row consume it, and the TOTAL row passes a
/// synthesised [`LatestStatus`] rather than a real one.
fn dashboard_cell(col: DashColumn, s: &LatestStatus) -> Cell<'static> {
    match col {
        DashColumn::Id => Cell::from("".to_string()), // filled in by the caller
        DashColumn::Threads => {
            // Append a `*` when the daemon has marked this
            // instance manual-sticky.  Gives operators a
            // visual cue that the engine is currently
            // ignoring this vm, without needing to switch
            // panes to check `busctl`.
            let s_txt = if s.sticky {
                format!("{}*", s.thread_count)
            } else {
                s.thread_count.to_string()
            };
            Cell::from(s_txt)
        }
        DashColumn::Iops => Cell::from(format!(
            "{}/{}/{}",
            humanize(s.iops_r),
            humanize(s.iops_w),
            humanize(s.iops_o)
        )),
        DashColumn::Bandwidth => Cell::from(format!(
            "{} / {}",
            fmt_bandwidth_mbps(s.bw_r_mb),
            fmt_bandwidth_mbps(s.bw_w_mb)
        )),
        DashColumn::Cpu => Cell::from(fmt_cpu_triple(
            s.cpu_pct_avg,
            s.cpu_pct_median,
            s.cpu_pct_total,
            s.cpu_pct,
        )),
        DashColumn::UsPerIop => Cell::from(fmt_cpu_per_iop(s.cpu_us_per_iop)),
        DashColumn::LatRead => Cell::from(fmt_lat(s.lat_r_us)),
        DashColumn::LatWrite => Cell::from(fmt_lat(s.lat_w_us)),
        DashColumn::QueueDepth => Cell::from(fmt_qd(s)),
        DashColumn::NumQueues => Cell::from(
            s.num_queues
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
        ),
    }
}

/// Bottom dashboard: one row per vm, plus a TOTAL row
/// when there are >=2 vms.  Column set is operator-
/// configurable via the `c` modal ([`DashColumn::ALL`] carries
/// the canonical display order); the focused row is
/// highlighted so `+` / `-` land somewhere obvious.
fn draw_dashboard(f: &mut ratatui::Frame, area: Rect, ui: &UiState, snap: &RenderSnapshot) {
    let visible: Vec<DashColumn> = ui.visible_columns().collect();
    let header = Row::new(
        visible
            .iter()
            .map(|c| Cell::from(c.label()).style(Style::default().add_modifier(Modifier::BOLD)))
            .collect::<Vec<_>>(),
    )
    .height(1);

    let mut rows: Vec<Row> = Vec::with_capacity(snap.vms.len() + 1);
    let mut total = LatestStatus::default();

    for (i, d) in snap.vms.iter().enumerate() {
        let s = &d.status;
        total.thread_count = total.thread_count.saturating_add(s.thread_count);
        total.iops_r += s.iops_r;
        total.iops_w += s.iops_w;
        total.iops_o += s.iops_o;
        total.bw_r_mb += s.bw_r_mb;
        total.bw_w_mb += s.bw_w_mb;
        total.cpu_pct += s.cpu_pct;
        total.qd_total = Some(
            total
                .qd_total
                .unwrap_or(0)
                .saturating_add(s.qd_total.unwrap_or(0)),
        );

        let focused = ui.focused == Some(Focus::Vm(i));
        let base = Style::default().fg(ui.vm_color(d.color_idx));
        // Dim the row when the daemon reports the last refresh
        // failed for this instance -- typically means the
        // backend went away and the vm is about to be
        // reaped.  Focused rows still highlight normally so
        // the operator can still see what they have picked.
        let mut row_style = if focused {
            base.add_modifier(Modifier::REVERSED)
        } else {
            base
        };
        if !s.alive {
            row_style = row_style.add_modifier(Modifier::DIM);
        }
        let cells: Vec<Cell> = visible
            .iter()
            .map(|&c| {
                if c == DashColumn::Id {
                    Cell::from(shorten_id(&d.id, d.vcpu_count))
                } else {
                    dashboard_cell(c, s)
                }
            })
            .collect();
        rows.push(Row::new(cells).style(row_style));
    }

    if snap.vms.len() >= 2 {
        let focused_total = ui.focused == Some(Focus::Total);
        let base = Style::default()
            .fg(ui.theme().aggregate)
            .add_modifier(Modifier::BOLD);
        let row_style = if focused_total {
            base.add_modifier(Modifier::REVERSED)
        } else {
            base
        };
        // Fleet-wide us/iop is derived from the fleet totals
        // rather than being a mean over per-VM figures --
        // the daemon does the same thing on its aggregate
        // status line.
        let total_iops = total.iops_r + total.iops_w + total.iops_o;
        total.cpu_us_per_iop = if total_iops > 0.0 {
            (total.cpu_pct / 100.0) * 1_000_000.0 / total_iops
        } else {
            f64::NAN
        };
        let cells: Vec<Cell> = visible
            .iter()
            .map(|&c| match c {
                DashColumn::Id => Cell::from("TOTAL"),
                DashColumn::LatRead | DashColumn::LatWrite => Cell::from("-".to_string()),
                DashColumn::QueueDepth => Cell::from(fmt_qd_total(total.qd_total)),
                DashColumn::NumQueues => Cell::from("-".to_string()),
                other => dashboard_cell(other, &total),
            })
            .collect();
        rows.push(Row::new(cells).style(row_style));
    }

    let widths: Vec<Constraint> = visible
        .iter()
        .map(|c| Constraint::Length(c.width()))
        .collect();
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("vms"));
    f.render_widget(table, area);
}

// ---------------------------------------------------------------------------
// Low-level Canvas renderer for every line plot
// ---------------------------------------------------------------------------

/// Render a titled, bordered Canvas: a series of coloured
/// polylines plus (optional) per-line text annotations.  The
/// Y-axis auto-scales; ticks use `humanize()` when
/// `y_humanize` is on so >=10k values collapse to "10.0k",
/// ">=1M" to "1.0M", etc.
///
/// `series` is a slice of `(colour, points)`; `annots` is a
/// parallel slice of `(colour, [(t, old, new, y), ...])`.
/// Annotations render as `"<old>→<new>"` with green for
/// scale-ups and red for scale-downs.
#[allow(clippy::too_many_arguments)]
fn render_line_canvas(
    f: &mut ratatui::Frame,
    area: Rect,
    title: &str,
    x_lo: f64,
    x_hi: f64,
    y_lo: f64,
    y_hi: f64,
    series: &[(Color, Vec<(f64, f64)>)],
    annots: &[AnnotSeries],
    y_humanize: bool,
    // `highlight`: series indices to draw at all.  When
    // non-empty, only these indices render -- so focusing a
    // single vm hides every other vm (and the
    // aggregate when a per-VM focus is in effect).  When
    // empty, every series draws normally.  This is the
    // "highlighting hides everything else" ergonomics the
    // operators asked for: it is unambiguous which line the
    // operator is tracking, no matter how tightly the fleet
    // clusters.
    highlight: &[usize],
    axis_color: Color,
    // Log-scale flag.  When on, every series y-value is
    // transformed by `log10(v+1)` before painting and the
    // Y-axis labels are still shown as raw (linear) values so
    // the operator's mental model stays "the trace hit N
    // ops".  `log10(1) = 0` keeps zero-valued samples on the
    // baseline.
    log_y: bool,
    // Manual Y-max override in *raw* units (pre-transform).
    // `None` means "autoscale from data" (the historical
    // behaviour); `Some(v)` locks the top of the Y axis at
    // `v` even if data would fit lower or bulge higher.  Set
    // via the `y` hotkey.
    y_max_override: Option<f64>,
    // X-axis time-label mode.  When `false` (the default) the
    // canvas renders labels as elapsed seconds ("-30s" ... "0s").
    // When `true` the labels are `HH:MM:SS` local wall-clock,
    // derived from `x_wall_clock_epoch_s + t`.  Independent of
    // the log toggle -- the X axis is always linear on time.
    x_wall_clock: bool,
    x_wall_clock_epoch_s: u64,
) {
    // Manual Y-max wins over data-driven autoscale so the
    // operator can lock a comparison scale across scale-up
    // events.
    let y_hi_raw = match y_max_override {
        Some(v) => v.max(y_lo + 1.0),
        None => y_hi.max(y_lo + 1.0),
    };
    let ty = move |v: f64| if log_y { (v + 1.0).log10() } else { v };
    let y_lo_t = ty(y_lo);
    let y_hi_t = ty(y_hi_raw).max(y_lo_t + 1e-9);
    // Owned copies for the closure -- Canvas' paint fn is
    // `Fn`, so it cannot capture `&mut`.  Apply the log
    // transform up front so the paint fn stays a pure line
    // draw.
    let series: Vec<(Color, Vec<(f64, f64)>)> = series
        .iter()
        .map(|(c, v)| (*c, v.iter().map(|(x, y)| (*x, ty(*y))).collect()))
        .collect();
    let annots: Vec<AnnotSeries> = annots
        .iter()
        .map(|(c, v)| {
            (
                *c,
                v.iter().map(|(t, o, n, y)| (*t, *o, *n, ty(*y))).collect(),
            )
        })
        .collect();
    let highlight: Vec<usize> = highlight.to_vec();
    // "Focus mode": non-empty `highlight` selects the ONE
    // (or few) series the operator wants to see -- draw only
    // them.  Everything else stays hidden so the plot never
    // gets crowded with lines the operator is not chasing.
    // Empty `highlight` reverts to "draw everything".
    let focus_hide = !highlight.is_empty();

    let mut title_spans = vec![
        Span::styled(
            title.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   y="),
        Span::raw(if y_humanize {
            humanize(y_hi_raw)
        } else {
            format!("{y_hi_raw:.0}")
        }),
    ];
    if log_y {
        title_spans.push(Span::styled("  [log]", Style::default().fg(axis_color)));
    }
    if y_max_override.is_some() {
        title_spans.push(Span::styled("  [locked]", Style::default().fg(axis_color)));
    }
    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::from(title_spans));

    let canvas = Canvas::default()
        .block(block)
        .marker(Marker::Braille)
        .x_bounds([x_lo, x_hi])
        .y_bounds([y_lo_t, y_hi_t])
        .paint(move |ctx| {
            // Data lines.  In focus mode we only paint the
            // highlighted series -- everything else is
            // suppressed so the operator sees a clean plot of
            // just the trace(s) they picked.  This is the
            // "hide all others" behaviour operators asked for:
            // making the focused line bolder was not
            // sufficient when a busy fleet has ten lines
            // stacked on top of each other.
            for (i, (color, points)) in series.iter().enumerate() {
                if focus_hide && !highlight.contains(&i) {
                    continue;
                }
                for w in points.windows(2) {
                    ctx.draw(&CanvasLine {
                        x1: w[0].0,
                        y1: w[0].1,
                        x2: w[1].0,
                        y2: w[1].1,
                        color: *color,
                    });
                }
            }
            // Text annotations.  The direction (green =
            // scale-up, red = scale-down) rides on `fg` so
            // the arrow itself is unambiguous; the vm's
            // own line colour rides on `bg` so the operator
            // can trace an annotation back to its line even
            // when several lines cluster inside one cell.
            for (color, list) in &annots {
                for (t, old_n, new_n, y) in list {
                    let (arrow_color, marker_text) = if new_n >= old_n {
                        (Color::Green, format!("{old_n}→{new_n}"))
                    } else {
                        (Color::Red, format!("{old_n}→{new_n}"))
                    };
                    ctx.print(
                        *t,
                        *y,
                        Line::from(Span::styled(
                            marker_text,
                            Style::default()
                                .fg(arrow_color)
                                .bg(*color)
                                .add_modifier(Modifier::BOLD),
                        )),
                    );
                }
            }
            // Y-axis labels: place at min/mid/max of the
            // *transformed* range so they land on the correct
            // rows, but display the *raw* value they map back
            // to.  In linear mode the mid is the arithmetic
            // mean of y_lo/y_hi_raw; in log mode we snap
            // labels to friendly decades between the range
            // ends so the reader gets a "10 / 100 / 1k"
            // spread instead of odd non-round numbers.
            let lbl = |v: f64| -> String {
                if y_humanize {
                    humanize(v)
                } else {
                    format!("{v:.0}")
                }
            };
            if log_y {
                // In log mode we want the three shown values
                // to be evenly spaced *on the log axis*.  Pick
                // `y_lo`, geometric-mid, `y_hi_raw`.
                let geo_mid = ((y_lo.max(1.0)) * y_hi_raw).sqrt();
                ctx.print(
                    x_lo,
                    y_hi_t,
                    Line::from(Span::styled(lbl(y_hi_raw), Style::default().fg(axis_color))),
                );
                ctx.print(
                    x_lo,
                    ty(geo_mid),
                    Line::from(Span::styled(lbl(geo_mid), Style::default().fg(axis_color))),
                );
                ctx.print(
                    x_lo,
                    y_lo_t,
                    Line::from(Span::styled(lbl(y_lo), Style::default().fg(axis_color))),
                );
            } else {
                let mid_raw = (y_lo + y_hi_raw) / 2.0;
                ctx.print(
                    x_lo,
                    y_hi_t,
                    Line::from(Span::styled(lbl(y_hi_raw), Style::default().fg(axis_color))),
                );
                ctx.print(
                    x_lo,
                    ty(mid_raw),
                    Line::from(Span::styled(lbl(mid_raw), Style::default().fg(axis_color))),
                );
                ctx.print(
                    x_lo,
                    y_lo_t,
                    Line::from(Span::styled(lbl(y_lo), Style::default().fg(axis_color))),
                );
            }
            // X-axis timestamps.  Two labels: the left-hand
            // edge of the window and the right-hand edge.  The
            // rendering mode is bool-flag driven: elapsed
            // seconds (default) or wall-clock (`x` hotkey).
            let x_fmt = |t: f64| -> String {
                if x_wall_clock {
                    let epoch = x_wall_clock_epoch_s as i64 + t as i64;
                    // Portable HH:MM:SS with `chrono`-free
                    // arithmetic: strip whole days and reduce
                    // to seconds-of-day.  We label in UTC
                    // (`std::time` gives no TZ helpers and we
                    // do not want to pull `chrono` in for one
                    // string), which matches the systemd
                    // journal's own timestamps -- operators
                    // comparing the plot to their `journalctl`
                    // output see the same wall clock.
                    let secs = epoch.rem_euclid(86_400) as u64;
                    let h = secs / 3600;
                    let m = (secs % 3600) / 60;
                    let s = secs % 60;
                    format!("{h:02}:{m:02}:{s:02}")
                } else {
                    let dt = t - x_hi;
                    if dt.abs() < 0.5 {
                        "now".to_string()
                    } else {
                        format!("{dt:.0}s")
                    }
                }
            };
            // Multi-tick X-axis: pick a "nice" tick interval
            // based on the visible window span and print a
            // label at every whole multiple of that step, plus
            // a short "|" marker just above each label so the
            // axis reads as a real ruler rather than just two
            // corner numbers.  The interval steps
            // (1/2/5/10/15/30 seconds, then 1/2/5/10/15
            // minutes) are chosen so a ~50-char-wide plot
            // shows 4-8 ticks -- enough to read a time off but
            // not so many that they crowd.
            let span = (x_hi - x_lo).max(1.0);
            let step = pick_x_tick_step(span);
            // Round the first tick to a whole multiple of
            // `step` so tick labels always land on
            // human-readable boundaries (e.g. every 30s means
            // "0, 30, 60, ...", not "3, 33, 63, ...").
            let first = (x_lo / step).ceil() * step;
            let mut t = first;
            let y_tick_top = y_lo_t + (y_hi_t - y_lo_t) * 0.03;
            while t <= x_hi + 1e-6 {
                // Short vertical tick above the label so the
                // axis reads as a ruler.  The Canvas
                // coordinate system is continuous; two Points
                // (baseline + slightly above) at the same X
                // paint as a "|" glyph on most terminals.
                ctx.draw(&Points {
                    coords: &[(t, y_lo_t), (t, y_tick_top)],
                    color: axis_color,
                });
                ctx.print(
                    t,
                    y_lo_t,
                    Line::from(Span::styled(x_fmt(t), Style::default().fg(axis_color))),
                );
                t += step;
            }
        });
    f.render_widget(canvas, area);
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Shorten UUID-shaped ids so they fit into the dashboard's
/// id column.  Non-UUID ids pass through unchanged, with a
/// trailing `[vcpus=N]` when the backend supplied a vCPU count
/// (so operators can eyeball the ceiling `+` will hit).
fn shorten_id(id: &str, vcpu_count: Option<u32>) -> String {
    let base = if id.len() > 22 {
        format!("{}…{}", &id[..16], &id[id.len() - 4..])
    } else {
        id.to_string()
    };
    match vcpu_count {
        Some(n) => format!("{base} vc={n}"),
        None => base,
    }
}

/// Compact number formatter: "12.3k" / "4.5M" / "678".
fn humanize(v: f64) -> String {
    if v.is_nan() || v <= 0.0 {
        return "0".into();
    }
    if v >= 1.0e6 {
        format!("{:.1}M", v / 1.0e6)
    } else if v >= 1.0e4 {
        // "> 10k switch to thousands" per the user request:
        // once the plot hits 5-digit territory, kilos are the
        // more readable unit.
        format!("{:.1}k", v / 1.0e3)
    } else {
        format!("{v:.0}")
    }
}

/// Pick a "nice" X-axis tick interval (in seconds) for a
/// plot that shows `span` seconds of data.  The chosen steps
/// escalate 1 -> 2 -> 5 -> 10 -> 15 -> 30 -> 60 -> 120 ->
/// 300 -> 600 -> 900 -> 1800 -> 3600 seconds, so a
/// ~60-second window ticks every 10 s and a 30-minute window
/// ticks every 5 min.  Kept in a small table rather than a
/// generic power-of-ten routine because the operator-visible
/// axis is always seconds -- there is no "10 000 seconds"
/// case worth handling gracefully, and the table doubles as
/// documentation of the intended visual density.
fn pick_x_tick_step(span: f64) -> f64 {
    // We aim for roughly 5-8 ticks across the window.  Pick
    // the smallest table entry whose "expected tick count"
    // sits in that band.
    const STEPS: &[f64] = &[
        1.0, 2.0, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0, 1800.0, 3600.0,
    ];
    for &s in STEPS {
        if span / s <= 8.0 {
            return s;
        }
    }
    // Fallback for very long windows (>= 8 hours):  keep the
    // largest table entry.  A well-configured TUI never gets
    // here, but the return type is `f64` so we always hand
    // back *something*.
    *STEPS.last().unwrap()
}

/// Format a decimal-seconds value for use as the initial
/// text inside the RefreshSecs prompt buffer.  Chosen so the
/// most common cadences round-trip cleanly: `1s` shows as
/// `1`, `0.25s` shows as `0.25`, `10s` shows as `10`.  All
/// digits are ASCII so the buffer's own filter passes them
/// through unchanged.
fn fmt_prompt_secs(s: f64) -> String {
    if !s.is_finite() || s <= 0.0 {
        return String::new();
    }
    if (s.fract()).abs() < 1e-6 {
        format!("{}", s.round() as u64)
    } else {
        // strip trailing zeros for a tidier default
        let mut s = format!("{s}");
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        s
    }
}

/// Human-friendly duration formatter for the title bar's
/// `refresh` and `scroll` fields.  Sub-second values render
/// in milliseconds (`250ms`), whole-second values render as
/// integer seconds (`5s`), and everything in between renders
/// with a single decimal (`1.5s`).  Chosen so an operator
/// glancing at the title bar can tell "which knob am I on"
/// without doing unit conversion in their head.
fn fmt_duration_secs(s: f64) -> String {
    if !s.is_finite() || s <= 0.0 {
        return "0".to_string();
    }
    if s < 1.0 {
        format!("{}ms", (s * 1000.0).round() as u64)
    } else if (s.fract()).abs() < 0.05 {
        format!("{}s", s.round() as u64)
    } else {
        format!("{s:.1}s")
    }
}

/// Render the CPU dashboard column as
/// `avg / median / total` (all in integer percent).  Falls
/// back to a legacy single-column `total%` render when the
/// daemon did not populate the avg / median fields (i.e. the
/// TUI is talking to a pre-percent-column build).
///
/// The three cells are separated by plain `/` rather than
/// `" / "` so an 18-char column can fit up to three 3-digit
/// values with a bit of margin.  The status-line printer on
/// the daemon uses the same shape so an operator eyeballing
/// the periodic log and the TUI dashboard sees identical
/// numbers.
fn fmt_cpu_triple(
    avg: Option<u64>,
    median: Option<u64>,
    total: Option<u64>,
    fallback_total_pct: f64,
) -> String {
    match (avg, median, total) {
        (Some(a), Some(m), Some(t)) => format!("{a}/{m}/{t}"),
        _ => format!("{fallback_total_pct:.0}"),
    }
}

/// Adaptive bandwidth formatter.  Input is MB/s (matches the
/// on-status representation); output picks the friendliest SI
/// suffix from `B/s` through `GB/s` so a mostly-idle vm
/// does not report `0 MB/s` for 400 KB/s of activity and a
/// screaming NVMe does not read as `4200 MB/s` when `4.2 GB/s`
/// is what an operator wants to see.
///
/// Zero and negative values collapse to `"0"` (unit-less)
/// because the dashboard renders the pair `"r/w"` for two
/// directions and cramming a `B/s` suffix onto a zero slot
/// wastes column width without teaching the operator anything
/// new.
fn fmt_bandwidth_mbps(mb_per_sec: f64) -> String {
    if !mb_per_sec.is_finite() || mb_per_sec <= 0.0 {
        return "0".into();
    }
    let bps = mb_per_sec * 1.0e6;
    if bps >= 1.0e9 {
        format!("{:.1}GB/s", bps / 1.0e9)
    } else if bps >= 1.0e6 {
        format!("{:.0}MB/s", bps / 1.0e6)
    } else if bps >= 1.0e3 {
        format!("{:.0}KB/s", bps / 1.0e3)
    } else {
        format!("{:.0}B/s", bps)
    }
}

/// Latency triple → `p50/p95/p99` string, or `-/-/-` when the
/// backend did not report it.
fn fmt_lat(lat: Option<(u64, u64, u64)>) -> String {
    match lat {
        Some((p50, p95, p99)) => format!("{p50}/{p95}/{p99}"),
        None => "-/-/-".into(),
    }
}

/// CPU microseconds per IOP → operator-facing cell string.
/// `NaN` (idle tick, no IOPS) renders as `"NaN"` so the
/// dashboard mirrors the daemon's status line convention;
/// finite values are rounded to whole microseconds because the
/// noise floor of a /proc walker over a 1-5 s tick is well
/// above 1 µs.
fn fmt_cpu_per_iop(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else {
        format!("{v:.0}")
    }
}

/// Darken a palette colour so read / write curves in the
/// latency pane are visually distinguishable without a
/// separate palette.
fn dim(c: Color) -> Color {
    match c {
        Color::Cyan => Color::Blue,
        Color::LightMagenta => Color::Magenta,
        Color::LightGreen => Color::Green,
        Color::Yellow => Color::LightYellow,
        Color::LightBlue => Color::Blue,
        Color::LightRed => Color::Red,
        Color::LightCyan => Color::DarkGray,
        Color::White => Color::Gray,
        other => other,
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Connect to the daemon on the system bus.  The TUI is a
    // pure daemon client -- there is no direct-backend fallback
    // path any more, so an unreachable daemon is a fatal
    // condition.  We surface it with a clear message here
    // rather than silently spawning a sampler that will fail
    // every tick with no obvious cause.
    let daemon = connect_daemon_proxy().await.map_err(|e| {
        anyhow!(
            "iothread-tui requires an io-thread-controller daemon on the system bus \
             ({}, {}): {e}",
            DBUS_INTERFACE,
            DBUS_OBJECT_PATH
        )
    })?;
    tracing::info!(
        target: "iothread-tui",
        "connected to daemon; polling GetSnapshot every {}",
        fmt_duration_secs(cli.refresh_secs)
    );

    // Convert CLI seconds -> internal nanoseconds, silently
    // clamping to a 50 ms floor.  `f64 * 1e9` fits inside u64
    // for every plausible refresh cadence; a nonsensical `NaN`
    // survives as u64::MAX via `as u64` saturation but we
    // guard with the 50 ms floor anyway.
    let refresh_ns_init = (cli.refresh_secs.max(0.05) * 1.0e9) as u64;
    let shared = Arc::new(Shared {
        started: Instant::now(),
        vms: RwLock::new(Vec::new()),
        aggregate: RwLock::new(VecDeque::new()),
        paused: AtomicBool::new(false),
        refresh_ns: AtomicU64::new(refresh_ns_init),
        daemon_failed_streak: AtomicU32::new(0),
        daemon_last_error: RwLock::new(None),
        daemon,
    });

    // Sampler owns the D-Bus polling + roster reconciliation;
    // there is no separate rediscovery loop because every
    // `GetSnapshot` reply already carries the full daemon-side
    // inventory.
    let sampler = tokio::spawn(sampler_loop(shared.clone(), cli.clone()));

    let ui_result = run_ui(shared, cli).await;
    sampler.abort();
    match ui_result? {
        UiExit::Quit => Ok(()),
    }
}
