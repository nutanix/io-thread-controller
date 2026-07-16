# io-thread-controller

`io-thread-controller` discovers VMs through pluggable backends, refreshes
their I/O-worker state, asks the selected scaling engine for decisions, and
keeps the live inventory synchronized with backend discovery.

## Core control loop

Backends and scaling engines are independent extension points around the
controller-owned inventory:

```text
                         +-------------------------+
                         | Controller              |
                         | owns inventory + engine |
                         | refreshes and actuates  |
                         +-----------+-------------+
                                     |
                  +------------------+------------------+
                  |                                     |
                  v                                     v
+--------------------------------+     +--------------------------------+
| Backend        (e.g. QEMU)     |     | Scaling engine                 |
| discovers zero or more VMs     |     | evaluates the refreshed fleet |
| creates one Instance per VM    |     | returns one scaling plan       |
| supplies per-VM InstanceClient |     | observes applied outcomes      |
+-----------------+--------------+     +--------------------------------+
                  |
                  v
    +---------------------------+
    | Instance data record      |
    | one VM + InstanceClient   |
    | (operations for that VM)  |
    +---------------------------+
```

Scaling decisions pass through VM ownership, controller thread-count, vCPU,
and host-CPU guards before the controller changes the backend pool. One atomic
`vm-ownership.json` registry below `/run/io-thread-controller` preserves
managed and unmanaged classifications across daemon restarts.

```text
                          startup
                             |
                             v
                  load config + select engine
                             |
                             v
+----------------> discover backend VMs <------------------+
|                            |                             |
|                            v                             |
|             +------ reconcile live inventory <-----+     |
|             |                                      |     |
|             v                                      |     |
|        wait for event                              |     |
|       /              \                             |     |
| poll timer       backend/inotify event             |     |
|     |                   |                          |     |
|     v                   +--------------------------+     |
| refresh every VM concurrently                            |
|     |                                                    |
|     +-- refresh failed --> remove VM --> rediscover------+
|     |                                    on next event   |
|     v                                                    |
| engine evaluates the refreshed fleet                     |
|     |                                                    |
|     v                                                    |
+--- Hold                                                  |
|     |                                                    |
+-- Scale --> guards reject -------------------------------+
                |                                          |
                v                                          |
        set backend thread count                           |
                |                                          |
      +---------+---------+                                |
      |                   |                                |
    error              success                             |
      |                   |                                |
      v                   v                                |
 log failure       update live count                       |
      |                   |                                |
      +---------+---------+                                |
                |                                          |
                +------------------------------------------+
```

## Scale-up state machine

```text
refreshed
   |
   +-- below threshold / at controller maximum ----------> hold
   |
   +-- saturated --> propose current + 1
                         |
                         +-- backend ownership rejects -> hold
                         |
                         +-- host CPU guard -------------> hold
                         |
                         +-- target outside bounds ------> hold
                         |
                         +-- target > vCPUs -------------> hold
                         |
                         +-- backend error -------------> report failure
                         |
                         +-- backend accepts -----------> report success
```

## Controller architecture

An `Instance` is a backend-neutral data record for one VM, not a backend trait.
It stores VM identity, latest metrics, process-local override state, and an
`InstanceClient` trait object that performs operations on exactly that VM.

A `Backend` is the fleet-level adapter for a whole class of VMs. It owns
backend-wide configuration and discovers zero or more `Instance` values. The
two abstractions are separate because discovery has fleet scope, while metric
queries and thread-count changes need an independently stateful handle for each
VM. This keeps the controller independent of libvirt, QMP, control sockets, and
other transport details.

After a successful action, the engine waits
`scale_validation_sample_polls` complete samples. A scale-up is reverted unless
IOPS gained at least `scale_up_min_gain_percent`; a scale-down is reverted when
IOPS lost more than `scale_down_revert_drop_percent`. Setting either percentage
to zero disables that direction's validation.

## QEMU backend

The QEMU backend discovers active VMs through libvirt and sends QMP commands
through `virDomainQemuMonitorCommand`. Its configuration is loaded from
`backends.d/qemu.json`. Named IOThreads and virtqueue mappings can be inspected
or changed through the backend CLI and D-Bus operations.

## Status line

Every tick the daemon emits one INFO line per tracked VM and one aggregate line
on the `status` tracing target. With `--print-status-header`, it also emits a
`#`-prefixed legend on startup.

### Per-VM line

The examples are wrapped with backslashes for readability; each emitted record
occupies one line.

```
INFO vm=vm-a thr=4 iops=155593/0/0 bw_mb_s=20394/0 \
  cpu=90/358 iops_1_5_15m=154995/153840/151220 \
  cpu_us_per_io_1_5_15m=23/16/19
```

- `vm`: VM identifier.
- `thr`: matched worker threads.
- `iops`: current read / write / other operations per second.
- `iops_1_5_15m`: average total IOPS over rolling 1m / 5m / 15m windows.
- `bw_mb_s`: current read / write bandwidth in MB/s.
- `cpu`: average per-thread / total pool CPU percentage.
- `cpu_us_per_io_1_5_15m`: CPU microseconds per completed I/O over the same
  windows.

A dash (`-`) means no complete sample is available for that window.

### Aggregate line

```
INFO tracked=1 total_threads=4 \
  iops_1_5_15m=154995/153840/151220 aggregate
```

`tracked` is the current VM count, `total_threads` is their combined worker
count, and each aggregate IOPS cell is the sum of the corresponding per-VM
rolling rate.

### Scaling verdicts

When the threshold engine decides to change the thread count
you get one INFO line on the `engine` target using the same
`vm=<id>` convention as the status lines:

```
INFO vm=vm-1615a59c-... util=0.8955 action="up" thr=4->5
```

`vm` is the same identifier used by status lines, `util` is
the per-thread utilisation that triggered the decision, `action`
is `up`, `down`, or `revert`, and `thr` is the requested
`<old>-><new>` transition.

Failed actuations (backend rejected the `SetThreadCount` call)
surface as a WARN on the `controller` target with the same
`vm=..., dir=..., from=..., to=..., error=...` shape; the
success path is intentionally silent because the engine's
own INFO already describes the move.

## D-Bus

The daemon owns:

- bus name: `com.nutanix.io_thread_controller1`
- object path: `/com/nutanix/io_thread_controller1`
- interface: `com.nutanix.io_thread_controller1`

Debug builds expose `SetThreadCount(vm, threads, sticky)` to change a tracked
instance's thread count. Setting `sticky=true` suppresses automatic scaling for
that instance until a later call clears it. This debug-only override is kept in
memory and is lost when the daemon restarts.

`GetStats()` returns a JSON fleet snapshot. For example:

```json
{
  "tick": 42,
  "vms": [{
    "vm": "vm-1",
    "thread_count": 3,
    "manual_scaling_sticky": false,
    "scaling_allowed": true,
    "vcpu_count": 8,
    "per_thread_util": 0.72,
    "read_io_count": 1000,
    "write_io_count": 250,
    "other_io_count": 0
  }]
}
```

```sh
busctl --system call \
  com.nutanix.io_thread_controller1 \
  /com/nutanix/io_thread_controller1 \
  com.nutanix.io_thread_controller1 \
  GetStats
```

`GetStats` is available in release builds and world-readable under the shipped
D-Bus policy.

```sh
busctl --system call \
  com.nutanix.io_thread_controller1 \
  /com/nutanix/io_thread_controller1 \
  com.nutanix.io_thread_controller1 \
  SetThreadCount sub <vm> 4 true
```

Release builds do not expose this method. The shipped D-Bus policy restricts
the debug method to root.

Backends that support named IOThreads can also expose
`GetIoThreadVqMapping`, `AddIoThread`, `DelIoThread`, and
`SetIoThreadVqMapping`.

## Log style

`--log-style=auto` uses the compact systemd form when `JOURNAL_STREAM` is set
and the full human form otherwise. For example, journald already supplies the
timestamp and service name, so the daemon emits:

```
INFO vm=vm-1 thr=4 iops=12000/0/0 ...
```

Use `--log-style=human` to retain the timestamp and tracing target when running
interactively, or `--log-style=systemd` to force compact output for another log
collector.

## `iothread-tui`

`iothread-tui` is a ratatui-based operator dashboard for the
whole io-thread-controller fleet.  It is a **pure daemon
client**: at startup it connects to
`com.nutanix.io_thread_controller1` on the system D-Bus,
polls `GetSnapshot()` at the user's `--refresh-secs` cadence,
and renders whatever roster the daemon reports.  A running
daemon is therefore mandatory — the TUI refuses to start
without one (fail-fast is friendlier than silently spawning
a poller that will time out on every tick).

Because the daemon already owns backend discovery,
reconciliation, and every perf counter, the TUI stays
backend-agnostic without linking backend implementations.
Add a new backend to the daemon and the TUI picks it up on the
next `GetSnapshot` reply, with no TUI rebuild required.

It ships as a second binary of the same crate.  `make` (the
default target) builds both the daemon and the TUI; set
`TUI=0` on the `make` line to skip it on lean build hosts:

```
make                        # builds io-thread-controller + iothread-tui
make TUI=0                  # daemon only
cargo build --features tui  # equivalent to the make default
./target/debug/iothread-tui --help
```

Add `QEMU=1` (or `--features qemu`) for the upstream-QEMU
daemon backend; the TUI itself does not link libvirt because
it never sees a backend directly.

### Layout

```
 iothread-tui  IOPS @ 4 KiB  refresh 500ms  vms=3  theme=default-dark   [h]elp [q]uit [m]etric [p]ause [r]efresh [t]heme 12345 plots ↑↓ focus s+/- threads
┌── perf (IOPS @ 4 KiB)   y=200k ───────────────────────────────────────┐
│                          4→5                                          │
│         ╭╮        ╭─╮  ┌─╯          ╭─────                            │
│        ╱  ╲──────╯   ╰─╯     3→2   ╱                                  │
│       ╱                     ─────┘                                    │
│  ─── vm-1  ─── vm-2  ─── TOTAL (grey)                                 │
└───────────────────────────────────────────────────────────────────────┘
┌── cpu (%)   y=400 ────────────────────────────────────────────────────┐
│         ...                                                           │
└───────────────────────────────────────────────────────────────────────┘
┌── vms ────────────────────────────────────────────────────────────┐
│ id                    thr  iops r/w/o  bw r/w  cpu%  us/iop  lat_r ...  qd
│ 1615a59c…dc4a vc=4      4  61k/0/0     240/0    380  6       250/... 239
│ 8ac13990…7a11 vc=8      2  30k/0/0     120/0    195  7       200/... 123
│ TOTAL                   6  91k/0/0     360/0    575  6       -/-/-   362
└───────────────────────────────────────────────────────────────────────┘
```

* Up to five **stackable plot panes** share the top of the
  frame; each is individually toggleable and equal-height splits
  scale their Y-axis when more than one is on:

    - `1` — **perf**  (IOPS or MB/s, toggle with `m`)
    - `2` — **latency**  (µs; four lines per vm — read
      avg + read p99 + write avg + write p99.  The avg line
      is the histogram-derived arithmetic mean the daemon
      publishes on `SnapshotLatency.avg` so operators can
      see both the typical and the tail-case in the same
      pane.)
    - `3` — **cpu**  (per-pool `%`; a 4-thread pool at 100%
      reads as 400)
    - `4` — **queue depth**  (per-VM virtqueue depth)
    - `5` — **cpu (µs / iop)**  (efficiency: lower is
      better; NaN samples on idle ticks are skipped so the
      trace does not jump to infinity)

* Per-VM traces are colour-cycled through the palette so
  overlapping lines stay legible.  When at least **two**
  vms are tracked, a grey **aggregate** line joins each
  plot (sum across every live vm), and a bold **TOTAL**
  row is appended to the dashboard.

* **Scale events** on the perf pane render as bold text
  `"<old>→<new>"` — green for scale-ups, red for scale-downs
  — placed on the line so operators can see both the direction
  and the magnitude of every actuation without leaving the
  plot.

* **Y-axis auto-collapses** to kilos once the value crosses
  10 000 (e.g. `12.3k`, `1.2M`) so a plot going from idle to
  6-digit IOPS reads cleanly at both scales.

* The dashboard's `cpu %` column shows `per_thread_util *
  num_threads * 100`, i.e. total CPU across the whole pool.
  The `vc=N` suffix on the `id` column is the guest vCPU count
  the backend reported at discovery time, so operators can
  eyeball how close `+` will drive the pool to the vCPU cap.

* **Dashboard columns are configurable.**  Press `c` to open
  a modal checkbox picker; toggle each column on/off with
  `Space` / `Enter` and move the cursor with `↑` / `↓` (or
  `j` / `k`).  The default set is `id · thr · iops · bw ·
  cpu · us/iop · qd · queues`; the read/write latency
  triples stay off by default because they are noisy on the
  narrow terminal widths operators typically SSH into.  `id`
  is a required column (marked `[·]` in the picker) so an
  operator cannot accidentally hide the vm label.

* The `qd` column reads as `avg/median/total`, computed by
  the daemon across every SCSI command virtqueue on the
  freshest snapshot.  A separate `queues` column shows the
  raw VQ count so `avg/median` are easy to sanity-check.
  Both show `-` on backends that do not model VQs.

* A `*` suffix on the `thr` column means the vm is
  currently manual-sticky (the daemon's
  `manual_scaling_sticky` flag is set).  Rows for vms
  whose most recent daemon refresh failed (`alive=false`)
  render dimmed.

### Hotkeys

| Key                     | Effect                                                    |
|-------------------------|-----------------------------------------------------------|
| `q`, `Esc`, `Ctrl-C`    | Quit                                                      |
| `h`, `?`                | Show/hide the modal help overlay                          |
| `m`                     | Toggle perf pane between IOPS and MB/s                    |
| `p`, `Space`            | Pause: freezes the X-axis *and* the sampler / rediscovery loops |
| `r`                     | Prompt for a new refresh cadence in ms (50 – 60 000)      |
| `c`                     | Open dashboard-columns modal (Space/Enter toggle, ↑↓ move, `c`/`Esc` close) |
| `t`                     | Cycle theme (default-dark / default-light / vim-desert / solarized-dark / gruvbox) |
| `1` / `2` / `3` / `4` / `5` | Toggle the perf / lat / cpu / qd / cpu-per-iop pane   |
| `↑`, `k`                | Focus previous vm — **hides every other line**        |
| `↓`, `j`                | Focus next vm; last slot is `TOTAL` when >=2          |
| `Esc`                   | Clear focus (repaint every vm)                        |
| `+`, `=`                | Ask the daemon to add one thread to the focused vm     |
| `-`, `_`                | Ask the daemon to remove one thread from the focused vm |
| `s`                     | Toggle sticky mode for `+` / `-` (title bar shows `[STICKY]`) |

**`+` / `-` always route through the daemon.**  Both
per-frame stats (via `GetSnapshot`) and manual actuations
(via `SetThreadCount(vm, threads, sticky)`) go over
`com.nutanix.io_thread_controller1` on
`/com/nutanix/io_thread_controller1`, so the daemon is the
single source of truth for `manual_scaling_sticky`.  A wire
error on either verb logs at WARN on the `iothread-tui`
tracing target (and shows up in the daemon journal too) but
otherwise leaves the UI running so the operator can retry.
There is no direct-backend fallback path — a missing daemon
is a fatal condition the binary refuses to start with.

Sticky mode is armed by pressing `s` (toggle).  The title bar
shows `[STICKY]` while it is on, and every subsequent `+`/`-`
carries `sticky=true`.  Press `s` again to disarm.  We
deliberately do **not** wire a `Ctrl-+` / `Ctrl--` variant —
tmux swallows and rewrites those key events depending on its
own bindings and they never reach the TUI reliably.

**Focus hides every other line.**  With no focus, every
vm draws.  Once you focus a vm (`↑`/`↓`, or select
`TOTAL` on a >=2-vm fleet) the plots repaint with only
the highlighted line(s) — the plot becomes a solo view of the
thing you are tracking.  `Esc` clears focus and brings every
line back.  `+` / `-` are ignored on `TOTAL` because
broadcasting a scale-op to a room full of VMs is almost always
a mistake.  Scale annotations (`<old>→<new>`) render on top
of the line with the direction colour in the foreground (green
up, red down) *and* the vm's own line colour painted
behind the glyph, so overlapping annotations across
close-together lines stay attributable at a glance.

**Themes** (`t` to cycle) swap the per-VM palette, axis
label colour, and aggregate/accent colours. Five ship: two
"default" variants tuned for dark and light terminal
backgrounds, plus vim-desert, solarized-dark, and gruvbox.
Vm colour assignment is theme-independent, so switching
themes recolours every trace consistently.

### Command line

```
iothread-tui [OPTIONS]

  --select <PATTERN>       Substring of the per-VM id
                           the daemon publishes.
                           Repeatable; empty list = every
                           vm the daemon reports.
  --refresh-secs <SECONDS> `GetSnapshot` poll cadence. Decimals
                           are accepted; values below 0.05 are
                           clamped. [default: 1.0]
  --metric <iops|bandwidth>  Perf pane metric at startup;
                             toggle at runtime with `m`.
                             [default: iops]
  --iop-size <BYTES>       Displayed alongside "IOPS" in the
                           perf legend (e.g. "IOPS @ 4 KiB").
                           Purely informational.  [default: 4096]
  --window-s <SECONDS>     Rolling window shown on the X axis.
                           [default: 120]
```

Backend socket paths, libvirt URIs, and the rediscovery
cadence are all owned by the daemon now (see the
`backends.d/` config directory).  The TUI has no knobs for
any of them.

### Examples

Watch every backend the daemon reports, at 500 ms cadence,
plotting IOPS with the perf + CPU panes on:

```
iothread-tui --refresh-secs 0.5
# then press '3' to also open the cpu pane
```

Follow a single VM by UUID prefix, plotting MB/s over a 60 s
window:

```
iothread-tui --select 1615a59c --metric bandwidth --window-s 60
```

Manually nudge the pool for the focused vm:

```
# in the TUI: ↓ ↓  to focus vm-3, then '+' three times.
```
