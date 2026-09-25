// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Bounded per-VM history for uptime-style 1m/5m/15m metrics.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

/// Longest retained status window.
const MAX_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Delta recorded for one successful sampling interval.
#[derive(Debug, Clone, Copy)]
struct TickSample {
    // FIXME wall_ts variable should be renamed to sampled_at
    wall_ts: Instant,
    io_ops_delta: u64,
    cpu_ns_delta: u64,
    wall_dt_ns: u64,
}

/// Rolling deltas plus the cumulative-counter baseline for the next tick.
#[derive(Debug, Default, Clone)]
pub struct RollingMetrics {
    samples: VecDeque<TickSample>,
    previous_wall_ts: Option<Instant>,
    previous_io_ops: Option<u64>,
    previous_cpu_ticks: Option<u64>,
}

impl RollingMetrics {
    /// Construct an empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Diff cumulative backend and `/proc` counters and append one interval.
    ///
    /// The first call establishes a baseline. A counter reset also replaces the
    /// baseline without manufacturing a large wrapped delta.
    pub fn push_from_procfs_delta(
        &mut self,
        now: Instant,
        io_ops_cumulative: u64,
        cpu_ticks_cumulative: u64,
        clock_ticks_per_second: f64,
    ) {
        let Some(previous_wall_ts) = self.previous_wall_ts else {
            self.set_baseline(now, io_ops_cumulative, cpu_ticks_cumulative);
            return;
        };
        let (Some(previous_io_ops), Some(previous_cpu_ticks)) =
            (self.previous_io_ops, self.previous_cpu_ticks)
        else {
            self.set_baseline(now, io_ops_cumulative, cpu_ticks_cumulative);
            return;
        };
        let Some(elapsed) = now.checked_duration_since(previous_wall_ts) else {
            self.set_baseline(now, io_ops_cumulative, cpu_ticks_cumulative);
            return;
        };
        if io_ops_cumulative < previous_io_ops
            || cpu_ticks_cumulative < previous_cpu_ticks
            || clock_ticks_per_second <= 0.0
        {
            self.set_baseline(now, io_ops_cumulative, cpu_ticks_cumulative);
            return;
        }

        let cpu_tick_delta = cpu_ticks_cumulative - previous_cpu_ticks;
        let cpu_ns_delta =
            (cpu_tick_delta as f64 * 1_000_000_000.0 / clock_ticks_per_second) as u64;
        self.samples.push_back(TickSample {
            wall_ts: now,
            io_ops_delta: io_ops_cumulative - previous_io_ops,
            cpu_ns_delta,
            wall_dt_ns: elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
        });
        self.set_baseline(now, io_ops_cumulative, cpu_ticks_cumulative);
        self.trim(now);
    }

    /// Append an interval from backend-computed per-thread utilisation.
    pub fn push_from_backend_util(
        &mut self,
        now: Instant,
        io_ops_cumulative: u64,
        per_thread_util: f64,
        thread_count: u32,
    ) {
        let (Some(previous_wall_ts), Some(previous_io_ops)) =
            (self.previous_wall_ts, self.previous_io_ops)
        else {
            self.set_backend_baseline(now, io_ops_cumulative);
            return;
        };
        let Some(elapsed) = now.checked_duration_since(previous_wall_ts) else {
            self.set_backend_baseline(now, io_ops_cumulative);
            return;
        };
        if io_ops_cumulative < previous_io_ops {
            self.set_backend_baseline(now, io_ops_cumulative);
            return;
        }

        let wall_dt_ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        let cpu_ns_delta =
            (wall_dt_ns as f64 * per_thread_util.clamp(0.0, 1.0) * f64::from(thread_count))
                .min(u64::MAX as f64) as u64;
        self.samples.push_back(TickSample {
            wall_ts: now,
            io_ops_delta: io_ops_cumulative - previous_io_ops,
            cpu_ns_delta,
            wall_dt_ns,
        });
        self.set_backend_baseline(now, io_ops_cumulative);
        self.trim(now);
    }

    /// Append an already-computed delta, used by fleet aggregation.
    pub fn push_delta(&mut self, now: Instant, io_ops_delta: u64, cpu_ns_delta: u64) {
        let Some(previous_wall_ts) = self.previous_wall_ts else {
            self.previous_wall_ts = Some(now);
            return;
        };
        let Some(elapsed) = now.checked_duration_since(previous_wall_ts) else {
            self.previous_wall_ts = Some(now);
            return;
        };
        self.samples.push_back(TickSample {
            wall_ts: now,
            io_ops_delta,
            cpu_ns_delta,
            wall_dt_ns: elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
        });
        self.previous_wall_ts = Some(now);
        self.trim(now);
    }

    /// Return the newest I/O and CPU deltas.
    pub fn last_delta(&self) -> Option<(u64, u64)> {
        self.samples
            .back()
            .map(|sample| (sample.io_ops_delta, sample.cpu_ns_delta))
    }

    /// Average IOPS over retained intervals inside `window`.
    pub fn iops_over(&self, window: Duration) -> Option<u64> {
        let (io_ops, _, wall_ns) = self.totals(window)?;
        (wall_ns > 0).then(|| {
            (u128::from(io_ops) * 1_000_000_000 / u128::from(wall_ns)).min(u128::from(u64::MAX))
                as u64
        })
    }

    /// CPU microseconds consumed per completed I/O inside `window`.
    pub fn cpu_us_per_io_over(&self, window: Duration) -> Option<u64> {
        let (io_ops, cpu_ns, _) = self.totals(window)?;
        (io_ops > 0).then(|| cpu_ns / io_ops / 1_000)
    }

    /// Number of retained sampling intervals.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether no complete sampling interval has been retained.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    fn set_baseline(&mut self, now: Instant, io_ops: u64, cpu_ticks: u64) {
        self.previous_wall_ts = Some(now);
        self.previous_io_ops = Some(io_ops);
        self.previous_cpu_ticks = Some(cpu_ticks);
    }

    fn set_backend_baseline(&mut self, now: Instant, io_ops: u64) {
        self.previous_wall_ts = Some(now);
        self.previous_io_ops = Some(io_ops);
        self.previous_cpu_ticks = None;
    }

    fn trim(&mut self, now: Instant) {
        // If `now` is still within MAX_WINDOW of the
        // opaque Instant origin, keep every sample.
        let Some(cutoff) = now.checked_sub(MAX_WINDOW) else {
            return;
        };
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.wall_ts < cutoff)
        {
            self.samples.pop_front();
        }
    }

    fn totals(&self, window: Duration) -> Option<(u64, u64, u64)> {
        let newest = self.samples.back()?.wall_ts;
        // Same as trim: a failed checked_sub means "include all retained samples".
        let cutoff = newest.checked_sub(window);
        let mut io_ops = 0u64;
        let mut cpu_ns = 0u64;
        let mut wall_ns = 0u64;
        for sample in self
            .samples
            .iter()
            .filter(|sample| cutoff.is_none_or(|cutoff| sample.wall_ts >= cutoff))
        {
            io_ops = io_ops.saturating_add(sample.io_ops_delta);
            cpu_ns = cpu_ns.saturating_add(sample.cpu_ns_delta);
            wall_ns = wall_ns.saturating_add(sample.wall_dt_ns);
        }
        Some((io_ops, cpu_ns, wall_ns))
    }
}

/// Render 1m/5m/15m cells, using `-` until a window has a sample.
pub fn format_1_5_15<F>(metrics: &RollingMetrics, getter: F) -> String
where
    F: Fn(&RollingMetrics, Duration) -> Option<u64>,
{
    [1, 5, 15]
        .map(|minutes| {
            getter(metrics, Duration::from_secs(minutes * 60))
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        })
        .join("/")
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn expected_cpu_ns(cpu_tick_delta: u64, clock_ticks_per_second: f64) -> u64 {
        (cpu_tick_delta as f64 * 1_000_000_000.0 / clock_ticks_per_second) as u64
    }

    proptest! {
        #[test]
        fn first_sample_leaves_the_window_empty(
            io_ops in any::<u64>(),
            cpu_ticks in any::<u64>(),
            hz in prop::num::f64::ANY,
        ) {
            let mut metrics = RollingMetrics::new();
            metrics.push_from_procfs_delta(Instant::now(), io_ops, cpu_ticks, hz);
            prop_assert!(metrics.is_empty());
            prop_assert!(metrics.iops_over(Duration::from_secs(60)).is_none());
            prop_assert!(metrics.cpu_us_per_io_over(Duration::from_secs(60)).is_none());
        }

        #[test]
        fn backwards_counter_or_non_positive_hz_keeps_len_and_resets_baseline(
            io0 in 1u64..1_000_000,
            cpu0 in 1u64..1_000_000,
            io_delta in 0u64..10_000,
            cpu_delta in 0u64..10_000,
            elapsed_ms in 1u64..5_000,
            reset_kind in 0u8..3,
        ) {
            let hz = 100.0;
            let mut metrics = RollingMetrics::new();
            let t0 = Instant::now();
            let io1 = io0 + io_delta;
            let cpu1 = cpu0 + cpu_delta;
            metrics.push_from_procfs_delta(t0, io0, cpu0, hz);
            metrics.push_from_procfs_delta(t0 + Duration::from_millis(elapsed_ms), io1, cpu1, hz);
            prop_assert_eq!(metrics.len(), 1);

            let t_reset = t0 + Duration::from_millis(elapsed_ms + 1);
            let (base_io, base_cpu) = match reset_kind {
                0 => {
                    metrics.push_from_procfs_delta(t_reset, io1 - 1, cpu1, hz);
                    (io1 - 1, cpu1)
                }
                1 => {
                    metrics.push_from_procfs_delta(t_reset, io1, cpu1 - 1, hz);
                    (io1, cpu1 - 1)
                }
                _ => {
                    metrics.push_from_procfs_delta(t_reset, io1 + 1, cpu1 + 1, 0.0);
                    (io1 + 1, cpu1 + 1)
                }
            };
            prop_assert_eq!(metrics.len(), 1);

            let t_next = t_reset + Duration::from_millis(elapsed_ms);
            metrics.push_from_procfs_delta(t_next, base_io + io_delta, base_cpu + cpu_delta, hz);
            let (observed_io, observed_cpu_ns) = metrics.last_delta().unwrap();
            prop_assert_eq!(observed_io, io_delta);
            prop_assert_eq!(observed_cpu_ns, expected_cpu_ns(cpu_delta, hz));
        }

        #[test]
        fn monotonic_pair_matches_elapsed_rate_formula(
            io0 in 0u64..1_000_000,
            cpu0 in 0u64..1_000_000,
            io_delta in 1u64..100_000,
            cpu_delta in 0u64..100_000,
            elapsed_ms in 1u64..60_000,
            hz in 1.0f64..10_000.0,
        ) {
            let mut metrics = RollingMetrics::new();
            let t0 = Instant::now();
            let elapsed = Duration::from_millis(elapsed_ms);
            metrics.push_from_procfs_delta(t0, io0, cpu0, hz);
            metrics.push_from_procfs_delta(t0 + elapsed, io0 + io_delta, cpu0 + cpu_delta, hz);

            let wall_ns = u64::try_from(elapsed.as_nanos()).unwrap();
            let expected_iops = (u128::from(io_delta) * 1_000_000_000 / u128::from(wall_ns)) as u64;
            prop_assert_eq!(metrics.iops_over(Duration::from_secs(60)), Some(expected_iops));

            let cpu_ns = expected_cpu_ns(cpu_delta, hz);
            prop_assert_eq!(
                metrics.cpu_us_per_io_over(Duration::from_secs(60)),
                Some(cpu_ns / io_delta / 1_000)
            );
        }
    }
}
