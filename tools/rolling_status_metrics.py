#!/usr/bin/env python3
"""Compute rolling status metrics from controller log lines.

Input format: newline-delimited log lines with key=value fields and an RFC3339
(or RFC3339-like) timestamp at the beginning of the line.

Expected fields (if present):
- read_iops, write_iops, other_iops
- cpu_pct_avg, cpu_pct_total

The tool emits 1m/5m/15m windows:
- iops_1_5_15m
- cpu_pct_avg_1_5_15m
- cpu_pct_total_1_5_15m
- cpu_us_per_io_1_5_15m
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Iterable

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}[T ][^\s]+)")
KV_RE = re.compile(r"\b([a-zA-Z0-9_]+)=([^\s]+)")

WINDOWS = [60, 300, 900]  # seconds => 1m, 5m, 15m


@dataclass
class Sample:
    ts: dt.datetime
    iops: float | None
    cpu_pct_avg: float | None
    cpu_pct_total: float | None


def parse_ts(raw: str) -> dt.datetime | None:
    val = raw.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(val)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_line(line: str) -> Sample | None:
    ts_m = TS_RE.search(line)
    if not ts_m:
        return None
    ts = parse_ts(ts_m.group(1))
    if ts is None:
        return None

    fields = {k: v.rstrip(",") for (k, v) in KV_RE.findall(line)}

    def f64(name: str) -> float | None:
        raw = fields.get(name)
        if raw is None or raw == "-":
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    read_iops = f64("read_iops")
    write_iops = f64("write_iops")
    other_iops = f64("other_iops")
    iops = None
    if read_iops is not None or write_iops is not None or other_iops is not None:
        iops = (read_iops or 0.0) + (write_iops or 0.0) + (other_iops or 0.0)

    return Sample(
        ts=ts,
        iops=iops,
        cpu_pct_avg=f64("cpu_pct_avg"),
        cpu_pct_total=f64("cpu_pct_total"),
    )


def mean(values: Iterable[float]) -> float | None:
    vals = list(values)
    if not vals:
        return None
    return sum(vals) / len(vals)


def fmt_triplet(values: list[float | None], precision: int = 2) -> str:
    out = []
    for v in values:
        out.append("-" if v is None else f"{v:.{precision}f}")
    return "/".join(out)


def compute(samples: Deque[Sample], now: dt.datetime) -> tuple[str, str, str, str]:
    iops_out: list[float | None] = []
    cpu_avg_out: list[float | None] = []
    cpu_total_out: list[float | None] = []
    cpu_us_per_io_out: list[float | None] = []

    for w in WINDOWS:
        cutoff = now - dt.timedelta(seconds=w)
        window = [s for s in samples if s.ts >= cutoff]

        iops = mean(s.iops for s in window if s.iops is not None)
        cpu_avg = mean(s.cpu_pct_avg for s in window if s.cpu_pct_avg is not None)
        cpu_total = mean(s.cpu_pct_total for s in window if s.cpu_pct_total is not None)

        cpu_us_per_io = None
        if iops is not None and iops > 0 and cpu_total is not None:
            # Approximate: (%CPU across all workers * 1e6 us/s) / IO/s.
            cpu_us_per_io = (cpu_total / 100.0) * 1_000_000.0 / iops

        iops_out.append(iops)
        cpu_avg_out.append(cpu_avg)
        cpu_total_out.append(cpu_total)
        cpu_us_per_io_out.append(cpu_us_per_io)

    return (
        fmt_triplet(iops_out, 2),
        fmt_triplet(cpu_avg_out, 2),
        fmt_triplet(cpu_total_out, 2),
        fmt_triplet(cpu_us_per_io_out, 2),
    )


def run(path: Path) -> int:
    samples: Deque[Sample] = deque()
    max_window = max(WINDOWS)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = parse_line(line)
            if s is None:
                continue
            samples.append(s)
            while samples and (s.ts - samples[0].ts).total_seconds() > max_window:
                samples.popleft()

            iops, cpu_avg, cpu_total, cpu_us_per_io = compute(samples, s.ts)
            print(
                f"ts={s.ts.isoformat()} "
                f"iops_1_5_15m={iops} "
                f"cpu_pct_avg_1_5_15m={cpu_avg} "
                f"cpu_pct_total_1_5_15m={cpu_total} "
                f"cpu_us_per_io_1_5_15m={cpu_us_per_io}"
            )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path, help="Path to controller log file")
    args = parser.parse_args()
    return run(args.log)


if __name__ == "__main__":
    raise SystemExit(main())
