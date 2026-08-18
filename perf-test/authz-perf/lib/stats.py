from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class LatencyStats:
    n: int
    min: float
    p50: float
    p95: float
    p99: float
    max: float
    max_to_p50_ratio: float


def _percentile(sorted_samples: Sequence[float], pct: float) -> float:
    if not sorted_samples:
        return 0.0
    if len(sorted_samples) == 1:
        return float(sorted_samples[0])
    k = (len(sorted_samples) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_samples[int(k)])
    d0 = sorted_samples[f] * (c - k)
    d1 = sorted_samples[c] * (k - f)
    return float(d0 + d1)


def compute_stats(samples_ms: Sequence[float]) -> LatencyStats:
    if not samples_ms:
        return LatencyStats(
            n=0, min=0.0, p50=0.0, p95=0.0, p99=0.0, max=0.0, max_to_p50_ratio=0.0
        )
    sorted_samples = sorted(float(x) for x in samples_ms)
    p50 = _percentile(sorted_samples, 50)
    max_val = sorted_samples[-1]
    ratio = max_val / p50 if p50 > 0 else 0.0
    return LatencyStats(
        n=len(sorted_samples),
        min=sorted_samples[0],
        p50=p50,
        p95=_percentile(sorted_samples, 95),
        p99=_percentile(sorted_samples, 99),
        max=max_val,
        max_to_p50_ratio=ratio,
    )


def stats_to_dict(stats: LatencyStats) -> dict[str, float | int]:
    return {
        "n": stats.n,
        "min": round(stats.min, 3),
        "p50": round(stats.p50, 3),
        "p95": round(stats.p95, 3),
        "p99": round(stats.p99, 3),
        "max": round(stats.max, 3),
        "max_to_p50_ratio": round(stats.max_to_p50_ratio, 3),
    }
