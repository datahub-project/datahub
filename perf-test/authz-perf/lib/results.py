from __future__ import annotations

import json
import queue
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from lib.stats import compute_stats, stats_to_dict

RESULTS_DIR = Path.home() / ".datahub" / "authz-perf-results"
SCHEMA_VERSION = 2


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id() -> str:
    return f"{utc_now_iso()}-{uuid.uuid4().hex[:8]}"


class JsonlWriter:
    """Thread-safe append-only JSONL writer."""

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._queue: queue.Queue[Optional[dict]] = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        with self.output_path.open("a", encoding="utf-8") as fh:
            while not self._stop.is_set() or not self._queue.empty():
                try:
                    row = self._queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                if row is None:
                    break
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
                fh.flush()
                self._queue.task_done()

    def append(self, row: dict) -> None:
        self._queue.put(row)

    def close(self) -> None:
        self._stop.set()
        self._queue.put(None)
        self._thread.join(timeout=30)


def build_result_row(
    *,
    run_id: str,
    metadata: dict,
    persona: str,
    operation: str,
    query_source: str,
    stress_target: str,
    membership_count: int,
    cache_phase: str,
    harness: dict,
    samples_ms: List[float],
    metric_key: str,
    performance_profile: str,
    cold_first_ms: Optional[float] = None,
    request_health: Optional[dict] = None,
    response_size: Optional[dict] = None,
    correctness: Optional[dict] = None,
) -> dict:
    stats = compute_stats(samples_ms)
    row: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "recorded_at": utc_now_iso(),
        **metadata,
        "harness": harness,
        "persona": persona,
        "operation": operation,
        "metric_key": metric_key,
        "performance_profile": performance_profile,
        "query_source": query_source,
        "stress_target": stress_target,
        "membership_count": membership_count,
        "cache_phase": cache_phase,
        "samples_ms": [round(x, 3) for x in samples_ms],
        "stats": stats_to_dict(stats),
        "request_health": request_health
        or {
            "total_requests": len(samples_ms),
            "success_count": len(samples_ms),
            "failure_count": 0,
            "status_codes": {200: len(samples_ms)} if samples_ms else {},
            "failures": [],
        },
        "response_size": response_size
        or {
            "avg_kb": 0.0,
            "min_kb": 0.0,
            "max_kb": 0.0,
            "sample_count": len(samples_ms),
        },
        "correctness": correctness
        or {"init_passed": True, "full_correctness": False, "assertions_checked": []},
    }
    if cold_first_ms is not None:
        row["cold_first_ms"] = round(cold_first_ms, 3)
    return row


def write_summary(output_path: Path, rows: List[dict]) -> Path:
    summary_path = output_path.with_suffix(".summary.json")
    by_key: Dict[str, dict] = {}
    for row in rows:
        mk = row.get("metric_key") or row["operation"]
        key = f"{row['persona']}:{mk}:{row['cache_phase']}"
        by_key[key] = {
            "persona": row["persona"],
            "operation": row["operation"],
            "metric_key": mk,
            "performance_profile": row.get("performance_profile"),
            "cache_phase": row["cache_phase"],
            "stats": row["stats"],
            "run_id": row["run_id"],
        }
    summary_path.write_text(
        json.dumps(
            {"generated_at": utc_now_iso(), "results": list(by_key.values())}, indent=2
        )
        + "\n",
        encoding="utf-8",
    )
    return summary_path


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows
