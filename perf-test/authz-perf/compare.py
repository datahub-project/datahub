#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

HARNESS_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(HARNESS_ROOT))

from lib.results import load_jsonl


def _row_effective_expectation(row: dict) -> int:
    correctness = row.get("correctness", {})
    return int(
        correctness.get(
            "expected_status_code",
            correctness.get("fixture_expected_status_code", 200),
        )
    )


def _row_metric_key(row: dict) -> str:
    metric_key = row.get("metric_key")
    if metric_key:
        return str(metric_key)
    return f"{row['operation']}@expect{_row_effective_expectation(row)}"


def _aggregate(rows: List[dict]) -> Dict[Tuple[str, str, str, str], dict]:
    """Key: (run_id, persona, metric_key, cache_phase) -> row (last wins)."""
    out: Dict[Tuple[str, str, str, str], dict] = {}
    for row in rows:
        key = (
            row.get("run_id", ""),
            row["persona"],
            _row_metric_key(row),
            row.get("cache_phase", "warm"),
        )
        out[key] = row
    return out


def _execution_modes(rows: List[dict]) -> set[str]:
    modes = set()
    for row in rows:
        harness = row.get("harness", {})
        modes.add(harness.get("execution_mode", "isolated"))
    return modes


def _deployment_summary(rows: List[dict]) -> dict[str, str | None]:
    if not rows:
        return {}
    deployment = rows[0].get("deployment", {})
    return {
        "target_name": deployment.get("target_name"),
        "gms_host": deployment.get("gms_host"),
        "gms_url": deployment.get("gms_url"),
        "gms_version": deployment.get("gms_version"),
    }


def _authorization_summary(rows: List[dict]) -> dict[str, bool | None]:
    if not rows:
        return {}
    deployment = rows[0].get("deployment", {})
    auth = deployment.get("authorization", {})
    return {"view_enabled": auth.get("view_enabled")}


def _authorization_warnings(
    baseline_rows: List[dict],
    candidate_rows: List[dict],
) -> List[str]:
    base = _authorization_summary(baseline_rows)
    cand = _authorization_summary(candidate_rows)
    warnings: List[str] = []
    base_view = base.get("view_enabled")
    cand_view = cand.get("view_enabled")
    if base_view is not None and cand_view is not None and base_view != cand_view:
        warnings.append(
            "WARN: deployment.authorization.view_enabled differs: "
            f"baseline={base_view!r} candidate={cand_view!r}. "
            "Metrics are keyed by effective expectation (metric_key); compare "
            "matching keys only (e.g. getDomain@expect403 vs getDomain@expect200 "
            "are separate performance profiles)."
        )
    return warnings


def _deployment_warnings(
    baseline_rows: List[dict],
    candidate_rows: List[dict],
    *,
    ignore_deployment: bool,
) -> List[str]:
    if ignore_deployment:
        return []
    base = _deployment_summary(baseline_rows)
    cand = _deployment_summary(candidate_rows)
    warnings: List[str] = []
    for key in ("gms_url", "gms_version", "gms_host"):
        if base.get(key) and cand.get(key) and base.get(key) != cand.get(key):
            warnings.append(
                f"WARN: deployment.{key} differs: baseline={base.get(key)!r} "
                f"candidate={cand.get(key)!r}"
            )
    return warnings


def _unmatched_metric_warnings(
    base: Dict[Tuple[str, str, str, str], dict],
    cand: Dict[Tuple[str, str, str, str], dict],
    *,
    persona_filter: str | None,
    operation_filter: str | None,
) -> List[str]:
    def _apply_filters(keys: set[Tuple[str, str, str, str]]) -> set[Tuple[str, str, str, str]]:
        filtered = keys
        if persona_filter:
            filtered = {k for k in filtered if k[1] == persona_filter}
        if operation_filter:
            filtered = {
                k
                for k in filtered
                if k[2] == operation_filter
                or k[2].startswith(f"{operation_filter}@")
            }
        return filtered

    base_keys = _apply_filters(set(base.keys()))
    cand_keys = _apply_filters(set(cand.keys()))
    only_base = sorted(base_keys - cand_keys)
    only_cand = sorted(cand_keys - base_keys)
    lines: List[str] = []
    if only_base:
        lines.append(
            f"WARN: {len(only_base)} metric_key(s) only in baseline "
            "(different performance profile or missing run):"
        )
        for key in only_base[:8]:
            _, persona, metric_key, phase = key
            profile = base[key].get("performance_profile")
            suffix = f" profile={profile}" if profile else ""
            lines.append(f"  baseline-only: {persona}/{metric_key}/{phase}{suffix}")
        if len(only_base) > 8:
            lines.append(f"  ... +{len(only_base) - 8} more")
    if only_cand:
        lines.append(
            f"WARN: {len(only_cand)} metric_key(s) only in candidate "
            "(different performance profile or missing run):"
        )
        for key in only_cand[:8]:
            _, persona, metric_key, phase = key
            profile = cand[key].get("performance_profile")
            suffix = f" profile={profile}" if profile else ""
            lines.append(f"  candidate-only: {persona}/{metric_key}/{phase}{suffix}")
        if len(only_cand) > 8:
            lines.append(f"  ... +{len(only_cand) - 8} more")
    return lines


def compare_runs(
    baseline_rows: List[dict],
    candidate_rows: List[dict],
    *,
    threshold_p95_ratio: float,
    threshold_max_ratio: float | None,
    persona_filter: str | None,
    operation_filter: str | None,
    allow_concurrent_compare: bool,
    ignore_deployment: bool = False,
) -> tuple[int, List[str]]:
    lines: List[str] = []
    lines.extend(_authorization_warnings(baseline_rows, candidate_rows))
    lines.extend(
        _deployment_warnings(
            baseline_rows, candidate_rows, ignore_deployment=ignore_deployment
        )
    )

    base_dep = _deployment_summary(baseline_rows)
    cand_dep = _deployment_summary(candidate_rows)
    if base_dep.get("gms_host") or base_dep.get("gms_version"):
        lines.append(
            "baseline deployment: "
            f"host={base_dep.get('gms_host')} version={base_dep.get('gms_version')}"
        )
    if cand_dep.get("gms_host") or cand_dep.get("gms_version"):
        lines.append(
            "candidate deployment: "
            f"host={cand_dep.get('gms_host')} version={cand_dep.get('gms_version')}"
        )

    base_modes = _execution_modes(baseline_rows)
    cand_modes = _execution_modes(candidate_rows)
    if base_modes != cand_modes and not allow_concurrent_compare:
        return 1, [
            f"execution_mode mismatch: baseline={base_modes} candidate={cand_modes}. "
            "Use --allow-concurrent-compare to override."
        ]

    base = _aggregate(baseline_rows)
    cand = _aggregate(candidate_rows)
    exit_code = 0

    keys = sorted(set(base.keys()) & set(cand.keys()))
    if persona_filter:
        keys = [k for k in keys if k[1] == persona_filter]
    if operation_filter:
        keys = [
            k
            for k in keys
            if k[2] == operation_filter or k[2].startswith(f"{operation_filter}@")
        ]

    lines.extend(
        _unmatched_metric_warnings(
            base,
            cand,
            persona_filter=persona_filter,
            operation_filter=operation_filter,
        )
    )

    lines.append(
        f"{'persona':<24} {'metric_key':<36} {'profile':<24} {'phase':<6} "
        f"{'p50 Δ':>8} {'p95 Δ':>8} {'max Δ':>8} {'p95 ratio':>10} {'kb Δ':>8}"
    )
    lines.append("-" * 132)

    for key in keys:
        _, persona, metric_key, phase = key
        if base[key].get("cache_phase") != cand[key].get("cache_phase"):
            continue
        b_stats = base[key]["stats"]
        c_stats = cand[key]["stats"]
        b_size = base[key].get("response_size", {})
        c_size = cand[key].get("response_size", {})
        p50_delta = c_stats["p50"] - b_stats["p50"]
        p95_delta = c_stats["p95"] - b_stats["p95"]
        max_delta = c_stats["max"] - b_stats["max"]
        p95_ratio = c_stats["p95"] / b_stats["p95"] if b_stats["p95"] > 0 else 0.0
        max_ratio = c_stats["max"] / b_stats["max"] if b_stats["max"] > 0 else 0.0
        kb_delta = round(
            c_size.get("avg_kb", 0.0) - b_size.get("avg_kb", 0.0), 2
        )
        profile = cand[key].get("performance_profile") or base[key].get(
            "performance_profile", ""
        )

        flag = ""
        if p95_ratio > threshold_p95_ratio:
            flag = " REGRESSION(p95)"
            exit_code = 1
        if threshold_max_ratio and max_ratio > threshold_max_ratio:
            flag += " REGRESSION(max)"
            exit_code = 1

        lines.append(
            f"{persona:<24} {metric_key:<36} {profile:<24} {phase:<6} "
            f"{p50_delta:>+8.1f} {p95_delta:>+8.1f} {max_delta:>+8.1f} "
            f"{p95_ratio:>10.2f} {kb_delta:>+8.2f}{flag}"
        )

    if not keys:
        lines.append("(no matching metric_key rows)")
    return exit_code, lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare authz perf JSONL results")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--threshold-p95-ratio", type=float, default=1.25)
    parser.add_argument("--threshold-max-ratio", type=float, default=None)
    parser.add_argument("--persona", default=None)
    parser.add_argument(
        "--operation",
        default=None,
        help="GraphQL operation or metric_key prefix (e.g. getDomain or getDomain@expect403)",
    )
    parser.add_argument("--allow-concurrent-compare", action="store_true")
    parser.add_argument(
        "--ignore-deployment",
        action="store_true",
        help="Do not warn when gms_url/gms_version/gms_host differ",
    )
    args = parser.parse_args(argv or sys.argv[1:])

    baseline_rows = load_jsonl(args.baseline)
    candidate_rows = load_jsonl(args.candidate)
    code, lines = compare_runs(
        baseline_rows,
        candidate_rows,
        threshold_p95_ratio=args.threshold_p95_ratio,
        threshold_max_ratio=args.threshold_max_ratio,
        persona_filter=args.persona,
        operation_filter=args.operation,
        allow_concurrent_compare=args.allow_concurrent_compare,
        ignore_deployment=args.ignore_deployment,
    )
    print("\n".join(lines))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
