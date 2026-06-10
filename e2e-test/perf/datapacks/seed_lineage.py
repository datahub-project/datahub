"""
Lineage seeder for DataHub performance testing.

Reads manifest.json produced by seed_search.py and adds multi-layer lineage:

  Raw → Staging → Trusted → Curated → Analytics → Mart/Reporting
    └─ DataFlow (pipeline per platform+db)
         └─ DataJob (transform per tier transition)
              └─ DataJobInputOutput (input/output dataset lists)

Each non-raw dataset gets an UpstreamLineage aspect pointing at 1-3 datasets
from the tier below, within the same platform+db when possible.

Run (after seed_search.py):
    python e2e-test/perf/datapacks/seed_lineage.py \\
        --manifest e2e-test/perf/seed-output/manifest.json \\
        --host    http://localhost:8080 \\
        [--token  <PAT>] \\
        [--workers 4] \\
        [--hops   3]

MCPs emitted per downstream dataset:
  1 UpstreamLineage + 1 DataJobInfo + 1 DataJobInputOutput
  + 1 DataFlowInfo per (platform, db) pipeline (amortised)

Cleanup:
    datahub delete by-filter --query "urn:li:dataFlow:(airflow,*_etl,PROD)" --hard
    datahub delete by-filter --query "urn:li:dataJob:(*,*_to_*)" --hard
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataFlowInfoClass,
        DataJobInfoClass,
        DataJobInputOutputClass,
        DatasetLineageTypeClass,
        UpstreamClass,
        UpstreamLineageClass,
    )
except ImportError as exc:
    print(
        f"ERROR: datahub SDK not found ({exc}).\n"
        "Install it with: pip install -r e2e-test/perf/implementation_1/requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from datapacks.base_seeder import run_seed  # noqa: E402


# ── Tier ordering ──────────────────────────────────────────────────────────────
# Lower number = closer to raw source. Tier 0 has no upstreams.

TIER_ORDER: dict[str, int] = {
    "raw": 0,
    "bronze": 1,
    "staging": 2,
    "silver": 3,
    "trusted": 4,
    "gold": 5,
    "curated": 6,
    "analytics": 7,
    "mart": 8,
    "reporting": 9,
}

_MAX_UPSTREAMS = 3  # max upstream datasets per downstream dataset


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class LineageSeedConfig:
    manifest_path: str
    host: str = "http://localhost:8080"
    token: str | None = None
    workers: int = 4
    batch_size: int = 100
    seed: int = 42
    hops: int = 3
    output_dir: str = "e2e-test/perf/seed-output"


# ── URN helpers ────────────────────────────────────────────────────────────────

def _parse_dataset_urn(urn: str) -> tuple[str, str, str, str]:
    """Return (platform, db, schema, table) from a dataset URN."""
    # urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.raw.perf_search_orders_fact_000001,PROD)
    inner = urn.removeprefix("urn:li:dataset:(").removesuffix(",PROD)")
    platform_part, rest = inner.split(",", 1)
    platform = platform_part.removeprefix("urn:li:dataPlatform:")
    db, schema, table = rest.split(".", 2)
    return platform, db, schema, table


def _flow_urn(platform: str, db: str) -> str:
    return f"urn:li:dataFlow:(airflow,{platform}_{db}_etl,PROD)"


def _job_urn(flow_urn: str, from_schema: str, to_schema: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{from_schema}_to_{to_schema})"


def _audit_stamp() -> AuditStampClass:
    return AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    )


# ── Graph builder ──────────────────────────────────────────────────────────────

def build_lineage_graph(
    all_urns: list[str],
    hops: int,
    seed: int,
) -> dict[str, list[str]]:
    """
    Build upstream_map: {downstream_urn: [upstream_urn, ...]}

    Datasets are grouped by schema tier. ``hops`` controls how many tier
    transitions to create, counting from the lowest tier present in the data:

      hops=1 → tier_0 ← tier_1  (e.g. raw ← staging)
      hops=2 → tier_0 ← tier_1 ← tier_2  (raw ← staging ← trusted)
      hops=3 → … ← tier_3  (raw ← staging ← trusted ← curated)

    Non-consecutive tier numbers in TIER_ORDER are handled by walking the
    sorted list of *present* tiers, not by requiring TIER_ORDER[T-1] to exist.
    Same-platform/same-db upstream candidates are preferred; falls back to any
    dataset in the next-lower tier when none are available locally.
    """
    rng = random.Random(seed)

    # Parse all URNs
    parsed: list[tuple[str, str, str, str, str]] = []
    for urn in all_urns:
        try:
            platform, db, schema, table = _parse_dataset_urn(urn)
            parsed.append((urn, platform, db, schema, table))
        except (ValueError, IndexError):
            continue

    # Bucket by tier and by (platform, db, tier)
    tier_buckets: dict[int, list[tuple]] = {}
    local_buckets: dict[tuple[str, str, int], list[str]] = {}
    for urn, platform, db, schema, table in parsed:
        tier = TIER_ORDER.get(schema, -1)
        if tier < 0:
            continue
        tier_buckets.setdefault(tier, []).append((urn, platform, db, schema, table))
        local_buckets.setdefault((platform, db, tier), []).append(urn)

    # Walk tiers by *relative hop index*, not absolute tier number.
    # available_tiers = [0, 2, 4, 6, 7, 8] for our SCHEMA_LAYERS vocabulary.
    # hop 1 links available_tiers[1] ← available_tiers[0], etc.
    available_tiers = sorted(tier_buckets.keys())
    upstream_map: dict[str, list[str]] = {}

    for hop in range(1, min(hops, len(available_tiers) - 1) + 1):
        tier = available_tiers[hop]
        prev_tier = available_tiers[hop - 1]

        for urn, platform, db, schema, table in tier_buckets[tier]:
            candidates = local_buckets.get((platform, db, prev_tier), [])
            if not candidates:
                # No same-platform/db candidates — fall back to any dataset
                # from the previous tier.
                candidates = [u for u, *_ in tier_buckets[prev_tier]]
            if not candidates:
                continue
            n = rng.randint(1, min(_MAX_UPSTREAMS, len(candidates)))
            upstream_map[urn] = rng.sample(candidates, n)

    return upstream_map


# ── MCP generator ──────────────────────────────────────────────────────────────

def generate_lineage_mcps(
    upstream_map: dict[str, list[str]],
) -> Iterator[MetadataChangeProposalWrapper]:
    """
    Yield all lineage MCPs in this order:
      1. UpstreamLineage per downstream dataset
      2. DataFlowInfo per (platform, db) pipeline
      3. DataJobInfo per tier transition
      4. DataJobInputOutput per tier transition
    """
    audit = _audit_stamp()

    # Collect tier transitions: (platform, db, from_schema, to_schema) → (inputs, outputs)
    transitions: dict[tuple[str, str, str, str], tuple[list[str], list[str]]] = {}

    # ── 1. UpstreamLineage ────────────────────────────────────────────────────
    for downstream_urn, upstream_urns in upstream_map.items():
        yield MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=u,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                        auditStamp=audit,
                    )
                    for u in upstream_urns
                ]
            ),
        )

        # Group by tier-transition for DataJob creation
        try:
            d_platform, d_db, d_schema, _ = _parse_dataset_urn(downstream_urn)
            u_schema = _parse_dataset_urn(upstream_urns[0])[2]
        except (ValueError, IndexError):
            continue

        key = (d_platform, d_db, u_schema, d_schema)
        ins, outs = transitions.setdefault(key, ([], []))
        ins.extend(upstream_urns)
        outs.append(downstream_urn)

    # ── 2. DataFlowInfo — one per (platform, db) ──────────────────────────────
    emitted_flows: set[str] = set()
    for platform, db, from_schema, to_schema in transitions:
        flow_urn = _flow_urn(platform, db)
        if flow_urn not in emitted_flows:
            emitted_flows.add(flow_urn)
            yield MetadataChangeProposalWrapper(
                entityUrn=flow_urn,
                aspect=DataFlowInfoClass(
                    name=f"{platform} · {db} ETL pipeline",
                    description=(
                        f"Automated ETL pipeline processing data for {db} on {platform}. "
                        "Seeded by DataHub perf-test datapacks."
                    ),
                    customProperties={
                        "platform": platform,
                        "database": db,
                        "orchestrator": "airflow",
                        "perf_seed": "true",
                    },
                ),
            )

    # ── 3 + 4. DataJobInfo + DataJobInputOutput — one job per tier transition ─
    for (platform, db, from_schema, to_schema), (input_urns, output_urns) in transitions.items():
        flow_urn = _flow_urn(platform, db)
        job_urn = _job_urn(flow_urn, from_schema, to_schema)

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=f"{from_schema.capitalize()} → {to_schema.capitalize()} transform",
                type="SQL",
                description=(
                    f"Transforms {db} data from the {from_schema} layer "
                    f"to the {to_schema} layer."
                ),
                flowUrn=flow_urn,
                customProperties={
                    "source_schema": from_schema,
                    "target_schema": to_schema,
                    "perf_seed": "true",
                },
            ),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=list(dict.fromkeys(input_urns)),
                outputDatasets=list(dict.fromkeys(output_urns)),
            ),
        )


# ── Main orchestrator ──────────────────────────────────────────────────────────

def seed_lineage(cfg: LineageSeedConfig) -> None:
    manifest_path = Path(cfg.manifest_path)
    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        print("Run seed_search.py first.", file=sys.stderr)
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text())
    all_urns: list[str] = manifest.get("sample_urns", {}).get("dataset", [])
    if not all_urns:
        print("ERROR: manifest contains no dataset URNs", file=sys.stderr)
        sys.exit(1)

    print("DataHub Lineage Seeder")
    print(f"  manifest  : {manifest_path}")
    print(f"  datasets  : {len(all_urns):,} URNs")
    print(f"  host      : {cfg.host}")
    print(f"  hops      : {cfg.hops} tier transitions")
    print()

    print("Building lineage graph …")
    upstream_map = build_lineage_graph(all_urns, hops=cfg.hops, seed=cfg.seed)
    total_edges = sum(len(v) for v in upstream_map.values())

    # Tier distribution for reporting
    tier_dist: dict[str, int] = {}
    for urn in all_urns:
        try:
            schema = _parse_dataset_urn(urn)[2]
            tier_dist[schema] = tier_dist.get(schema, 0) + 1
        except (ValueError, IndexError):
            pass

    print(f"  {len(upstream_map):,} downstream datasets  |  {total_edges:,} upstream edges")
    print(f"  Schema tiers: { {k: v for k, v in sorted(tier_dist.items())} }")
    # Rough MCP count: UpstreamLineage + DataJobInfo + DataJobInputOutput + DataFlowInfo
    total_mcps_est = len(upstream_map) * 3 + len(set(
        f"{_parse_dataset_urn(u)[0]}_{_parse_dataset_urn(u)[1]}"
        for u in upstream_map
        if len(_parse_dataset_urn(u)) == 4
    ))
    print(f"  ~{total_mcps_est:,} MCPs to emit")
    print()

    start_time = time.monotonic()
    stats = run_seed(
        host=cfg.host,
        token=cfg.token,
        workers=cfg.workers,
        batch_size=cfg.batch_size,
        mcp_iter=generate_lineage_mcps(upstream_map),
        total_mcps=total_mcps_est,
    )
    elapsed = time.monotonic() - start_time

    # Write lineage manifest
    output_dir = Path(cfg.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    lineage_manifest = {
        "schema_version": "1.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_manifest": str(manifest_path),
        "config": {"host": cfg.host, "hops": cfg.hops, "seed": cfg.seed},
        "stats": {
            "downstream_datasets": len(upstream_map),
            "total_upstream_edges": total_edges,
            "emitted_mcps": stats.emitted,
            "errors": stats.errors,
            "elapsed_seconds": round(elapsed, 1),
            "avg_mcp_per_sec": round(stats.emitted / elapsed, 1) if elapsed else 0,
        },
        "topology": {
            "schema_tier_counts": tier_dist,
        },
        "sample_downstream_urns": list(upstream_map.keys())[:100],
    }
    manifest_out = output_dir / "lineage-manifest.json"
    manifest_out.write_text(json.dumps(lineage_manifest, indent=2))

    print()
    print("=" * 60)
    print(f"Lineage seeding complete in {elapsed / 60:.1f} min")
    print(f"  Downstream datasets : {len(upstream_map):,}")
    print(f"  Upstream edges      : {total_edges:,}")
    print(f"  Emitted MCPs        : {stats.emitted:,}")
    print(f"  Errors              : {stats.errors:,}")
    print(f"  Avg rate            : {stats.emitted / elapsed:.0f} MCP/s")
    print(f"  Manifest            : {manifest_out}")
    print()
    print("Next step — explore lineage in DataHub or run a lineage load test:")
    print(f"  datahub graphql --query 'query lineage {{ searchAcrossLineage(...) }}' --format json")

    if stats.errors > stats.emitted * 0.05:
        print(
            f"\nWARNING: error rate {stats.errors / max(stats.emitted, 1) * 100:.1f}% > 5%",
            file=sys.stderr,
        )
        sys.exit(1)


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> LineageSeedConfig:
    p = argparse.ArgumentParser(
        description="Seed DataHub with lineage for performance testing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--manifest", type=str, default="e2e-test/perf/seed-output/manifest.json",
        help="Path to manifest.json written by seed_search.py",
    )
    p.add_argument("--host",       type=str, default="http://localhost:8080")
    p.add_argument("--token",      type=str, default=None)
    p.add_argument("--workers",    type=int, default=4)
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--seed",       type=int, default=42)
    p.add_argument(
        "--hops", type=int, default=3,
        help="Max lineage chain depth (number of tier transitions). "
             "3 = raw→staging→trusted→curated",
    )
    p.add_argument("--output", type=str, default="e2e-test/perf/seed-output")
    args = p.parse_args()
    return LineageSeedConfig(
        manifest_path=args.manifest,
        host=args.host,
        token=args.token or os.environ.get("DATAHUB_GMS_TOKEN"),
        workers=args.workers,
        batch_size=args.batch_size,
        seed=args.seed,
        hops=args.hops,
        output_dir=args.output,
    )


if __name__ == "__main__":
    cfg = _parse_args()
    seed_lineage(cfg)
