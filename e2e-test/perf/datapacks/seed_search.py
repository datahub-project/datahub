"""
Search-optimised DataHub seeder for performance testing.

Generates N datasets with realistic names, schemas, tags, and ownership,
then emits them to GMS via multi-threaded REST.  After seeding it writes
manifest.json so the Locust search test can use real URNs without GMS
round-trips at test startup.

Usage (from repo root):
    python e2e-test/perf/datapacks/seed_search.py \\
        --count 1000000 \\
        --host  http://localhost:8080 \\
        [--token  <PAT>] \\
        [--workers  8] \\
        [--batch-size 200] \\
        [--seed  42] \\
        [--output  e2e-test/perf/seed-output]

Throughput expectations (single GMS pod, 6 aspects per dataset):
  ┌─────────┬──────────┬──────────────────────────────┐
  │ Workers │ MCP/s    │ Time for 1 M datasets (~6 M MCPs) │
  ├─────────┼──────────┼──────────────────────────────┤
  │  4      │ ~600     │ ~166 min                     │
  │  8      │ ~1 200   │ ~83 min                      │
  │ 16      │ ~2 000   │ ~50 min                      │
  └─────────┴──────────┴──────────────────────────────┘
Numbers measured on a laptop GMS (Docker, no Kafka consumers).
Production clusters are 2-4× faster.

Cleanup (after testing):
    datahub delete by-filter \\
        --query "urn:li:dataset:(urn:li:dataPlatform:*,perf_search_*,*)" \\
        --batch-size 1000 --workers 4 --hard
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

# ── SDK imports ───────────────────────────────────────────────────────────────
try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        BooleanTypeClass,
        BrowsePathsClass,
        ChangeTypeClass,
        DateTypeClass,
        GlobalTagsClass,
        NumberTypeClass,
        OtherSchemaClass,
        OwnerClass,
        OwnershipClass,
        OwnershipTypeClass,
        SchemaFieldClass,
        SchemaFieldDataTypeClass,
        SchemaMetadataClass,
        StatusClass,
        StringTypeClass,
        TagAssociationClass,
        DatasetPropertiesClass,
    )
except ImportError as exc:
    print(
        f"ERROR: datahub SDK not found ({exc}).\n"
        "Install it with:\n"
        "  pip install -r e2e-test/perf/requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

# ── Vocabulary ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))  # make 'datapacks' importable

from datapacks.base_seeder import _Stats, _worker, _progress_reporter  # noqa: E402
from datapacks.vocab.words import (  # noqa: E402
    COLUMN_POOL,
    COLUMN_TYPE_MAP,
    DATABASE_NAMES,
    DESCRIPTION_FILLERS,
    DESCRIPTION_TEMPLATES,
    DOMAIN_NAMES,
    OWNER_NAMES,
    PLATFORMS,
    SCHEMA_LAYERS,
    TABLE_PREFIXES,
    TABLE_SUFFIXES,
    TAG_NAMES,
)

# ── SDK type class lookup ─────────────────────────────────────────────────────
_SDK_TYPE: dict[str, type] = {
    "string":  StringTypeClass,
    "number":  NumberTypeClass,
    "boolean": BooleanTypeClass,
    "date":    DateTypeClass,
}
# Map native → sdk_key (derived from COLUMN_TYPE_MAP)
_NATIVE_TO_SDK: dict[str, str] = {
    "VARCHAR(255)":  "string",
    "TEXT":          "string",
    "VARCHAR(64)":   "string",
    "CHAR(2)":       "string",
    "BIGINT":        "number",
    "INTEGER":       "number",
    "SMALLINT":      "number",
    "FLOAT":         "number",
    "DOUBLE":        "number",
    "DECIMAL(18,2)": "number",
    "BOOLEAN":       "boolean",
    "DATE":          "date",
    "TIMESTAMP":     "date",
    "TIMESTAMP_NTZ": "date",
    "TIMESTAMP_LTZ": "date",
}


# ── Configuration ─────────────────────────────────────────────────────────────

@dataclass
class SeedConfig:
    count: int = 1_000_000
    host: str = "http://localhost:8080"
    token: str | None = None
    workers: int = 8
    batch_size: int = 200       # MCPs per worker emit call
    seed: int = 42
    output_dir: str = "e2e-test/perf/seed-output"
    min_columns: int = 10
    max_columns: int = 30
    tags_per_dataset: int = 2   # average; drawn from [1, 4]
    owners_per_dataset: int = 2


# ── URN builders ──────────────────────────────────────────────────────────────

# Topology: PLATFORMS × DATABASE_NAMES × SCHEMA_LAYERS slots
# For 1 M datasets: 6 × 15 × 10 = 900 topology slots → ~1 111 tables/slot
_N_PLATFORMS = len(PLATFORMS)
_N_DBS = len(DATABASE_NAMES)
_N_SCHEMAS = len(SCHEMA_LAYERS)
_TOPO_SLOTS = _N_PLATFORMS * _N_DBS * _N_SCHEMAS  # 900


def _dataset_urn(i: int, rng: random.Random) -> tuple[str, str, str, str, str]:
    """
    Return (urn, platform, db, schema, table) for dataset index i.
    Topology is deterministic; table name is seeded-random for realism.
    """
    platform = PLATFORMS[i % _N_PLATFORMS]
    db = DATABASE_NAMES[(i // _N_PLATFORMS) % _N_DBS]
    schema = SCHEMA_LAYERS[(i // (_N_PLATFORMS * _N_DBS)) % _N_SCHEMAS]
    table_slot = i // _TOPO_SLOTS
    prefix = TABLE_PREFIXES[i % len(TABLE_PREFIXES)]
    suffix = TABLE_SUFFIXES[(i // len(TABLE_PREFIXES)) % len(TABLE_SUFFIXES)]
    table = f"perf_search_{prefix}{suffix}_{table_slot:06d}"
    qualified_name = f"{db}.{schema}.{table}"
    urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{qualified_name},PROD)"
    return urn, platform, db, schema, table


# ── MCP generators (pure functions) ──────────────────────────────────────────

def _mk_dataset_properties(
    urn: str, table: str, db: str, schema: str, rng: random.Random
) -> MetadataChangeProposalWrapper:
    tmpl = rng.choice(DESCRIPTION_TEMPLATES)
    desc = tmpl.format(
        entity=rng.choice(DESCRIPTION_FILLERS["entity"]),
        source=rng.choice(DESCRIPTION_FILLERS["source"]),
        domain=rng.choice(DESCRIPTION_FILLERS["domain"]),
        cadence=rng.choice(DESCRIPTION_FILLERS["cadence"]),
        period=rng.choice(DESCRIPTION_FILLERS["period"]),
        team=rng.choice(DESCRIPTION_FILLERS["team"]),
        partition=rng.choice(DESCRIPTION_FILLERS["partition"]),
        regulation=rng.choice(DESCRIPTION_FILLERS["regulation"]),
    )
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(
            name=table.replace("perf_search_", ""),
            description=desc,
            qualifiedName=f"{db}.{schema}.{table}",
            customProperties={
                "perf_seed": "true",
                "schema_layer": schema,
                "domain": rng.choice(DOMAIN_NAMES),
            },
        ),
    )


def _mk_schema_metadata(
    urn: str, platform: str, num_columns: int, rng: random.Random
) -> MetadataChangeProposalWrapper:
    # dict.fromkeys preserves order while deduplicating — guards against any
    # duplicate entries in COLUMN_POOL that would cause GMS schema validation to reject
    # the MCP with "duplicated field paths".
    col_names: list[str] = list(
        dict.fromkeys(rng.sample(COLUMN_POOL, min(num_columns, len(COLUMN_POOL))))
    )
    fields: list[SchemaFieldClass] = []
    for col in col_names:
        native, sdk_key = rng.choice(COLUMN_TYPE_MAP)
        sdk_cls = _SDK_TYPE[sdk_key]
        fields.append(
            SchemaFieldClass(
                fieldPath=col,
                type=SchemaFieldDataTypeClass(type=sdk_cls()),
                nativeDataType=native,
                nullable=rng.random() < 0.3,
                description=f"Column {col}",
            )
        )
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=SchemaMetadataClass(
            schemaName="default",
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        ),
    )


def _mk_global_tags(urn: str, rng: random.Random) -> MetadataChangeProposalWrapper:
    n = rng.randint(1, 4)
    chosen: list[str] = rng.sample(TAG_NAMES, min(n, len(TAG_NAMES)))
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=f"urn:li:tag:{t}")
                for t in chosen
            ]
        ),
    )


def _mk_ownership(urn: str, rng: random.Random) -> MetadataChangeProposalWrapper:
    n = rng.randint(1, 3)
    chosen: list[str] = rng.sample(OWNER_NAMES, min(n, len(OWNER_NAMES)))
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=OwnershipClass(
            owners=[
                OwnerClass(
                    owner=f"urn:li:corpuser:{name}",
                    type=OwnershipTypeClass.DATAOWNER,
                )
                for name in chosen
            ]
        ),
    )


def _mk_status(urn: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=StatusClass(removed=False),
    )


def _mk_browse_paths(
    urn: str, platform: str, db: str, schema: str
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=BrowsePathsClass(paths=[f"/{platform}/{db}/{schema}"]),
    )


# ── Core generator (pure, streaming) ─────────────────────────────────────────

def generate_mcps(cfg: SeedConfig) -> Iterator[tuple[str, MetadataChangeProposalWrapper]]:
    """
    Yield (dataset_urn, mcp) for every MCP in the seed.
    6 MCPs per dataset: properties, schema, tags, ownership, status, browse.
    Total for 1 M datasets: 6 M MCPs.
    """
    global_rng = random.Random(cfg.seed)  # for topology choices only

    for i in range(cfg.count):
        # Per-dataset RNG — deterministic from (global_seed, index)
        rng = random.Random(cfg.seed + i)

        urn, platform, db, schema, table = _dataset_urn(i, rng)
        num_cols = rng.randint(cfg.min_columns, cfg.max_columns)

        yield urn, _mk_dataset_properties(urn, table, db, schema, rng)
        yield urn, _mk_schema_metadata(urn, platform, num_cols, rng)
        yield urn, _mk_global_tags(urn, rng)
        yield urn, _mk_ownership(urn, rng)
        yield urn, _mk_status(urn)
        yield urn, _mk_browse_paths(urn, platform, db, schema)


# ── Manifest writer ───────────────────────────────────────────────────────────

def _write_manifest(
    output_dir: Path,
    cfg: SeedConfig,
    sample_urns: list[str],
    counts: dict[str, int],
    elapsed_s: float,
    run_id: str,
) -> Path:
    manifest = {
        "schema_version": "1.0",
        "run_id": run_id,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config": {
            "count": cfg.count,
            "host": cfg.host,
            "workers": cfg.workers,
            "batch_size": cfg.batch_size,
            "seed": cfg.seed,
            "min_columns": cfg.min_columns,
            "max_columns": cfg.max_columns,
        },
        "counts": counts,
        "elapsed_seconds": round(elapsed_s, 1),
        "avg_mcp_per_sec": round(counts.get("total_mcps", 0) / elapsed_s, 1) if elapsed_s else 0,
        # 1000 sample URNs for Locust to use in queries
        "sample_urns": {
            "dataset": sample_urns[:1000],
        },
        "vocabulary": {
            "platforms": PLATFORMS,
            "tags": TAG_NAMES,
            "owners": OWNER_NAMES,
            "domains": DOMAIN_NAMES,
            "column_pool_size": len(COLUMN_POOL),
        },
        "locust_hint": (
            "Load this file in on_start(): "
            "self.dataset_urns = manifest['sample_urns']['dataset']"
        ),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    return path


# ── Main seeding orchestrator ─────────────────────────────────────────────────

def seed(cfg: SeedConfig) -> None:
    run_id = f"perf-search-{cfg.seed}-{int(time.time())}"
    total_mcps = cfg.count * 6  # 6 aspects per dataset
    output_dir = Path(cfg.output_dir)

    print(f"DataHub Search Seeder")
    print(f"  run_id   : {run_id}")
    print(f"  host     : {cfg.host}")
    print(f"  datasets : {cfg.count:,}")
    print(f"  MCPs     : {total_mcps:,}  (6 per dataset)")
    print(f"  workers  : {cfg.workers}")
    print(f"  seed     : {cfg.seed}")
    print(f"  output   : {output_dir}")
    print()

    # Bounded queue for backpressure — generator blocks when workers are full
    import queue as _queue
    import threading as _threading
    q: _queue.Queue = _queue.Queue(maxsize=cfg.workers * 4)
    stats = _Stats()

    # Start worker threads (base_seeder._worker takes host/token directly)
    threads: list[_threading.Thread] = []
    for wid in range(cfg.workers):
        t = _threading.Thread(
            target=_worker, args=(wid, q, cfg.host, cfg.token, stats),
            daemon=True, name=f"worker-{wid}",
        )
        t.start()
        threads.append(t)

    # Start progress reporter
    stop_progress = _threading.Event()
    progress_thread = _threading.Thread(
        target=_progress_reporter,
        args=(stats, total_mcps, stop_progress),
        daemon=True,
    )
    progress_thread.start()

    # Collect sample URNs as we generate (first 1000 datasets)
    sample_urns: list[str] = []
    seen_urns: set[str] = set()

    start_time = time.monotonic()
    batch: list[MetadataChangeProposalWrapper] = []

    try:
        for urn, mcp in generate_mcps(cfg):
            batch.append(mcp)
            if urn not in seen_urns and len(sample_urns) < 1000:
                sample_urns.append(urn)
                seen_urns.add(urn)
            if len(batch) >= cfg.batch_size:
                q.put(batch)
                batch = []

        # Flush remainder
        if batch:
            q.put(batch)

    except KeyboardInterrupt:
        print("\nInterrupted — flushing remaining work …")

    # Send poison pills to stop workers
    for _ in threads:
        q.put(None)

    # Wait for all workers to finish
    for t in threads:
        t.join()

    stop_progress.set()
    progress_thread.join(timeout=2)

    elapsed = time.monotonic() - start_time

    counts = {
        "dataset": cfg.count,
        "datasetProperties": cfg.count,
        "schemaMetadata": cfg.count,
        "globalTags": cfg.count,
        "ownership": cfg.count,
        "status": cfg.count,
        "browsePaths": cfg.count,
        "total_mcps": stats.emitted,
        "errors": stats.errors,
    }

    manifest_path = _write_manifest(output_dir, cfg, sample_urns, counts, elapsed, run_id)

    print()
    print("=" * 60)
    print(f"Seeding complete in {elapsed/60:.1f} min")
    print(f"  Emitted  : {stats.emitted:,} MCPs")
    print(f"  Errors   : {stats.errors:,}")
    print(f"  Avg rate : {stats.emitted / elapsed:.0f} MCP/s")
    print(f"  Manifest : {manifest_path}")
    print()
    print("Next step — run a load test:")
    print(f"  # Implementation 1 (plain Locust):")
    print(f"  cd e2e-test/perf/implementation_1")
    print(f"  locust -f tests/search/graphql_search_ramp.py -H {cfg.host}")
    print()
    print(f"  # Implementation 2 (Grasshopper — from e2e-test/perf/):")
    print(f"  pytest implementation_2/tests/test_search_load.py \\")
    print(f"      --scenario_file=implementation_2/scenarios/search_scenarios.yaml \\")
    print(f"      --scenario_name=search_smoke -H {cfg.host}")

    if stats.errors > stats.emitted * 0.05:
        print(
            f"\nWARNING: error rate {stats.errors/max(stats.emitted,1)*100:.1f}% > 5% — "
            "check GMS connectivity or increase --batch-size.",
            file=sys.stderr,
        )
        sys.exit(1)


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args() -> SeedConfig:
    p = argparse.ArgumentParser(
        description="Seed DataHub with search-optimised synthetic datasets.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--count",      type=int,   default=1_000_000, help="Number of datasets to generate")
    p.add_argument("--host",       type=str,   default="http://localhost:8080", help="GMS base URL")
    p.add_argument("--token",      type=str,   default=None,      help="DataHub PAT (omit for no-auth)")
    p.add_argument("--workers",    type=int,   default=8,         help="Parallel emitter threads")
    p.add_argument("--batch-size", type=int,   default=200,       help="MCPs per worker batch")
    p.add_argument("--seed",       type=int,   default=42,        help="RNG seed for reproducibility")
    p.add_argument("--output",     type=str,   default="e2e-test/perf/seed-output", help="Output dir for manifest.json")
    p.add_argument("--min-cols",   type=int,   default=10,        help="Min columns per dataset")
    p.add_argument("--max-cols",   type=int,   default=30,        help="Max columns per dataset")
    args = p.parse_args()
    return SeedConfig(
        count=args.count,
        host=args.host,
        token=args.token or os.environ.get("DATAHUB_GMS_TOKEN"),
        workers=args.workers,
        batch_size=args.batch_size,
        seed=args.seed,
        output_dir=args.output,
        min_columns=args.min_cols,
        max_columns=args.max_cols,
    )


if __name__ == "__main__":
    cfg = _parse_args()
    seed(cfg)
