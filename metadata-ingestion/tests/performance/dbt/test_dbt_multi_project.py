"""Scale/throughput harness for multi-project (glob manifest_path) dbt ingestion.

Runs the dbt source in-process and drains its workunits, reporting wall-clock,
workunits/sec, peak memory, and the source's multi-project counters.

    # Algorithmic scalability sweep against an in-process S3 mock (self-seeding):
    python -m tests.performance.dbt.test_dbt_multi_project --moto --sizes 3,10,100

    # Real-world throughput against a bucket seeded via seed_projects:
    python -m tests.performance.dbt.test_dbt_multi_project \
        --manifest-glob "s3://bucket/prefix/*/manifest.json" --profile my-profile

    # Emit a recipe for a manual end-to-end run against a live DataHub instance:
    python -m tests.performance.dbt.test_dbt_multi_project \
        --manifest-glob "s3://bucket/prefix/*/manifest.json" --emit-recipe dbt_perf.yml
"""

import argparse
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import humanfriendly
import psutil

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.utilities.perf_timer import PerfTimer
from tests.performance.dbt.seed_projects import seed
from tests.performance.helpers import workunit_sink

logger = logging.getLogger(__name__)

_MOTO_BUCKET = "dbt-multi-project-perf"
_MOTO_REGION = "us-east-1"
# The jaffle_shop template artifacts were generated with the bigquery adapter.
_TARGET_PLATFORM = "bigquery"


@dataclass
class RunResult:
    seconds: float
    workunits: int
    peak_memory_delta: int
    manifests_loaded: int
    manifests_failed: int
    duplicates: int


def run_ingestion(
    manifest_glob: str, aws_connection: Optional[Dict[str, str]]
) -> RunResult:
    config = DBTCoreConfig(
        manifest_path=manifest_glob,
        aws_connection=aws_connection,
        target_platform=_TARGET_PLATFORM,
        # The default (PATCH) requires a DataHub graph connection; this harness
        # measures the source in isolation.
        write_semantics="OVERRIDE",
    )
    source = DBTCoreSource(config, PipelineContext(run_id="dbt-multi-project-perf"))
    pre_memory = psutil.Process(os.getpid()).memory_info().rss
    with PerfTimer() as timer:
        workunits, peak_memory = workunit_sink(source.get_workunits())
    report = source.report
    duplicates = sum(
        counter or 0
        for counter in (
            report.duplicate_models_detected,
            report.duplicate_node_unique_ids_detected,
            report.duplicate_exposure_unique_ids_detected,
        )
    )
    return RunResult(
        seconds=timer.elapsed_seconds(digits=2),
        workunits=workunits,
        peak_memory_delta=peak_memory - pre_memory,
        manifests_loaded=report.manifests_loaded,
        manifests_failed=report.manifests_failed,
        duplicates=duplicates,
    )


def run_moto_sweep(
    sizes: List[int], include_run_results: bool
) -> List[Tuple[str, RunResult]]:
    from moto import mock_aws

    results: List[Tuple[str, RunResult]] = []
    with mock_aws():
        boto3.client("s3", region_name=_MOTO_REGION).create_bucket(Bucket=_MOTO_BUCKET)
        for size in sizes:
            target = f"s3://{_MOTO_BUCKET}/n{size}"
            logger.info(f"Seeding {size} projects into mock bucket at {target}")
            seed(target, size, include_run_results=include_run_results)
            result = run_ingestion(
                f"{target}/*/manifest.json",
                aws_connection={"aws_region": _MOTO_REGION},
            )
            # Every sweep run doubles as a correctness check of the glob fan-out.
            assert result.manifests_loaded == size, result
            assert result.manifests_failed == 0, result
            assert result.duplicates == 0, result
            assert result.workunits > 0, result
            results.append((f"n{size}", result))
    return results


def run_real(
    manifest_glob: str, profile: Optional[str], region: str
) -> List[Tuple[str, RunResult]]:
    aws_connection: Optional[Dict[str, str]] = None
    if manifest_glob.startswith("s3://"):
        aws_connection = {"aws_region": region}
        if profile:
            aws_connection["aws_profile"] = profile
    return [("run", run_ingestion(manifest_glob, aws_connection))]


def emit_recipe(
    path: str, manifest_glob: str, region: str, profile: Optional[str]
) -> None:
    profile_line = f"\n      aws_profile: {profile}" if profile else ""
    recipe = f"""source:
  type: dbt
  config:
    manifest_path: "{manifest_glob}"
    target_platform: {_TARGET_PLATFORM}
    aws_connection:
      aws_region: {region}{profile_line}
sink:
  type: datahub-rest
  config:
    server: "${{DATAHUB_GMS_URL:-http://localhost:8080}}"
    token: "${{DATAHUB_GMS_TOKEN:-}}"
"""
    Path(path).write_text(recipe)
    logger.info(f"Wrote recipe to {path}; run with: datahub ingest -c {path}")


def print_results(results: List[Tuple[str, RunResult]]) -> None:
    print(
        f"{'run':>8} {'projects':>9} {'failed':>7} {'dups':>5}"
        f" {'workunits':>10} {'seconds':>8} {'wu/s':>8} {'peak mem':>10}"
    )
    for label, r in results:
        wu_per_sec = r.workunits / r.seconds if r.seconds else 0.0
        print(
            f"{label:>8} {r.manifests_loaded:>9} {r.manifests_failed:>7}"
            f" {r.duplicates:>5} {r.workunits:>10} {r.seconds:>8.2f}"
            f" {wu_per_sec:>8.1f} {humanfriendly.format_size(r.peak_memory_delta):>10}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--moto",
        action="store_true",
        help="sweep against an in-process S3 mock, seeding each size itself",
    )
    mode.add_argument(
        "--manifest-glob",
        help="run once against an already-seeded glob, e.g. 's3://bucket/prefix/*/manifest.json'",
    )
    parser.add_argument(
        "--sizes", default="3,10,100", help="moto mode: comma-separated project counts"
    )
    parser.add_argument(
        "--run-results",
        action="store_true",
        help="moto mode: also seed and ingest run_results.json per project",
    )
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument(
        "--emit-recipe",
        metavar="PATH",
        help="write a datahub ingest recipe for --manifest-glob and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if args.emit_recipe:
        if not args.manifest_glob:
            parser.error("--emit-recipe requires --manifest-glob")
        emit_recipe(args.emit_recipe, args.manifest_glob, args.region, args.profile)
        return

    if args.moto:
        sizes = [int(s) for s in args.sizes.split(",")]
        results = run_moto_sweep(sizes, args.run_results)
    else:
        results = run_real(args.manifest_glob, args.profile, args.region)
    print_results(results)


if __name__ == "__main__":
    main()
