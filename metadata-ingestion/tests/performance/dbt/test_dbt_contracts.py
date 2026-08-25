"""Performance benchmark for ``DBTSourceBase.create_contract_mcps``.

Feeds synthetic ``DBTNode`` objects straight into the contract emission
path, bypassing manifest/catalog parsing. The scale sweep is designed to
make an O(models × tests) regression in the test-indexing loop obvious.

Usage:
    pytest tests/performance/dbt/test_dbt_contracts.py::test_benchmark -s --log-cli-level=INFO
    python -m tests.performance.dbt.test_dbt_contracts
"""

import logging
import os
from typing import List, Tuple

import pytest

from datahub.configuration.env_vars import is_ci
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTConstraint,
    DBTContract,
    DBTNode,
)
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.utilities.perf_timer import PerfTimer
from tests.performance.helpers import workunit_sink

logger = logging.getLogger(__name__)

# Single scale on CI to stay within the time budget, full sweep locally.
MODEL_COUNT_OPTIONS: List[int] = [1000] if is_ci() else [100, 1000, 5000]

# Per-model shape. Held fixed across scales so num_models is the only
# variable. Approximates a production dbt project with wide tables and
# a handful of enforced constraints.
COLUMNS_PER_MODEL = 20
CONSTRAINED_COLUMNS_PER_MODEL = 5
TESTS_PER_MODEL = 3
CONTRACT_TAGGED_TEST_FRACTION = 0.5

# Loose throughput floor; the goal is to catch order-of-magnitude regressions,
# not to micro-benchmark the constraint loop.
MIN_MODELS_PER_SECOND = 100.0 if is_ci() else 500.0


def _build_contracted_model(
    idx: int,
    columns_per_model: int,
    constrained_columns_per_model: int,
) -> DBTNode:
    """Build a contracted DBTNode covering every constraint emission path."""
    columns: List[DBTColumn] = []
    for col_idx in range(columns_per_model):
        constraints: List[DBTConstraint] = []
        if col_idx == 0:
            constraints.append(DBTConstraint(type="primary_key"))
            constraints.append(DBTConstraint(type="not_null"))
        elif col_idx == 1:
            constraints.append(DBTConstraint(type="unique"))
        elif col_idx == 2:
            constraints.append(
                DBTConstraint(
                    type="foreign_key",
                    name=f"fk_col_2_model_{idx}",
                    expression="ref_table(id)",
                )
            )
        elif col_idx == 3:
            constraints.append(
                DBTConstraint(
                    type="check",
                    name=f"ck_col_3_model_{idx}",
                    expression=f"col_3 >= {idx}",
                )
            )
        elif col_idx < constrained_columns_per_model:
            constraints.append(DBTConstraint(type="not_null"))

        columns.append(
            DBTColumn(
                name=f"col_{col_idx}",
                comment="",
                description=f"Column {col_idx}",
                index=col_idx,
                data_type="varchar" if col_idx % 2 else "integer",
                constraints=constraints,
            )
        )

    # Composite PK over (col_0, col_1) exercises the multi-column path.
    model_constraints = [
        DBTConstraint(
            type="primary_key",
            name=f"pk_composite_model_{idx}",
            columns=["col_0", "col_1"],
        ),
    ]

    return DBTNode(
        dbt_name=f"model.bench_project.model_{idx}",
        dbt_adapter="snowflake",
        dbt_package_name="bench_project",
        database="BENCH_DB",
        schema="BENCH_SCHEMA",
        name=f"model_{idx}",
        alias=f"model_{idx}",
        dbt_file_path=f"models/model_{idx}.sql",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description=f"Benchmark model {idx}",
        upstream_nodes=[],
        materialization="table",
        catalog_type="BASE TABLE",
        missing_from_catalog=False,
        meta={},
        query_tag={},
        tags=[],
        owner="",
        language="sql",
        raw_code=None,
        compiled_code=None,
        columns=columns,
        contract=DBTContract(
            enforced=True,
            alias_types=True,
            checksum=f"checksum_{idx}",
        ),
        model_constraints=model_constraints,
    )


def _build_test_node(
    test_idx: int,
    model: DBTNode,
    tagged: bool,
    tag_prefix: str,
    contract_test_tag: str,
) -> DBTNode:
    tags = [tag_prefix + contract_test_tag] if tagged else []
    return DBTNode(
        dbt_name=f"test.bench_project.{model.name}_test_{test_idx}",
        dbt_adapter="snowflake",
        dbt_package_name="bench_project",
        database=None,
        schema="BENCH_SCHEMA",
        name=f"{model.name}_test_{test_idx}",
        alias=None,
        dbt_file_path=f"tests/{model.name}_test_{test_idx}.sql",
        node_type="test",
        max_loaded_at=None,
        comment="",
        description="",
        upstream_nodes=[model.dbt_name],
        materialization=None,
        catalog_type=None,
        missing_from_catalog=True,
        meta={},
        query_tag={},
        tags=tags,
        owner="",
        language="sql",
        raw_code=None,
        compiled_code=None,
        columns=[],
    )


def generate_contract_workload(
    num_models: int,
    *,
    columns_per_model: int = COLUMNS_PER_MODEL,
    constrained_columns_per_model: int = CONSTRAINED_COLUMNS_PER_MODEL,
    tests_per_model: int = TESTS_PER_MODEL,
    contract_tagged_test_fraction: float = CONTRACT_TAGGED_TEST_FRACTION,
    tag_prefix: str = "dbt:",
    contract_test_tag: str = "contract",
) -> Tuple[List[DBTNode], List[DBTNode]]:
    """Return a deterministic ``(non_test_nodes, test_nodes)`` pair."""
    model_nodes = [
        _build_contracted_model(
            idx=i,
            columns_per_model=columns_per_model,
            constrained_columns_per_model=constrained_columns_per_model,
        )
        for i in range(num_models)
    ]

    tagged_every = (
        max(int(round(1 / contract_tagged_test_fraction)), 1)
        if contract_tagged_test_fraction > 0
        else 0
    )

    test_nodes: List[DBTNode] = []
    global_test_idx = 0
    for model in model_nodes:
        for t in range(tests_per_model):
            tagged = tagged_every > 0 and (global_test_idx % tagged_every == 0)
            test_nodes.append(
                _build_test_node(
                    test_idx=t,
                    model=model,
                    tagged=tagged,
                    tag_prefix=tag_prefix,
                    contract_test_tag=contract_test_tag,
                )
            )
            global_test_idx += 1

    return model_nodes, test_nodes


def _make_source() -> DBTCoreSource:
    config = DBTCoreConfig(
        manifest_path="temp/manifest.json",
        catalog_path="temp/catalog.json",
        target_platform="snowflake",
        ingest_contracts=True,
        ingest_column_constraints_as_assertions=True,
        enable_meta_mapping=False,
    )
    ctx = PipelineContext(run_id="dbt-contract-benchmark")
    return DBTCoreSource(config, ctx)


def run_benchmark(
    num_models: int,
) -> Tuple[float, int, int]:
    """Return ``(elapsed_seconds, num_mcps, peak_memory_bytes)``."""
    source = _make_source()
    non_test_nodes, test_nodes = generate_contract_workload(num_models=num_models)

    all_nodes_map = {n.dbt_name: n for n in non_test_nodes}
    for t in test_nodes:
        all_nodes_map[t.dbt_name] = t

    with PerfTimer() as timer:
        mcps = list(
            source.create_contract_mcps(non_test_nodes, test_nodes, all_nodes_map)
        )
        num_mcps = len(mcps)
        # workunit_sink is reused solely for its peak-RSS tracking loop.
        _, peak_memory = workunit_sink(iter(mcps))  # type: ignore[arg-type]

    return timer.elapsed_seconds(), num_mcps, peak_memory


@pytest.mark.integration
def test_benchmark() -> None:
    import psutil  # lazy so the module loads under stripped envs

    pre_mem = psutil.Process(os.getpid()).memory_info().rss

    results: List[dict] = []
    for num_models in MODEL_COUNT_OPTIONS:
        elapsed, num_mcps, peak_memory = run_benchmark(num_models=num_models)
        models_per_second = num_models / elapsed if elapsed > 0 else 0.0
        results.append(
            {
                "num_models": num_models,
                "elapsed": elapsed,
                "num_mcps": num_mcps,
                "models_per_second": models_per_second,
                "peak_memory_delta": peak_memory - pre_mem,
            }
        )
        logger.info(
            f"num_models={num_models:>6} "
            f"elapsed={elapsed:>6.2f}s "
            f"mcps={num_mcps:>7} "
            f"throughput={models_per_second:>8.1f} models/s "
            f"Δrss={(peak_memory - pre_mem) / (1024 * 1024):>6.1f} MiB"
        )

    print()
    print("=" * 90)
    print("dbt contract ingestion benchmark")
    print("=" * 90)
    print(
        f"| {'Models':>8} | {'Elapsed (s)':>12} | "
        f"{'MCPs':>10} | {'Models/s':>10} | {'Δ RSS (MiB)':>12} |"
    )
    print(f"| {'-' * 8} | {'-' * 12} | {'-' * 10} | {'-' * 10} | {'-' * 12} |")
    for r in results:
        print(
            f"| {r['num_models']:>8} | "
            f"{r['elapsed']:>12.2f} | "
            f"{r['num_mcps']:>10} | "
            f"{r['models_per_second']:>10.1f} | "
            f"{r['peak_memory_delta'] / (1024 * 1024):>12.1f} |"
        )
    print("=" * 90)
    print()

    failing = [r for r in results if r["models_per_second"] < MIN_MODELS_PER_SECOND]
    assert not failing, (
        f"Performance regression: {len(failing)} configuration(s) below "
        f"{MIN_MODELS_PER_SECOND} models/s threshold: {failing}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_benchmark()
