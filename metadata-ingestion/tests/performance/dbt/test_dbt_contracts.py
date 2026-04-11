"""Performance benchmark for dbt Data Contract ingestion.

Exercises the contract-specific code path (``create_contract_mcps`` and its
helpers) in isolation from manifest/catalog parsing, so the numbers reflect
the cost of contract emission specifically rather than general dbt ingestion
overhead.

The hotspot of interest is ``_get_contract_tests_for_node``, which scans the
full test-node list for every contracted model. At N models × M tests, the
worst case is O(N·M); this benchmark makes that cost measurable and gives us
a regression guardrail as the feature evolves.

Usage:
    # Run as a pytest test (CI-safe scale).
    pytest tests/performance/dbt/test_dbt_contracts.py::test_benchmark -s --log-cli-level=INFO

    # Run directly for ad-hoc benchmarking across larger scales.
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

# CI runners are noisy and time-limited, so we only exercise a single scale there.
# Locally we sweep a few orders of magnitude to make the per-model cost visible.
MODEL_COUNT_OPTIONS: List[int] = [1000] if is_ci() else [100, 1000, 5000]

# Structural parameters held fixed across scales so the only variable is the
# number of contracted models. Values approximate a "chunky" production dbt
# project: wide tables with a handful of enforced constraints and a few
# contract-tagged tests per model.
COLUMNS_PER_MODEL = 20
CONSTRAINED_COLUMNS_PER_MODEL = 5
TESTS_PER_MODEL = 3
CONTRACT_TAGGED_TEST_FRACTION = 0.5

# Per-model throughput floor. This is intentionally loose — the goal is to
# catch order-of-magnitude regressions (e.g. an accidentally quadratic pass
# over test_nodes), not to micro-benchmark the constraint loop.
MIN_MODELS_PER_SECOND = 100.0 if is_ci() else 500.0


def _build_contracted_model(
    idx: int,
    columns_per_model: int,
    constrained_columns_per_model: int,
) -> DBTNode:
    """Build a single contracted DBTNode with a realistic constraint mix.

    Each model carries every constraint type the feature supports so the
    benchmark exercises all emission paths at scale — not just ``not_null``
    and ``primary_key``:

    - col_0: ``primary_key`` + ``not_null`` (the PK decomposition path)
    - col_1: ``unique`` (the UNIQUE_PROPOTION assertion path)
    - col_2: ``foreign_key`` with an expression (the _NATIVE_ path)
    - col_3: ``check`` with an expression (another _NATIVE_ path)
    - cols 4..constrained-1: ``not_null`` (the single-constraint path)
    - remaining cols: no constraints (exercise the skip-over path)

    A composite ``primary_key`` on col_0 + col_1 is also added as a
    model-level constraint so the multi-column assertion path is
    exercised. ``model_constraints`` is intentionally non-empty even
    though col_0 already has its own PK — real dbt projects frequently
    have both column-level and model-level constraints, and the benchmark
    should catch any regression that treats the combination poorly.
    """
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

    model_constraints = [
        # Composite primary key over (col_0, col_1) exercises the
        # multi-column unique + per-column not_null decomposition path.
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
    """Build a dbt test node attached to ``model``.

    ``tagged`` controls whether the test carries the contract tag, so we can
    measure both the happy path (test is emitted as part of the contract) and
    the skip path (``_get_contract_tests_for_node`` has to scan past it).
    """
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
    """Build a synthetic workload of contracted models + associated tests.

    Returns a ``(non_test_nodes, test_nodes)`` pair in the shape that
    ``create_contract_mcps`` expects. The workload is fully deterministic so
    runs are comparable across invocations.
    """
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
    """Build a minimal ``DBTCoreSource`` wired for contract ingestion.

    The config paths point at a throwaway directory because the benchmark
    bypasses manifest/catalog loading entirely — we feed synthetic DBTNode
    objects straight into ``create_contract_mcps``.
    """
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
    """Run a single benchmark configuration.

    Returns ``(elapsed_seconds, num_mcps, peak_memory_bytes)``.
    """
    source = _make_source()
    non_test_nodes, test_nodes = generate_contract_workload(num_models=num_models)

    # create_contract_mcps needs the full node map so it can call
    # get_upstreams_for_test to compute URN references identical to the ones
    # create_test_entity_mcps emits. The benchmark's workload has no filtered
    # upstreams, so this is just the union of non_test_nodes and test_nodes.
    all_nodes_map = {n.dbt_name: n for n in non_test_nodes}
    for t in test_nodes:
        all_nodes_map[t.dbt_name] = t

    # Wrap the generator in a workunit_sink-compatible adapter to reuse the
    # existing peak-memory helper (it expects MetadataWorkUnit instances, but
    # only touches ``.id`` via the enumerate loop).
    with PerfTimer() as timer:
        mcps = list(
            source.create_contract_mcps(non_test_nodes, test_nodes, all_nodes_map)
        )
        num_mcps = len(mcps)
        # Consume via workunit_sink-style loop purely to capture peak RSS.
        _, peak_memory = workunit_sink(iter(mcps))  # type: ignore[arg-type]

    return timer.elapsed_seconds(), num_mcps, peak_memory


@pytest.mark.integration
def test_benchmark() -> None:
    """Run benchmark across configured scales and log a results table.

    Asserts only a loose per-model throughput floor to catch order-of-magnitude
    regressions. Precise numbers will vary between environments.
    """
    import psutil  # imported lazily so the module loads under stripped envs

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
