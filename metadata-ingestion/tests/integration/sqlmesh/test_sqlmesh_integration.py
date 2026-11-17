"""Integration tests for SqlmeshSource.

Strategy: full source pipeline run against a comprehensive mocked SqlmeshContext
that returns deterministic fixture data. The pipeline writes to a local JSON file
sink and the output is compared against a golden file.

Run tests:
    pytest tests/integration/sqlmesh/ -v

Re-generate the golden file after intentional changes:
    pytest tests/integration/sqlmesh/ -v --update-golden-files
"""

from __future__ import annotations

import json
import pathlib
from typing import Any, Iterable, List, Type, TypeVar
from unittest.mock import MagicMock, patch

import pytest
import sqlglot
import time_machine
from sqlglot import exp

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sqlmesh.models import _CapabilityProbes
from datahub.ingestion.source.sqlmesh.sqlmesh_config import SqlmeshSourceConfig
from datahub.ingestion.source.sqlmesh.sqlmesh_source import SqlmeshSource
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    DataPlatformInstanceClass,
    SiblingsClass,
    UpstreamLineageClass,
)
from datahub.testing import mce_helpers

_AspectT = TypeVar("_AspectT")


def _aspects_of_type(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> List[_AspectT]:
    # wu.metadata is a union (MCE/MCP/MCPW); only MCP/MCPW carry `.aspect`.
    # Narrowing by isinstance keeps the assertions runtime-safe and mypy-clean.
    out: List[_AspectT] = []
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, aspect_type):
            out.append(aspect)
    return out


pytestmark = pytest.mark.integration_batch_2

# Explicit UTC offset so time_machine doesn't interpret this as host-local time —
# a naive value shifts the emitted "now" timestamps by the runner's TZ, which makes
# golden regeneration environment-dependent.
FROZEN_TIME = "2024-07-01 00:00:00+00:00"
INTEGRATION_DIR = pathlib.Path(__file__).parent
GOLDEN_FILE = INTEGRATION_DIR / "sqlmesh_mces_golden.json"
HAPPY_PATH_GOLDEN_FILE = INTEGRATION_DIR / "sqlmesh_mces_happy_path_golden.json"


# ---------------------------------------------------------------------------
# Fixtures — deterministic fake SQLMesh project
# ---------------------------------------------------------------------------


def _make_col_type(name: str) -> MagicMock:
    t = MagicMock()
    t.__str__ = lambda self: name  # type: ignore[method-assign, misc, assignment]
    return t


def _make_model(
    name: str,
    columns: dict[str, str],
    depends_on: set[str],
    description: str | None = None,
    kind: str = "FULL",
    audits: list[tuple[str, dict[str, Any]]] | None = None,
) -> MagicMock:
    model = MagicMock()
    model.name = name
    model.columns_to_types = {
        col: _make_col_type(dtype) for col, dtype in columns.items()
    }
    model.depends_on = depends_on
    model.description = description
    model.column_descriptions = {}
    model.tags = []
    model.owner = None
    model.audits = audits or []
    model.cron = None
    model.interval_unit = None
    model.start = None
    model.time_column = None
    model.partitioned_by = []
    model.grains = []
    k = MagicMock()
    k.__str__ = lambda self: kind  # type: ignore[method-assign, misc, assignment]
    k.model_kind_name = kind
    k.is_embedded = False
    model.kind = k
    return model


def _make_snapshot(model_name: str, physical_name: str) -> MagicMock:
    snapshot = MagicMock()
    snapshot.name = model_name
    phys = MagicMock()
    phys.__str__ = lambda self: physical_name  # type: ignore[method-assign, misc, assignment]
    snapshot.table_name = MagicMock(return_value=phys)
    return snapshot


def _build_fake_sqlmesh_context() -> MagicMock:
    """Three interconnected SQLMesh models with realistic column types."""
    raw_orders = _make_model(
        name="myschema.raw_orders",
        columns={
            "id": "BIGINT",
            "customer_id": "BIGINT",
            "status": "VARCHAR",
            "amount": "DOUBLE",
            "created_at": "TIMESTAMP",
        },
        depends_on=set(),
        description="Raw orders loaded from source system.",
        kind="FULL",
    )
    orders = _make_model(
        name="myschema.orders",
        columns={
            "order_id": "BIGINT",
            "customer_id": "BIGINT",
            "status": "VARCHAR",
            "amount": "DOUBLE",
            "order_date": "DATE",
        },
        depends_on={"myschema.raw_orders"},
        description="Cleaned and enriched orders.",
        kind="FULL",
        # A built-in column audit (→ one CUSTOM assertion per column with a field
        # target) and a project-defined audit unknown to the built-in map (→ one
        # CUSTOM assertion carrying its own SQL logic). Both must serialise as
        # AssertionType.CUSTOM in the golden.
        audits=[
            (
                "not_null",
                {
                    "columns": exp.Array(
                        expressions=[
                            exp.column("order_id"),
                            exp.column("customer_id"),
                        ]
                    )
                },
            ),
            (
                "assert_amount_positive",
                {"criteria": sqlglot.condition("amount >= 0")},
            ),
        ],
    )
    order_items = _make_model(
        name="myschema.order_items",
        columns={
            "order_id": "BIGINT",
            "item_id": "BIGINT",
            "quantity": "INT",
            "unit_price": "DOUBLE",
            "ds": "DATE",
        },
        depends_on={"myschema.orders"},
        description=None,
        kind="INCREMENTAL_BY_TIME_RANGE",
    )

    models: dict[str, Any] = {
        "myschema.raw_orders": raw_orders,
        "myschema.orders": orders,
        "myschema.order_items": order_items,
    }

    snap_raw = _make_snapshot(
        "myschema.raw_orders",
        "mywarehouse.sqlmesh__myschema.myschema__raw_orders__1234567890",
    )
    snap_orders = _make_snapshot(
        "myschema.orders",
        "mywarehouse.sqlmesh__myschema.myschema__orders__2345678901",
    )
    snap_items = _make_snapshot(
        "myschema.order_items",
        "mywarehouse.sqlmesh__myschema.myschema__order_items__3456789012",
    )
    # Key snapshots by model name, mirroring the real SQLMesh ``ctx.snapshots``
    # contract (the source looks them up via ``snapshots.get(model_name)``), so
    # the fixture actually exercises name-based lookup rather than opaque ints.
    snapshots = {
        "myschema.raw_orders": snap_raw,
        "myschema.orders": snap_orders,
        "myschema.order_items": snap_items,
    }

    ctx = MagicMock()
    ctx.models = models
    ctx.snapshots = snapshots
    # get_model resolves the fixture models by name (and None for undeclared
    # refs), so lineage routing (managed vs external vs undeclared-warehouse) is
    # driven by real lookups instead of a blanket truthy MagicMock.
    ctx.get_model = models.get
    return ctx


# ---------------------------------------------------------------------------
# Golden-file integration test
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_sqlmesh_ingestion_golden_file(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    output_path = tmp_path / "sqlmesh_mces.json"

    pipeline = Pipeline.create(
        {
            # Pin the run id so systemMetadata.runId is deterministic; the
            # default appends a random suffix, which would make the golden's
            # hard-coded runId values flaky.
            "run_id": "sqlmesh-test",
            "source": {
                "type": "sqlmesh",
                "config": {
                    "project_path": "/fake/sqlmesh_project",
                    "gateway": "my_warehouse",
                    "environment": "prod",
                    "target_platform": "snowflake",
                    "env": "PROD",
                    "convert_urns_to_lowercase": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )

    with (
        patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            return_value=_build_fake_sqlmesh_context(),
        ),
        # Degraded path: state store and warehouse unreachable, so no
        # fingerprint-rebuild operations or row-count profiles are emitted.
        # Pin it explicitly rather than relying on how the MagicMock context
        # happens to answer the probe — the happy-path golden covers the
        # full-signal case.
        patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source._probe_capabilities",
            return_value=_CapabilityProbes(
                has_state=False, has_warehouse_query=False, has_graph=False
            ),
        ),
    ):
        pipeline.run()

    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_FILE,
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


# ---------------------------------------------------------------------------
# Happy-path golden: state store + warehouse reachable, plus audit results
# ---------------------------------------------------------------------------


def _build_happy_path_context() -> MagicMock:
    """Same project as the minimal golden, but with SQLMesh state and the
    warehouse both reachable.

    Snapshots are keyed by model name (so the operation/profile lookups hit),
    carry an ``updated_ts`` for the fingerprint-rebuild ``OperationAspect``, and
    the engine adapter answers ``COUNT(*)`` for ``DatasetProfile.rowCount``.
    ``engine_adapters`` is an empty dict so profiling falls back to the single
    default adapter rather than a MagicMock stand-in.
    """
    ctx = _build_fake_sqlmesh_context()

    physical_names = {
        "myschema.raw_orders": "mywarehouse.sqlmesh__myschema.myschema__raw_orders__1234567890",
        "myschema.orders": "mywarehouse.sqlmesh__myschema.myschema__orders__2345678901",
        "myschema.order_items": "mywarehouse.sqlmesh__myschema.myschema__order_items__3456789012",
    }
    snapshots_by_name: dict[str, Any] = {}
    for name, phys in physical_names.items():
        snap = _make_snapshot(name, phys)
        snap.updated_ts = 1_719_792_000_000  # 2024-07-01, fixed for determinism
        snapshots_by_name[name] = snap
    ctx.snapshots = snapshots_by_name

    adapter = MagicMock()
    adapter.fetchone.return_value = (100,)
    ctx.engine_adapter = adapter
    ctx.engine_adapters = {}
    return ctx


@time_machine.travel(FROZEN_TIME)
def test_sqlmesh_happy_path_golden_file(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """End-to-end golden for the full-signal path: assertion definitions plus
    run events (with an incident on failure), fingerprint-rebuild operations,
    and row-count profiles. The minimal golden covers the degraded path where
    state/warehouse are unreachable; this one proves the wiring through
    get_workunits_internal when they are."""
    audit_results_path = tmp_path / "audit_results.json"
    audit_results_path.write_text(
        json.dumps(
            {
                "metadata": {"generated_at": "2024-07-01T00:00:00"},
                "results": [
                    {
                        "model": "myschema.orders",
                        "audit": "not_null",
                        "columns": ["order_id"],
                        "status": "pass",
                        "failing_rows": 0,
                    },
                    {
                        "model": "myschema.orders",
                        "audit": "not_null",
                        "columns": ["customer_id"],
                        "status": "fail",
                        "failing_rows": 5,
                    },
                    {
                        "model": "myschema.orders",
                        "audit": "assert_amount_positive",
                        "status": "pass",
                        "failing_rows": 0,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "sqlmesh_mces_happy_path.json"

    pipeline = Pipeline.create(
        {
            # Pin the run id (see the minimal golden test) for a deterministic
            # systemMetadata.runId in the golden.
            "run_id": "sqlmesh-test",
            "source": {
                "type": "sqlmesh",
                "config": {
                    "project_path": "/fake/sqlmesh_project",
                    "gateway": "my_warehouse",
                    "environment": "prod",
                    "target_platform": "snowflake",
                    "env": "PROD",
                    "convert_urns_to_lowercase": True,
                    "audit_results_path": str(audit_results_path),
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )

    with (
        patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            return_value=_build_happy_path_context(),
        ),
        patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source._probe_capabilities",
            return_value=_CapabilityProbes(
                has_state=True, has_warehouse_query=True, has_graph=False
            ),
        ),
    ):
        pipeline.run()

    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=HAPPY_PATH_GOLDEN_FILE,
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


# ---------------------------------------------------------------------------
# Structural completeness tests (run without golden file)
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_sqlmesh_event_count_and_coverage() -> None:
    """All three entity types and key aspects must appear in the output."""
    config = SqlmeshSourceConfig.model_validate(
        {
            "project_path": "/fake/proj",
            "target_platform": "snowflake",
            "env": "PROD",
            "convert_urns_to_lowercase": True,
        }
    )
    source = SqlmeshSource(config, PipelineContext(run_id="test-structural"))

    with patch(
        "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
        return_value=_build_fake_sqlmesh_context(),
    ):
        workunits = list(source.get_workunits_internal())

    aspect_types = {
        type(getattr(wu.metadata, "aspect", None)).__name__
        for wu in workunits
        if getattr(wu.metadata, "aspect", None) is not None
    }

    # Core aspects expected
    assert "DatasetPropertiesClass" in aspect_types, "Missing DatasetProperties"
    assert "SchemaMetadataClass" in aspect_types, "Missing SchemaMetadata"
    assert "UpstreamLineageClass" in aspect_types, "Missing UpstreamLineage"
    assert "SiblingsClass" in aspect_types, "Missing Siblings"
    assert "DataPlatformInstanceClass" in aspect_types, "Missing DataPlatformInstance"

    # Event count: 3 models × (dataPlatformInstance + schemaMetadata + datasetProperties)
    # + 2 upstreamLineage + 3 SiblingsClass writes + 3 warehouse sibling patches
    # = 17 minimum from get_workunits_internal()
    assert len(workunits) >= 17, f"Too few events: {len(workunits)}"

    # Siblings: each model writes one SiblingsClass aspect on the SQLMesh entity;
    # the warehouse counterpart is a DatasetPatchBuilder patch (GenericAspect,
    # is_primary_source=False), not a SiblingsClass. So exactly 3 SiblingsClass.
    sibling_wus = [
        wu
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
    ]
    assert len(sibling_wus) == 3, (
        f"Expected 3 SiblingsClass MCPs, got {len(sibling_wus)}"
    )

    # ... plus one warehouse-side sibling patch per model, marked non-authoritative.
    warehouse_sibling_patches = [
        wu
        for wu in workunits
        if wu.is_primary_source is False
        and "snowflake" in str(getattr(wu.metadata, "entityUrn", ""))
    ]
    assert len(warehouse_sibling_patches) == 3, (
        f"Expected 3 warehouse sibling patches, got {len(warehouse_sibling_patches)}"
    )

    # Lineage: orders → raw_orders, order_items → orders
    lineage_wus = [
        wu
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
    ]
    assert len(lineage_wus) == 2, f"Expected 2 lineage edges, got {len(lineage_wus)}"

    # dataPlatformInstance emitted for each logical model (SDK V2 auto-emits).
    # Containers also emit dataPlatformInstance, so filter to dataset entities only.
    platform_instance_wus = [
        wu
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), DataPlatformInstanceClass)
        and "dataset" in getattr(wu.metadata, "entityType", "").lower()
    ]
    assert len(platform_instance_wus) == 3, (
        f"Expected 3 dataPlatformInstance aspects on datasets, got {len(platform_instance_wus)}"
    )

    # Audits on the orders model become CUSTOM assertions (never FRESHNESS/VOLUME,
    # which the rework removed): not_null over two columns + one custom-SQL audit.
    assertion_infos = _aspects_of_type(workunits, AssertionInfoClass)
    assert len(assertion_infos) == 3, (
        f"Expected 3 CUSTOM assertions, got {len(assertion_infos)}"
    )
    assert all(info.type == "CUSTOM" for info in assertion_infos), (
        "All SQLMesh audits must map to AssertionType.CUSTOM"
    )

    assert source.report.models_scanned == 3
    assert len(source.report.models_failed) == 0
