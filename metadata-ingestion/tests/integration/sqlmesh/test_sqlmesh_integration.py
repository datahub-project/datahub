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

import pathlib
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.source.sqlmesh.sqlmesh_source import SqlmeshSource
from datahub.testing import mce_helpers

# Register source in case pyproject.toml entry points haven't been regenerated yet
source_registry.register("sqlmesh", SqlmeshSource, override=True)

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2024-07-01 00:00:00"
INTEGRATION_DIR = pathlib.Path(__file__).parent
GOLDEN_FILE = INTEGRATION_DIR / "sqlmesh_mces_golden.json"


# ---------------------------------------------------------------------------
# Fixtures — deterministic fake SQLMesh project
# ---------------------------------------------------------------------------


def _make_col_type(name: str) -> MagicMock:
    t = MagicMock()
    t.__str__ = lambda self: name
    return t


def _make_model(
    name: str,
    columns: dict[str, str],
    depends_on: set[str],
    description: str | None = None,
    kind: str = "FULL",
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
    model.audits = []
    model.cron = None
    model.start = None
    model.time_column = None
    model.partitioned_by = []
    model.grains = []
    k = MagicMock()
    k.__str__ = lambda self: kind
    k.model_kind_name = kind
    k.is_embedded = False
    model.kind = k
    return model


def _make_snapshot(model_name: str, physical_name: str) -> MagicMock:
    snapshot = MagicMock()
    snapshot.name = model_name
    phys = MagicMock()
    phys.__str__ = lambda self: physical_name
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
    snapshots = {1: snap_raw, 2: snap_orders, 3: snap_items}

    ctx = MagicMock()
    ctx.models = models
    ctx.snapshots = snapshots
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
            "source": {
                "type": "sqlmesh",
                "config": {
                    "projects": [
                        {
                            "project_path": "/fake/sqlmesh_project",
                            "gateway": "my_warehouse",
                            "environment": "prod",
                        }
                    ],
                    "target_platform": "snowflake",
                    "env": "PROD",
                    "convert_urns_to_lowercase": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )

    with patch(
        "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
        return_value=_build_fake_sqlmesh_context(),
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
# Structural completeness tests (run without golden file)
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_sqlmesh_event_count_and_coverage() -> None:
    """All three entity types and key aspects must appear in the output."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.sqlmesh.sqlmesh_config import SqlmeshSourceConfig
    from datahub.ingestion.source.sqlmesh.sqlmesh_source import SqlmeshSource
    from datahub.metadata.schema_classes import (
        DataPlatformInstanceClass,
        SiblingsClass,
        UpstreamLineageClass,
    )

    config = SqlmeshSourceConfig.model_validate(
        {
            "projects": [{"project_path": "/fake/proj"}],
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
        type(wu.metadata.aspect).__name__
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
    # + 2 upstreamLineage + 6 sibling MCPs = 17 minimum from get_workunits_internal()
    assert len(workunits) >= 17, f"Too few events: {len(workunits)}"

    # Siblings: 3 models × 2 MCPs each = 6
    sibling_wus = [
        wu
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
    ]
    assert len(sibling_wus) == 6, f"Expected 6 sibling MCPs, got {len(sibling_wus)}"

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

    assert source.report.models_scanned == 3
    assert len(source.report.models_failed) == 0
