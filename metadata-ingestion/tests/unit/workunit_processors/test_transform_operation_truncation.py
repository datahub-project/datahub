from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.configuration.source_common import TransformOperationLengthConfigMixin
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig, ClickHouseSource
from datahub.ingestion.workunit_processors.auto_incremental_lineage import (
    AutoIncrementalLineageProcessor,
)
from datahub.ingestion.workunit_processors.ensure_aspect_size import (
    EnsureAspectSizeProcessor,
)
from datahub.ingestion.workunit_processors.transform_operation import (
    truncate_transform_operation,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)

UPSTREAM = make_dataset_urn("clickhouse", "db.orders")
DOWNSTREAM = make_dataset_urn("clickhouse", "db.orders_summary")
LOGIC = "SQL: SUM(col_a) AS total"


@pytest.mark.parametrize(
    "value, max_length, expected",
    [
        (LOGIC, 100, LOGIC),
        (LOGIC, 8, "SQL: SUM(col_..."),
        (LOGIC, 17, LOGIC),
        ("COPY: col_a AS col_a", 0, "COPY"),
        ("col_a + col_b", 5, "col_a..."),
        ("col_a + col_b", 0, None),
    ],
)
def test_truncate_transform_operation(
    value: str, max_length: int, expected: Optional[str]
) -> None:
    assert truncate_transform_operation(value, max_length) == expected


def _ctx(max_length: Optional[int] = None) -> WorkunitProcessorContext:
    return WorkunitProcessorContext(
        source_report=SourceReport(),
        pipeline_context=PipelineContext(run_id="test"),
        source_config=TransformOperationLengthConfigMixin(
            max_transform_operation_length=max_length
        ),
        platform="clickhouse",
        source_platform="clickhouse",
    )


def _fine_grained(
    transform_operations: List[Optional[str]],
) -> List[FineGrainedLineageClass]:
    return [
        FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[make_schema_field_urn(UPSTREAM, f"col_{i}")],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[make_schema_field_urn(DOWNSTREAM, f"col_{i}")],
            transformOperation=transform_operation,
        )
        for i, transform_operation in enumerate(transform_operations)
    ]


def _lineage(
    transform_operations: List[Optional[str]],
) -> UpstreamLineageClass:
    return UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPSTREAM, type=DatasetLineageTypeClass.TRANSFORMED)
        ],
        fineGrainedLineages=_fine_grained(transform_operations),
    )


def _lineage_wu(transform_operations: List[Optional[str]]) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM,
        aspect=_lineage(transform_operations),
    ).as_workunit()


def test_incremental_lineage_applies_configured_transform_length() -> None:
    processor = AutoIncrementalLineageProcessor.create(_ctx(max_length=8))

    [out] = list(processor.process([_lineage_wu([LOGIC, "COPY: col_b", None])]))

    assert isinstance(out.metadata, MetadataChangeProposalClass)
    assert out.metadata.changeType == "PATCH"
    assert out.metadata.aspect is not None
    patch_value = out.metadata.aspect.value.decode()
    assert "SUM(col_..." in patch_value
    assert "SUM(col_a) AS total" not in patch_value
    assert "COPY: col_b" in patch_value
    assert processor.report.num_transform_operations_truncated == 1


def test_incremental_lineage_keeps_full_logic_when_length_unset() -> None:
    processor = AutoIncrementalLineageProcessor.create(_ctx(max_length=None))

    [out] = list(processor.process([_lineage_wu([LOGIC])]))

    assert isinstance(out.metadata, MetadataChangeProposalClass)
    assert out.metadata.aspect is not None
    assert "SUM(col_a) AS total" in out.metadata.aspect.value.decode()
    assert processor.report.num_transform_operations_truncated == 0


def test_incremental_lineage_truncates_mce_before_patch() -> None:
    processor = AutoIncrementalLineageProcessor.create(_ctx(max_length=8))
    mce_wu = MetadataWorkUnit(
        id="mce",
        mce=MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(
                urn=DOWNSTREAM,
                aspects=[_lineage([LOGIC])],
            )
        ),
    )

    outs = list(processor.process([mce_wu]))
    patches = [
        wu.metadata
        for wu in outs
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "upstreamLineage"
    ]
    assert len(patches) == 1
    assert patches[0].aspect is not None
    assert "SUM(col_..." in patches[0].aspect.value.decode()
    assert "SUM(col_a) AS total" not in patches[0].aspect.value.decode()


def test_upsert_size_guard_leaves_logic_when_aspect_fits() -> None:
    processor = EnsureAspectSizeProcessor.create(
        WorkunitProcessorContext(
            source_report=SourceReport(),
            pipeline_context=MagicMock(),
            source_config=None,
            platform=None,
        )
    )
    lineage = _lineage([LOGIC])

    processor.ensure_upstream_lineage_size(DOWNSTREAM, lineage)

    assert lineage.fineGrainedLineages is not None
    assert lineage.fineGrainedLineages[0].transformOperation == LOGIC
    assert processor.report.num_truncations_by_aspect.get("upstreamLineage", 0) == 0


def test_upsert_size_guard_shortens_logic_until_aspect_fits() -> None:
    processor = EnsureAspectSizeProcessor.create(
        WorkunitProcessorContext(
            source_report=SourceReport(),
            pipeline_context=MagicMock(),
            source_config=None,
            platform=None,
        )
    )
    # Tiny budget so the long expression must be cut, while table-level upstream stays.
    processor.payload_constraint = 900
    long_logic = "SQL: " + ("x" * 2000)
    lineage = _lineage([long_logic])

    processor.ensure_upstream_lineage_size(DOWNSTREAM, lineage)

    assert lineage.fineGrainedLineages is not None
    truncated = lineage.fineGrainedLineages[0].transformOperation
    assert truncated is not None
    assert truncated != long_logic
    assert truncated.startswith("SQL: ")
    assert truncated.endswith("...")
    assert len(lineage.upstreams) == 1
    assert processor.report.num_truncations_by_aspect.get("upstreamLineage", 0) == 1


def test_sql_source_truncates_before_incremental_lineage_patch(monkeypatch):
    source = ClickHouseSource(
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "incremental_lineage": True,
                "max_transform_operation_length": 8,
            }
        ),
        PipelineContext(run_id="test"),
    )
    monkeypatch.setattr(
        source, "get_workunits_internal", lambda: iter([_lineage_wu([LOGIC])])
    )

    [patch] = [
        wu.metadata
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "upstreamLineage"
    ]

    assert patch.changeType == "PATCH"
    assert patch.aspect is not None
    patch_value = patch.aspect.value.decode()
    assert "SUM(col_..." in patch_value
    assert "SUM(col_a) AS total" not in patch_value
