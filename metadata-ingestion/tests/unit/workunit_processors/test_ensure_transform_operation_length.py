from typing import List, Optional

import pytest

from datahub.configuration.source_common import TransformOperationLengthConfigMixin
from datahub.emitter.mce_builder import (
    make_data_job_urn,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig, ClickHouseSource
from datahub.ingestion.workunit_processors.ensure_transform_operation_length import (
    EnsureTransformOperationLengthProcessor,
    truncate_transform_operation,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
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


def _ctx(max_length: Optional[int]) -> WorkunitProcessorContext:
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


def _lineage_wu(transform_operations: List[Optional[str]]) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=UPSTREAM, type=DatasetLineageTypeClass.TRANSFORMED
                )
            ],
            fineGrainedLineages=_fine_grained(transform_operations),
        ),
    ).as_workunit()


def test_processor_truncates_dataset_and_datajob_lineage() -> None:
    ctx = _ctx(max_length=8)
    assert EnsureTransformOperationLengthProcessor.should_enable(ctx)
    processor = EnsureTransformOperationLengthProcessor.create(ctx)
    datajob_wu = MetadataChangeProposalWrapper(
        entityUrn=make_data_job_urn("mssql", "db.dbo", "my_procedure"),
        aspect=DataJobInputOutputClass(
            inputDatasets=[UPSTREAM],
            outputDatasets=[DOWNSTREAM],
            fineGrainedLineages=_fine_grained([LOGIC]),
        ),
    ).as_workunit()

    dataset_out, datajob_out = processor.process(
        [_lineage_wu([LOGIC, "COPY: col_b", None]), datajob_wu]
    )

    lineage = dataset_out.get_aspect_of_type(UpstreamLineageClass)
    assert lineage is not None
    assert lineage.fineGrainedLineages is not None
    assert [f.transformOperation for f in lineage.fineGrainedLineages] == [
        "SQL: SUM(col_...",
        "COPY: col_b",
        None,
    ]
    assert [u.dataset for u in lineage.upstreams] == [UPSTREAM]
    datajob_lineage = datajob_out.get_aspect_of_type(DataJobInputOutputClass)
    assert datajob_lineage is not None
    assert datajob_lineage.fineGrainedLineages is not None
    assert datajob_lineage.fineGrainedLineages[0].transformOperation == (
        "SQL: SUM(col_..."
    )
    assert processor.report.num_transform_operations_truncated == 2


def test_processor_truncates_mce_lineage() -> None:
    processor = EnsureTransformOperationLengthProcessor.create(_ctx(max_length=8))
    mce_wu = MetadataWorkUnit(
        id="mce",
        mce=MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(
                urn=DOWNSTREAM,
                aspects=[
                    UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=UPSTREAM,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                        ],
                        fineGrainedLineages=_fine_grained([LOGIC]),
                    )
                ],
            )
        ),
    )

    [out] = processor.process([mce_wu])

    lineage = out.get_aspect_of_type(UpstreamLineageClass)
    assert lineage is not None
    assert lineage.fineGrainedLineages is not None
    assert lineage.fineGrainedLineages[0].transformOperation == "SQL: SUM(col_..."


def test_processor_disabled_by_default() -> None:
    assert not EnsureTransformOperationLengthProcessor.should_enable(_ctx(None))


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
