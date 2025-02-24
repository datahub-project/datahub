from typing import List

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.sink.file import write_metadata_file
from tests.test_helpers import mce_helpers

platform = "platform"
system_metadata = models.SystemMetadataClass(lastObserved=1643871600000, runId="run-id")


def make_lineage_aspect(
    dataset_name: str,
    upstreams: List[str],
    timestamp: int = 0,
    columns: List[str] = [],
    include_cll: bool = False,
) -> models.UpstreamLineageClass:
    """
    Generates dataset properties and upstream lineage aspects
    with simple column to column lineage between current dataset and all upstreams
    """

    dataset_urn = make_dataset_urn(platform, dataset_name)
    return models.UpstreamLineageClass(
        upstreams=[
            models.UpstreamClass(
                dataset=upstream_urn,
                type=models.DatasetLineageTypeClass.TRANSFORMED,
                auditStamp=models.AuditStampClass(
                    time=timestamp, actor="urn:li:corpuser:unknown"
                ),
            )
            for upstream_urn in upstreams
        ],
        fineGrainedLineages=(
            [
                models.FineGrainedLineageClass(
                    upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[
                        make_schema_field_urn(upstream_urn, col)
                        for upstream_urn in upstreams
                    ],
                    downstreams=[make_schema_field_urn(dataset_urn, col)],
                )
                for col in columns
            ]
            if include_cll
            else None
        ),
    )


def base_table_lineage_aspect() -> models.UpstreamLineageClass:
    return make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
    )


def base_cll_aspect(timestamp: int = 0) -> models.UpstreamLineageClass:
    return make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        timestamp=timestamp,
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )


def test_incremental_table_lineage(tmp_path, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_table_lineage.json"
    golden_file = test_resources_dir / "incremental_table_lineage_golden.json"

    urn = make_dataset_urn(platform, "dataset1")
    aspect = base_table_lineage_aspect()

    processed_wus = auto_incremental_lineage(
        incremental_lineage=True,
        stream=[
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=aspect, systemMetadata=system_metadata
            ).as_workunit()
        ],
    )

    write_metadata_file(
        test_file,
        [wu.metadata for wu in processed_wus],
    )
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig, output_path=test_file, golden_path=golden_file
    )


def test_incremental_table_lineage_empty_upstreams(tmp_path, pytestconfig):
    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[],
    )

    processed_wus = auto_incremental_lineage(
        incremental_lineage=True,
        stream=[
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=aspect, systemMetadata=system_metadata
            ).as_workunit()
        ],
    )

    assert [wu.metadata for wu in processed_wus] == []


def test_incremental_column_lineage(tmp_path, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_column_lineage.json"
    golden_file = test_resources_dir / "incremental_column_lineage_golden.json"

    urn = make_dataset_urn(platform, "dataset1")
    aspect = base_cll_aspect()

    processed_wus = auto_incremental_lineage(
        incremental_lineage=True,
        stream=[
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=aspect, systemMetadata=system_metadata
            ).as_workunit()
        ],
    )

    write_metadata_file(
        test_file,
        [wu.metadata for wu in processed_wus],
    )
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig, output_path=test_file, golden_path=golden_file
    )


def test_incremental_lineage_pass_through(tmp_path, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "test_incremental_lineage_pass_through.json"
    golden_file = test_resources_dir / "test_incremental_lineage_pass_through.json"

    urn = builder.make_dataset_urn("bigquery", "downstream")
    dataset_mce = builder.make_lineage_mce(
        [
            builder.make_dataset_urn("bigquery", "upstream1"),
            builder.make_dataset_urn("bigquery", "upstream2"),
        ],
        urn,
    )
    props = models.DatasetPropertiesClass(name="downstream")
    assert isinstance(dataset_mce.proposedSnapshot, models.DatasetSnapshotClass)
    dataset_mce.proposedSnapshot.aspects.append(props)

    ownership = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=models.OwnershipClass(owners=[]),
        systemMetadata=system_metadata,
    )

    processed_wus = auto_incremental_lineage(
        incremental_lineage=True,
        stream=auto_workunit([dataset_mce, ownership]),
    )

    write_metadata_file(
        test_file,
        [wu.metadata for wu in processed_wus],
    )
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig, output_path=test_file, golden_path=golden_file
    )
