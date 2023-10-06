from typing import List
from unittest.mock import MagicMock

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_helpers import auto_incremental_lineage
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
        fineGrainedLineages=[
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
        else None,
    )


def test_incremental_table_lineage(tmp_path, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_table_lineage.json"
    golden_file = test_resources_dir / "incremental_table_lineage_golden.json"

    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
    )

    processed_wus = auto_incremental_lineage(
        graph=None,
        incremental_lineage=True,
        include_column_level_lineage=False,
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


def test_incremental_column_lineage_no_gms_aspect(tmp_path, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_cll_no_gms_aspect.json"
    golden_file = test_resources_dir / "incremental_cll_no_gms_aspect_golden.json"

    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = None

    processed_wus = auto_incremental_lineage(
        graph=mock_graph,
        incremental_lineage=True,
        include_column_level_lineage=True,
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


def test_incremental_column_lineage_same_gms_aspect(tmp_path, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_cll_same_gms_aspect.json"
    golden_file = test_resources_dir / "incremental_cll_same_gms_aspect_golden.json"

    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    processed_wus = auto_incremental_lineage(
        graph=mock_graph,
        incremental_lineage=True,
        include_column_level_lineage=True,
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


def test_incremental_column_lineage_less_upstreams_in_gms_aspect(
    tmp_path, pytestconfig
):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_cll_less_upstreams_in_gms_aspect.json"
    golden_file = (
        test_resources_dir / "incremental_cll_less_upstreams_in_gms_aspect_golden.json"
    )

    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = make_lineage_aspect(
        "dataset1",
        upstreams=[make_dataset_urn(platform, name) for name in ["upstream1"]],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    processed_wus = auto_incremental_lineage(
        graph=mock_graph,
        incremental_lineage=True,
        include_column_level_lineage=True,
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


def test_incremental_column_lineage_more_upstreams_in_gms_aspect(
    tmp_path, pytestconfig
):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_cll_more_upstreams_in_gms_aspect.json"
    golden_file = (
        test_resources_dir / "incremental_cll_more_upstreams_in_gms_aspect_golden.json"
    )

    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name)
            for name in ["upstream1", "upstream2", "upstream3"]
        ],
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    processed_wus = auto_incremental_lineage(
        graph=mock_graph,
        incremental_lineage=True,
        include_column_level_lineage=True,
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


def test_incremental_column_lineage_same_upstream_updated_audit_stamp(
    tmp_path, pytestconfig
):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api/source_helpers"
    test_file = tmp_path / "incremental_cll_same_gms_aspect_updated_audit_stamp.json"
    golden_file = (
        test_resources_dir
        / "incremental_cll_same_gms_aspect_updated_audit_stamp_golden.json"
    )

    urn = make_dataset_urn(platform, "dataset1")
    aspect = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        timestamp=1643871600000,
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = make_lineage_aspect(
        "dataset1",
        upstreams=[
            make_dataset_urn(platform, name) for name in ["upstream1", "upstream2"]
        ],
        timestamp=0,
        columns=["col_a", "col_b", "col_c"],
        include_cll=True,
    )

    processed_wus = auto_incremental_lineage(
        graph=mock_graph,
        incremental_lineage=True,
        include_column_level_lineage=True,
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
