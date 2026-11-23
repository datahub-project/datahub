"""Glue storage lineage must point at the dataset URN the S3 source emits.

Glue and S3 recipes commonly cover the same buckets. For the Glue -> S3 lineage edge
to land on the S3 dataset, Glue has to build the upstream URN the same way the S3
source does: same ``platform_instance`` / ``env`` (via ``target_platform_configs.s3``)
and the same ``{table}`` folding of partition directories (via the S3 recipe's
``path_specs``).
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest
from moto import mock_aws

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.data_lake_common.config import (
    DataLakeLineageProviderConfig,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.source import S3Source, TableData

FLOW_URN = "urn:li:dataFlow:(urn:li:dataPlatform:glue,flow,PROD)"
TABLE_SPEC = "s3://my-bucket/{table}/*/*.parquet"
AWS_CONFIG: Dict[str, str] = {
    "aws_region": "us-east-1",
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
}


def _glue_source(**config: Any) -> GlueSource:
    return GlueSource(
        ctx=PipelineContext(run_id="glue-s3-coexistence"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            emit_storage_lineage=True,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            extract_transforms=False,
            **config,
        ),
    )


def _glue_table(location: str) -> Dict[str, Any]:
    return {
        "Name": "my_table",
        "DatabaseName": "my_db",
        "CatalogId": "123456789012",
        "StorageDescriptor": {
            "Columns": [{"Name": "col_a", "Type": "string", "Comment": ""}],
            "Location": location,
        },
    }


def _upstream_lineage_mcps(
    source: GlueSource, location: str
) -> List[MetadataChangeProposalWrapper]:
    return [
        wu.metadata
        for wu in source._gen_table_wu(_glue_table(location))
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, models.UpstreamLineageClass)
    ]


def _storage_upstream(source: GlueSource, location: str) -> Optional[str]:
    """The single storage upstream URN Glue emits for ``location``, or None."""
    mcps = _upstream_lineage_mcps(source, location)
    if not mcps:
        return None
    assert len(mcps) == 1
    upstreams = mcps[0].aspect.upstreams  # type: ignore[union-attr]
    assert len(upstreams) == 1
    return upstreams[0].dataset


def _s3_source_dataset_urn(
    file_path: str,
    path_spec: str,
    platform_instance: Optional[str],
    env: str,
) -> str:
    """The dataset URN the S3 source emits for the table that ``file_path`` belongs to,
    computed through the S3 source's own naming code rather than a copy of its rule."""
    with mock_aws():
        s3_source = S3Source.create(
            config_dict={
                "path_specs": [{"include": path_spec}],
                "platform_instance": platform_instance,
                "env": env,
                "aws_config": AWS_CONFIG,
            },
            ctx=PipelineContext(run_id="s3-coexistence"),
        )
        spec = s3_source.source_config.path_specs[0]
        table_name, table_path = s3_source.extract_table_name_and_path(spec, file_path)
        table_data = TableData(
            display_name=table_name,
            is_s3=True,
            full_path=file_path,
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            table_path=table_path,
            size_in_bytes=1,
            number_of_files=1,
        )
        urns = {
            wu.metadata.entityUrn
            for wu in s3_source.ingest_table(table_data, spec)
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, models.DatasetPropertiesClass)
        }
    assert len(urns) == 1
    urn = urns.pop()
    assert urn is not None
    return urn


# --- Change 1: target_platform_configs["s3"] -------------------------------------


def test_storage_lineage_without_s3_target_config_is_unchanged() -> None:
    source = _glue_source()

    assert _storage_upstream(
        source, "s3://my-bucket/my_table"
    ) == make_s3_urn_for_lineage("s3://my-bucket/my_table", "PROD")


def test_storage_lineage_applies_s3_target_instance_and_env() -> None:
    source = _glue_source(
        target_platform_configs={
            "s3": {"platform_instance": "lake", "env": "DEV"},
        }
    )

    assert (
        _storage_upstream(source, "s3://my-bucket/my_table")
        == "urn:li:dataset:(urn:li:dataPlatform:s3,lake.my-bucket/my_table,DEV)"
    )


def test_storage_lineage_s3_target_env_falls_back_to_glue_env() -> None:
    source = _glue_source(
        env="QA",
        target_platform_configs={"s3": {"platform_instance": "lake"}},
    )

    assert (
        _storage_upstream(source, "s3://my-bucket/my_table")
        == "urn:li:dataset:(urn:li:dataPlatform:s3,lake.my-bucket/my_table,QA)"
    )


def test_storage_lineage_s3_target_env_without_instance() -> None:
    source = _glue_source(target_platform_configs={"s3": {"env": "DEV"}})

    assert _storage_upstream(
        source, "s3://my-bucket/my_table"
    ) == make_s3_urn_for_lineage("s3://my-bucket/my_table", "DEV")


def test_job_s3_node_applies_s3_target_instance() -> None:
    source = _glue_source(
        target_platform_configs={"s3": {"platform_instance": "lake"}},
    )
    node = {
        "NodeType": "DataSink",
        "Id": "DataSink0",
        "Args": [
            {"Name": "connection_type", "Value": '"s3"', "Param": False},
            {
                "Name": "connection_options",
                "Value": '{"path": "s3://my-bucket/output/"}',
                "Param": False,
            },
        ],
    }

    result = source.process_dataflow_node(node, FLOW_URN)

    assert result is not None
    assert (
        result["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:s3,lake.my-bucket/output,PROD)"
    )


# --- Change 2: fold the Location to the {table} boundary with path_specs ----------


def test_partition_location_folds_to_table_with_matching_path_spec() -> None:
    source = _glue_source(
        datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
    )

    assert _storage_upstream(
        source, "s3://my-bucket/my_table/dt=2024-01-01"
    ) == make_s3_urn_for_lineage("s3://my-bucket/my_table", "PROD")


def test_partition_location_without_path_specs_keeps_raw_location() -> None:
    source = _glue_source()

    assert _storage_upstream(
        source, "s3://my-bucket/my_table/dt=2024-01-01/"
    ) == make_s3_urn_for_lineage("s3://my-bucket/my_table/dt=2024-01-01", "PROD")


def test_excluded_location_emits_no_storage_lineage() -> None:
    source = _glue_source(
        datalake_lineage_config={
            "path_specs": [
                {"include": TABLE_SPEC, "exclude": ["**/my_table/**"]},
            ]
        },
    )

    assert _upstream_lineage_mcps(source, "s3://my-bucket/my_table/dt=2024-01-01") == []
    assert source.report.num_storage_lineage_skipped_by_path_spec == 1
    assert "Storage lineage skipped by path spec" in [
        i.title for i in source.report.infos
    ]


def test_unmatched_location_emits_no_storage_lineage_when_ignored() -> None:
    source = _glue_source(
        datalake_lineage_config={
            "path_specs": [{"include": "s3://other-bucket/{table}/*.parquet"}],
            "ignore_non_path_spec_path": True,
        },
    )

    assert _upstream_lineage_mcps(source, "s3://my-bucket/my_table/dt=2024-01-01") == []
    assert source.report.num_storage_lineage_skipped_by_path_spec == 1


def test_non_s3_location_is_counted_and_emits_no_storage_lineage() -> None:
    # Glue storage lineage is S3-only; any other URI is skipped, but visibly.
    source = _glue_source()

    assert _upstream_lineage_mcps(source, "gs://my-bucket/my_table") == []
    assert source.report.num_storage_lineage_non_s3_location == 1
    assert "Storage lineage skipped for non-S3 location" in [
        i.title for i in source.report.infos
    ]


def test_path_spec_failure_is_reported_and_does_not_abort_table() -> None:
    # A path spec that blows up on one odd Location must not take the rest of the
    # table (or the run) down with it: the table's other aspects are still emitted.
    source = _glue_source(
        datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
    )

    with patch.object(
        DataLakeLineageProviderConfig, "get_path", side_effect=RuntimeError("boom")
    ):
        wus = list(source._gen_table_wu(_glue_table("s3://my-bucket/my_table")))

    assert not any(
        isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, models.UpstreamLineageClass)
        for wu in wus
    )
    assert any(
        isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, models.DatasetPropertiesClass)
        for wu in wus
    )
    assert "Failed to resolve storage location" in [
        w.title for w in source.report.warnings
    ]


def test_downstream_direction_uses_folded_urn() -> None:
    source = _glue_source(
        glue_storage_lineage_direction="downstream",
        datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
        target_platform_configs={"s3": {"platform_instance": "lake"}},
    )

    mcps = _upstream_lineage_mcps(source, "s3://my-bucket/my_table/dt=2024-01-01")

    assert [m.entityUrn for m in mcps] == [
        "urn:li:dataset:(urn:li:dataPlatform:s3,lake.my-bucket/my_table,PROD)"
    ]


# --- Coexistence contract: Glue's upstream == the S3 source's dataset URN ----------


@pytest.mark.parametrize("platform_instance", [None, "lake"])
@pytest.mark.parametrize(
    "location, file_path",
    [
        # Hive-style partition directory.
        (
            "s3://my-bucket/my_table/dt=2024-01-01",
            "s3://my-bucket/my_table/dt=2024-01-01/part-0.parquet",
        ),
        # Snapshot-style directory whose name changes on every run.
        (
            "s3://my-bucket/my_table/run=1700000000.123/",
            "s3://my-bucket/my_table/run=1700000000.123/part-0.parquet",
        ),
        # Location already at the table root.
        (
            "s3://my-bucket/my_table/",
            "s3://my-bucket/my_table/dt=2024-01-01/part-0.parquet",
        ),
    ],
)
def test_glue_upstream_equals_s3_source_dataset_urn(
    location: str, file_path: str, platform_instance: Optional[str]
) -> None:
    env = "DEV"
    s3_urn = _s3_source_dataset_urn(file_path, TABLE_SPEC, platform_instance, env)
    glue = _glue_source(
        datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
        target_platform_configs={
            "s3": {"platform_instance": platform_instance, "env": env}
        },
    )

    assert _storage_upstream(glue, location) == s3_urn


def test_non_s3_path_spec_warns(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="datahub.ingestion.source.aws.glue"):
        GlueSourceConfig(
            aws_region="us-east-1",
            emit_storage_lineage=True,
            datalake_lineage_config={
                "path_specs": [
                    {"include": TABLE_SPEC},
                    {"include": "gs://my-bucket/{table}/*.parquet"},
                ]
            },
        )

    assert any(
        "gs://my-bucket/{table}/*.parquet" in r.message and "S3" in r.message
        for r in caplog.records
    )


def test_path_specs_without_storage_lineage_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="datahub.ingestion.source.aws.glue"):
        GlueSourceConfig(
            aws_region="us-east-1",
            datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
        )

    assert any("emit_storage_lineage" in r.message for r in caplog.records)


def test_s3_path_specs_with_storage_lineage_do_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="datahub.ingestion.source.aws.glue"):
        GlueSourceConfig(
            aws_region="us-east-1",
            emit_storage_lineage=True,
            datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
        )

    assert caplog.records == []


def test_path_spec_model_is_shared_with_s3_source() -> None:
    # The recipe contract is "copy path_specs from the S3 recipe", which only holds if
    # Glue parses them with the very same model.
    config = GlueSourceConfig(
        aws_region="us-east-1",
        datalake_lineage_config={"path_specs": [{"include": TABLE_SPEC}]},
    )
    assert isinstance(config.datalake_lineage_config.path_specs[0], PathSpec)
