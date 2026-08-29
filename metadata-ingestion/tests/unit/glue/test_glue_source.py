import base64
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Type, cast
from unittest.mock import MagicMock, patch

import boto3
import pydantic
import pytest
import time_machine
from botocore.stub import Stubber
from moto import mock_aws

import datahub.metadata.schema_classes as models
from datahub.api.entities.external.lake_formation_external_entites import (
    LakeFormationTag,
)
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws.glue import (
    GLUE_NATIVE_CONNECTION_TYPE_MAP,
    JDBC_PLATFORM_MAP,
    GlueProfilingConfig,
    GlueSource,
    GlueSourceConfig,
    _redact_secret_fields_in_dataflow_script,
    _sanitize_jdbc_url,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    MapTypeClass,
    RecordTypeClass,
    StringTypeClass,
)
from datahub.testing import mce_helpers
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)
from tests.unit.glue.test_glue_source_stubs import (
    empty_database,
    flights_database,
    get_bucket_tagging,
    get_databases_delta_response,
    get_databases_response,
    get_databases_response_for_lineage,
    get_databases_response_profiling,
    get_databases_response_views,
    get_databases_response_with_mixed_database,
    get_databases_response_with_resource_link,
    get_dataflow_graph_response_1,
    get_dataflow_graph_response_2,
    get_dataflow_graph_response_3,
    get_delta_tables_response_1,
    get_delta_tables_response_2,
    get_jobs_response,
    get_jobs_response_empty,
    get_object_body_1,
    get_object_body_2,
    get_object_body_3,
    get_object_response_1,
    get_object_response_2,
    get_object_response_3,
    get_object_tagging,
    get_tables_lineage_response_1,
    get_tables_response_1,
    get_tables_response_2,
    get_tables_response_for_mixed_database,
    get_tables_response_for_target_database,
    get_tables_response_profiling_1,
    get_tables_response_views,
    mixed_database,
    normal_table_in_mixed_database,
    resource_link_database,
    resource_link_table_in_mixed_database,
    tables_1,
    tables_2,
    tables_profiling_1,
    target_database_tables,
    test_database,
)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

test_resources_dir = Path(__file__).parent


def glue_source(
    platform_instance: Optional[str] = None,
    mock_datahub_graph_instance: Optional[DataHubGraph] = None,
    use_s3_bucket_tags: bool = True,
    use_s3_object_tags: bool = True,
    extract_delta_schema_from_parameters: bool = False,
    emit_storage_lineage: bool = False,
    include_column_lineage: bool = False,
    extract_transforms: bool = True,
    extract_lakeformation_tags: bool = False,
    incremental_properties: bool = False,
    propagate_lakeformation_tags: bool = False,
    include_view_lineage: bool = False,
) -> GlueSource:
    pipeline_context = PipelineContext(run_id="glue-source-tes")
    if mock_datahub_graph_instance:
        pipeline_context.graph = mock_datahub_graph_instance
    return GlueSource(
        ctx=pipeline_context,
        config=GlueSourceConfig(
            aws_region="us-west-2",
            extract_transforms=extract_transforms,
            platform_instance=platform_instance,
            catalog_id=None,
            use_s3_bucket_tags=use_s3_bucket_tags,
            use_s3_object_tags=use_s3_object_tags,
            extract_delta_schema_from_parameters=extract_delta_schema_from_parameters,
            emit_storage_lineage=emit_storage_lineage,
            include_column_lineage=include_column_lineage,
            extract_lakeformation_tags=extract_lakeformation_tags,
            incremental_properties=incremental_properties,
            propagate_lakeformation_tags=propagate_lakeformation_tags,
            include_view_lineage=include_view_lineage,
        ),
    )


def glue_source_with_profiling(
    platform_instance: Optional[str] = None,
    use_s3_bucket_tags: bool = False,
    use_s3_object_tags: bool = False,
    extract_delta_schema_from_parameters: bool = False,
) -> GlueSource:
    profiling_config = GlueProfilingConfig(
        enabled=True,
        profile_table_level_only=False,
        row_count="row_count",
        column_count="column_count",
        unique_count="unique_count",
        unique_proportion="unique_proportion",
        null_count="null_count",
        null_proportion="null_proportion",
        min="min",
        max="max",
        mean="mean",
        median="median",
        stdev="stdev",
    )

    return GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            extract_transforms=False,
            platform_instance=platform_instance,
            use_s3_bucket_tags=use_s3_bucket_tags,
            use_s3_object_tags=use_s3_object_tags,
            extract_delta_schema_from_parameters=extract_delta_schema_from_parameters,
            profiling=profiling_config,
        ),
    )


column_type_test_cases: Dict[str, Tuple[str, Type]] = {
    "char": ("char", StringTypeClass),
    "array": ("array<int>", ArrayTypeClass),
    "map": ("map<string, int>", MapTypeClass),
    "struct": ("struct<a:int, b:string>", RecordTypeClass),
}


@pytest.mark.parametrize(
    "hive_column_type, expected_type",
    column_type_test_cases.values(),
    ids=column_type_test_cases.keys(),
)
def test_column_type(hive_column_type: str, expected_type: Type) -> None:
    avro_schema = get_avro_schema_for_hive_column(
        f"test_column_{hive_column_type}", hive_column_type
    )
    schema_fields = avro_schema_to_mce_fields(json.dumps(avro_schema))
    actual_schema_field_type = schema_fields[0].type
    assert isinstance(actual_schema_field_type.type, expected_type)


@pytest.mark.parametrize(
    "platform_instance, mce_file, mce_golden_file",
    [
        (None, "glue_mces.json", "glue_mces_golden.json"),
        (
            "some_instance_name",
            "glue_mces_platform_instance.json",
            "glue_mces_platform_instance_golden.json",
        ),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_ingest(
    tmp_path: Path,
    pytestconfig: pytest.Config,
    platform_instance: str,
    mce_file: str,
    mce_golden_file: str,
) -> None:
    glue_source_instance = glue_source(platform_instance=platform_instance)

    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_1,
            {"DatabaseName": "flights-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_2,
            {"DatabaseName": "test-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            {"TableList": []},
            {"DatabaseName": "empty-database"},
        )

        glue_stubber.add_response("get_jobs", get_jobs_response, {})
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_1,
            {"PythonScript": get_object_body_1},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_2,
            {"PythonScript": get_object_body_2},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_3,
            {"PythonScript": get_object_body_3},
        )

        with Stubber(glue_source_instance.s3_client) as s3_stubber:
            for _ in range(
                len(get_tables_response_1["TableList"])
                + len(get_tables_response_2["TableList"])
            ):
                s3_stubber.add_response(
                    "get_bucket_tagging",
                    get_bucket_tagging(),
                )
                s3_stubber.add_response(
                    "get_object_tagging",
                    get_object_tagging(),
                )

            s3_stubber.add_response(
                "get_object",
                get_object_response_1(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-1.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_2(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-2.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_3(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-3.py",
                },
            )

            mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]

            glue_stubber.assert_no_pending_responses()
            s3_stubber.assert_no_pending_responses()

            write_metadata_file(tmp_path / mce_file, mce_objects)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_file,
        golden_path=test_resources_dir / mce_golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_view_lineage_ingest(
    tmp_path: Path,
    pytestconfig: pytest.Config,
) -> None:
    # End-to-end golden test for the VIRTUAL_VIEW path: exercises View subtype +
    # ViewProperties emission and the full register-schemas-then-flush ordering
    # that produces view->table and view->view (column-level) lineage.
    glue_source_instance = glue_source(
        include_view_lineage=True,
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
        extract_transforms=False,
    )

    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_response_views, {})
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_views,
            {"DatabaseName": "my_db"},
        )

        mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]
        glue_stubber.assert_no_pending_responses()

        write_metadata_file(tmp_path / "glue_views_mces.json", mce_objects)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "glue_views_mces.json",
        golden_path=test_resources_dir / "glue_views_mces_golden.json",
    )


def test_platform_config():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2", platform="athena"),
    )
    assert source.platform == "athena"


def test_catalog_to_platform_instance_resolves_owning_account():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            platform_instance="ingestion_acct",
            catalog_to_platform_instance={
                "arn:aws:glue:us-west-2:111122223333": {
                    "platform_instance": "domain_a",
                    # A non-default env, so the assertion below actually proves the override
                    # was applied rather than falling through to the source's default (PROD).
                    "env": "DEV",
                }
            },
        ),
    )

    # Owning account present in the map -> stamp its instance/env.
    mapped = source._resolve_platform_instance("111122223333")
    assert mapped.platform_instance == "domain_a"
    assert mapped.env == "DEV"

    # Account not in the map -> fall back to the source's own platform_instance.
    fallback = source._resolve_platform_instance("999988887777")
    assert fallback.platform_instance == "ingestion_acct"

    # No catalog id -> fall back as well.
    assert source._resolve_platform_instance(None).platform_instance == "ingestion_acct"


def test_catalog_to_platform_instance_partial_override_keeps_source_instance():
    # An entry that overrides only env must keep the source's own platform_instance (the two
    # overrides are independent), and vice versa.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            platform_instance="ingestion_acct",
            env="PROD",
            catalog_to_platform_instance={
                "arn:aws:glue:us-west-2:111122223333": {"env": "DEV"},
            },
        ),
    )
    resolved = source._resolve_platform_instance("111122223333")
    assert resolved.platform_instance == "ingestion_acct"
    assert resolved.env == "DEV"


def test_catalog_to_platform_instance_is_region_specific():
    # The same account in two regions is two different catalogs, so the key includes region.
    # Each source must resolve the SAME account to its own region's instance — this fails if the
    # lookup ignored region (account-only keying).
    catalog_map = {
        "arn:aws:glue:us-west-2:111122223333": {"platform_instance": "domain_a_usw2"},
        "arn:aws:glue:eu-west-1:111122223333": {"platform_instance": "domain_a_euw1"},
    }

    source_euw1 = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="eu-west-1",
            platform_instance="ingestion_acct",
            catalog_to_platform_instance=catalog_map,
        ),
    )
    assert (
        source_euw1._resolve_platform_instance("111122223333").platform_instance
        == "domain_a_euw1"
    )

    source_usw2 = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            platform_instance="ingestion_acct",
            catalog_to_platform_instance=catalog_map,
        ),
    )
    assert (
        source_usw2._resolve_platform_instance("111122223333").platform_instance
        == "domain_a_usw2"
    )


def test_resource_link_emits_upstream_edge_to_owner():
    # A Lake Formation resource link (shared-transactions) points at transactions in the owner
    # account 432143214321. The consumer-instance table should get an upstream edge to the owner's
    # table URN, stamped with the owner account's instance.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:123412341234": {
                    "platform_instance": "consumer_inst"
                },
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )
    upstreams = source._resource_link_owner_upstreams(
        resource_link_table_in_mixed_database, "urn:li:dataset:(consumer)"
    )

    assert [u.dataset for u in upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:glue,owner_inst.test-database.transactions,PROD)"
    ]
    assert upstreams[0].type == models.DatasetLineageTypeClass.COPY


def test_resource_link_owner_upstreams_handles_malformed_target():
    # A TargetTable missing its Name must not crash the whole run; it should be skipped with a
    # reported warning and no upstream produced.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-east-1"),
    )
    malformed = {
        "Name": "shared-transactions",
        "DatabaseName": "mixed-database",
        "TargetTable": {"CatalogId": "432143214321", "DatabaseName": "test-database"},
    }

    upstreams = source._resource_link_owner_upstreams(
        malformed, "urn:li:dataset:(consumer)"
    )

    assert upstreams == []
    assert source.report.warnings
    assert source.report.num_resource_link_missing_target == 1


def test_resource_link_upstream_merged_with_storage_lineage():
    # A resource link with a backfilled StorageDescriptor (so storage lineage also fires) must not
    # lose either edge: the owner upstream and the S3 storage upstream must land in a SINGLE
    # UpstreamLineage aspect on the dataset, not two aspects that clobber each other.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            platform_instance="consumer_inst",
            emit_storage_lineage=True,
            include_column_lineage=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )
    table = {
        "Name": "shared-transactions",
        "DatabaseName": "mixed-database",
        "CatalogId": "123412341234",
        "TargetTable": {
            "CatalogId": "432143214321",
            "DatabaseName": "test-database",
            "Name": "transactions",
        },
        "StorageDescriptor": {
            "Columns": [{"Name": "txn_id", "Type": "bigint", "Comment": ""}],
            "Location": "s3://owner-bucket/transactions",
        },
    }

    wus = list(source._gen_table_wu(table))
    upstream_aspects = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, models.UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1
    datasets = {u.dataset for u in upstream_aspects[0].upstreams}
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:glue,owner_inst.test-database.transactions,PROD)"
        in datasets
    )
    assert any("dataPlatform:s3" in d for d in datasets)


def test_resource_link_no_self_lineage_when_owner_unresolved():
    # No catalog mapping -> the owner falls back to the source's own instance/env. If the link and
    # its target share a name, the owner URN equals the dataset's own URN; a self-upstream edge is
    # meaningless and must be skipped.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-east-1", platform_instance="acct"),
    )
    table = {
        "Name": "shared",
        "DatabaseName": "db",
        "CatalogId": "111122223333",
        "TargetTable": {
            "CatalogId": "111122223333",
            "DatabaseName": "db",
            "Name": "shared",
        },
    }
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,acct.db.shared,PROD)"

    upstreams = source._resource_link_owner_upstreams(table, dataset_urn)

    assert upstreams == []
    assert source.report.num_resource_link_self_referential == 1


CONSUMER_LINK_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:glue,"
    "consumer_inst.mixed-database.shared-transactions,PROD)"
)
OWNER_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:glue,"
    "owner_inst.test-database.transactions,PROD)"
)


def _resource_link_schema_source(resolve: bool = True) -> GlueSource:
    return GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            resolve_resource_link_schema=resolve,
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:123412341234": {
                    "platform_instance": "consumer_inst"
                },
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )


def _owner_schema() -> models.SchemaMetadataClass:
    return models.SchemaMetadataClass(
        schemaName="transactions",
        platform="urn:li:dataPlatform:glue",
        version=0,
        hash="",
        platformSchema=models.MySqlDDLClass(tableSchema=""),
        fields=[
            models.SchemaFieldClass(
                fieldPath="txn_id",
                type=models.SchemaFieldDataTypeClass(type=models.NumberTypeClass()),
                nativeDataType="bigint",
            )
        ],
    )


def test_resource_link_schema_resolved_from_datahub():
    # The owner account has already been ingested, so its schema is in DataHub. The link should
    # carry a copy of the owner's schema (re-stamped with the link's own name), read from the graph
    # with no cross-account Glue call.
    source = _resource_link_schema_source()
    graph = MagicMock()
    graph.get_schema_metadata.return_value = _owner_schema()
    source.ctx.graph = graph

    schema = source._resource_link_schema(
        resource_link_table_in_mixed_database,
        CONSUMER_LINK_URN,
        "mixed-database.shared-transactions",
    )

    assert schema is not None
    assert [f.fieldPath for f in schema.fields] == ["txn_id"]
    assert schema.schemaName == "mixed-database.shared-transactions"
    graph.get_schema_metadata.assert_called_once_with(OWNER_TABLE_URN)
    assert source.report.num_resource_link_schema_from_datahub == 1


def test_resource_link_schema_resolved_from_glue_when_absent_in_datahub():
    # The owner account is not (yet) in DataHub, so the graph returns None. Fall back to a
    # cross-account glue:GetTable on the target and build the schema from its StorageDescriptor.
    source = _resource_link_schema_source()
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    source.ctx.graph = graph

    with Stubber(source.glue_client) as stubber:
        stubber.add_response(
            "get_table",
            {
                "Table": {
                    "Name": "transactions",
                    "DatabaseName": "test-database",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "txn_id", "Type": "bigint"},
                            {"Name": "amount", "Type": "double"},
                        ]
                    },
                }
            },
            {
                "CatalogId": "432143214321",
                "DatabaseName": "test-database",
                "Name": "transactions",
            },
        )
        schema = source._resource_link_schema(
            resource_link_table_in_mixed_database,
            CONSUMER_LINK_URN,
            "mixed-database.shared-transactions",
        )

    assert schema is not None
    assert {f.fieldPath.split(".")[-1] for f in schema.fields} == {"txn_id", "amount"}
    assert source.report.num_resource_link_schema_from_glue == 1


def test_resource_link_schema_unresolved_leaves_link_schemaless():
    # Neither DataHub nor a cross-account Glue read can supply the schema (e.g. owner not ingested
    # and no glue:GetTable permission). The link stays schemaless — no crash — and it's counted.
    source = _resource_link_schema_source()
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    source.ctx.graph = graph

    with Stubber(source.glue_client) as stubber:
        stubber.add_client_error(
            "get_table", service_error_code="AccessDeniedException"
        )
        schema = source._resource_link_schema(
            resource_link_table_in_mixed_database,
            CONSUMER_LINK_URN,
            "mixed-database.shared-transactions",
        )

    assert schema is None
    assert source.report.num_resource_link_schema_unresolved == 1
    # The Glue failure is surfaced (with its exception); the generic summary is NOT also emitted, so
    # one unresolved owner produces exactly one report entry.
    titles = [w.title for w in source.report.warnings]
    assert "Failed to read resource-link owner schema from Glue" in titles
    assert "Resource link schema could not be resolved" not in titles


def test_resource_link_schema_datahub_error_falls_back_to_glue():
    # A GMS/network error reading the owner's schema must not abort the run: it warns and falls back
    # to the cross-account Glue read.
    source = _resource_link_schema_source()
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = Exception("gms unavailable")
    source.ctx.graph = graph

    with Stubber(source.glue_client) as stubber:
        stubber.add_response(
            "get_table",
            {
                "Table": {
                    "Name": "transactions",
                    "DatabaseName": "test-database",
                    "StorageDescriptor": {
                        "Columns": [{"Name": "txn_id", "Type": "bigint"}]
                    },
                }
            },
            {
                "CatalogId": "432143214321",
                "DatabaseName": "test-database",
                "Name": "transactions",
            },
        )
        schema = source._resource_link_schema(
            resource_link_table_in_mixed_database,
            CONSUMER_LINK_URN,
            "mixed-database.shared-transactions",
        )

    assert schema is not None
    assert source.report.num_resource_link_schema_from_glue == 1
    assert "Failed to read resource-link owner schema from DataHub" in [
        w.title for w in source.report.warnings
    ]


def test_resource_link_schema_skipped_when_owner_is_self():
    # No catalog mapping -> the owner falls back to the source's own instance/env and, when the
    # names coincide, equals the dataset's own URN. Resolving a schema against itself is meaningless,
    # so it returns None without consulting DataHub or Glue.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-east-1", platform_instance="acct"),
    )
    graph = MagicMock()
    source.ctx.graph = graph
    table = {
        "Name": "shared",
        "DatabaseName": "db",
        "CatalogId": "111122223333",
        "TargetTable": {
            "CatalogId": "111122223333",
            "DatabaseName": "db",
            "Name": "shared",
        },
    }
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,acct.db.shared,PROD)"

    assert source._resource_link_schema(table, dataset_urn, "db.shared") is None
    graph.get_schema_metadata.assert_not_called()


def test_resource_link_schema_stamped_with_link_name_and_isolated():
    # Two links to the same owner each present under their OWN schemaName, and an in-place
    # schema-field mutation (as add_dataset_schema_tags does) on one link must not leak to the other
    # — each link gets its own deep copy of the resolved owner schema.
    source = _resource_link_schema_source()
    graph = MagicMock()
    graph.get_schema_metadata.return_value = _owner_schema()
    source.ctx.graph = graph

    def link(name: str) -> Dict:
        return {
            "Name": name,
            "DatabaseName": "mixed-database",
            "CatalogId": "123412341234",
            "TargetTable": {
                "CatalogId": "432143214321",
                "DatabaseName": "test-database",
                "Name": "transactions",
            },
        }

    schema_a = source._resource_link_schema(
        link("link_a"), "urn:li:dataset:(link_a)", "mixed-database.link_a"
    )
    assert schema_a is not None
    assert schema_a.schemaName == "mixed-database.link_a"
    # Simulate an in-place field mutation as add_dataset_schema_tags does.
    schema_a.fields[0].globalTags = models.GlobalTagsClass(
        tags=[models.TagAssociationClass(tag="urn:li:tag:pii")]
    )

    schema_b = source._resource_link_schema(
        link("link_b"), "urn:li:dataset:(link_b)", "mixed-database.link_b"
    )
    assert schema_b is not None
    assert schema_b.schemaName == "mixed-database.link_b"
    # Resolving link_b must not have overwritten link_a's name or leaked its mutation.
    assert schema_a.schemaName == "mixed-database.link_a"
    assert schema_b.fields[0].globalTags is None
    # Each link resolves independently (no cache): the owner schema is fetched once per link.
    assert source.report.num_resource_link_schema_from_datahub == 2


def test_resource_link_schema_not_resolved_when_disabled():
    # With resolve_resource_link_schema=False the link stays schemaless even though the owner's
    # schema is available in DataHub — the graph must not be consulted at all.
    source = _resource_link_schema_source(resolve=False)
    graph = MagicMock()
    graph.get_schema_metadata.return_value = _owner_schema()
    source.ctx.graph = graph

    schema = source._resource_link_schema(
        resource_link_table_in_mixed_database,
        CONSUMER_LINK_URN,
        "mixed-database.shared-transactions",
    )

    assert schema is None
    graph.get_schema_metadata.assert_not_called()


def test_resource_link_downstream_direction_emits_separate_dataset_aspect():
    # In downstream storage-lineage mode the storage aspect targets the S3 URN, so the resource-link
    # owner edge must be emitted as its own aspect on the dataset URN (no clobber, no merge).
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            platform_instance="consumer_inst",
            emit_storage_lineage=True,
            glue_storage_lineage_direction="downstream",
            include_column_lineage=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )
    table = {
        "Name": "shared-transactions",
        "DatabaseName": "mixed-database",
        "CatalogId": "123412341234",
        "TargetTable": {
            "CatalogId": "432143214321",
            "DatabaseName": "test-database",
            "Name": "transactions",
        },
        "StorageDescriptor": {
            "Columns": [{"Name": "txn_id", "Type": "bigint", "Comment": ""}],
            "Location": "s3://owner-bucket/transactions",
        },
    }

    by_entity = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in source._gen_table_wu(table)
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, models.UpstreamLineageClass)
    }
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,consumer_inst.mixed-database.shared-transactions,PROD)"
    # One aspect on the S3 URN (storage downstream) and one on the dataset URN (resource link).
    assert any(e and "dataPlatform:s3" in e for e in by_entity)
    assert dataset_urn in by_entity
    assert [u.dataset for u in by_entity[dataset_urn].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:glue,owner_inst.test-database.transactions,PROD)"
    ]


def test_dataflow_node_uses_owning_catalog_instance():
    # A Glue ETL job that reads a cross-account table via from_catalog(catalog_id=...) surfaces the
    # catalog id in the node args. The job's input/output dataset URN must be stamped with the
    # owning account's instance so it matches the dataset entity's URN (built the same way).
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            platform_instance="ingestion",
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )
    node = {
        "NodeType": "DataSource",
        "Id": "DataSource0",
        "Args": [
            {"Name": "database", "Value": '"test-database"', "Param": False},
            {"Name": "table_name", "Value": '"transactions"', "Param": False},
            {"Name": "catalog_id", "Value": '"432143214321"', "Param": False},
        ],
    }

    result = source.process_dataflow_node(
        node, "urn:li:dataFlow:(urn:li:dataPlatform:glue,flow,PROD)"
    )

    assert result is not None
    assert result["urn"] == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,owner_inst.test-database.transactions,PROD)"
    )


def test_dataflow_node_without_catalog_id_uses_source_instance():
    # No catalog id in the node args (the common case) -> the source's own instance, unchanged.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            platform_instance="ingestion",
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )
    node = {
        "NodeType": "DataSource",
        "Id": "DataSource0",
        "Args": [
            {"Name": "database", "Value": '"local-database"', "Param": False},
            {"Name": "table_name", "Value": '"events"', "Param": False},
        ],
    }

    result = source.process_dataflow_node(
        node, "urn:li:dataFlow:(urn:li:dataPlatform:glue,flow,PROD)"
    )

    assert result is not None
    assert result["urn"] == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,ingestion.local-database.events,PROD)"
    )


def test_catalog_to_platform_instance_accepts_govcloud_arn():
    # GovCloud/China ARNs use non-`aws` partitions; a catalog ARN in those partitions must both
    # validate and resolve.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-gov-west-1",
            platform_instance="ingestion",
            catalog_to_platform_instance={
                "arn:aws-us-gov:glue:us-gov-west-1:111122223333": {
                    "platform_instance": "gov_owner"
                },
            },
        ),
    )
    assert (
        source._resolve_platform_instance("111122223333").platform_instance
        == "gov_owner"
    )


def test_catalog_to_platform_instance_accepts_china_arn():
    # The China partition (aws-cn / cn-* regions) must validate and resolve just like GovCloud.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="cn-north-1",
            platform_instance="ingestion",
            catalog_to_platform_instance={
                "arn:aws-cn:glue:cn-north-1:111122223333": {
                    "platform_instance": "cn_owner"
                },
            },
        ),
    )
    assert (
        source._resolve_platform_instance("111122223333").platform_instance
        == "cn_owner"
    )


def test_catalog_to_platform_instance_accepts_unrecognized_region():
    # botocore doesn't recognize a not-yet-published region; the partition lookup falls back to
    # commercial `aws`, so a valid future-region ARN validates instead of being wrongly rejected.
    config = GlueSourceConfig(
        aws_region="xx-future-1",
        catalog_to_platform_instance={
            "arn:aws:glue:xx-future-1:111122223333": {"platform_instance": "x"},
        },
    )
    assert config.catalog_to_platform_instance


def test_get_glue_arn_uses_region_partition():
    # The qualifiedName ARN must use the region's partition (GovCloud here), not a hardcoded `aws`.
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-gov-west-1"),
    )
    assert source.get_glue_arn(account_id="123456789012", database="db", table="t") == (
        "arn:aws-us-gov:glue:us-gov-west-1:123456789012:table/db/t"
    )


def test_catalog_to_platform_instance_rejects_partition_region_mismatch():
    # The commercial `aws` partition with a GovCloud region is internally inconsistent: the lookup
    # derives the partition from the region (aws-us-gov), so this key would never match. Reject it
    # at config load rather than silently falling back to the wrong instance.
    with pytest.raises(pydantic.ValidationError):
        GlueSourceConfig(
            aws_region="us-gov-west-1",
            catalog_to_platform_instance={
                "arn:aws:glue:us-gov-west-1:111122223333": {"platform_instance": "x"},
            },
        )


def test_dataflow_node_uses_source_catalog_id_fallback():
    # A job that reads via from_catalog() with no explicit catalog_id, when the whole source targets
    # a cross-account catalog_id, must still stamp the owning instance (fallback to source catalog).
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            platform_instance="ingestion",
            catalog_id="432143214321",
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )
    node = {
        "NodeType": "DataSource",
        "Id": "DataSource0",
        "Args": [
            {"Name": "database", "Value": '"test-database"', "Param": False},
            {"Name": "table_name", "Value": '"transactions"', "Param": False},
        ],
    }

    result = source.process_dataflow_node(
        node, "urn:li:dataFlow:(urn:li:dataPlatform:glue,flow,PROD)"
    )

    assert result is not None
    assert result["urn"] == (
        "urn:li:dataset:(urn:li:dataPlatform:glue,owner_inst.test-database.transactions,PROD)"
    )


def test_catalog_to_platform_instance_rejects_malformed_arn_key():
    # A typo'd key would silently never match and fall back to the wrong instance, defeating the
    # whole point of the feature. Reject it at config load instead.
    with pytest.raises(pydantic.ValidationError):
        GlueSourceConfig(
            aws_region="us-east-1",
            catalog_to_platform_instance={
                "111122223333": {"platform_instance": "domain_a"},
            },
        )


def test_glue_ingest_cross_account_and_resource_links(
    tmp_path: Path, pytestconfig: pytest.Config
) -> None:
    # End-to-end pipeline test for the cross-account features: a mixed database containing a normal
    # table (consumer account) and a Lake Formation resource link pointing at a table owned by
    # account 432143214321. Expect: consumer tables under "consumer_domain", the resource link
    # carrying a table-level upstream edge to the owner's table under "owner_domain", and the link's
    # schema resolved from the owning table (via the cross-account glue:GetTable fallback, since no
    # DataHub graph is wired here).
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            platform_instance="consumer_domain",
            extract_transforms=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            catalog_to_platform_instance={
                "arn:aws:glue:us-east-1:432143214321": {
                    "platform_instance": "owner_domain"
                },
            },
        ),
    )

    with Stubber(source.glue_client) as glue_stubber:
        glue_stubber.add_response(
            "get_databases", get_databases_response_with_mixed_database, {}
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_for_mixed_database,
            {"DatabaseName": "mixed-database"},
        )
        # The resource link's schema is resolved from its owning table via a cross-account
        # glue:GetTable on the target (no DataHub graph is wired in this test).
        glue_stubber.add_response(
            "get_table",
            {
                "Table": {
                    "Name": "transactions",
                    "DatabaseName": "test-database",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "txn_id", "Type": "bigint"},
                            {"Name": "amount", "Type": "double"},
                            {"Name": "currency", "Type": "string"},
                        ]
                    },
                }
            },
            {
                "CatalogId": "432143214321",
                "DatabaseName": "test-database",
                "Name": "transactions",
            },
        )

        mce_objects = [wu.metadata for wu in source.get_workunits()]
        glue_stubber.assert_no_pending_responses()

        write_metadata_file(tmp_path / "glue_mces_cross_account.json", mce_objects)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "glue_mces_cross_account.json",
        golden_path=test_resources_dir / "glue_mces_cross_account_golden.json",
    )


def test_extract_urns_from_query_applies_target_platform_instance():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            target_platform_configs={
                "postgres": {"platform_instance": "pg_core", "env": "PROD"}
            },
        ),
    )

    urns = source._extract_urns_from_query(
        query="SELECT * FROM public.orders",
        platform="postgres",
        database="mydb",
        flow_urn="urn:li:dataFlow:(urn:li:dataPlatform:glue,flow,PROD)",
        node_label="node-1",
    )

    assert urns is not None
    # The configured platform_instance must be part of the parsed upstream URN so it matches the
    # postgres connector's own URNs.
    assert any("pg_core" in urn for urn in urns)


@mock_aws
def test_glue_moto_cross_account_and_resource_link_lineage() -> None:
    """End-to-end ingestion against a moto-backed Glue catalog.

    The Stubber-based golden test feeds the source hand-written API responses; this test runs the
    real boto3 -> Glue path so it proves the *wiring* the fix depends on:
      1. the ``CatalogId`` the Glue API returns on each table drives ``platform_instance`` stamping
         (the configured mapping is consulted, not just the source's own ``platform_instance``);
      2. a Lake Formation resource link (``TargetTable``) emits a table-level upstream edge to the
         *owner's* URN — stamped with the owner account's instance from the map; and
      3. the resource link's schema is resolved from the owning table (here via the cross-account
         glue:GetTable fallback, since no DataHub graph is wired in this test) so its columns are
         visible on the ingested link dataset.
    """
    region = "us-east-1"
    # moto stamps every table it returns with its default account, regardless of CatalogId; that
    # account therefore stands in for the ingesting (consumer) catalog here.
    consumer_account = "123456789012"
    owner_account = "222222222222"

    glue = boto3.client("glue", region_name=region)

    # The owner-account table the resource link points at. It lives in its own database, excluded
    # from this ingestion via database_pattern — it's only the target of the link's lineage edge.
    glue.create_database(DatabaseInput={"Name": "owner_db"})
    glue.create_table(
        DatabaseName="owner_db",
        TableInput={
            "Name": "events_source",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "event_id", "Type": "bigint"},
                    {"Name": "amount", "Type": "double"},
                ],
                "Location": "s3://owner-bucket/events",
            },
        },
    )

    # Consumer database: a normal table plus a Lake Formation resource link (no columns of its own).
    glue.create_database(DatabaseInput={"Name": "analytics_db"})
    glue.create_table(
        DatabaseName="analytics_db",
        TableInput={
            "Name": "events",
            "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
        },
    )
    glue.create_table(
        DatabaseName="analytics_db",
        TableInput={
            "Name": "shared_events",
            "TargetTable": {
                "CatalogId": owner_account,
                "DatabaseName": "owner_db",
                "Name": "events_source",
            },
        },
    )

    source = GlueSource(
        ctx=PipelineContext(run_id="glue-moto-test"),
        config=GlueSourceConfig(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region=region,
            platform_instance="ingestion_acct",
            extract_transforms=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            database_pattern={"allow": ["analytics_db"]},
            catalog_to_platform_instance={
                f"arn:aws:glue:{region}:{consumer_account}": {
                    "platform_instance": "consumer_inst"
                },
                f"arn:aws:glue:{region}:{owner_account}": {
                    "platform_instance": "owner_inst"
                },
            },
        ),
    )

    wus = list(source.get_workunits())

    snapshot_urns = {
        wu.metadata.proposedSnapshot.urn
        for wu in wus
        if isinstance(wu.metadata, models.MetadataChangeEventClass)
    }

    events_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:glue,"
        "consumer_inst.analytics_db.events,PROD)"
    )
    shared_events_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:glue,"
        "consumer_inst.analytics_db.shared_events,PROD)"
    )
    owner_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:glue,"
        "owner_inst.owner_db.events_source,PROD)"
    )

    # (1) The mapping is consulted for the table's own CatalogId: tables are stamped with the mapped
    # "consumer_inst", not the source's own "ingestion_acct". owner_db is excluded by the pattern,
    # so it is never emitted as a dataset (it exists only as the lineage target).
    assert events_urn in snapshot_urns
    assert shared_events_urn in snapshot_urns
    assert all("ingestion_acct" not in urn for urn in snapshot_urns)
    assert all("owner_db" not in urn for urn in snapshot_urns)

    # (2) The resource link stitches back to the owner account's URN via TargetTable.CatalogId.
    upstreams = [
        aspect
        for wu in wus
        if wu.get_urn() == shared_events_urn
        for aspect in [wu.get_aspect_of_type(models.UpstreamLineageClass)]
        if aspect is not None
    ]
    assert len(upstreams) == 1
    assert [u.dataset for u in upstreams[0].upstreams] == [owner_urn]

    # (3) The link's schema is resolved from the owning table (via cross-account glue:GetTable here)
    # so its columns are visible on the ingested link dataset.
    schemas = [
        aspect
        for wu in wus
        if wu.get_urn() == shared_events_urn
        for aspect in [wu.get_aspect_of_type(models.SchemaMetadataClass)]
        if aspect is not None
    ]
    assert len(schemas) == 1
    assert {f.fieldPath.split(".")[-1] for f in schemas[0].fields} == {
        "event_id",
        "amount",
    }
    assert source.report.num_resource_link_schema_from_glue == 1


@pytest.mark.parametrize(
    "ignore_resource_links, all_databases_and_tables_result",
    [
        (True, ([], [])),
        (False, ([resource_link_database], target_database_tables)),
    ],
)
def test_ignore_resource_links(ignore_resource_links, all_databases_and_tables_result):
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="eu-west-1",
            ignore_resource_links=ignore_resource_links,
        ),
    )

    with Stubber(source.glue_client) as glue_stubber:
        glue_stubber.add_response(
            "get_databases",
            get_databases_response_with_resource_link,
            {},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_for_target_database,
            {"DatabaseName": "resource-link-test-database"},
        )

        assert source.get_all_databases_and_tables() == all_databases_and_tables_result


@pytest.mark.parametrize(
    "ignore_resource_links, expected_tables",
    [
        # When ignore_resource_links is True, the table-level resource link
        # (TargetTable) is dropped and only the normal table is yielded.
        (True, [normal_table_in_mixed_database]),
        # When ignore_resource_links is False, both tables are yielded.
        (
            False,
            [normal_table_in_mixed_database, resource_link_table_in_mixed_database],
        ),
    ],
)
def test_ignore_resource_links_filters_table_level_links(
    ignore_resource_links, expected_tables
):
    """Regression test for CUS-8715.

    Lake Formation supports table-granularity sharing where a regular database
    contains tables that are resource links (i.e. tables with a TargetTable
    field). Database-level filtering does not catch these, so the table-level
    filter must drop them when ignore_resource_links is enabled.
    """
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="eu-west-1",
            ignore_resource_links=ignore_resource_links,
        ),
    )

    with Stubber(source.glue_client) as glue_stubber:
        glue_stubber.add_response(
            "get_databases",
            get_databases_response_with_mixed_database,
            {},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_for_mixed_database,
            {"DatabaseName": "mixed-database"},
        )

        databases, tables = source.get_all_databases_and_tables()

    assert databases == [mixed_database]
    assert tables == expected_tables


def test_platform_must_be_valid():
    with pytest.raises(pydantic.ValidationError):
        GlueSource(
            ctx=PipelineContext(run_id="glue-source-test"),
            config=GlueSourceConfig(aws_region="us-west-2", platform="data-warehouse"),
        )


def test_config_without_platform():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2"),
    )
    assert source.platform == "glue"


def test_get_databases_filters_by_catalog():
    def format_databases(databases):
        return set(d["Name"] for d in databases)

    all_catalogs_source: GlueSource = GlueSource(
        config=GlueSourceConfig(aws_region="us-west-2"),
        ctx=PipelineContext(run_id="glue-source-test"),
    )
    with Stubber(all_catalogs_source.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_response, {})

        expected = [flights_database, test_database, empty_database]
        actual = all_catalogs_source.get_all_databases()
        assert format_databases(actual) == format_databases(expected)
        assert all_catalogs_source.report.databases.dropped_entities.as_obj() == []

    catalog_id = "123412341234"
    single_catalog_source: GlueSource = GlueSource(
        config=GlueSourceConfig(catalog_id=catalog_id, aws_region="us-west-2"),
        ctx=PipelineContext(run_id="glue-source-test"),
    )
    with Stubber(single_catalog_source.glue_client) as glue_stubber:
        glue_stubber.add_response(
            "get_databases", get_databases_response, {"CatalogId": catalog_id}
        )

        expected = [flights_database, test_database]
        actual = single_catalog_source.get_all_databases()
        assert format_databases(actual) == format_databases(expected)
        assert single_catalog_source.report.databases.dropped_entities.as_obj() == [
            "empty-database"
        ]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_stateful(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    deleted_actor_golden_mcs = "{}/glue_deleted_actor_mces_golden.json".format(
        test_resources_dir
    )

    stateful_config = {
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 100.0,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
    }

    source_config_dict: Dict[str, Any] = {
        "extract_transforms": False,
        "aws_region": "eu-east-1",
        **stateful_config,
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "glue",
            "config": source_config_dict,
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "statefulpipeline",
        "run_id": "glue-2020_04_14-07_00_00-xds5dj",
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        with patch(
            "datahub.ingestion.source.aws.glue.GlueSource.get_all_databases_and_tables",
        ) as mock_get_all_databases_and_tables:
            tables_on_first_call = tables_1
            tables_on_second_call = tables_2
            mock_get_all_databases_and_tables.side_effect = [
                ([flights_database], tables_on_first_call),
                ([test_database], tables_on_second_call),
            ]

            pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
            checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

            assert checkpoint1
            assert checkpoint1.state

            # Capture MCEs of second run to validate Status(removed=true)
            deleted_mces_path = "{}/{}".format(tmp_path, "glue_deleted_mces.json")
            pipeline_config_dict["sink"]["type"] = "file"
            pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}

            # Do the second run of the pipeline.
            pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
            checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

            assert checkpoint2
            assert checkpoint2.state

            # Validate that all providers have committed successfully.
            validate_all_providers_have_committed_successfully(
                pipeline=pipeline_run1, expected_providers=1
            )
            validate_all_providers_have_committed_successfully(
                pipeline=pipeline_run2, expected_providers=1
            )

            # Validate against golden MCEs where Status(removed=true)
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=deleted_mces_path,
                golden_path=deleted_actor_golden_mcs,
            )

            # Perform all assertions on the states. The deleted table should not be
            # part of the second state
            state1 = cast(BaseSQLAlchemyCheckpointState, checkpoint1.state)
            state2 = cast(BaseSQLAlchemyCheckpointState, checkpoint2.state)
            difference_urns = set(
                state1.get_urns_not_in(type="*", other_checkpoint_state=state2)
            )
            assert difference_urns == {
                "urn:li:dataset:(urn:li:dataPlatform:glue,flights-database.avro,PROD)",
                "urn:li:container:0b9f1f731ecf6743be6207fec3dc9cba",
            }


def test_glue_with_delta_schema_ingest(
    tmp_path: Path,
    pytestconfig: pytest.Config,
) -> None:
    glue_source_instance = glue_source(
        platform_instance="delta_platform_instance",
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
        extract_delta_schema_from_parameters=True,
    )

    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_delta_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_delta_tables_response_1,
            {"DatabaseName": "delta-database"},
        )
        glue_stubber.add_response("get_jobs", get_jobs_response_empty, {})

        mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]

        glue_stubber.assert_no_pending_responses()

        assert glue_source_instance.get_report().num_dataset_valid_delta_schema == 1

        write_metadata_file(tmp_path / "glue_delta_mces.json", mce_objects)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "glue_delta_mces.json",
        golden_path=test_resources_dir / "glue_delta_mces_golden.json",
    )


def test_glue_with_malformed_delta_schema_ingest(
    tmp_path: Path,
    pytestconfig: pytest.Config,
) -> None:
    glue_source_instance = glue_source(
        platform_instance="delta_platform_instance",
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
        extract_delta_schema_from_parameters=True,
    )

    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_delta_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_delta_tables_response_2,
            {"DatabaseName": "delta-database"},
        )
        glue_stubber.add_response("get_jobs", get_jobs_response_empty, {})

        mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]

        glue_stubber.assert_no_pending_responses()

        assert glue_source_instance.get_report().num_dataset_invalid_delta_schema == 1

        write_metadata_file(tmp_path / "glue_malformed_delta_mces.json", mce_objects)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "glue_malformed_delta_mces.json",
        golden_path=test_resources_dir / "glue_malformed_delta_mces_golden.json",
    )


@pytest.mark.parametrize(
    "platform_instance, mce_file, mce_golden_file",
    [
        (None, "glue_mces.json", "glue_mces_golden_table_lineage.json"),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_ingest_include_table_lineage(
    tmp_path: Path,
    pytestconfig: pytest.Config,
    mock_datahub_graph_instance: DataHubGraph,
    platform_instance: str,
    mce_file: str,
    mce_golden_file: str,
) -> None:
    glue_source_instance = glue_source(
        platform_instance=platform_instance,
        mock_datahub_graph_instance=mock_datahub_graph_instance,
        emit_storage_lineage=True,
    )

    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_1,
            {"DatabaseName": "flights-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_2,
            {"DatabaseName": "test-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            {"TableList": []},
            {"DatabaseName": "empty-database"},
        )
        glue_stubber.add_response("get_jobs", get_jobs_response, {})
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_1,
            {"PythonScript": get_object_body_1},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_2,
            {"PythonScript": get_object_body_2},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_3,
            {"PythonScript": get_object_body_3},
        )

        with Stubber(glue_source_instance.s3_client) as s3_stubber:
            for _ in range(
                len(get_tables_response_1["TableList"])
                + len(get_tables_response_2["TableList"])
            ):
                s3_stubber.add_response(
                    "get_bucket_tagging",
                    get_bucket_tagging(),
                )
                s3_stubber.add_response(
                    "get_object_tagging",
                    get_object_tagging(),
                )

            s3_stubber.add_response(
                "get_object",
                get_object_response_1(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-1.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_2(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-2.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_3(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-3.py",
                },
            )

            mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]
            glue_stubber.assert_no_pending_responses()
            s3_stubber.assert_no_pending_responses()

            write_metadata_file(tmp_path / mce_file, mce_objects)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_file,
        golden_path=test_resources_dir / mce_golden_file,
    )


@pytest.mark.parametrize(
    "platform_instance, mce_file, mce_golden_file",
    [
        (None, "glue_mces.json", "glue_mces_golden_table_column_lineage.json"),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_ingest_include_column_lineage(
    tmp_path: Path,
    pytestconfig: pytest.Config,
    mock_datahub_graph_instance: DataHubGraph,
    platform_instance: str,
    mce_file: str,
    mce_golden_file: str,
) -> None:
    glue_source_instance = glue_source(
        platform_instance=platform_instance,
        mock_datahub_graph_instance=mock_datahub_graph_instance,
        emit_storage_lineage=True,
        include_column_lineage=True,
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
        extract_transforms=False,
    )

    # fake the server response
    def fake_schema_metadata(entity_urn: str) -> models.SchemaMetadataClass:
        return models.SchemaMetadataClass(
            schemaName="crawler-public-us-west-2/flight/avro",
            platform="urn:li:dataPlatform:s3",  # important <- platform must be an urn
            version=0,
            hash="",
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="yr",
                    type=models.SchemaFieldDataTypeClass(type=models.NumberTypeClass()),
                    nativeDataType="int",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="flightdate",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="uniquecarrier",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="airlineid",
                    type=models.SchemaFieldDataTypeClass(type=models.NumberTypeClass()),
                    nativeDataType="int",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="carrier",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="flightnum",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
                models.SchemaFieldClass(
                    fieldPath="origin",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                    # use this to provide the type of the field in the source system's vernacular
                ),
            ],
        )

    glue_source_instance.ctx.graph.get_schema_metadata = fake_schema_metadata  # type: ignore

    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response(
            "get_databases", get_databases_response_for_lineage, {}
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_lineage_response_1,
            {"DatabaseName": "flights-database-lineage"},
        )

        mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]
        glue_stubber.assert_no_pending_responses()

        write_metadata_file(tmp_path / mce_file, mce_objects)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_file,
        golden_path=test_resources_dir / mce_golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_ingest_with_profiling(
    tmp_path: Path,
    pytestconfig: pytest.Config,
) -> None:
    glue_source_instance = glue_source_with_profiling()
    mce_file = "glue_mces.json"
    mce_golden_file = "glue_mces_golden_profiling.json"
    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_response_profiling, {})

        glue_stubber.add_response(
            "get_tables",
            get_tables_response_profiling_1,
            {"DatabaseName": "flights-database-profiling"},
        )

        glue_stubber.add_response(
            "get_table",
            {"Table": tables_profiling_1[0]},
            {"DatabaseName": "flights-database-profiling", "Name": "avro-profiling"},
        )

        mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]

        glue_stubber.assert_no_pending_responses()

        write_metadata_file(tmp_path / mce_file, mce_objects)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_file,
        golden_path=test_resources_dir / mce_golden_file,
    )


@pytest.mark.parametrize(
    "platform_instance, mce_file, mce_golden_file",
    [
        (
            None,
            "glue_mces_lake_formation_tags.json",
            "glue_mces_lake_formation_tags_golden.json",
        ),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_ingest_with_lake_formation_tag_extraction(
    tmp_path: Path,
    pytestconfig: pytest.Config,
    platform_instance: str,
    mock_datahub_graph_instance: DataHubGraph,
    mce_file: str,
    mce_golden_file: str,
) -> None:
    glue_source_instance = glue_source(
        platform_instance=platform_instance,
        extract_lakeformation_tags=True,
        mock_datahub_graph_instance=mock_datahub_graph_instance,
    )

    # Mock the Lake Formation client responses
    def mock_get_resource_lf_tags(*args, **kwargs):
        resource = kwargs.get("Resource", {})
        if "Database" in resource:
            # Mock database LF tags
            return {
                "LFTagOnDatabase": [
                    {
                        "CatalogId": "123412341234",
                        "TagKey": "Environment",
                        "TagValues": ["Production", "Test"],
                    },
                    {
                        "CatalogId": "123412341234",
                        "TagKey": "Owner",
                        "TagValues": ["DataTeam"],
                    },
                ]
            }
        elif "Table" in resource:
            # Mock table LF tags - different tags per table for more realistic testing
            table_name = resource["Table"]["Name"]
            if table_name == "avro":
                return {
                    "LFTagsOnTable": [
                        {
                            "CatalogId": "123412341234",
                            "TagKey": "DataClassification",
                            "TagValues": ["Sensitive"],
                        },
                        {
                            "CatalogId": "123412341234",
                            "TagKey": "BusinessUnit",
                            "TagValues": ["Finance"],
                        },
                    ]
                }
            elif table_name == "json":
                return {
                    "LFTagsOnTable": [
                        {
                            "CatalogId": "123412341234",
                            "TagKey": "DataClassification",
                            "TagValues": ["Public"],
                        },
                        {
                            "CatalogId": "123412341234",
                            "TagKey": "BusinessUnit",
                            "TagValues": ["Marketing"],
                        },
                    ]
                }
            else:
                return {"LFTagsOnTable": []}
        return {}

    # Mock list_lf_tags for getting all LF tags
    def mock_list_lf_tags(*args, **kwargs):
        return {
            "LFTags": [
                {"TagKey": "Environment", "TagValues": ["Production", "Test", "Dev"]},
                {"TagKey": "Owner", "TagValues": ["DataTeam", "Analytics"]},
                {
                    "TagKey": "DataClassification",
                    "TagValues": ["Public", "Internal", "Sensitive"],
                },
                {
                    "TagKey": "BusinessUnit",
                    "TagValues": ["Finance", "Marketing", "Sales"],
                },
            ]
        }

    # Mock PlatformResourceRepository.search_by_filter to return empty list
    def mock_search_by_filter(*args, **kwargs):
        return []

    with (
        patch(
            "datahub.api.entities.external.external_entities.PlatformResourceRepository.search_by_filter",
            side_effect=mock_search_by_filter,
        ),
        patch.object(
            glue_source_instance.lf_client,
            "get_resource_lf_tags",
            side_effect=mock_get_resource_lf_tags,
        ),
        patch.object(
            glue_source_instance.lf_client,
            "list_lf_tags",
            side_effect=mock_list_lf_tags,
        ),
        Stubber(glue_source_instance.glue_client) as glue_stubber,
    ):
        # Set up Glue client stubs
        glue_stubber.add_response("get_databases", get_databases_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_1,
            {"DatabaseName": "flights-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_2,
            {"DatabaseName": "test-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            {"TableList": []},
            {"DatabaseName": "empty-database"},
        )

        glue_stubber.add_response("get_jobs", get_jobs_response, {})
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_1,
            {"PythonScript": get_object_body_1},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_2,
            {"PythonScript": get_object_body_2},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_3,
            {"PythonScript": get_object_body_3},
        )

        with Stubber(glue_source_instance.s3_client) as s3_stubber:
            for _ in range(
                len(get_tables_response_1["TableList"])
                + len(get_tables_response_2["TableList"])
            ):
                s3_stubber.add_response(
                    "get_bucket_tagging",
                    get_bucket_tagging(),
                )
                s3_stubber.add_response(
                    "get_object_tagging",
                    get_object_tagging(),
                )

            s3_stubber.add_response(
                "get_object",
                get_object_response_1(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-1.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_2(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-2.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_3(),
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-3.py",
                },
            )

            # Execute the source and collect work units
            mce_objects = [wu.metadata for wu in glue_source_instance.get_workunits()]

            glue_stubber.assert_no_pending_responses()
            s3_stubber.assert_no_pending_responses()

            write_metadata_file(tmp_path / mce_file, mce_objects)

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_file,
        golden_path=test_resources_dir / mce_golden_file,
    )


def _lf_dict(key: str, value: str) -> Dict[str, Any]:
    return {"CatalogId": "123412341234", "TagKey": key, "TagValues": [value]}


def _table_lf_response(
    *,
    database_tags: Optional[List[Dict[str, Any]]] = None,
    table_tags: Optional[List[Dict[str, Any]]] = None,
    column_tags: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    """Build a GetResourceLFTags response for a Table resource."""
    response: Dict[str, Any] = {}
    if database_tags is not None:
        response["LFTagOnDatabase"] = database_tags
    if table_tags is not None:
        response["LFTagsOnTable"] = table_tags
    if column_tags is not None:
        response["LFTagsOnColumns"] = [
            {"Name": name, "LFTags": tags} for name, tags in column_tags.items()
        ]
    return response


def test_no_lakeformation_extraction_when_disabled() -> None:
    # With extract_lakeformation_tags disabled (the default), no Lake Formation
    # API calls are made and no LF tags are applied — even though column-tag
    # extraction defaults to True. The master flag gates all LF behavior.
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    assert source.source_config.extract_lakeformation_tags is False
    source.lf_client = MagicMock()
    table = {
        "Name": "sales",
        "DatabaseName": "datahub_test",
        "CatalogId": "123412341234",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }
    workunits = list(
        source._extract_record(
            "urn:li:dataset:(urn:li:dataPlatform:glue,datahub_test.sales,PROD)",
            table,
            "datahub_test.sales",
        )
    )

    source.lf_client.get_resource_lf_tags.assert_not_called()
    for wu in workunits:
        mce = wu.metadata
        if isinstance(mce, models.MetadataChangeEventClass):
            for aspect in mce.proposedSnapshot.aspects:
                assert not isinstance(aspect, models.GlobalTagsClass)
                if isinstance(aspect, models.SchemaMetadataClass):
                    assert all(f.globalTags is None for f in aspect.fields)


def _resolved_table_tag_names(source: GlueSource, response: Dict[str, Any]) -> Set[str]:
    with patch.object(source.lf_client, "get_resource_lf_tags", return_value=response):
        lf = source.get_table_lf_tags("123412341234", "flights-database", "avro")
    resolved = source._resolve_table_lf_tags(lf)
    return {
        tag.to_datahub_tag_urn().name
        for tag in [*resolved.direct, *resolved.propagated]
    }


def test_database_lf_tags_propagate_to_tables() -> None:
    # The database's tags arrive in the same table response (LFTagOnDatabase).
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    response = _table_lf_response(
        database_tags=[
            _lf_dict("Environment", "Production"),
            _lf_dict("Owner", "DataTeam"),
        ],
        table_tags=[_lf_dict("DataClassification", "Sensitive")],
    )
    names = _resolved_table_tag_names(source, response)

    # Directly-assigned table tag plus both inherited database tags.
    assert names == {
        "DataClassification:Sensitive",
        "Environment:Production",
        "Owner:DataTeam",
    }


def test_database_lf_tags_not_propagated_when_disabled() -> None:
    source = glue_source(extract_lakeformation_tags=True)
    response = _table_lf_response(
        database_tags=[_lf_dict("Environment", "Production")],
        table_tags=[_lf_dict("DataClassification", "Sensitive")],
    )
    names = _resolved_table_tag_names(source, response)

    assert names == {"DataClassification:Sensitive"}


def test_database_lf_tags_propagation_dedupes_table_tags() -> None:
    # The same tag is assigned to both the database and the table.
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    response = _table_lf_response(
        database_tags=[_lf_dict("Owner", "DataTeam")],
        table_tags=[_lf_dict("Owner", "DataTeam")],
    )
    names = _resolved_table_tag_names(source, response)

    assert names == {"Owner:DataTeam"}


def test_table_lf_tag_value_overrides_database_value_for_same_key() -> None:
    # Same key assigned at both levels with different values. In Lake Formation a
    # tag key holds a single value per resource and the table-level assignment
    # overrides the inherited database value.
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    response = _table_lf_response(
        database_tags=[_lf_dict("module", "Customers")],
        table_tags=[_lf_dict("module", "Sales")],
    )
    names = _resolved_table_tag_names(source, response)

    # Table value wins; the inherited database value is dropped.
    assert names == {"module:Sales"}


def test_column_lf_tags_parsed_from_table_response() -> None:
    source = glue_source(extract_lakeformation_tags=True)
    response = _table_lf_response(
        table_tags=[_lf_dict("DataClassification", "Sensitive")],
        column_tags={"customer_id": [_lf_dict("pii", "true")]},
    )
    with patch.object(source.lf_client, "get_resource_lf_tags", return_value=response):
        lf = source.get_table_lf_tags("123412341234", "flights-database", "sales")

    assert {t.to_datahub_tag_urn().name for t in lf.column_tags["customer_id"]} == {
        "pii:true"
    }


def test_column_lf_tags_attached_to_schema_field() -> None:
    source = glue_source(extract_lakeformation_tags=True)
    table = {
        "Name": "sales",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "customer_id", "Type": "string"},
            ]
        },
    }
    column_tags = {
        "customer_id": [
            LakeFormationTag(key="pii", value="true", catalog="123412341234")
        ]
    }
    schema = source._get_glue_schema_metadata(table, "sales", column_tags)
    assert schema is not None
    tagged = {
        f.fieldPath: [t.tag for t in f.globalTags.tags]
        for f in schema.fields
        if f.globalTags
    }
    # Only the directly-tagged column carries the tag.
    assert len(tagged) == 1
    field_path, tags = next(iter(tagged.items()))
    assert field_path.endswith("customer_id")
    assert tags == [make_tag_urn("pii:true")]


def _column_tag_names_by_field(schema: Any) -> Dict[str, Set[str]]:
    return {
        f.fieldPath.split(".")[-1]: {t.tag for t in f.globalTags.tags}
        for f in schema.fields
        if f.globalTags
    }


def test_lf_tag_parsing_expands_multi_value_tags() -> None:
    source = glue_source(extract_lakeformation_tags=True)
    response = _table_lf_response(
        table_tags=[
            {
                "CatalogId": "123412341234",
                "TagKey": "Environment",
                "TagValues": ["Production", "Test"],
            }
        ],
    )
    with patch.object(source.lf_client, "get_resource_lf_tags", return_value=response):
        lf = source.get_table_lf_tags("123412341234", "flights-database", "avro")

    assert {t.to_datahub_tag_urn().name for t in lf.table_tags} == {
        "Environment:Production",
        "Environment:Test",
    }


def test_lf_tag_parsing_skips_entries_without_tag_key() -> None:
    source = glue_source(extract_lakeformation_tags=True)
    response = _table_lf_response(
        table_tags=[
            {"CatalogId": "123412341234", "TagValues": ["orphan"]},  # no TagKey
            _lf_dict("Owner", "DataTeam"),
        ],
    )
    with patch.object(source.lf_client, "get_resource_lf_tags", return_value=response):
        lf = source.get_table_lf_tags("123412341234", "flights-database", "avro")

    assert {t.to_datahub_tag_urn().name for t in lf.table_tags} == {"Owner:DataTeam"}


def test_get_table_lf_tags_reports_warning_on_error() -> None:
    source = glue_source(extract_lakeformation_tags=True)
    with patch.object(
        source.lf_client,
        "get_resource_lf_tags",
        side_effect=Exception("AccessDenied"),
    ):
        lf = source.get_table_lf_tags("123412341234", "flights-database", "avro")

    # Failure is surfaced, not swallowed, and degrades gracefully to no tags.
    assert lf.table_tags == []
    assert lf.database_tags == []
    assert lf.column_tags == {}
    assert len(list(source.report.warnings)) >= 1


def test_column_lf_tags_inherit_parent_tags_when_propagating() -> None:
    # Matches Snowflake's with_lineage behavior: table/database tags flow to
    # every column when propagation is enabled.
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    table = {
        "Name": "sales",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "customer_id", "Type": "string"},
            ]
        },
    }
    column_tags = {
        "customer_id": [LakeFormationTag(key="pii", value="true", catalog="c")]
    }
    parent_tags = [
        LakeFormationTag(key="DataClassification", value="Sensitive", catalog="c"),
        LakeFormationTag(key="Environment", value="Production", catalog="c"),
    ]
    schema = source._get_glue_schema_metadata(table, "sales", column_tags, parent_tags)
    by_col = _column_tag_names_by_field(schema)

    # Every column inherits the table/database tags...
    assert by_col["id"] == {
        make_tag_urn("DataClassification:Sensitive"),
        make_tag_urn("Environment:Production"),
    }
    # ...and the directly-tagged column also keeps its own tag.
    assert by_col["customer_id"] == {
        make_tag_urn("pii:true"),
        make_tag_urn("DataClassification:Sensitive"),
        make_tag_urn("Environment:Production"),
    }


def test_column_direct_tag_overrides_inherited_parent_for_same_key() -> None:
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    table = {
        "Name": "sales",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }
    # Same key assigned directly on the column and inherited from the parent.
    column_tags = {
        "id": [LakeFormationTag(key="DataClassification", value="Public", catalog="c")]
    }
    parent_tags = [
        LakeFormationTag(key="DataClassification", value="Sensitive", catalog="c")
    ]
    schema = source._get_glue_schema_metadata(table, "sales", column_tags, parent_tags)
    by_col = _column_tag_names_by_field(schema)

    # Column's own value wins; inherited value is dropped.
    assert by_col["id"] == {make_tag_urn("DataClassification:Public")}


def test_propagated_table_tag_is_marked_as_propagated() -> None:
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    response = _table_lf_response(
        database_tags=[_lf_dict("Owner", "DataTeam")],
        table_tags=[_lf_dict("DataClassification", "Sensitive")],
    )
    with patch.object(source.lf_client, "get_resource_lf_tags", return_value=response):
        lf = source.get_table_lf_tags("123412341234", "flights-database", "sales")
    resolved = source._resolve_table_lf_tags(lf)
    global_tags = source._build_lf_global_tags(
        resolved.direct, resolved.propagated, propagated_origin="urn:li:container:db"
    )
    assert global_tags is not None
    by_urn = {a.tag: a for a in global_tags.tags}

    # Directly-assigned table tag is not marked as propagated.
    assert by_urn[make_tag_urn("DataClassification:Sensitive")].attribution is None
    # Inherited database tag is marked as propagated, with origin + legacy context.
    propagated = by_urn[make_tag_urn("Owner:DataTeam")]
    assert propagated.attribution is not None
    assert propagated.attribution.sourceDetail["propagated"] == "true"
    assert propagated.attribution.sourceDetail["origin"] == "urn:li:container:db"
    assert propagated.context is not None
    assert json.loads(propagated.context)["propagated"] == "true"


def test_propagated_column_tag_is_marked_as_propagated() -> None:
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
    )
    table = {
        "Name": "sales",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "customer_id", "Type": "string"},
            ]
        },
    }
    column_tags = {
        "customer_id": [LakeFormationTag(key="pii", value="true", catalog="c")]
    }
    parent_tags = [
        LakeFormationTag(key="DataClassification", value="Sensitive", catalog="c")
    ]
    schema = source._get_glue_schema_metadata(
        table,
        "sales",
        column_tags,
        parent_tags,
        "urn:li:dataset:(urn:li:dataPlatform:glue,datahub_test.sales,PROD)",
    )
    assert schema is not None
    by_field = {
        f.fieldPath.split(".")[-1]: {a.tag: a for a in f.globalTags.tags}
        for f in schema.fields
        if f.globalTags
    }
    # Inherited tag on customer_id is marked propagated; its own pii tag is not.
    assert by_field["customer_id"][make_tag_urn("pii:true")].attribution is None
    inherited = by_field["customer_id"][make_tag_urn("DataClassification:Sensitive")]
    assert inherited.attribution is not None
    assert inherited.attribution.sourceDetail["propagated"] == "true"
    # A column with only inherited tags is also marked.
    assert (
        by_field["id"][make_tag_urn("DataClassification:Sensitive")].attribution
        is not None
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_lf_tags_propagated_and_column_tagged_end_to_end() -> None:
    """One GetResourceLFTags call drives table propagation and column tagging."""
    source = glue_source(
        extract_lakeformation_tags=True,
        propagate_lakeformation_tags=True,
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
        extract_transforms=False,
    )

    def mock_get_resource_lf_tags(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        resource = kwargs.get("Resource", {})
        if "Database" in resource:
            return {"LFTagOnDatabase": [_lf_dict("Environment", "Production")]}
        # Table resource: AWS returns DB, table, and column tags together.
        return _table_lf_response(
            database_tags=[_lf_dict("Environment", "Production")],
            table_tags=[_lf_dict("DataClassification", "Sensitive")],
            column_tags={"uniquecarrier": [_lf_dict("pii", "true")]},
        )

    with (
        patch.object(
            source.lf_client,
            "get_resource_lf_tags",
            side_effect=mock_get_resource_lf_tags,
        ),
        Stubber(source.glue_client) as glue_stubber,
    ):
        glue_stubber.add_response("get_databases", get_databases_response, {})
        glue_stubber.add_response(
            "get_tables", get_tables_response_1, {"DatabaseName": "flights-database"}
        )
        glue_stubber.add_response(
            "get_tables", get_tables_response_2, {"DatabaseName": "test-database"}
        )
        glue_stubber.add_response(
            "get_tables", {"TableList": []}, {"DatabaseName": "empty-database"}
        )

        mce_objects = [wu.metadata for wu in source.get_workunits()]

    table_tags = None
    column_tag_fields = set()
    for mce in mce_objects:
        if not isinstance(mce, models.MetadataChangeEventClass):
            continue
        snapshot = mce.proposedSnapshot
        if "flights-database.avro" not in snapshot.urn:
            continue
        for aspect in snapshot.aspects:
            if isinstance(aspect, models.GlobalTagsClass):
                table_tags = {assoc.tag for assoc in aspect.tags}
            if isinstance(aspect, models.SchemaMetadataClass):
                for f in aspect.fields:
                    if f.globalTags and any(
                        t.tag == make_tag_urn("pii:true") for t in f.globalTags.tags
                    ):
                        column_tag_fields.add(f.fieldPath)

    assert table_tags is not None, "flights-database.avro had no GlobalTags aspect"
    assert make_tag_urn("DataClassification:Sensitive") in table_tags
    # The database tag was propagated down to the table.
    assert make_tag_urn("Environment:Production") in table_tags
    # The column tag landed on the matching schema field.
    assert any(fp.endswith("uniquecarrier") for fp in column_tag_fields)


def _make_jdbc_node(
    node_id: str,
    node_type: str,
    connection_type: str,
    jdbc_url: str,
    dbtable: str,
) -> Dict[str, Any]:
    return {
        "Id": node_id,
        "NodeType": node_type,
        "Args": [
            {
                "Name": "connection_type",
                "Value": f'"{connection_type}"',
                "Param": False,
            },
            {
                "Name": "connection_options",
                "Value": f'{{"url": "{jdbc_url}", "dbtable": "{dbtable}"}}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }


@pytest.mark.parametrize(
    "jdbc_url, expected_platform, expected_database",
    [
        ("jdbc:postgresql://myhost:5432/mydb", "postgres", "mydb"),
        ("jdbc:mysql://myhost:3306/mydb", "mysql", "mydb"),
        ("jdbc:mariadb://myhost:3306/mydb", "mysql", "mydb"),
        ("jdbc:redshift://myhost:5439/mydb", "redshift", "mydb"),
        ("jdbc:oracle://myhost:1521/mydb", "oracle", "mydb"),
        ("jdbc:sqlserver://myhost:1433/mydb", "mssql", "mydb"),
        ("jdbc:sqlserver://myhost:1433;databaseName=mydb", "mssql", "mydb"),
        ("jdbc:postgresql://myhost:5432/mydb?sslmode=require", "postgres", "mydb"),
    ],
)
def test_parse_jdbc_url(
    jdbc_url: str, expected_platform: str, expected_database: str
) -> None:
    source = glue_source()
    platform, database = source._parse_jdbc_url(jdbc_url)
    assert platform == expected_platform
    assert database == expected_database


def test_parse_jdbc_url_invalid() -> None:
    source = glue_source()
    with pytest.raises(ValueError, match="Not a valid JDBC URL"):
        source._parse_jdbc_url("postgresql://myhost/mydb")


@pytest.mark.parametrize(
    "connection_type, jdbc_url, dbtable, expected_urn",
    [
        (
            "postgresql",
            "jdbc:postgresql://myhost:5432/mydb",
            "public.customers",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.customers,PROD)",
        ),
        (
            "postgresql",
            "jdbc:postgresql://myhost:5432/mydb",
            "customers",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.customers,PROD)",
        ),
        (
            "mysql",
            "jdbc:mysql://myhost:3306/mydb",
            "myschema.orders",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.myschema.orders,PROD)",
        ),
        (
            "mysql",
            "jdbc:mysql://myhost:3306/mydb",
            "orders",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.orders,PROD)",
        ),
    ],
)
def test_process_dataflow_node_jdbc(
    connection_type: str,
    jdbc_url: str,
    dbtable: str,
    expected_urn: str,
) -> None:
    source = glue_source()
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = _make_jdbc_node(
        node_id="DataSource0",
        node_type="DataSource",
        connection_type=connection_type,
        jdbc_url=jdbc_url,
        dbtable=dbtable,
    )

    result = source.process_dataflow_node(node, flow_urn)

    assert result is not None
    assert result["urn"] == expected_urn


def test_process_dataflow_node_jdbc_missing_url() -> None:
    source = glue_source()
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = {
        "Id": "DataSource0",
        "NodeType": "DataSource",
        "Args": [
            {"Name": "connection_type", "Value": '"postgresql"', "Param": False},
            {
                "Name": "connection_options",
                "Value": '{"dbtable": "public.customers"}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }

    result = source.process_dataflow_node(node, flow_urn)

    assert result is None
    assert source.report.warnings


def test_jdbc_platform_map_coverage() -> None:
    source = glue_source()
    for jdbc_protocol, expected_platform in JDBC_PLATFORM_MAP.items():
        url = f"jdbc:{jdbc_protocol}://host:1234/db"
        platform, database = source._parse_jdbc_url(url)
        assert platform == expected_platform, f"Failed for {jdbc_protocol}"
        assert database == "db"


def _make_glue_connection_node(
    node_id: str,
    node_type: str,
    connection_name: str,
    dbtable: str,
) -> Dict[str, Any]:
    return {
        "Id": node_id,
        "NodeType": node_type,
        "Args": [
            {
                "Name": "connection_options",
                "Value": f'{{"connectionName": "{connection_name}", "dbtable": "{dbtable}"}}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }


@pytest.mark.parametrize(
    "dbtable, expected_urn",
    [
        (
            "public.customers",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.customers,PROD)",
        ),
        (
            "customers",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.customers,PROD)",
        ),
    ],
)
def test_process_dataflow_node_glue_connection_jdbc(
    dbtable: str, expected_urn: str
) -> None:
    source = glue_source()
    source.glue_client.get_connection = lambda **kw: {  # type: ignore[method-assign]
        "Connection": {
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://myhost:5432/mydb",
            },
        }
    }
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = _make_glue_connection_node(
        "DataSource0", "DataSource", "My PG Connection", dbtable
    )

    result = source.process_dataflow_node(node, flow_urn)

    assert result is not None
    assert result["urn"] == expected_urn


@pytest.mark.parametrize(
    "conn_type, dbtable, expected_urn",
    [
        (
            "POSTGRESQL",
            "public.orders",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)",
        ),
        (
            "POSTGRESQL",
            "orders",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)",
        ),
        (
            "MYSQL",
            "orders",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.orders,PROD)",
        ),
    ],
)
def test_process_dataflow_node_glue_connection_native(
    conn_type: str, dbtable: str, expected_urn: str
) -> None:
    source = glue_source()
    source.glue_client.get_connection = lambda **kw: {  # type: ignore[method-assign]
        "Connection": {
            "ConnectionType": conn_type,
            "ConnectionProperties": {
                "HOST": "myhost",
                "DATABASE": "mydb",
            },
        }
    }
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = _make_glue_connection_node(
        "DataSource0", "DataSource", "My Connection", dbtable
    )

    result = source.process_dataflow_node(node, flow_urn)

    assert result is not None
    assert result["urn"] == expected_urn


def test_process_dataflow_node_glue_connection_missing_dbtable() -> None:
    source = glue_source()
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = {
        "Id": "DataSource0",
        "NodeType": "DataSource",
        "Args": [
            {
                "Name": "connection_options",
                "Value": '{"connectionName": "My Connection"}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }

    result = source.process_dataflow_node(node, flow_urn)

    assert result is None
    assert source.report.warnings


def test_process_dataflow_node_glue_connection_fetch_failure() -> None:
    source = glue_source()

    def _raise(**kw: Any) -> None:
        raise Exception("Connection not found")

    source.glue_client.get_connection = _raise  # type: ignore[method-assign]
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = _make_glue_connection_node("DataSource0", "DataSource", "Missing", "mytable")

    result = source.process_dataflow_node(node, flow_urn)

    assert result is None
    assert source.report.warnings


def test_resolve_glue_connection_caching() -> None:
    source = glue_source()
    call_count = 0

    def _get_connection(**kw: Any) -> Dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {
            "Connection": {
                "ConnectionType": "POSTGRESQL",
                "ConnectionProperties": {"HOST": "h", "DATABASE": "db"},
            }
        }

    source.glue_client.get_connection = _get_connection  # type: ignore[method-assign]
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"

    source._resolve_glue_connection("My Connection", flow_urn)
    source._resolve_glue_connection("My Connection", flow_urn)

    assert call_count == 1


def test_glue_native_connection_type_map_coverage() -> None:
    source = glue_source()
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    for conn_type, expected_platform in GLUE_NATIVE_CONNECTION_TYPE_MAP.items():
        source._glue_connection_cache.clear()

        def _make_get_connection(ct: str) -> Any:
            def _get_connection(**kw: Any) -> Dict[str, Any]:
                return {
                    "Connection": {
                        "ConnectionType": ct,
                        "ConnectionProperties": {"HOST": "h", "DATABASE": "db"},
                    }
                }

            return _get_connection

        source.glue_client.get_connection = _make_get_connection(conn_type)  # type: ignore[method-assign]
        result = source._resolve_glue_connection(conn_type, flow_urn)
        assert result is not None, f"Failed for {conn_type}"
        assert result[0] == expected_platform


def test_resolve_glue_connection_spark_properties_fallback() -> None:
    """SparkProperties (v2 schema) is used when ConnectionProperties has no JDBC_CONNECTION_URL."""
    source = glue_source()
    source.glue_client.get_connection = lambda **kw: {  # type: ignore[method-assign]
        "Connection": {
            "ConnectionType": "JDBC",
            "ConnectionProperties": {},
            "SparkProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://myhost:5432/mydb"
            },
        }
    }
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"

    result = source._resolve_glue_connection("My Connection", flow_urn)

    assert result == ("postgres", "mydb")


@pytest.mark.parametrize(
    "query, expected_dbtable",
    [
        ("SELECT * FROM public.customers WHERE id = 1", "public.customers"),
        ("SELECT id, name FROM orders", "orders"),
    ],
)
def test_process_dataflow_node_glue_connection_query_fallback(
    query: str, expected_dbtable: str
) -> None:
    """query ConnectionOption is used when dbtable is absent (single-table queries)."""
    source = glue_source()
    source.glue_client.get_connection = lambda **kw: {  # type: ignore[method-assign]
        "Connection": {
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://myhost:5432/mydb",
            },
        }
    }
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = {
        "Id": "DataSource0",
        "NodeType": "DataSource",
        "Args": [
            {
                "Name": "connection_options",
                "Value": f'{{"connectionName": "My PG Connection", "query": "{query}"}}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }

    result = source.process_dataflow_node(node, flow_urn)

    assert result is not None
    assert expected_dbtable in result["urn"]


def test_process_dataflow_node_glue_connection_query_multi_table() -> None:
    """Multi-table JOIN queries produce multiple dataset URNs via dataset_urns."""
    source = glue_source()
    source.glue_client.get_connection = lambda **kw: {  # type: ignore[method-assign]
        "Connection": {
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://myhost:5432/mydb",
            },
        }
    }
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = {
        "Id": "DataSource0",
        "NodeType": "DataSource",
        "Args": [
            {
                "Name": "connection_options",
                "Value": '{"connectionName": "My PG Connection", "query": "SELECT a.id FROM orders a JOIN customers b ON a.cid = b.id"}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }

    result = source.process_dataflow_node(node, flow_urn)

    assert result is not None
    assert not source.report.warnings
    orders_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)"
    customers_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.customers,PROD)"
    )
    assert result["urn"] in (orders_urn, customers_urn)
    assert set(result["dataset_urns"]) == {orders_urn, customers_urn}


def test_process_dataflow_node_jdbc_query_fallback() -> None:
    """query ConnectionOption is used in the direct JDBC path when dbtable is absent."""
    source = glue_source()
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = {
        "Id": "DataSource0",
        "NodeType": "DataSource",
        "Args": [
            {
                "Name": "connection_type",
                "Value": '"postgresql"',
                "Param": False,
            },
            {
                "Name": "connection_options",
                "Value": '{"url": "jdbc:postgresql://myhost:5432/mydb", "query": "SELECT * FROM public.orders"}',
                "Param": False,
            },
        ],
        "LineNumber": 1,
    }

    result = source.process_dataflow_node(node, flow_urn)

    assert result is not None
    assert (
        result["urn"]
        == "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)"
    )


@pytest.mark.parametrize(
    "raw_url, expected_safe",
    [
        (
            "jdbc:postgresql://myhost:5432/mydb",
            "jdbc:postgresql://myhost:5432/mydb",
        ),
        (
            "jdbc:postgresql://admin:secret123@myhost:5432/mydb",
            "jdbc:postgresql://myhost:5432/mydb",
        ),
        (
            "jdbc:postgresql://myhost:5432/mydb?user=admin&password=secret",
            "jdbc:postgresql://myhost:5432/mydb",
        ),
        (
            "jdbc:postgresql://admin:secret@myhost:5432/mydb?ssl=true&password=extra",
            "jdbc:postgresql://myhost:5432/mydb",
        ),
    ],
)
def test_sanitize_jdbc_url(raw_url: str, expected_safe: str) -> None:
    assert _sanitize_jdbc_url(raw_url) == expected_safe


@pytest.mark.parametrize(
    "secret_name",
    [
        "password",
        "sfPassword",
        "PASSWORD",
        "secret",
        "client_secret",
        "aws_secret_access_key",
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_glue_redact_job_script_secret_fields(secret_name):
    secret_value = "kjdsg8uh834jksdnj"

    script = f"""
        datasource = glueContext.create_dynamic_frame.from_options(
            frame = transformed,
            connection_type = "postgresql",
            connection_options = {{
                "url": "jdbc:postgresql://your-PostgresqlDB-Endpoint",
                "dbtable": "your_table",
                "user": "your-Posgresql-User",
                "{secret_name}": "{secret_value}"
            }}
        )
    """

    assert _redact_secret_fields_in_dataflow_script(script) == script.replace(
        secret_value, "*****"
    )


# ── incremental_properties (PATCH mode) ──────────────────────────────────────


def test_incremental_properties_emits_patch_mcps(
    pytestconfig: pytest.Config, tmp_path: Path, mock_time: None
) -> None:
    source = glue_source(
        incremental_properties=True,
        extract_transforms=False,
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
    )

    with Stubber(source.glue_client) as glue_stubber:
        glue_stubber.add_response("get_databases", get_databases_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_1,
            {"DatabaseName": "flights-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_2,
            {"DatabaseName": "test-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            {"TableList": []},
            {"DatabaseName": "empty-database"},
        )
        glue_stubber.add_response(
            "get_jobs",
            get_jobs_response_empty,
            {},
        )

        workunits = list(source.get_workunits())

    # Find patch MCPs for datasetProperties
    patch_wus = [
        wu
        for wu in workunits
        if wu.metadata
        and isinstance(wu.metadata, models.MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
    ]
    assert len(patch_wus) > 0, "Expected patch MCPs for datasetProperties"

    # Verify patch MCPs are PATCH changeType with JSON Patch operations
    for wu in patch_wus:
        assert isinstance(wu.metadata, models.MetadataChangeProposalClass)
        assert wu.metadata.changeType == "PATCH"
        assert wu.metadata.aspect is not None
        patch_ops = json.loads(wu.metadata.aspect.value)
        assert isinstance(patch_ops, list)
        assert all("op" in op and "path" in op for op in patch_ops)

    # Verify that dataset MCEs do NOT contain DatasetProperties aspect
    mce_wus = [
        wu
        for wu in workunits
        if wu.metadata
        and isinstance(wu.metadata, models.MetadataChangeEventClass)
        and wu.metadata.proposedSnapshot
    ]
    for wu in mce_wus:
        assert isinstance(wu.metadata, models.MetadataChangeEventClass)
        snapshot = wu.metadata.proposedSnapshot
        if snapshot and hasattr(snapshot, "aspects"):
            for aspect in snapshot.aspects:
                assert not isinstance(aspect, models.DatasetPropertiesClass), (
                    "DatasetPropertiesClass should not be in MCE snapshot in PATCH mode"
                )


# ── extract_column_parameters (structured properties) ─────────────────────────


def _make_glue_source_with_column_params() -> GlueSource:
    pipeline_context = PipelineContext(run_id="glue-col-params-test")
    return GlueSource(
        ctx=pipeline_context,
        config=GlueSourceConfig(
            aws_region="us-west-2",
            extract_transforms=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            extract_column_parameters=True,
        ),
    )


def _make_table_with_column_params() -> Dict[str, Any]:
    return {
        "Name": "test_table",
        "DatabaseName": "test_db",
        "StorageDescriptor": {
            "Columns": [
                {
                    "Name": "col_a",
                    "Type": "string",
                    "Parameters": {
                        "iceberg.field.id": "1",
                        "iceberg.field.optional": "true",
                    },
                },
                {
                    "Name": "col_b",
                    "Type": "int",
                    "Parameters": {"iceberg.field.id": "2"},
                },
                {
                    "Name": "col_no_params",
                    "Type": "boolean",
                },
            ]
        },
        "PartitionKeys": [
            {
                "Name": "dt",
                "Type": "string",
                "Parameters": {"iceberg.field.id": "3"},
            }
        ],
    }


def test_column_param_property_urn_sanitizes_special_chars() -> None:
    assert GlueSource._column_param_property_urn("iceberg.field.id") == (
        "urn:li:structuredProperty:io.datahubproject.glue.column.iceberg.field.id"
    )
    assert GlueSource._column_param_property_urn("some-key with spaces!") == (
        "urn:li:structuredProperty:io.datahubproject.glue.column.some_key_with_spaces_"
    )


def test_get_column_param_workunits_emits_definitions_once() -> None:
    """Each unique key's StructuredPropertyDefinition should be emitted only once per run."""
    source = _make_glue_source_with_column_params()
    table = _make_table_with_column_params()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD)"

    workunits = list(source._get_column_param_workunits(table, dataset_urn))

    definition_urns = [
        wu.get_urn()
        for wu in workunits
        if wu.get_aspect_of_type(models.StructuredPropertyDefinitionClass) is not None
    ]
    # iceberg.field.id appears on col_a, col_b, and dt — definition should be emitted once
    assert (
        definition_urns.count(
            "urn:li:structuredProperty:io.datahubproject.glue.column.iceberg.field.id"
        )
        == 1
    )
    # iceberg.field.optional only appears on col_a
    assert (
        definition_urns.count(
            "urn:li:structuredProperty:io.datahubproject.glue.column.iceberg.field.optional"
        )
        == 1
    )


def test_get_column_param_workunits_skips_columns_without_params() -> None:
    """Columns with no Parameters should produce no work units."""
    source = _make_glue_source_with_column_params()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD)"
    table = {
        "Name": "t",
        "DatabaseName": "db",
        "StorageDescriptor": {
            "Columns": [{"Name": "col_no_params", "Type": "boolean"}]
        },
        "PartitionKeys": [],
    }

    workunits = list(source._get_column_param_workunits(table, dataset_urn))

    assert workunits == []


def test_get_column_param_workunits_values_assigned_correctly() -> None:
    """StructuredProperties aspect on each field should carry the correct values."""
    source = _make_glue_source_with_column_params()
    table = _make_table_with_column_params()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD)"

    workunits = list(source._get_column_param_workunits(table, dataset_urn))

    # Find the StructuredProperties workunit for col_a (v2 typed field path)
    col_a_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD),[version=2.0].[type=string].col_a)"
    col_a_props_wu = next(
        (
            wu
            for wu in workunits
            if wu.get_urn() == col_a_urn
            and wu.get_aspect_of_type(models.StructuredPropertiesClass) is not None
        ),
        None,
    )
    assert col_a_props_wu is not None
    aspect = col_a_props_wu.get_aspect_of_type(models.StructuredPropertiesClass)
    assert aspect is not None
    assigned_urns = {a.propertyUrn for a in aspect.properties}
    assert (
        "urn:li:structuredProperty:io.datahubproject.glue.column.iceberg.field.id"
        in assigned_urns
    )
    assert (
        "urn:li:structuredProperty:io.datahubproject.glue.column.iceberg.field.optional"
        in assigned_urns
    )
    id_assignment = next(
        a
        for a in aspect.properties
        if a.propertyUrn
        == "urn:li:structuredProperty:io.datahubproject.glue.column.iceberg.field.id"
    )
    assert id_assignment.values == ["1"]


def test_seen_definitions_not_re_emitted_across_tables() -> None:
    """Once a definition has been emitted for a key, it must not appear again for a second table."""
    source = _make_glue_source_with_column_params()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD)"
    table = _make_table_with_column_params()

    first_run = list(source._get_column_param_workunits(table, dataset_urn))
    second_run = list(source._get_column_param_workunits(table, dataset_urn))

    first_defs = [
        wu
        for wu in first_run
        if wu.get_aspect_of_type(models.StructuredPropertyDefinitionClass) is not None
    ]
    second_defs = [
        wu
        for wu in second_run
        if wu.get_aspect_of_type(models.StructuredPropertyDefinitionClass) is not None
    ]
    assert len(first_defs) > 0
    assert second_defs == []


def test_get_column_param_workunits_uses_v2_field_path() -> None:
    """schemaField URNs must use the v2 typed path from get_schema_fields_for_hive_column."""
    source = _make_glue_source_with_column_params()
    table = _make_table_with_column_params()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD)"

    workunits = list(source._get_column_param_workunits(table, dataset_urn))

    assignment_urns = {
        wu.get_urn()
        for wu in workunits
        if wu.get_aspect_of_type(models.StructuredPropertiesClass) is not None
    }
    # v2 typed paths must be used
    assert (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD),[version=2.0].[type=string].col_a)"
        in assignment_urns
    )
    # bare column names must not appear as field paths
    assert (
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD),col_a)"
        not in assignment_urns
    )


def test_get_column_param_definitions_emitted_via_graph() -> None:
    """When ctx.graph is set, definitions are emitted synchronously via emit_mcp, not as workunits."""
    pipeline_context = PipelineContext(run_id="test-graph-emit")
    pipeline_context.graph = MagicMock(spec=DataHubGraph)
    source = GlueSource(
        ctx=pipeline_context,
        config=GlueSourceConfig(
            aws_region="us-west-2",
            extract_transforms=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            extract_column_parameters=True,
        ),
    )
    table = _make_table_with_column_params()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,test_db.test_table,PROD)"

    workunits = list(source._get_column_param_workunits(table, dataset_urn))

    # definitions go to graph.emit_mcp, not into the workunit stream
    assert pipeline_context.graph.emit_mcp.called
    definition_wus = [
        wu
        for wu in workunits
        if wu.get_aspect_of_type(models.StructuredPropertyDefinitionClass) is not None
    ]
    assert definition_wus == []
    # assignment workunits still flow through the pipeline normally
    assignment_wus = [
        wu
        for wu in workunits
        if wu.get_aspect_of_type(models.StructuredPropertiesClass) is not None
    ]
    assert len(assignment_wus) > 0


def _subtypes_and_view_props(
    workunits: List[Any],
) -> Tuple[Optional[List[str]], Optional[models.ViewPropertiesClass]]:
    sub_types: Optional[List[str]] = None
    view_props: Optional[models.ViewPropertiesClass] = None
    for wu in workunits:
        sub_type_aspect = wu.get_aspect_of_type(models.SubTypesClass)
        if sub_type_aspect is not None:
            sub_types = sub_type_aspect.typeNames
        view_props_aspect = wu.get_aspect_of_type(models.ViewPropertiesClass)
        if view_props_aspect is not None:
            view_props = view_props_aspect
    return sub_types, view_props


def test_glue_virtual_view_classified_as_view_with_presto_definition() -> None:
    # Athena/Presto views are stored in Glue as VIRTUAL_VIEW tables whose
    # ViewOriginalText is a base64-encoded JSON blob wrapped in a Presto marker.
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    original_sql = "SELECT id, name FROM my_db.my_schema.events"
    encoded = base64.b64encode(
        json.dumps({"originalSql": original_sql, "columns": []}).encode()
    ).decode()
    table = {
        "Name": "events_view",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "VIRTUAL_VIEW",
        "ViewOriginalText": f"/* Presto View: {encoded} */",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }

    sub_types, view_props = _subtypes_and_view_props(list(source._gen_table_wu(table)))

    assert sub_types == [DatasetSubTypes.VIEW]
    assert view_props is not None
    assert view_props.viewLogic == original_sql
    assert view_props.materialized is False
    assert view_props.viewLanguage == "SQL"


def test_glue_virtual_view_with_raw_sql_definition() -> None:
    # Spark/Hive views registered in the catalog keep the raw SQL in
    # ViewOriginalText without the Presto encoding.
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    raw_sql = "SELECT * FROM my_db.my_schema.orders"
    table = {
        "Name": "orders_view",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "VIRTUAL_VIEW",
        "ViewOriginalText": raw_sql,
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }

    sub_types, view_props = _subtypes_and_view_props(list(source._gen_table_wu(table)))

    assert sub_types == [DatasetSubTypes.VIEW]
    assert view_props is not None
    assert view_props.viewLogic == raw_sql


def test_glue_view_definition_falls_back_to_view_expanded_text() -> None:
    # When ViewOriginalText is absent, the raw SQL in ViewExpandedText is used.
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    raw_sql = "SELECT * FROM my_db.my_schema.orders"
    table = {
        "Name": "orders_view",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "VIRTUAL_VIEW",
        "ViewExpandedText": raw_sql,
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }

    sub_types, view_props = _subtypes_and_view_props(list(source._gen_table_wu(table)))

    assert sub_types == [DatasetSubTypes.VIEW]
    assert view_props is not None
    assert view_props.viewLogic == raw_sql


def test_glue_view_with_undecodable_presto_text_warns_and_drops_blob() -> None:
    # A view whose Presto marker wraps invalid base64 must NOT persist the encoded
    # blob as viewLogic; it falls back to empty and surfaces a warning.
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    table = {
        "Name": "broken_view",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "VIRTUAL_VIEW",
        "ViewOriginalText": "/* Presto View: not!valid!base64 */",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }

    sub_types, view_props = _subtypes_and_view_props(list(source._gen_table_wu(table)))

    assert sub_types == [DatasetSubTypes.VIEW]
    assert view_props is not None
    assert view_props.viewLogic == ""
    assert len(list(source.report.warnings)) >= 1


def test_glue_view_without_sql_definition_warns_and_emits_empty_logic() -> None:
    # A VIRTUAL_VIEW carrying neither ViewOriginalText nor ViewExpandedText is still
    # classified as a view, with empty viewLogic and a warning (not silent).
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    table = {
        "Name": "empty_view",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "VIRTUAL_VIEW",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }

    sub_types, view_props = _subtypes_and_view_props(list(source._gen_table_wu(table)))

    assert sub_types == [DatasetSubTypes.VIEW]
    assert view_props is not None
    assert view_props.viewLogic == ""
    assert len(list(source.report.warnings)) >= 1


def test_glue_view_lineage_links_view_to_upstream_table() -> None:
    # A VIRTUAL_VIEW whose SQL selects from a base table should yield an
    # upstreamLineage edge from the view to that table, parsed from the view SQL.
    source = glue_source(
        use_s3_bucket_tags=False,
        use_s3_object_tags=False,
        include_view_lineage=True,
    )
    assert source.aggregator is not None

    base_table = {
        "Name": "base_table",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
            ]
        },
    }
    original_sql = "SELECT id, name FROM base_table"
    encoded = base64.b64encode(
        json.dumps({"originalSql": original_sql, "columns": []}).encode()
    ).decode()
    view = {
        "Name": "events_view",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "VIRTUAL_VIEW",
        "ViewOriginalText": f"/* Presto View: {encoded} */",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
            ]
        },
    }

    # Drive both the base table (registers its schema) and the view (registers
    # schema + view definition), then flush the aggregator.
    list(source._gen_table_wu(base_table))
    list(source._gen_table_wu(view))
    mcps = list(source.aggregator.gen_metadata())

    view_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,my_db.events_view,PROD)"
    base_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,my_db.base_table,PROD)"
    upstream_lineages = [
        mcp.aspect
        for mcp in mcps
        if mcp.entityUrn == view_urn
        and isinstance(mcp.aspect, models.UpstreamLineageClass)
    ]
    assert upstream_lineages, "no upstreamLineage emitted for the view"
    lineage = upstream_lineages[0]
    upstream_urns = {u.dataset for u in lineage.upstreams}
    assert base_urn in upstream_urns

    # Schemas were registered (via _extract_record), so column-level lineage
    # should resolve the view's columns back to the base table's columns.
    assert lineage.fineGrainedLineages, "no column-level lineage emitted"
    column_edges = {
        (
            up.split(",")[-1].rstrip(")"),
            down.split(",")[-1].rstrip(")"),
        )
        for fgl in lineage.fineGrainedLineages
        for up in (fgl.upstreams or [])
        for down in (fgl.downstreams or [])
    }
    assert ("id", "id") in column_edges
    assert ("name", "name") in column_edges


def test_glue_view_lineage_disabled_creates_no_aggregator() -> None:
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    assert source.aggregator is None


def test_glue_view_definition_dialect_detection() -> None:
    # Presto/Athena views are Trino SQL; raw (Spark/Hive) views are parsed as Spark
    # (sqlglot's spark dialect is a superset of hive). The detected dialect drives
    # per-view SQL parsing for lineage.
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    encoded = base64.b64encode(
        json.dumps({"originalSql": "SELECT id FROM base_table", "columns": []}).encode()
    ).decode()
    presto = source._get_view_definition(
        {"ViewOriginalText": f"/* Presto View: {encoded} */"}, "my_db.presto_view"
    )
    raw = source._get_view_definition(
        {"ViewOriginalText": "SELECT id FROM base_table"}, "my_db.raw_view"
    )

    assert presto is not None and presto.dialect == "trino"
    assert raw is not None and raw.dialect == "spark"


def test_glue_external_table_remains_table_without_view_properties() -> None:
    source = glue_source(use_s3_bucket_tags=False, use_s3_object_tags=False)
    table = {
        "Name": "events",
        "DatabaseName": "my_db",
        "CatalogId": "123412341234",
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
    }

    sub_types, view_props = _subtypes_and_view_props(list(source._gen_table_wu(table)))

    assert sub_types == [DatasetSubTypes.TABLE]
    assert view_props is None
