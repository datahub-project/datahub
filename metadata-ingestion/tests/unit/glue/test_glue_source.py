import json
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Type, cast
from unittest.mock import patch

import pydantic
import pytest
from botocore.stub import Stubber
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
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
    _sanitize_jdbc_url,
)
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
    get_databases_response_with_resource_link,
    get_dataflow_graph_response_1,
    get_dataflow_graph_response_2,
    get_delta_tables_response_1,
    get_delta_tables_response_2,
    get_jobs_response,
    get_jobs_response_empty,
    get_object_body_1,
    get_object_body_2,
    get_object_response_1,
    get_object_response_2,
    get_object_tagging,
    get_tables_lineage_response_1,
    get_tables_response_1,
    get_tables_response_2,
    get_tables_response_for_target_database,
    get_tables_response_profiling_1,
    resource_link_database,
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
@freeze_time(FROZEN_TIME)
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


def test_platform_config():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2", platform="athena"),
    )
    assert source.platform == "athena"


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


@freeze_time(FROZEN_TIME)
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
@freeze_time(FROZEN_TIME)
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
@freeze_time(FROZEN_TIME)
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


@freeze_time(FROZEN_TIME)
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
@freeze_time(FROZEN_TIME)
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

    new_dataset_ids: List[str] = []
    new_dataset_mces: List[Any] = []
    s3_formats: DefaultDict[str, Set[Any]] = defaultdict(set)

    result = source.process_dataflow_node(
        node, flow_urn, new_dataset_ids, new_dataset_mces, s3_formats
    )

    assert result is not None
    assert result["urn"] == expected_urn
    assert new_dataset_mces == []


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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

    assert result is None
    assert source.report.warnings


def test_process_dataflow_node_glue_connection_fetch_failure() -> None:
    source = glue_source()

    def _raise(**kw: Any) -> None:
        raise Exception("Connection not found")

    source.glue_client.get_connection = _raise  # type: ignore[method-assign]
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    node = _make_glue_connection_node("DataSource0", "DataSource", "Missing", "mytable")

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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

    result = source.process_dataflow_node(node, flow_urn, [], [], defaultdict(set))

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
