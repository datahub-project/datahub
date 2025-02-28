import json
import logging
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigquery.table import Row, TableListItem

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    _BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX,
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryConnectionConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryDataset,
    BigqueryProject,
    BigQuerySchemaApi,
    BigqueryTable,
    BigqueryTableSnapshot,
    BigqueryView,
    get_projects,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.ingestion.source.bigquery_v2.lineage import (
    LineageEdge,
    LineageEdgeColumnMapping,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataset import ViewProperties
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    TimeStampClass,
)


def test_bigquery_uri():
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    assert config.get_sql_alchemy_url() == "bigquery://"


def test_bigquery_uri_on_behalf():
    config = BigQueryV2Config.parse_obj(
        {"project_id": "test-project", "project_on_behalf": "test-project-on-behalf"}
    )
    assert config.get_sql_alchemy_url() == "bigquery://test-project-on-behalf"


def test_bigquery_dataset_pattern():
    config = BigQueryV2Config.parse_obj(
        {
            "dataset_pattern": {
                "allow": [
                    "test-dataset",
                    "test-project.test-dataset",
                    ".*test-dataset",
                ],
                "deny": [
                    "^test-dataset-2$",
                    "project\\.second_dataset",
                ],
            },
        }
    )
    assert config.dataset_pattern.allow == [
        r".*\.test-dataset",
        r"test-project.test-dataset",
        r".*test-dataset",
    ]
    assert config.dataset_pattern.deny == [
        r"^.*\.test-dataset-2$",
        r"project\.second_dataset",
    ]

    config = BigQueryV2Config.parse_obj(
        {
            "dataset_pattern": {
                "allow": [
                    "test-dataset",
                    "test-project.test-dataset",
                    ".*test-dataset",
                ],
                "deny": [
                    "^test-dataset-2$",
                    "project\\.second_dataset",
                ],
            },
            "match_fully_qualified_names": False,
        }
    )
    assert config.dataset_pattern.allow == [
        r"test-dataset",
        r"test-project.test-dataset",
        r".*test-dataset",
    ]
    assert config.dataset_pattern.deny == [
        r"^test-dataset-2$",
        r"project\.second_dataset",
    ]


def test_bigquery_uri_with_credential():
    expected_credential_json = {
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "client_email": "test@acryl.io",
        "client_id": "test_client-id",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test@acryl.io",
        "private_key": "random_private_key",
        "private_key_id": "test-private-key",
        "project_id": "test-project",
        "token_uri": "https://oauth2.googleapis.com/token",
        "type": "service_account",
    }

    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
            "credential": {
                "project_id": "test-project",
                "private_key_id": "test-private-key",
                "private_key": "random_private_key",
                "client_email": "test@acryl.io",
                "client_id": "test_client-id",
            },
        }
    )

    try:
        assert config.get_sql_alchemy_url() == "bigquery://"
        assert config._credentials_path

        with open(config._credentials_path) as jsonFile:
            json_credential = json.load(jsonFile)
            jsonFile.close()

        credential = json.dumps(json_credential, sort_keys=True)
        expected_credential = json.dumps(expected_credential_json, sort_keys=True)
        assert expected_credential == credential

    except AssertionError as e:
        if config._credentials_path:
            os.unlink(str(config._credentials_path))
        raise e


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_with_project_ids(
    get_projects_client,
    get_bq_client_mock,
):
    client_mock = MagicMock()
    get_bq_client_mock.return_value = client_mock
    config = BigQueryV2Config.parse_obj(
        {
            "project_ids": ["test-1", "test-2"],
        }
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))
    assert get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    ) == [
        BigqueryProject("test-1", "test-1"),
        BigqueryProject("test-2", "test-2"),
    ]
    assert client_mock.list_projects.call_count == 0

    config = BigQueryV2Config.parse_obj(
        {"project_ids": ["test-1", "test-2"], "project_id": "test-3"}
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test2"))
    assert get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    ) == [
        BigqueryProject("test-1", "test-1"),
        BigqueryProject("test-2", "test-2"),
    ]
    assert client_mock.list_projects.call_count == 0


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_with_project_ids_overrides_project_id_pattern(
    get_projects_client,
    get_bigquery_client,
):
    config = BigQueryV2Config.parse_obj(
        {
            "project_ids": ["test-project", "test-project-2"],
            "project_id_pattern": {"deny": ["^test-project$"]},
        }
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    projects = get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    )
    assert projects == [
        BigqueryProject(id="test-project", name="test-project"),
        BigqueryProject(id="test-project-2", name="test-project-2"),
    ]


def test_platform_instance_config_always_none():
    config = BigQueryV2Config.parse_obj(
        {"include_data_platform_instance": True, "platform_instance": "something"}
    )
    assert config.platform_instance is None

    config = BigQueryV2Config.parse_obj(
        dict(platform_instance="something", project_id="project_id")
    )
    assert config.project_ids == ["project_id"]
    assert config.platform_instance is None


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_dataplatform_instance_aspect_returns_project_id(
    get_projects_client,
    get_bq_client_mock,
):
    project_id = "project_id"
    expected_instance = (
        f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:bigquery,{project_id})"
    )

    config = BigQueryV2Config.parse_obj({"include_data_platform_instance": True})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    schema_gen = source.bq_schema_extractor

    data_platform_instance = schema_gen.get_dataplatform_instance_aspect(
        "urn:li:test", project_id
    )
    metadata = data_platform_instance.metadata

    assert isinstance(metadata, MetadataChangeProposalWrapper)
    assert isinstance(metadata.aspect, DataPlatformInstanceClass)
    assert metadata.aspect.instance == expected_instance


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_dataplatform_instance_default_no_instance(
    get_projects_client,
    get_bq_client_mock,
):
    config = BigQueryV2Config.parse_obj({})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    schema_gen = source.bq_schema_extractor

    data_platform_instance = schema_gen.get_dataplatform_instance_aspect(
        "urn:li:test", "project_id"
    )
    metadata = data_platform_instance.metadata

    assert isinstance(metadata, MetadataChangeProposalWrapper)
    assert isinstance(metadata.aspect, DataPlatformInstanceClass)
    assert metadata.aspect.instance is None


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_with_single_project_id(
    get_projects_client,
    get_bq_client_mock,
):
    client_mock = MagicMock()
    get_bq_client_mock.return_value = client_mock
    config = BigQueryV2Config.parse_obj({"project_id": "test-3"})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))
    assert get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    ) == [
        BigqueryProject("test-3", "test-3"),
    ]
    assert client_mock.list_projects.call_count == 0


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_by_list(get_projects_client, get_bigquery_client):
    client_mock = MagicMock()
    get_bigquery_client.return_value = client_mock

    first_page = MagicMock()
    first_page.__iter__.return_value = iter(
        [
            SimpleNamespace(project_id="test-1", friendly_name="one"),
            SimpleNamespace(project_id="test-2", friendly_name="two"),
        ]
    )
    first_page.next_page_token = "token1"

    second_page = MagicMock()
    second_page.__iter__.return_value = iter(
        [
            SimpleNamespace(project_id="test-3", friendly_name="three"),
            SimpleNamespace(project_id="test-4", friendly_name="four"),
        ]
    )
    second_page.next_page_token = None

    client_mock.list_projects.side_effect = [first_page, second_page]

    config = BigQueryV2Config.parse_obj({})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))
    assert get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    ) == [
        BigqueryProject("test-1", "one"),
        BigqueryProject("test-2", "two"),
        BigqueryProject("test-3", "three"),
        BigqueryProject("test-4", "four"),
    ]
    assert client_mock.list_projects.call_count == 2


@patch.object(BigQuerySchemaApi, "get_projects")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_filter_by_pattern(
    get_projects_client, get_bq_client_mock, get_projects_mock
):
    get_projects_mock.return_value = [
        BigqueryProject("test-project", "Test Project"),
        BigqueryProject("test-project-2", "Test Project 2"),
    ]

    config = BigQueryV2Config.parse_obj(
        {"project_id_pattern": {"deny": ["^test-project$"]}}
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    projects = get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    )
    assert projects == [
        BigqueryProject(id="test-project-2", name="Test Project 2"),
    ]


@patch.object(BigQuerySchemaApi, "get_projects")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_list_empty(
    get_projects_client, get_bq_client_mock, get_projects_mock
):
    get_projects_mock.return_value = []

    config = BigQueryV2Config.parse_obj(
        {"project_id_pattern": {"deny": ["^test-project$"]}}
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    projects = get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    )
    assert len(source.report.failures) == 1
    assert projects == []


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_list_failure(
    get_projects_client: MagicMock,
    get_bq_client_mock: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    error_str = "my error"
    bq_client_mock = MagicMock()
    get_bq_client_mock.return_value = bq_client_mock
    bq_client_mock.list_projects.side_effect = GoogleAPICallError(error_str)

    config = BigQueryV2Config.parse_obj(
        {"project_id_pattern": {"deny": ["^test-project$"]}}
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        projects = get_projects(
            source.bq_schema_extractor.schema_api,
            source.report,
            source.filters,
        )
        assert len(caplog.records) == 2
        assert error_str in caplog.records[0].msg
    assert len(source.report.failures) == 1
    assert projects == []


@patch.object(BigQuerySchemaApi, "get_projects")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_list_fully_filtered(
    get_projects_mock, get_bq_client_mock, get_projects_client
):
    get_projects_mock.return_value = [BigqueryProject("test-project", "Test Project")]

    config = BigQueryV2Config.parse_obj(
        {"project_id_pattern": {"deny": ["^test-project$"]}}
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    projects = get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    )
    assert len(source.report.failures) == 0
    assert projects == []


@pytest.fixture
def bigquery_table() -> BigqueryTable:
    now = datetime.now(tz=timezone.utc)
    return BigqueryTable(
        name="table1",
        comment="comment1",
        created=now,
        last_altered=now,
        size_in_bytes=2400,
        rows_count=2,
        expires=now - timedelta(days=10),
        labels={"data_producer_owner_email": "games_team-nytimes_com"},
        num_partitions=1,
        max_partition_id="1",
        max_shard_id="1",
        active_billable_bytes=2400,
        long_term_billable_bytes=2400,
    )


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_gen_table_dataset_workunits(
    get_projects_client, get_bq_client_mock, bigquery_table
):
    project_id = "test-project"
    dataset_name = "test-dataset"
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": project_id,
            "capture_table_label_as_tag": True,
        }
    )
    source: BigqueryV2Source = BigqueryV2Source(
        config=config, ctx=PipelineContext(run_id="test")
    )
    schema_gen = source.bq_schema_extractor

    gen = schema_gen.gen_table_dataset_workunits(
        bigquery_table, [], project_id, dataset_name
    )
    mcps = list(gen)

    # Helper function to find MCP by aspect type
    def find_mcp_by_aspect(aspect_type):
        return next(
            mcp  # type: ignore
            for mcp in mcps
            if isinstance(mcp.metadata.aspect, aspect_type)  # type: ignore
        )

    # Assert StatusClass
    status_mcp = find_mcp_by_aspect(StatusClass)
    assert status_mcp.metadata.aspect.removed is False

    # Assert SchemaMetadataClass
    schema_mcp = find_mcp_by_aspect(SchemaMetadataClass)
    assert (
        schema_mcp.metadata.aspect.schemaName
        == f"{project_id}.{dataset_name}.{bigquery_table.name}"
    )
    assert schema_mcp.metadata.aspect.fields == []

    # Assert DatasetPropertiesClass
    dataset_props_mcp = find_mcp_by_aspect(DatasetPropertiesClass)
    assert dataset_props_mcp.metadata.aspect.name == bigquery_table.name
    assert (
        dataset_props_mcp.metadata.aspect.qualifiedName
        == f"{project_id}.{dataset_name}.{bigquery_table.name}"
    )
    assert dataset_props_mcp.metadata.aspect.description == bigquery_table.comment
    assert dataset_props_mcp.metadata.aspect.created == TimeStampClass(
        time=int(bigquery_table.created.timestamp() * 1000)
    )
    assert dataset_props_mcp.metadata.aspect.lastModified == TimeStampClass(
        time=int(bigquery_table.last_altered.timestamp() * 1000)
    )
    assert dataset_props_mcp.metadata.aspect.tags == []

    expected_custom_properties = {
        "expiration_date": str(bigquery_table.expires),
        "size_in_bytes": str(bigquery_table.size_in_bytes),
        "billable_bytes_active": str(bigquery_table.active_billable_bytes),
        "billable_bytes_long_term": str(bigquery_table.long_term_billable_bytes),
        "number_of_partitions": str(bigquery_table.num_partitions),
        "max_partition_id": str(bigquery_table.max_partition_id),
        "is_partitioned": "True",
        "max_shard_id": str(bigquery_table.max_shard_id),
        "is_sharded": "True",
    }
    assert (
        dataset_props_mcp.metadata.aspect.customProperties == expected_custom_properties
    )

    # Assert GlobalTagsClass
    global_tags_mcp = find_mcp_by_aspect(GlobalTagsClass)
    assert global_tags_mcp.metadata.aspect.tags == [
        TagAssociationClass(
            "urn:li:tag:data_producer_owner_email:games_team-nytimes_com"
        )
    ]

    # Assert ContainerClass
    container_mcp = find_mcp_by_aspect(ContainerClass)
    assert container_mcp is not None

    # Assert DataPlatformInstanceClass
    data_platform_instance_mcp = find_mcp_by_aspect(DataPlatformInstanceClass)
    assert data_platform_instance_mcp is not None

    # Assert SubTypesClass
    sub_types_mcp = find_mcp_by_aspect(SubTypesClass)
    assert sub_types_mcp.metadata.aspect.typeNames[1] == DatasetSubTypes.TABLE

    # Ensure all MCPs were checked
    # TODO: Test for PlatformResource MCPs as well
    assert len(mcps) >= 7


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_simple_upstream_table_generation(get_bq_client_mock, get_projects_client):
    a: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="a"
        )
    )
    b: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="b"
        )
    )

    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    lineage_metadata = {
        str(a): {
            LineageEdge(
                table=str(b), auditStamp=datetime.now(), column_mapping=frozenset()
            )
        }
    }
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata)

    assert len(upstreams) == 1
    assert list(upstreams)[0].table == str(b)


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_upstream_table_generation_with_temporary_table_without_temp_upstream(
    get_bq_client_mock,
    get_projects_client,
):
    a: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="a"
        )
    )
    b: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="_temp-dataset", table="b"
        )
    )

    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))

    lineage_metadata = {
        str(a): {
            LineageEdge(
                table=str(b), auditStamp=datetime.now(), column_mapping=frozenset()
            )
        }
    }
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata)
    assert list(upstreams) == []


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_upstream_table_column_lineage_with_temp_table(
    get_bq_client_mock, get_projects_client
):
    from datahub.ingestion.api.common import PipelineContext

    a: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="a"
        )
    )
    b: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="_temp-dataset", table="b"
        )
    )
    c: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="c"
        )
    )

    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )

    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    lineage_metadata = {
        str(a): {
            LineageEdge(
                table=str(b),
                auditStamp=datetime.now(),
                column_mapping=frozenset(
                    [
                        LineageEdgeColumnMapping(
                            "a_col1", in_columns=frozenset(["b_col2", "b_col3"])
                        )
                    ]
                ),
                column_confidence=0.8,
            )
        },
        str(b): {
            LineageEdge(
                table=str(c),
                auditStamp=datetime.now(),
                column_mapping=frozenset(
                    [
                        LineageEdgeColumnMapping(
                            "b_col2", in_columns=frozenset(["c_col1", "c_col2"])
                        ),
                        LineageEdgeColumnMapping(
                            "b_col3", in_columns=frozenset(["c_col2", "c_col3"])
                        ),
                    ]
                ),
                column_confidence=0.7,
            )
        },
    }
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata)
    assert len(upstreams) == 1

    upstream = list(upstreams)[0]
    assert upstream.table == str(c)
    assert upstream.column_mapping == frozenset(
        [
            LineageEdgeColumnMapping(
                "a_col1", in_columns=frozenset(["c_col1", "c_col2", "c_col3"])
            )
        ]
    )
    assert upstream.column_confidence == 0.7


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_upstream_table_generation_with_temporary_table_with_multiple_temp_upstream(
    get_bq_client_mock, get_projects_client
):
    a: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="a"
        )
    )
    b: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="_temp-dataset", table="b"
        )
    )
    c: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="c"
        )
    )
    d: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="_test-dataset", table="d"
        )
    )
    e: BigQueryTableRef = BigQueryTableRef(
        BigqueryTableIdentifier(
            project_id="test-project", dataset="test-dataset", table="e"
        )
    )

    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    lineage_metadata = {
        str(a): {
            LineageEdge(
                table=str(b), auditStamp=datetime.now(), column_mapping=frozenset()
            )
        },
        str(b): {
            LineageEdge(
                table=str(c), auditStamp=datetime.now(), column_mapping=frozenset()
            ),
            LineageEdge(
                table=str(d), auditStamp=datetime.now(), column_mapping=frozenset()
            ),
        },
        str(d): {
            LineageEdge(
                table=str(e), auditStamp=datetime.now(), column_mapping=frozenset()
            )
        },
    }
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata)
    sorted_list = list(upstreams)
    sorted_list.sort()
    assert sorted_list[0].table == str(c)
    assert sorted_list[1].table == str(e)


@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_table_processing_logic(
    get_projects_client, get_bq_client_mock, data_dictionary_mock
):
    client_mock = MagicMock()
    get_bq_client_mock.return_value = client_mock
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )

    tableListItems = [
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "test-table",
                }
            }
        ),
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "test-sharded-table_20220102",
                }
            }
        ),
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "test-sharded-table_20210101",
                }
            }
        ),
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "test-sharded-table_20220101",
                }
            }
        ),
    ]

    client_mock.list_tables.return_value = tableListItems
    data_dictionary_mock.get_tables_for_dataset.return_value = None

    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    schema_gen = source.bq_schema_extractor

    _ = list(
        schema_gen.get_tables_for_dataset(
            project_id="test-project", dataset=BigqueryDataset("test-dataset")
        )
    )

    assert data_dictionary_mock.call_count == 1

    # args only available from python 3.8 and that's why call_args_list is sooo ugly
    tables: Dict[str, TableListItem] = data_dictionary_mock.call_args_list[0][0][
        2
    ]  # alternatively
    for table in tables.keys():
        assert table in ["test-table", "test-sharded-table_20220102"]


@patch.object(BigQuerySchemaApi, "get_tables_for_dataset")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_table_processing_logic_date_named_tables(
    get_projects_client, get_bq_client_mock, data_dictionary_mock
):
    client_mock = MagicMock()
    get_bq_client_mock.return_value = client_mock
    # test that tables with date names are processed correctly
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project",
        }
    )

    tableListItems = [
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "test-table",
                }
            }
        ),
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "20220102",
                }
            }
        ),
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "20210101",
                }
            }
        ),
        TableListItem(
            {
                "tableReference": {
                    "projectId": "test-project",
                    "datasetId": "test-dataset",
                    "tableId": "20220103",
                }
            }
        ),
    ]

    client_mock.list_tables.return_value = tableListItems
    data_dictionary_mock.get_tables_for_dataset.return_value = None

    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    schema_gen = source.bq_schema_extractor

    _ = list(
        schema_gen.get_tables_for_dataset(
            project_id="test-project", dataset=BigqueryDataset("test-dataset")
        )
    )

    assert data_dictionary_mock.call_count == 1

    # args only available from python 3.8 and that's why call_args_list is sooo ugly
    tables: Dict[str, TableListItem] = data_dictionary_mock.call_args_list[0][0][
        2
    ]  # alternatively
    for table in tables.keys():
        assert tables[table].table_id in ["test-table", "20220103"]


def create_row(d: Dict[str, Any]) -> Row:
    values = []
    field_to_index = {}
    for i, (k, v) in enumerate(d.items()):
        field_to_index[k] = i
        values.append(v)
    return Row(tuple(values), field_to_index)


@pytest.fixture
def bigquery_view_1() -> BigqueryView:
    now = datetime.now(tz=timezone.utc)
    return BigqueryView(
        name="table1",
        created=now - timedelta(days=10),
        last_altered=now - timedelta(hours=1),
        comment="comment1",
        view_definition="CREATE VIEW 1",
        materialized=False,
        labels=None,
    )


@pytest.fixture
def bigquery_view_2() -> BigqueryView:
    now = datetime.now(tz=timezone.utc)
    return BigqueryView(
        name="table2",
        created=now,
        last_altered=None,
        comment="comment2",
        view_definition="CREATE VIEW 2",
        materialized=True,
        labels=None,
    )


@patch.object(BigQuerySchemaApi, "get_query_result")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_views_for_dataset(
    get_bq_client_mock: Mock,
    get_projects_client: MagicMock,
    query_mock: Mock,
    bigquery_view_1: BigqueryView,
    bigquery_view_2: BigqueryView,
) -> None:
    client_mock = MagicMock()
    get_bq_client_mock.return_value = client_mock
    assert bigquery_view_1.last_altered
    row1 = create_row(
        dict(
            table_name=bigquery_view_1.name,
            created=bigquery_view_1.created,
            last_altered=bigquery_view_1.last_altered.timestamp() * 1000,
            comment=bigquery_view_1.comment,
            view_definition=bigquery_view_1.view_definition,
            table_type="VIEW",
        )
    )
    row2 = create_row(  # Materialized view, no last_altered
        dict(
            table_name=bigquery_view_2.name,
            created=bigquery_view_2.created,
            comment=bigquery_view_2.comment,
            view_definition=bigquery_view_2.view_definition,
            table_type="MATERIALIZED VIEW",
        )
    )
    query_mock.return_value = [row1, row2]
    bigquery_data_dictionary = BigQuerySchemaApi(
        report=BigQueryV2Report().schema_api_perf,
        client=client_mock,
        projects_client=MagicMock(),
    )

    views = bigquery_data_dictionary.get_views_for_dataset(
        project_id="test-project",
        dataset_name="test-dataset",
        has_data_read=False,
        report=BigQueryV2Report(),
    )
    assert list(views) == [bigquery_view_1, bigquery_view_2]


@patch.object(
    BigQuerySchemaGenerator, "gen_dataset_workunits", lambda *args, **kwargs: []
)
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_gen_view_dataset_workunits(
    get_projects_client, get_bq_client_mock, bigquery_view_1, bigquery_view_2
):
    project_id = "test-project"
    dataset_name = "test-dataset"
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": project_id,
        }
    )
    source: BigqueryV2Source = BigqueryV2Source(
        config=config, ctx=PipelineContext(run_id="test")
    )
    schema_gen = source.bq_schema_extractor

    gen = schema_gen.gen_view_dataset_workunits(
        bigquery_view_1, [], project_id, dataset_name
    )
    mcp = cast(MetadataChangeProposalClass, next(iter(gen)).metadata)
    assert mcp.aspect == ViewProperties(
        materialized=bigquery_view_1.materialized,
        viewLanguage="SQL",
        viewLogic=bigquery_view_1.view_definition,
    )

    gen = schema_gen.gen_view_dataset_workunits(
        bigquery_view_2, [], project_id, dataset_name
    )
    mcp = cast(MetadataChangeProposalClass, next(iter(gen)).metadata)
    assert mcp.aspect == ViewProperties(
        materialized=bigquery_view_2.materialized,
        viewLanguage="SQL",
        viewLogic=bigquery_view_2.view_definition,
    )


@pytest.fixture
def bigquery_snapshot() -> BigqueryTableSnapshot:
    now = datetime.now(tz=timezone.utc)
    return BigqueryTableSnapshot(
        name="table-snapshot",
        created=now - timedelta(days=10),
        last_altered=now - timedelta(hours=1),
        comment="comment1",
        ddl="CREATE SNAPSHOT TABLE 1",
        size_in_bytes=None,
        rows_count=None,
        snapshot_time=now - timedelta(days=10),
        base_table_identifier=BigqueryTableIdentifier(
            project_id="test-project",
            dataset="test-dataset",
            table="test-table",
        ),
    )


@patch.object(BigQuerySchemaApi, "get_query_result")
@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_snapshots_for_dataset(
    get_projects_client: MagicMock,
    get_bq_client_mock: Mock,
    query_mock: Mock,
    bigquery_snapshot: BigqueryTableSnapshot,
) -> None:
    client_mock = MagicMock()
    get_bq_client_mock.return_value = client_mock
    assert bigquery_snapshot.last_altered
    assert bigquery_snapshot.base_table_identifier
    row1 = create_row(
        dict(
            table_name=bigquery_snapshot.name,
            created=bigquery_snapshot.created,
            last_altered=bigquery_snapshot.last_altered.timestamp() * 1000,
            comment=bigquery_snapshot.comment,
            ddl=bigquery_snapshot.ddl,
            snapshot_time=bigquery_snapshot.snapshot_time,
            table_type="SNAPSHOT",
            base_table_catalog=bigquery_snapshot.base_table_identifier.project_id,
            base_table_schema=bigquery_snapshot.base_table_identifier.dataset,
            base_table_name=bigquery_snapshot.base_table_identifier.table,
        )
    )
    query_mock.return_value = [row1]
    bigquery_data_dictionary = BigQuerySchemaApi(
        report=BigQueryV2Report().schema_api_perf,
        client=client_mock,
        projects_client=MagicMock(),
    )

    snapshots = bigquery_data_dictionary.get_snapshots_for_dataset(
        project_id="test-project",
        dataset_name="test-dataset",
        has_data_read=False,
        report=BigQueryV2Report(),
    )
    assert list(snapshots) == [bigquery_snapshot]


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_gen_snapshot_dataset_workunits(
    get_bq_client_mock, get_projects_client, bigquery_snapshot
):
    project_id = "test-project"
    dataset_name = "test-dataset"
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": project_id,
        }
    )
    source: BigqueryV2Source = BigqueryV2Source(
        config=config, ctx=PipelineContext(run_id="test")
    )
    schema_gen = source.bq_schema_extractor

    gen = schema_gen.gen_snapshot_dataset_workunits(
        bigquery_snapshot, [], project_id, dataset_name
    )
    mcp = cast(MetadataChangeProposalWrapper, list(gen)[2].metadata)
    dataset_properties = cast(DatasetPropertiesClass, mcp.aspect)
    assert dataset_properties.customProperties["snapshot_ddl"] == bigquery_snapshot.ddl
    assert dataset_properties.customProperties["snapshot_time"] == str(
        bigquery_snapshot.snapshot_time
    )


@pytest.mark.parametrize(
    "table_name, expected_table_prefix, expected_shard",
    [
        # Cases with Fully qualified name as input
        ("project.dataset.table", "project.dataset.table", None),
        ("project.dataset.table_20231215", "project.dataset.table", "20231215"),
        ("project.dataset.table_2023", "project.dataset.table_2023", None),
        # incorrectly handled special case where dataset itself is a sharded table if full name is specified
        ("project.dataset.20231215", "project.dataset.20231215", "20231215"),
        ("project1.dataset2.20231215", "project1.dataset2.20231215", "20231215"),
        # Cases with Just the table name as input
        ("table", "table", None),
        ("table20231215", "table", "20231215"),
        ("table_20231215", "table", "20231215"),
        ("table2_20231215", "table2", "20231215"),
        ("table220231215", "table220231215", None),
        ("table_1624046611000_name", "table_1624046611000_name", None),
        ("table_1624046611000", "table_1624046611000", None),
        # Special case where dataset itself is a sharded table
        ("20231215", None, "20231215"),
    ],
)
def test_get_table_and_shard_default(
    table_name: str, expected_table_prefix: Optional[str], expected_shard: Optional[str]
) -> None:
    with patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_audit.BigqueryTableIdentifier._BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX",
        _BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX,
    ):
        assert BigqueryTableIdentifier.get_table_and_shard(table_name) == (
            expected_table_prefix,
            expected_shard,
        )


@pytest.mark.parametrize(
    "table_name, expected_table_prefix, expected_shard",
    [
        # Cases with Fully qualified name as input
        ("project.dataset.table", "project.dataset.table", None),
        ("project.dataset.table_20231215", "project.dataset.table", "20231215"),
        ("project.dataset.table_2023", "project.dataset.table", "2023"),
        # incorrectly handled special case where dataset itself is a sharded table if full name is specified
        ("project.dataset.20231215", "project.dataset.20231215", None),
        ("project.dataset.2023", "project.dataset.2023", None),
        # Cases with Just the table name as input
        ("table", "table", None),
        ("table_20231215", "table", "20231215"),
        ("table_2023", "table", "2023"),
        ("table_1624046611000_name", "table_1624046611000_name", None),
        ("table_1624046611000", "table_1624046611000", None),
        ("table_1624046611", "table", "1624046611"),
        # Special case where dataset itself is a sharded table
        ("20231215", None, "20231215"),
        ("2023", None, "2023"),
    ],
)
def test_get_table_and_shard_custom_shard_pattern(
    table_name: str, expected_table_prefix: Optional[str], expected_shard: Optional[str]
) -> None:
    with patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_audit.BigqueryTableIdentifier._BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX",
        "((.+)[_$])?(\\d{4,10})$",
    ):
        assert BigqueryTableIdentifier.get_table_and_shard(table_name) == (
            expected_table_prefix,
            expected_shard,
        )


@pytest.mark.parametrize(
    "full_table_name, datahub_full_table_name",
    [
        ("project.dataset.table", "project.dataset.table"),
        ("project.dataset.table_20231215", "project.dataset.table"),
        ("project.dataset.table@1624046611000", "project.dataset.table"),
        ("project.dataset.table@-9600", "project.dataset.table"),
        ("project.dataset.table@-3600000", "project.dataset.table"),
        ("project.dataset.table@-3600000--1800000", "project.dataset.table"),
        ("project.dataset.table@1624046611000-1612046611000", "project.dataset.table"),
        ("project.dataset.table@-3600000-", "project.dataset.table"),
        ("project.dataset.table@1624046611000-", "project.dataset.table"),
        (
            "project.dataset.table_1624046611000_name",
            "project.dataset.table_1624046611000_name",
        ),
        ("project.dataset.table_1624046611000", "project.dataset.table_1624046611000"),
        ("project.dataset.table20231215", "project.dataset.table"),
        ("project.dataset.table_*", "project.dataset.table"),
        ("project.dataset.table_2023*", "project.dataset.table"),
        ("project.dataset.table_202301*", "project.dataset.table"),
        # Special case where dataset itself is a sharded table
        ("project.dataset.20230112", "project.dataset.dataset"),
    ],
)
def test_get_table_name(full_table_name: str, datahub_full_table_name: str) -> None:
    with patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_audit.BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX",
        "",
    ):
        assert (
            BigqueryTableIdentifier.from_string_name(full_table_name).get_table_name()
            == datahub_full_table_name
        )


def test_default_config_for_excluding_projects_and_datasets():
    config = BigQueryV2Config.parse_obj({})
    assert config.exclude_empty_projects is False
    config = BigQueryV2Config.parse_obj({"exclude_empty_projects": True})
    assert config.exclude_empty_projects


@patch.object(BigQueryConnectionConfig, "get_bigquery_client", new=lambda self: None)
@patch.object(BigQuerySchemaApi, "get_datasets_for_project_id")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_excluding_empty_projects_from_ingestion(
    get_projects_client,
    get_datasets_for_project_id_mock,
):
    project_id_with_datasets = "project-id-with-datasets"
    project_id_without_datasets = "project-id-without-datasets"

    def get_datasets_for_project_id_side_effect(
        project_id: str,
    ) -> List[BigqueryDataset]:
        return (
            []
            if project_id == project_id_without_datasets
            else [BigqueryDataset("some-dataset")]
        )

    get_datasets_for_project_id_mock.side_effect = (
        get_datasets_for_project_id_side_effect
    )

    base_config = {
        "project_ids": [project_id_with_datasets, project_id_without_datasets],
        "schema_pattern": AllowDenyPattern(deny=[".*"]),
        "include_usage_statistics": False,
        "include_table_lineage": False,
    }

    config = BigQueryV2Config.parse_obj(base_config)
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test-1"))
    assert len({wu.metadata.entityUrn for wu in source.get_workunits()}) == 2  # type: ignore

    config = BigQueryV2Config.parse_obj({**base_config, "exclude_empty_projects": True})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test-2"))
    assert len({wu.metadata.entityUrn for wu in source.get_workunits()}) == 1  # type: ignore


def test_bigquery_config_deprecated_schema_pattern():
    base_config = {
        "include_usage_statistics": False,
        "include_table_lineage": False,
    }

    config = BigQueryV2Config.parse_obj(base_config)
    assert config.dataset_pattern == AllowDenyPattern(allow=[".*"])  # default

    config_with_schema_pattern = {
        **base_config,
        "schema_pattern": AllowDenyPattern(deny=[".*"]),
    }
    config = BigQueryV2Config.parse_obj(config_with_schema_pattern)
    assert config.dataset_pattern == AllowDenyPattern(deny=[".*"])  # schema_pattern

    config_with_dataset_pattern = {
        **base_config,
        "dataset_pattern": AllowDenyPattern(deny=["temp.*"]),
    }
    config = BigQueryV2Config.parse_obj(config_with_dataset_pattern)
    assert config.dataset_pattern == AllowDenyPattern(
        deny=["temp.*"]
    )  # dataset_pattern


@patch.object(BigQueryV2Config, "get_bigquery_client")
@patch.object(BigQueryV2Config, "get_projects_client")
def test_get_projects_with_project_labels(
    get_projects_client,
    get_bq_client_mock,
):
    client_mock = MagicMock()

    get_projects_client.return_value = client_mock

    client_mock.search_projects.return_value = [
        SimpleNamespace(project_id="dev", display_name="dev_project"),
        SimpleNamespace(project_id="qa", display_name="qa_project"),
    ]

    config = BigQueryV2Config.parse_obj(
        {
            "project_labels": ["environment:dev", "environment:qa"],
        }
    )

    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))

    assert get_projects(
        source.bq_schema_extractor.schema_api,
        source.report,
        source.filters,
    ) == [
        BigqueryProject("dev", "dev_project"),
        BigqueryProject("qa", "qa_project"),
    ]
