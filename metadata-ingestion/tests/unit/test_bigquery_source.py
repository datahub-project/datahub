import json
import os
from datetime import datetime
from types import SimpleNamespace
from typing import Dict
from unittest.mock import patch

from google.cloud.bigquery.table import TableListItem

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryProject
from datahub.ingestion.source.bigquery_v2.lineage import LineageEdge


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


@patch("google.cloud.bigquery.client.Client")
def test_get_projects_with_project_ids(client_mock):
    config = BigQueryV2Config.parse_obj(
        {
            "project_ids": ["test-1", "test-2"],
        }
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))
    assert source._get_projects(client_mock) == [
        BigqueryProject("test-1", "test-1"),
        BigqueryProject("test-2", "test-2"),
    ]
    assert client_mock.list_projects.call_count == 0

    config = BigQueryV2Config.parse_obj(
        {"project_ids": ["test-1", "test-2"], "project_id": "test-3"}
    )
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test2"))
    assert source._get_projects(client_mock) == [
        BigqueryProject("test-1", "test-1"),
        BigqueryProject("test-2", "test-2"),
    ]
    assert client_mock.list_projects.call_count == 0


@patch("google.cloud.bigquery.client.Client")
def test_get_projects_with_single_project_id(client_mock):
    config = BigQueryV2Config.parse_obj({"project_id": "test-3"})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))
    assert source._get_projects(client_mock) == [
        BigqueryProject("test-3", "test-3"),
    ]
    assert client_mock.list_projects.call_count == 0


@patch("google.cloud.bigquery.client.Client")
def test_get_projects(client_mock):
    client_mock.list_projects.return_value = [
        SimpleNamespace(
            project_id="test-1",
            friendly_name="one",
        ),
        SimpleNamespace(
            project_id="test-2",
            friendly_name="two",
        ),
    ]

    config = BigQueryV2Config.parse_obj({})
    source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test1"))
    assert source._get_projects(client_mock) == [
        BigqueryProject("test-1", "one"),
        BigqueryProject("test-2", "two"),
    ]
    assert client_mock.list_projects.call_count == 1


def test_simple_upstream_table_generation():
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
    lineage_metadata = {str(a): {LineageEdge(table=str(b), auditStamp=datetime.now())}}
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata, [])

    assert len(upstreams) == 1
    assert list(upstreams)[0].table == str(b)


def test_upstream_table_generation_with_temporary_table_without_temp_upstream():
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

    lineage_metadata = {str(a): {LineageEdge(table=str(b), auditStamp=datetime.now())}}
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata, [])
    assert list(upstreams) == []


def test_upstream_table_generation_with_temporary_table_with_temp_upstream():
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
        str(a): {LineageEdge(table=str(b), auditStamp=datetime.now())},
        str(b): {LineageEdge(table=str(c), auditStamp=datetime.now())},
    }
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata, [])
    assert len(upstreams) == 1
    assert list(upstreams)[0].table == str(c)


def test_upstream_table_generation_with_temporary_table_with_multiple_temp_upstream():
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
        str(a): {LineageEdge(table=str(b), auditStamp=datetime.now())},
        str(b): {
            LineageEdge(table=str(c), auditStamp=datetime.now()),
            LineageEdge(table=str(d), auditStamp=datetime.now()),
        },
        str(d): {LineageEdge(table=str(e), auditStamp=datetime.now())},
    }
    upstreams = source.lineage_extractor.get_upstream_tables(a, lineage_metadata, [])
    sorted_list = list(upstreams)
    sorted_list.sort()
    assert sorted_list[0].table == str(c)
    assert sorted_list[1].table == str(e)


@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQueryDataDictionary.get_tables_for_dataset"
)
@patch("google.cloud.bigquery.client.Client")
def test_table_processing_logic(client_mock, data_dictionary_mock):
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

    _ = source.get_tables_for_dataset(
        conn=client_mock, project_id="test-project", dataset_name="test-dataset"
    )

    assert data_dictionary_mock.call_count == 1

    # args only available from python 3.8 and that's why call_args_list is sooo ugly
    tables: Dict[str, TableListItem] = data_dictionary_mock.call_args_list[0][0][
        3
    ]  # alternatively
    for table in tables.keys():
        assert table in ["test-table", "test-sharded-table_20220102"]


@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_schema.BigQueryDataDictionary.get_tables_for_dataset"
)
@patch("google.cloud.bigquery.client.Client")
def test_table_processing_logic_date_named_tables(client_mock, data_dictionary_mock):
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

    _ = source.get_tables_for_dataset(
        conn=client_mock, project_id="test-project", dataset_name="test-dataset"
    )

    assert data_dictionary_mock.call_count == 1

    # args only available from python 3.8 and that's why call_args_list is sooo ugly
    tables: Dict[str, TableListItem] = data_dictionary_mock.call_args_list[0][0][
        3
    ]  # alternatively
    for table in tables.keys():
        assert tables[table].table_id in ["test-table", "20220103"]
