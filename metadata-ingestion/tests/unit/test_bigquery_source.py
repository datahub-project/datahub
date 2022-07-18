import json
import os
from datetime import datetime

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.bigquery import BigQueryConfig, BigQuerySource
from datahub.ingestion.source.usage.bigquery_usage import BigQueryTableRef


def test_bigquery_uri():
    config = BigQueryConfig.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    assert config.get_sql_alchemy_url() == "bigquery://test-project"


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

    config = BigQueryConfig.parse_obj(
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

        assert config.get_sql_alchemy_url() == "bigquery://test-project"
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


def test_simple_upstream_table_generation():
    a: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="a"
    )
    b: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="b"
    )

    config = BigQueryConfig.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    source.lineage_metadata = {str(a): set([str(b)])}
    upstreams = source.get_upstream_tables(str(a), [])
    assert list(upstreams) == [b]


def test_error_on_missing_config():
    with pytest.raises(ConfigurationError):
        BigQueryConfig.parse_obj(
            {
                "project_id": "test-project",
                "use_exported_bigquery_audit_metadata": True,
            }
        )


def test_upstream_table_generation_with_temporary_table_without_temp_upstream():
    a: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="a"
    )
    b: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="_temp-dataset", table="b"
    )

    config = BigQueryConfig.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    source.lineage_metadata = {str(a): set([str(b)])}
    upstreams = source.get_upstream_tables(str(a), [])
    assert list(upstreams) == []


def test_upstream_table_generation_with_temporary_table_with_temp_upstream():
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.sql.bigquery import BigQueryConfig, BigQuerySource
    from datahub.ingestion.source.usage.bigquery_usage import BigQueryTableRef

    a: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="a"
    )
    b: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="_temp-dataset", table="b"
    )
    c: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="c"
    )

    config = BigQueryConfig.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    source.lineage_metadata = {str(a): set([str(b)]), str(b): set([str(c)])}
    upstreams = source.get_upstream_tables(str(a), [])
    assert list(upstreams) == [c]


def test_upstream_table_generation_with_temporary_table_with_multiple_temp_upstream():
    a: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="a"
    )
    b: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="_temp-dataset", table="b"
    )
    c: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="c"
    )
    d: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="_test-dataset", table="d"
    )
    e: BigQueryTableRef = BigQueryTableRef(
        project="test-project", dataset="test-dataset", table="e"
    )

    config = BigQueryConfig.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    source.lineage_metadata = {
        str(a): set([str(b)]),
        str(b): set([str(c), str(d)]),
        str(d): set([str(e)]),
    }
    upstreams = source.get_upstream_tables(str(a), [])
    assert list(upstreams).sort() == [c, e].sort()


def test_bq_get_profile_candidate_query_all_params():
    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": 1,
                "profile_table_size_limit": 5,
                "profile_table_row_limit": 50000,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    threshold_time = datetime.fromtimestamp(1648876349)
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "row_count<50000 and ROUND(size_bytes/POW(10,9),2)<5 and last_modified_time>=1648876349000 "
    )
    query = source.generate_profile_candidate_query(threshold_time, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_no_day_limit():
    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": None,
                "profile_table_size_limit": 5,
                "profile_table_row_limit": 50000,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "row_count<50000 and ROUND(size_bytes/POW(10,9),2)<5 "
    )
    query = source.generate_profile_candidate_query(None, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_no_size_limit():
    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": 1,
                "profile_table_size_limit": None,
                "profile_table_row_limit": 50000,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    threshold_time = datetime.fromtimestamp(1648876349)
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "row_count<50000 and last_modified_time>=1648876349000 "
    )
    query = source.generate_profile_candidate_query(threshold_time, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_no_row_limit():
    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": 1,
                "profile_table_size_limit": 5,
                "profile_table_row_limit": None,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    threshold_time = datetime.fromtimestamp(1648876349)
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "ROUND(size_bytes/POW(10,9),2)<5 and last_modified_time>=1648876349000 "
    )
    query = source.generate_profile_candidate_query(threshold_time, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_all_null():

    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": None,
                "profile_table_size_limit": None,
                "profile_table_row_limit": None,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    expected_query = ""
    query = source.generate_profile_candidate_query(None, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_only_row():
    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": None,
                "profile_table_size_limit": None,
                "profile_table_row_limit": 50000,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "row_count<50000 "
    )
    query = source.generate_profile_candidate_query(None, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_only_days():
    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": 1,
                "profile_table_size_limit": None,
                "profile_table_row_limit": None,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    threshold_time = datetime.fromtimestamp(1648876349)
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "last_modified_time>=1648876349000 "
    )
    query = source.generate_profile_candidate_query(threshold_time, "dataset_foo")
    assert query == expected_query


def test_bq_get_profile_candidate_query_only_size():

    config = BigQueryConfig.parse_obj(
        {
            "profiling": {
                "profile_if_updated_since_days": None,
                "profile_table_size_limit": 5,
                "profile_table_row_limit": None,
            }
        }
    )
    source = BigQuerySource(config=config, ctx=PipelineContext(run_id="test"))
    expected_query = (
        "SELECT table_id, size_bytes, last_modified_time, row_count, FROM dataset_foo.__TABLES__ WHERE "
        "ROUND(size_bytes/POW(10,9),2)<5 "
    )
    query = source.generate_profile_candidate_query(None, "dataset_foo")
    assert query == expected_query
