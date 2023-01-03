import json
import os

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config


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
    source.lineage_extractor.lineage_metadata = {str(a): set([str(b)])}
    upstreams = source.lineage_extractor.get_upstream_tables(str(a), [])
    assert list(upstreams) == [b]


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
    source.lineage_extractor.lineage_metadata = {str(a): set([str(b)])}
    upstreams = source.lineage_extractor.get_upstream_tables(str(a), [])
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
    source.lineage_extractor.lineage_metadata = {
        str(a): set([str(b)]),
        str(b): set([str(c)]),
    }
    upstreams = source.lineage_extractor.get_upstream_tables(str(a), [])
    assert list(upstreams) == [c]


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
    source.lineage_extractor.lineage_metadata = {
        str(a): set([str(b)]),
        str(b): set([str(c), str(d)]),
        str(d): set([str(e)]),
    }
    upstreams = source.lineage_extractor.get_upstream_tables(str(a), [])
    assert list(upstreams).sort() == [c, e].sort()
