import json
import os

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config


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


def test_bigquery_ref_extra_removal():
    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_*")
    )
    new_table_ref = table_ref.get_sanitized_table_ref()
    assert new_table_ref.table_identifier.table == "foo_yyyymmdd"
    assert (
        new_table_ref.table_identifier.project_id
        == table_ref.table_identifier.project_id
    )
    assert new_table_ref.table_identifier.dataset == table_ref.table_identifier.dataset

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_2022")
    )
    new_table_ref = table_ref.get_sanitized_table_ref()
    assert new_table_ref.table_identifier.table == "foo_2022"
    assert (
        new_table_ref.table_identifier.project_id
        == table_ref.table_identifier.project_id
    )
    assert new_table_ref.table_identifier.dataset == table_ref.table_identifier.dataset

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_20222110")
    )
    new_table_ref = table_ref.get_sanitized_table_ref()
    assert new_table_ref.table_identifier.table == "foo_yyyymmdd"
    assert (
        new_table_ref.table_identifier.project_id
        == table_ref.table_identifier.project_id
    )
    assert new_table_ref.table_identifier.dataset == table_ref.table_identifier.dataset

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo")
    )
    new_table_ref = table_ref.get_sanitized_table_ref()
    assert new_table_ref.table_identifier.table == "foo"
    assert (
        new_table_ref.table_identifier.project_id
        == table_ref.table_identifier.project_id
    )
    assert new_table_ref.table_identifier.dataset == table_ref.table_identifier.dataset

    table_ref = BigQueryTableRef(
        BigqueryTableIdentifier("project-1234", "dataset-4567", "foo_2016*")
    )
    new_table_ref = table_ref.get_sanitized_table_ref()
    assert new_table_ref.table_identifier.table == "foo_yyyymmdd"
    assert (
        new_table_ref.table_identifier.project_id
        == table_ref.table_identifier.project_id
    )
    assert new_table_ref.table_identifier.dataset == table_ref.table_identifier.dataset
