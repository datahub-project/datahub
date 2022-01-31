import json
import os

import pytest


@pytest.mark.integration
def test_bigquery_uri():
    from datahub.ingestion.source.sql.bigquery import BigQueryConfig

    config = BigQueryConfig.parse_obj(
        {
            "project_id": "test-project",
        }
    )
    assert config.get_sql_alchemy_url() == "bigquery://test-project"


@pytest.mark.integration
def test_bigquery_uri_with_credential():
    from datahub.ingestion.source.sql.bigquery import BigQueryConfig

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
        assert config.credentials_path

        with open(config.credentials_path) as jsonFile:
            json_credential = json.load(jsonFile)
            jsonFile.close()

        credential = json.dumps(json_credential, sort_keys=True)
        expected_credential = json.dumps(expected_credential_json, sort_keys=True)
        assert expected_credential == credential

    except AssertionError as e:
        if config.credentials_path:
            os.unlink(str(config.credentials_path))
        raise e
