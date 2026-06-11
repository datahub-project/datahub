import pytest

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.s3.duckdb_secrets import build_s3_secret_sql


def test_s3_secret_with_explicit_keys():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_region="us-east-1",
    )
    sql = build_s3_secret_sql(aws)
    assert "CREATE OR REPLACE SECRET" in sql
    assert "TYPE s3" in sql
    assert "KEY_ID 'AKIA_EXAMPLE'" in sql
    assert "SECRET 'secret_example'" in sql
    assert "REGION 'us-east-1'" in sql


def test_s3_secret_with_session_token():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_session_token="token_example",
    )
    sql = build_s3_secret_sql(aws)
    assert "SESSION_TOKEN 'token_example'" in sql


def test_s3_secret_falls_back_to_credential_chain():
    aws = AwsConnectionConfig(aws_region="us-east-1")  # no static keys
    sql = build_s3_secret_sql(aws)
    assert "PROVIDER credential_chain" in sql


def test_s3_secret_with_custom_endpoint():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_endpoint_url="https://minio.example.com:9000",
    )
    sql = build_s3_secret_sql(aws)
    # DuckDB ENDPOINT excludes the scheme.
    assert "ENDPOINT 'minio.example.com:9000'" in sql


def test_s3_secret_with_http_endpoint():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_endpoint_url="http://localhost:9000",
    )
    sql = build_s3_secret_sql(aws)
    assert "ENDPOINT 'localhost:9000'" in sql
    assert "USE_SSL false" in sql
    assert "USE_SSL true" not in sql


def test_s3_secret_with_schemeless_endpoint():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_endpoint_url="minio.example.com:9000",
    )
    sql = build_s3_secret_sql(aws)
    assert "ENDPOINT 'minio.example.com:9000'" in sql
    assert "USE_SSL false" in sql
    assert "USE_SSL true" not in sql


def test_s3_secret_rejects_endpoint_without_host():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_endpoint_url="https://",
    )
    with pytest.raises(ValueError, match="no host"):
        build_s3_secret_sql(aws)
