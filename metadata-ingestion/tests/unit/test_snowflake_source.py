import pytest

from datahub.ingestion.source.sql.snowflake import ExternalUrl


def test_external_url():
    external_url = ExternalUrl("s3://bucket-name")

    assert external_url.type == "s3"
    assert external_url.bucket == "bucket-name"
    assert external_url.key == ""
    assert external_url.url == "s3://bucket-name"
    assert external_url.is_known_platform()
    assert external_url.dataset_name == "bucket-name"

    external_url = ExternalUrl("s3n://bucket-name/patha/pathb")
    assert external_url.type == "s3n"
    assert external_url.platform_name == "s3"
    assert external_url.bucket == "bucket-name"
    assert external_url.key == "patha.pathb"
    assert external_url.url == "s3n://bucket-name/patha/pathb"
    assert external_url.is_known_platform()
    assert external_url.dataset_name == "bucket-name.patha.pathb"

    external_url = ExternalUrl("tmp://bucket-name")
    assert external_url.type == "tmp"
    assert not external_url.is_known_platform()


@pytest.mark.integration
def test_snowflake_uri():
    from datahub.ingestion.source.sql.snowflake import SnowflakeConfig

    config = SnowflakeConfig.parse_obj(
        {
            "username": "user",
            "password": "password",
            "host_port": "acctname",
            "database": "demo",
            "warehouse": "COMPUTE_WH",
            "role": "sysadmin",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "snowflake://user:password@acctname/?warehouse=COMPUTE_WH&role=sysadmin&application=acryl_datahub"
    )
