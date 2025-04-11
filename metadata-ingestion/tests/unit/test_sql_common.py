from typing import Dict, Optional
from unittest import mock

import pytest
from datahub.ingestion.source.sql.sql_common import (PipelineContext,
                                                     SQLAlchemySource)
from datahub.ingestion.source.sql.sql_config import (BasicSQLAlchemyConfig,
                                                     SQLCommonConfig)
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import \
    get_platform_from_sqlalchemy_uri
from pydantic import ValidationError


class _TestSQLAlchemyConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self):
        return "mysql+pymysql://user:pass@localhost:5330"


class _TestSQLAlchemySource(SQLAlchemySource):
    @classmethod
    def create(cls, config_dict, ctx):
        config = _TestSQLAlchemyConfig.parse_obj(config_dict)
        return cls(config, ctx, "TEST")


def get_test_sql_alchemy_source():
    return _TestSQLAlchemySource.create(
        config_dict={}, ctx=PipelineContext(run_id="test_ctx")
    )


def test_generate_foreign_key():
    source = get_test_sql_alchemy_source()
    fk_dict: Dict[str, str] = {
        "name": "test_constraint",
        "referred_table": "test_table",
        "referred_schema": "test_referred_schema",
        "constrained_columns": ["test_column"],  # type: ignore
        "referred_columns": ["test_referred_column"],  # type: ignore
    }
    foreign_key = source.get_foreign_key_metadata(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD)",
        schema="test_schema",
        fk_dict=fk_dict,
        inspector=mock.Mock(),
    )

    assert fk_dict.get("name") == foreign_key.name
    assert foreign_key.foreignFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_referred_schema.test_table,PROD),test_referred_column)"
    ]
    assert foreign_key.sourceFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD),test_column)"
    ]


def test_use_source_schema_for_foreign_key_if_not_specified():
    source = get_test_sql_alchemy_source()
    fk_dict: Dict[str, str] = {
        "name": "test_constraint",
        "referred_table": "test_table",
        "constrained_columns": ["test_column"],  # type: ignore
        "referred_columns": ["test_referred_column"],  # type: ignore
    }
    foreign_key = source.get_foreign_key_metadata(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD)",
        schema="test_schema",
        fk_dict=fk_dict,
        inspector=mock.Mock(),
    )

    assert fk_dict.get("name") == foreign_key.name
    assert foreign_key.foreignFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.test_table,PROD),test_referred_column)"
    ]
    assert foreign_key.sourceFields == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.base_urn,PROD),test_column)"
    ]


PLATFORM_FROM_SQLALCHEMY_URI_TEST_CASES: Dict[str, str] = {
    "awsathena://test_athena:3316/athenadb": "athena",
    "bigquery://test_bq:3316/bigquery": "bigquery",
    "clickhouse://test_clickhouse:3316/clickhousedb": "clickhouse",
    "druid://test_druid:1101/druiddb": "druid",
    "hive://test_hive:1201/hive": "hive",
    "mongodb://test_mongodb:1201/mongo": "mongodb",
    "mssql://test_mssql:1201/mssqldb": "mssql",
    "mysql://test_mysql:1201/mysql": "mysql",
    "oracle://test_oracle:3306/oracledb": "oracle",
    "pinot://test_pinot:3306/pinot": "pinot",
    "postgresql://test_postgres:5432/postgres": "postgres",
    "presto://test_presto:5432/prestodb": "presto",
    "redshift://test_redshift:5432/redshift": "redshift",
    "jdbc:postgres://test_redshift:5432/redshift.amazonaws": "redshift",
    "postgresql://test_redshift:5432/redshift.amazonaws": "redshift",
    "snowflake://test_snowflake:5432/snowflakedb": "snowflake",
    "trino://test_trino:5432/trino": "trino",
}


@pytest.mark.parametrize(
    "uri, expected_platform",
    PLATFORM_FROM_SQLALCHEMY_URI_TEST_CASES.items(),
    ids=PLATFORM_FROM_SQLALCHEMY_URI_TEST_CASES.keys(),
)
def test_get_platform_from_sqlalchemy_uri(uri: str, expected_platform: str) -> None:
    platform: str = get_platform_from_sqlalchemy_uri(uri)
    assert platform == expected_platform


def test_get_db_schema_with_dots_in_view_name():
    source = get_test_sql_alchemy_source()
    database, schema = source.get_db_schema(
        dataset_identifier="database.schema.long.view.name1"
    )
    assert database == "database"
    assert schema == "schema"


def test_test_connection_success():
    source = get_test_sql_alchemy_source()
    with mock.patch(
        "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_inspectors",
        side_effect=lambda: [],
    ):
        report = source.test_connection({})
        assert report is not None
        assert report.basic_connectivity
        assert report.basic_connectivity.capable
        assert report.basic_connectivity.failure_reason is None


def test_test_connection_failure():
    source = get_test_sql_alchemy_source()
    report = source.test_connection({})
    assert report is not None
    assert report.basic_connectivity
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert "Connection refused" in report.basic_connectivity.failure_reason


# Test cases for SQLAlchemyConnectionConfig validation
VALID_CONFIGS = [
    # Only URI
    {"sqlalchemy_uri": "postgresql://user:pass@host:port/db"},
    # Only host/port/scheme
    {"host_port": "host:port", "scheme": "postgresql"},
    # All provided (URI takes precedence in get_sql_alchemy_url, validation just checks presence)
    {
        "sqlalchemy_uri": "postgresql://user:pass@host:port/db",
        "host_port": "otherhost:otherport",
        "scheme": "mysql",
    },
    # With username/password
    {
        "host_port": "host:port",
        "scheme": "postgresql",
        "username": "user",
        "password": "password",
    },
]

INVALID_CONFIGS = [
    # Missing scheme
    {"host_port": "host:port"},
    # Missing host_port
    {"scheme": "postgresql"},
    # Missing all required fields
    {},
    # Empty strings for host/scheme (should fail validation)
    {"host_port": "", "scheme": ""},
    # Empty string for URI (should fail validation)
    {"sqlalchemy_uri": ""},
]


@pytest.mark.parametrize("config_dict", VALID_CONFIGS)
def test_sql_config_validation_valid(config_dict: Dict[str, str]) -> None:
    """Tests that BasicSQLAlchemyConfig validates successfully with valid combinations."""
    try:
        BasicSQLAlchemyConfig.parse_obj(config_dict)
    except ValidationError as e:
        pytest.fail(f"Validation failed unexpectedly for {config_dict}: {e}")


@pytest.mark.parametrize("config_dict", INVALID_CONFIGS)
def test_sql_config_validation_invalid(config_dict: Dict[str, str]) -> None:
    """Tests that BasicSQLAlchemyConfig raises ValidationError for invalid combinations."""
    with pytest.raises(ValidationError):
        BasicSQLAlchemyConfig.parse_obj(config_dict)


# Test cases for get_sql_alchemy_url
GET_URL_TEST_CASES = [
    # Only URI
    (
        {"sqlalchemy_uri": "postgresql+psycopg2://user:pass@host:5432/db"},
        None,
        None,
        "postgresql+psycopg2://user:pass@host:5432/db",
    ),
    # Only components (host, port, scheme, user, pass, db)
    (
        {
            "host_port": "host:5432",
            "scheme": "mysql+pymysql",
            "username": "user",
            "password": "password",
            "database": "db",
        },
        None,
        None,
        "mysql+pymysql://user:password@host:5432/db",
    ),
    # Components without port
    (
        {
            "host_port": "host",
            "scheme": "mysql+pymysql",
            "username": "user",
            "password": "password",
            "database": "db",
        },
        None,
        None,
        "mysql+pymysql://user:password@host/db",
    ),
    # Both URI and components (URI takes precedence)
    (
        {
            "sqlalchemy_uri": "postgresql+psycopg2://user:pass@host:5432/db",
            "host_port": "otherhost:1234",
            "scheme": "mysql",
            "username": "otheruser",
            "password": "otherpassword",
            "database": "otherdb",
        },
        None,
        None,
        "postgresql+psycopg2://user:pass@host:5432/db",
    ),
    # Components with database override argument
    (
        {
            "host_port": "host:5432",
            "scheme": "postgresql",
            "username": "user",
            "password": "password",
            "database": "original_db",
        },
        None,
        "override_db",
        "postgresql://user:password@host:5432/override_db",
    ),
    # Components with uri_opts argument
    (
        {
            "host_port": "host:5432",
            "scheme": "snowflake",
            "username": "user",
            "password": "password",
            "database": "db",
        },
        {"account": "test_account", "warehouse": "test_wh"},
        None,
        "snowflake://user:password@host:5432/db?account=test_account&warehouse=test_wh",
    ),
    # Components with options in config (should not affect URL directly unless passed as uri_opts)
    (
        {
            "host_port": "host:5432",
            "scheme": "mysql+pymysql",
            "username": "user",
            "password": "password",
            "database": "db",
            "options": {"connect_timeout": 10},
        },
        None,
        None,
        "mysql+pymysql://user:password@host:5432/db",
    ),
]


@pytest.mark.parametrize(
    "config_dict, uri_opts, database_override, expected_url",
    GET_URL_TEST_CASES,
)
def test_get_sql_alchemy_url(
    config_dict: Dict[str, str],
    uri_opts: Optional[Dict[str, str]],
    database_override: Optional[str],
    expected_url: str,
) -> None:
    """Tests the get_sql_alchemy_url method with various configurations."""
    config = BasicSQLAlchemyConfig.parse_obj(config_dict)
    generated_url = config.get_sql_alchemy_url(
        uri_opts=uri_opts, database=database_override
    )
    assert generated_url == expected_url
