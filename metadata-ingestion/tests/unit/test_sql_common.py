from typing import Dict
from unittest import mock

import pytest

from datahub.ingestion.source.sql.sql_common import PipelineContext, SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.ingestion.source.sql.stored_procedures.base import (
    get_procedure_flow_name,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)


class _TestSQLAlchemyConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self):
        return "mysql+pymysql://user:pass@localhost:5330"


class _TestSQLAlchemySource(SQLAlchemySource):
    @classmethod
    def create(cls, config_dict, ctx):
        config = _TestSQLAlchemyConfig.model_validate(config_dict)
        return cls(config, ctx, "TEST")


def get_test_sql_alchemy_source():
    return _TestSQLAlchemySource.create(
        config_dict={}, ctx=PipelineContext(run_id="test_ctx")
    )


class _TestTwoTierSQLAlchemyConfig(TwoTierSQLAlchemyConfig):
    # Test stub returning a fixed URL. The MRO has two different signatures
    # for `get_sql_alchemy_url` (TwoTierSQLAlchemyConfig uses `current_db`,
    # SQLAlchemyConnectionConfig uses `database`), so we silence the override
    # check rather than pick one and break the other.
    def get_sql_alchemy_url(self, *args: object, **kwargs: object) -> str:  # type: ignore[override]
        return "mysql+pymysql://user:pass@localhost:5330"


class _TestTwoTierSQLAlchemySource(TwoTierSQLAlchemySource):
    @classmethod
    def create(cls, config_dict, ctx):
        config = _TestTwoTierSQLAlchemyConfig.model_validate(config_dict)
        return cls(config, ctx, "TEST")


def test_three_tier_source_emits_schema_key_for_procedures():
    source = get_test_sql_alchemy_source()
    schema_key = source._get_procedure_schema_key("test_db", "test_schema")
    assert schema_key is not None
    assert schema_key.database == "test_db"
    assert schema_key.db_schema == "test_schema"


_TWO_TIER_CONFIG_DICT = {
    "host_port": "localhost:5330",
    "scheme": "mysql+pymysql",
}


def test_two_tier_source_omits_schema_key_for_procedures():
    """Two-tier sources (MySQL, MariaDB, Hive, …) pass `db_name == schema` to
    `_process_procedures`. Building a SchemaKey from that produces flow names
    like ``test_db.test_db.stored_procedures``; the override must instead
    yield None so the flow falls back to ``test_db.stored_procedures``."""
    source = _TestTwoTierSQLAlchemySource.create(
        config_dict=_TWO_TIER_CONFIG_DICT, ctx=PipelineContext(run_id="test_ctx")
    )
    assert source._get_procedure_schema_key("test_db", "test_db") is None


def test_procedure_flow_name_two_tier_does_not_duplicate_database():
    """End-to-end protection for the two-tier flow-name bug: when the source
    omits the SchemaKey, `get_procedure_flow_name` must produce
    ``{database}.stored_procedures`` (single ``test_db``), not
    ``{database}.{database}.stored_procedures``."""
    source = _TestTwoTierSQLAlchemySource.create(
        config_dict=_TWO_TIER_CONFIG_DICT, ctx=PipelineContext(run_id="test_ctx")
    )
    database_key = source._get_procedure_database_key("test_db")
    schema_key = source._get_procedure_schema_key("test_db", "test_db")

    flow_name = get_procedure_flow_name(database_key, schema_key)

    assert flow_name == "test_db.stored_procedures"


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


@pytest.mark.parametrize(
    "upstream_field_path,downstream_field_path,expected_simplified_upstream,expected_simplified_downstream",
    [
        (
            "[version=2.0].[type=string].employee_id",
            "[version=2.0].[type=string].employee_id",
            "employee_id",
            "employee_id",
        ),
        (
            "[version=2.0].[type=struct].[type=map].[type=struct].job_history",
            "[version=2.0].[type=struct].[type=map].[type=struct].job_history",
            "job_history",
            "job_history",
        ),
        (
            "[version=2.0].[type=struct].[type=array].[type=string].skills",
            "[version=2.0].[type=struct].[type=array].[type=string].skills",
            "skills",
            "skills",
        ),
        (" spaced . field ", "spaced.field", "spaced.field", "spaced.field"),
    ],
)
def test_fine_grained_lineages(
    upstream_field_path,
    downstream_field_path,
    expected_simplified_upstream,
    expected_simplified_downstream,
):
    source = get_test_sql_alchemy_source()

    downstream_field = SchemaFieldClass(
        fieldPath=downstream_field_path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="string",
    )

    lineages = source.get_fine_grained_lineages(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_db.test_schema.downstream_table,PROD)",
        upstream_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:TEST,test_db.test_schema.upstream_table,PROD)",
        schema_fields=[downstream_field],
    )

    assert lineages is not None
    assert len(lineages) == 1

    def get_field_from_urn(urn):
        return urn.split(",")[-1].rstrip(")")

    actual_downstream = get_field_from_urn(lineages[0].downstreams[0])
    actual_upstream = get_field_from_urn(lineages[0].upstreams[0])

    assert actual_downstream == expected_simplified_downstream
    assert actual_upstream == expected_simplified_upstream
