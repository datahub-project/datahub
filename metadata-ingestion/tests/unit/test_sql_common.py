from typing import Dict
from unittest import mock

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.sql.sql_common import PipelineContext, SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.ingestion.source.sql.trino import TrinoConfig, TrinoSource
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SchemalessClass,
    StringTypeClass,
    UpstreamLineageClass,
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


def get_test_trino_source(include_column_lineage: bool = True) -> TrinoSource:
    config = TrinoConfig(
        host_port="localhost:8080",
        database="iceberg_catalog",
        username="test",
        include_column_lineage=include_column_lineage,
        ingest_lineage_to_connectors=True,
    )
    return TrinoSource(
        config=config, ctx=PipelineContext(run_id="test"), platform="trino"
    )


def get_test_trino_schema_metadata(
    field_paths: list[str],
) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="iceberg_catalog.contextad.accountcontact",
        platform="urn:li:dataPlatform:trino",
        version=0,
        hash="",
        platformSchema=SchemalessClass(),
        fields=[
            SchemaFieldClass(
                fieldPath=path,
                nativeDataType="varchar",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            )
            for path in field_paths
        ],
    )


def test_trino_gen_lineage_workunit_includes_fine_grained_lineage_when_schema_provided():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    schema_metadata = get_test_trino_schema_metadata(
        ["accountid", "accountmanagerid", "businessdevid", "accountservicetype"]
    )

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert upstream_lineage is not None
    assert hasattr(upstream_lineage, "fineGrainedLineages")
    fgl = getattr(upstream_lineage, "fineGrainedLineages", None)
    assert fgl is not None
    assert len(fgl) == 4
    for fg in fgl:
        assert len(fg.upstreams) == 1
        assert len(fg.downstreams) == 1
        assert "iceberg" in fg.upstreams[0]
        assert "trino" in fg.downstreams[0]
    assert make_schema_field_urn(source_dataset_urn, "accountid") in [
        fg.upstreams[0] for fg in fgl
    ]
    assert make_schema_field_urn(dataset_urn, "accountid") in [
        fg.downstreams[0] for fg in fgl
    ]


def test_trino_gen_lineage_workunit_no_fine_grained_lineage_when_disabled():
    source = get_test_trino_source(include_column_lineage=False)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    schema_metadata = get_test_trino_schema_metadata(["accountid"])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert upstream_lineage is not None
    fgl = getattr(upstream_lineage, "fineGrainedLineages", None)
    assert fgl is None


def test_trino_gen_lineage_workunit_no_fine_grained_lineage_when_schema_none():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )

    workunits = list(source.gen_lineage_workunit(dataset_urn, source_dataset_urn, None))
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert upstream_lineage is not None
    fgl = getattr(upstream_lineage, "fineGrainedLineages", None)
    assert fgl is None


def test_trino_gen_lineage_workunit_no_fine_grained_lineage_when_schema_empty_fields():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    schema_metadata = get_test_trino_schema_metadata([])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert upstream_lineage is not None
    fgl = getattr(upstream_lineage, "fineGrainedLineages", None)
    assert fgl is None


def test_trino_gen_lineage_workunit_upstreams_present_with_or_without_cll():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,catalog.schema.table,PROD)"
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,schema.table,PROD)"

    workunits = list(source.gen_lineage_workunit(dataset_urn, source_dataset_urn, None))
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.upstreams[0].dataset == source_dataset_urn
