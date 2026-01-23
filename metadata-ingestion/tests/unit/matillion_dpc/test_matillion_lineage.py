"""Tests for Matillion lineage extraction."""

from typing import Optional

import pytest

from datahub.ingestion.source.matillion_dpc.config import NamespacePlatformMapping
from datahub.ingestion.source.matillion_dpc.matillion_lineage import OpenLineageParser
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionColumnLineageInfo,
    MatillionDatasetInfo,
)


@pytest.fixture
def parser():
    return OpenLineageParser()


@pytest.fixture
def parser_with_mappings():
    namespace_mappings = {
        "snowflake://prod-account": NamespacePlatformMapping(
            platform_instance="snowflake_prod", env="PROD"
        ),
        "postgresql://staging-db": NamespacePlatformMapping(
            platform_instance="postgres_staging", env="DEV"
        ),
    }
    platform_mappings = {"customdb": "postgres"}
    return OpenLineageParser(
        namespace_to_platform_instance=namespace_mappings,
        platform_mapping=platform_mappings,
        env="PROD",
    )


@pytest.mark.parametrize(
    "namespace,expected_platform,expected_host",
    [
        pytest.param(
            "snowflake://account.us-east-1",
            "snowflake",
            "account.us-east-1",
            id="snowflake",
        ),
        pytest.param(
            "postgresql://db-host:5432",
            "postgres",
            "db-host:5432",
            id="postgresql",
        ),
        pytest.param(
            "sqlserver://sql-server.example.com",
            "mssql",
            "sql-server.example.com",
            id="sqlserver",
        ),
        pytest.param(
            "unknown-platform",
            "unknown-platform",
            "unknown-platform",
            id="no_scheme",
        ),
    ],
)
def test_parse_namespace(
    parser: OpenLineageParser,
    namespace: str,
    expected_platform: str,
    expected_host: str,
) -> None:
    platform, host = parser._parse_namespace(namespace)
    assert platform == expected_platform
    assert host == expected_host


@pytest.mark.parametrize(
    "namespace,expected_instance,expected_env",
    [
        pytest.param(
            "snowflake://account.region",
            None,
            "PROD",
            id="no_mapping",
        ),
        pytest.param(
            "snowflake://prod-account",
            "snowflake_prod",
            "PROD",
            id="exact_match",
        ),
        pytest.param(
            "snowflake://prod-account.us-east-1/db/schema/table",
            "snowflake_prod",
            "PROD",
            id="prefix_match",
        ),
    ],
)
def test_get_platform_instance_info(
    parser_with_mappings: OpenLineageParser,
    namespace: str,
    expected_instance: Optional[str],
    expected_env: str,
) -> None:
    info = parser_with_mappings._get_platform_instance_info(namespace)
    assert info.platform_instance == expected_instance
    assert info.env == expected_env
    assert info.convert_urns_to_lowercase is False


def test_get_platform_instance_info_longest_prefix_match(parser_with_mappings):
    parser_with_mappings.namespace_to_platform_instance[
        "postgresql://staging-db.example"
    ] = NamespacePlatformMapping(platform_instance="postgres_staging_example", env="QA")

    info = parser_with_mappings._get_platform_instance_info(
        "postgresql://staging-db.example.com:5432/database"
    )
    assert info.platform_instance == "postgres_staging_example"
    assert info.env == "QA"


@pytest.mark.parametrize(
    "dataset_dict,expected_platform,expected_name",
    [
        pytest.param(
            {
                "namespace": "snowflake://account.region",
                "name": "DATABASE.SCHEMA.TABLE",
                "facets": {},
            },
            "snowflake",
            "DATABASE.SCHEMA.TABLE",
            id="snowflake_basic",
        ),
        pytest.param(
            {
                "namespace": "snowflake://prod-account.us-east-1",
                "name": "DATABASE.SCHEMA.TABLE",
                "facets": {},
            },
            "snowflake",
            "DATABASE.SCHEMA.TABLE",
            id="with_instance_mapping",
        ),
    ],
)
def test_extract_dataset_info(
    parser: OpenLineageParser,
    parser_with_mappings: OpenLineageParser,
    dataset_dict: dict,
    expected_platform: str,
    expected_name: str,
) -> None:
    p = parser_with_mappings if "prod-account" in dataset_dict["namespace"] else parser
    dataset_info = p._extract_dataset_info(dataset_dict, "input")

    assert dataset_info is not None
    assert dataset_info.platform == expected_platform
    assert dataset_info.name == expected_name


@pytest.mark.parametrize(
    "dataset_dict,should_be_none",
    [
        pytest.param(
            {"name": "TABLE", "facets": {}},
            True,
            id="missing_namespace",
        ),
        pytest.param(
            {"namespace": "snowflake://account", "facets": {}},
            True,
            id="missing_name",
        ),
    ],
)
def test_extract_dataset_info_invalid(
    parser: OpenLineageParser, dataset_dict: dict, should_be_none: bool
) -> None:
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert (dataset_info is None) == should_be_none


def test_parse_lineage_event_basic(parser):
    event = {
        "eventType": "COMPLETE",
        "job": {"namespace": "matillion://account.project", "name": "my-pipeline"},
        "inputs": [
            {
                "namespace": "postgresql://db-host:5432",
                "name": "source_schema.source_table",
                "facets": {},
            }
        ],
        "outputs": [
            {
                "namespace": "snowflake://account.region",
                "name": "TARGET_DB.TARGET_SCHEMA.TARGET_TABLE",
                "facets": {},
            }
        ],
    }

    inputs, outputs, column_lineages = parser.parse_lineage_event(event)

    assert len(inputs) == 1
    assert inputs[0].platform == "postgres"
    assert inputs[0].name == "source_schema.source_table"

    assert len(outputs) == 1
    assert outputs[0].platform == "snowflake"
    assert outputs[0].name == "TARGET_DB.TARGET_SCHEMA.TARGET_TABLE"

    assert len(column_lineages) == 0


def test_parse_lineage_event_with_column_lineage(parser):
    event = {
        "eventType": "COMPLETE",
        "job": {"namespace": "matillion://account.project", "name": "my-pipeline"},
        "inputs": [
            {
                "namespace": "postgresql://db:5432",
                "name": "source_schema.source_table",
                "facets": {},
            }
        ],
        "outputs": [
            {
                "namespace": "snowflake://account.region",
                "name": "TARGET_DB.SCHEMA.TABLE",
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "target_col1": {
                                "inputFields": [
                                    {
                                        "namespace": "postgresql://db:5432",
                                        "name": "source_schema.source_table",
                                        "field": "source_col1",
                                    }
                                ]
                            },
                            "target_col2": {
                                "inputFields": [
                                    {
                                        "namespace": "postgresql://db:5432",
                                        "name": "source_schema.source_table",
                                        "field": "source_col2",
                                    }
                                ]
                            },
                        }
                    }
                },
            }
        ],
    }

    inputs, outputs, column_lineages = parser.parse_lineage_event(event)

    assert len(inputs) == 1
    assert len(outputs) == 1
    assert len(column_lineages) == 2

    col_lineage_fields = {cl.downstream_field for cl in column_lineages}
    assert "target_col1" in col_lineage_fields
    assert "target_col2" in col_lineage_fields


def test_create_upstream_lineage_dataset_only(parser):
    input1 = MatillionDatasetInfo(
        platform="postgres",
        name="db.schema.table1",
        namespace="postgresql://host:5432",
        env="PROD",
    )
    input2 = MatillionDatasetInfo(
        platform="postgres",
        name="db.schema.table2",
        namespace="postgresql://host:5432",
        env="PROD",
    )
    output = MatillionDatasetInfo(
        platform="snowflake",
        name="DB.SCHEMA.OUTPUT",
        namespace="snowflake://account",
        env="PROD",
    )

    upstream_lineage = parser.create_upstream_lineage([input1, input2], output, [])

    assert len(upstream_lineage.upstreams) == 2
    assert all(u.type == "TRANSFORMED" for u in upstream_lineage.upstreams)
    assert upstream_lineage.fineGrainedLineages is None


def test_create_upstream_lineage_with_column_lineage(parser):
    input_dataset = MatillionDatasetInfo(
        platform="postgres",
        name="db.schema.source",
        namespace="postgresql://host:5432",
        env="PROD",
    )
    output_dataset = MatillionDatasetInfo(
        platform="snowflake",
        name="DB.SCHEMA.TARGET",
        namespace="snowflake://account",
        env="PROD",
    )

    column_lineages = [
        MatillionColumnLineageInfo(
            downstream_field="target_col",
            upstream_datasets=[input_dataset],
            upstream_fields=["source_col"],
        )
    ]

    upstream_lineage = parser.create_upstream_lineage(
        [input_dataset], output_dataset, column_lineages
    )

    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) == 1

    fine_grained = upstream_lineage.fineGrainedLineages[0]
    assert len(fine_grained.upstreams) == 1
    assert len(fine_grained.downstreams) == 1
    assert "target_col" in fine_grained.downstreams[0]
    assert "source_col" in fine_grained.upstreams[0]


def test_make_dataset_urn(parser):
    dataset = MatillionDatasetInfo(
        platform="snowflake",
        name="DATABASE.SCHEMA.TABLE",
        namespace="snowflake://account",
        platform_instance="snowflake_prod",
        env="PROD",
    )

    urn = parser._make_dataset_urn(dataset)

    assert "urn:li:dataset:" in urn
    assert "snowflake" in urn
    assert "DATABASE.SCHEMA.TABLE" in urn
    assert "PROD" in urn


def test_custom_platform_mapping(parser_with_mappings):
    platform, _ = parser_with_mappings._parse_namespace("customdb://host")
    assert platform == "postgres"


@pytest.mark.parametrize(
    "num_inputs,num_outputs",
    [
        pytest.param(0, 1, id="empty_inputs"),
        pytest.param(2, 2, id="multiple_inputs_outputs"),
    ],
)
def test_parse_lineage_event_various_counts(
    parser: OpenLineageParser, num_inputs: int, num_outputs: int
) -> None:
    event = {
        "eventType": "COMPLETE",
        "job": {"namespace": "matillion://account.project", "name": "pipeline"},
        "inputs": [
            {
                "namespace": "postgresql://db:5432",
                "name": f"schema.table{i}",
                "facets": {},
            }
            for i in range(num_inputs)
        ],
        "outputs": [
            {
                "namespace": "snowflake://account",
                "name": f"DB.SCHEMA.OUTPUT{i}",
                "facets": {},
            }
            for i in range(num_outputs)
        ],
    }

    inputs, outputs, column_lineages = parser.parse_lineage_event(event)

    assert len(inputs) == num_inputs
    assert len(outputs) == num_outputs


@pytest.mark.parametrize(
    "name,platform,database,schema,expected",
    [
        pytest.param(
            "table", "snowflake", "db", "sch", "db.sch.table", id="3tier_full"
        ),
        pytest.param(
            "table", "snowflake", None, "sch", "sch.table", id="3tier_schema_only"
        ),
        pytest.param("table", "mysql", "db", None, "db.table", id="2tier"),
        pytest.param("table", "snowflake", None, None, "table", id="no_defaults"),
        pytest.param(
            "schema.table",
            "snowflake",
            "db",
            "public",
            "db.schema.table",
            id="2part_3tier",
        ),
        pytest.param(
            "database.table",
            "mysql",
            "prod",
            "public",
            "database.table",
            id="2part_2tier",
        ),
        pytest.param(
            "db.schema.table",
            "snowflake",
            "other_db",
            "other_sch",
            "db.schema.table",
            id="3part",
        ),
    ],
)
def test_normalize_name(
    name: str,
    platform: str,
    database: Optional[str],
    schema: Optional[str],
    expected: str,
) -> None:
    result = MatillionDatasetInfo.normalize_name(
        name, platform, database=database, schema=schema
    )
    assert result == expected


@pytest.mark.parametrize(
    "field_name,platform,expected",
    [
        pytest.param("CUSTOMER_ID", "snowflake", "customer_id", id="snowflake_upper"),
        pytest.param("OrderDate", "snowflake", "orderdate", id="snowflake_mixed"),
        pytest.param(
            "already_lower", "snowflake", "already_lower", id="snowflake_lower"
        ),
        pytest.param("customer_id", "postgres", "customer_id", id="postgres_lower"),
        pytest.param("CUSTOMER_ID", "mysql", "CUSTOMER_ID", id="mysql_upper"),
        pytest.param("OrderDate", "bigquery", "OrderDate", id="bigquery_mixed"),
    ],
)
def test_normalize_field_name(
    parser: OpenLineageParser, field_name: str, platform: str, expected: str
) -> None:
    result = parser._normalize_field_name(field_name, platform)
    assert result == expected


def test_column_lineage_with_snowflake_lowercasing(parser: OpenLineageParser) -> None:
    event = {
        "eventType": "COMPLETE",
        "job": {"namespace": "matillion://account.project", "name": "pipeline"},
        "inputs": [
            {
                "namespace": "snowflake://account.region",
                "name": "SOURCE_DB.SOURCE_SCHEMA.SOURCE_TABLE",
                "facets": {},
            }
        ],
        "outputs": [
            {
                "namespace": "snowflake://account.region",
                "name": "TARGET_DB.TARGET_SCHEMA.TARGET_TABLE",
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "TARGET_COL": {
                                "inputFields": [
                                    {
                                        "namespace": "snowflake://account.region",
                                        "name": "SOURCE_DB.SOURCE_SCHEMA.SOURCE_TABLE",
                                        "field": "SOURCE_COL",
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        ],
    }

    inputs, outputs, column_lineages = parser.parse_lineage_event(event)

    assert len(outputs) == 1
    output_dataset = outputs[0]

    upstream_lineage = parser.create_upstream_lineage(
        inputs, output_dataset, column_lineages
    )

    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) == 1

    fine_grained = upstream_lineage.fineGrainedLineages[0]

    assert fine_grained.downstreams is not None
    assert fine_grained.upstreams is not None
    assert "target_col" in fine_grained.downstreams[0].lower()
    assert "source_col" in fine_grained.upstreams[0].lower()


@pytest.mark.parametrize(
    "namespace,convert_lowercase,name,expected_name",
    [
        pytest.param(
            "snowflake://prod",
            True,
            "PROD_DB.PUBLIC.MY_TABLE",
            "prod_db.public.my_table",
            id="snowflake_lowercase",
        ),
        pytest.param(
            "postgresql://staging",
            False,
            "MyDatabase.MySchema.MyTable",
            "MyDatabase.MySchema.MyTable",
            id="postgresql_preserve_case",
        ),
    ],
)
def test_convert_urns_to_lowercase(
    namespace: str, convert_lowercase: bool, name: str, expected_name: str
) -> None:
    namespace_mappings = {
        namespace: NamespacePlatformMapping(
            platform_instance="test_instance",
            env="PROD",
            convert_urns_to_lowercase=convert_lowercase,
        ),
    }
    parser = OpenLineageParser(namespace_to_platform_instance=namespace_mappings)

    dataset_dict = {
        "namespace": namespace,
        "name": name,
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == expected_name


def test_normalize_name_with_namespace_mapping():
    namespace_mappings = {
        "snowflake://prod": NamespacePlatformMapping(
            platform_instance="snowflake_prod",
            env="PROD",
            database="PROD_DB",
            schema="PUBLIC",
        ),
        "mysql://prod": NamespacePlatformMapping(
            platform_instance="mysql_prod", env="PROD", database="prod_db", schema=None
        ),
    }
    parser = OpenLineageParser(namespace_to_platform_instance=namespace_mappings)

    dataset_dict = {
        "namespace": "snowflake://prod",
        "name": "my_table",
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "PUBLIC.my_table"

    dataset_dict = {
        "namespace": "mysql://prod",
        "name": "my_table",
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "my_table"


def test_unmapped_namespace_extracts_platform_with_defaults(parser):
    event = {
        "eventType": "COMPLETE",
        "eventTime": "2024-01-15T10:00:00.000Z",
        "job": {
            "namespace": "matillion://prod",
            "name": "test_pipeline",
            "facets": {},
        },
        "inputs": [
            {
                "namespace": "postgresql://unmapped-host.example.com:5432",
                "name": "my_schema.my_table",
                "facets": {},
            }
        ],
        "outputs": [
            {
                "namespace": "bigquery",
                "name": "my_project.my_dataset.output_table",
                "facets": {},
            }
        ],
    }

    input_datasets, output_datasets, column_lineages = parser.parse_lineage_event(event)

    assert len(input_datasets) == 1
    assert len(output_datasets) == 1

    input_dataset = input_datasets[0]
    assert input_dataset.platform == "postgres"
    assert input_dataset.env == "PROD"
    assert input_dataset.platform_instance is None
    assert input_dataset.name == "my_schema.my_table"

    output_dataset = output_datasets[0]
    assert output_dataset.platform == "bigquery"
    assert output_dataset.env == "PROD"
    assert output_dataset.platform_instance is None

    input_urn = parser._make_dataset_urn(input_dataset)
    output_urn = parser._make_dataset_urn(output_dataset)

    assert "urn:li:dataset:(urn:li:dataPlatform:postgres" in input_urn
    assert "my_schema.my_table" in input_urn
    assert "PROD" in input_urn

    assert "urn:li:dataset:(urn:li:dataPlatform:bigquery" in output_urn
    assert "my_project.my_dataset.output_table" in output_urn


@pytest.mark.parametrize(
    "event,expected_sql",
    [
        pytest.param(
            {
                "job": {
                    "namespace": "matillion://account.project",
                    "name": "test_pipeline",
                    "facets": {
                        "sql": {
                            "query": "SELECT col1, col2 FROM input_table WHERE col1 > 100",
                            "_producer": "https://www.matillion.com",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SqlJobFacet.json",
                        }
                    },
                }
            },
            "SELECT col1, col2 FROM input_table WHERE col1 > 100",
            id="with_sql",
        ),
        pytest.param(
            {
                "job": {
                    "namespace": "matillion://account.project",
                    "name": "test_pipeline",
                    "facets": {"jobType": {"jobType": "ORCHESTRATION"}},
                }
            },
            None,
            id="no_sql",
        ),
        pytest.param(
            {
                "job": {
                    "namespace": "matillion://account.project",
                    "name": "test_pipeline",
                    "facets": {"sql": None},
                }
            },
            None,
            id="null_sql",
        ),
    ],
)
def test_extract_sql_from_event(
    parser: OpenLineageParser, event: dict, expected_sql: Optional[str]
) -> None:
    sql = parser.extract_sql_from_event(event)
    assert sql == expected_sql
