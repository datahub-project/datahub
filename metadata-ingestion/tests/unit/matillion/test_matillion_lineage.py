"""Tests for Matillion lineage extraction."""

import pytest

from datahub.ingestion.source.matillion.config import NamespacePlatformMapping
from datahub.ingestion.source.matillion.matillion_lineage import OpenLineageParser
from datahub.ingestion.source.matillion.models import (
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


def test_parse_namespace_snowflake(parser):
    platform, host = parser._parse_namespace("snowflake://account.us-east-1")
    assert platform == "snowflake"
    assert host == "account.us-east-1"


def test_parse_namespace_postgresql(parser):
    platform, host = parser._parse_namespace("postgresql://db-host:5432")
    assert platform == "postgres"  # Maps postgresql to postgres
    assert host == "db-host:5432"


def test_parse_namespace_sqlserver(parser):
    platform, host = parser._parse_namespace("sqlserver://sql-server.example.com")
    assert platform == "mssql"  # Maps sqlserver to mssql
    assert host == "sql-server.example.com"


def test_parse_namespace_no_scheme(parser):
    platform, host = parser._parse_namespace("unknown-platform")
    assert platform == "unknown-platform"
    assert host == "unknown-platform"


def test_get_platform_instance_info_no_mapping(parser):
    info = parser._get_platform_instance_info("snowflake://account.region")
    assert info.platform_instance is None
    assert info.env == "PROD"
    assert info.database is None
    assert info.default_schema is None
    assert info.convert_urns_to_lowercase is False


def test_get_platform_instance_info_with_exact_match(parser_with_mappings):
    info = parser_with_mappings._get_platform_instance_info("snowflake://prod-account")
    assert info.platform_instance == "snowflake_prod"
    assert info.env == "PROD"
    assert info.database is None
    assert info.default_schema is None
    assert info.convert_urns_to_lowercase is False


def test_get_platform_instance_info_with_prefix_match(parser_with_mappings):
    info = parser_with_mappings._get_platform_instance_info(
        "snowflake://prod-account.us-east-1/db/schema/table"
    )
    assert info.platform_instance == "snowflake_prod"
    assert info.env == "PROD"
    assert info.database is None
    assert info.default_schema is None
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
    assert info.database is None
    assert info.default_schema is None
    assert info.convert_urns_to_lowercase is False


def test_extract_dataset_info(parser):
    dataset_dict = {
        "namespace": "snowflake://account.region",
        "name": "DATABASE.SCHEMA.TABLE",
        "facets": {},
    }

    dataset_info = parser._extract_dataset_info(dataset_dict, "input")

    assert dataset_info is not None
    assert dataset_info.platform == "snowflake"
    assert dataset_info.name == "DATABASE.SCHEMA.TABLE"
    assert dataset_info.namespace == "snowflake://account.region"
    assert dataset_info.platform_instance is None
    assert dataset_info.env == "PROD"


def test_extract_dataset_info_with_instance_mapping(parser_with_mappings):
    dataset_dict = {
        "namespace": "snowflake://prod-account.us-east-1",
        "name": "DATABASE.SCHEMA.TABLE",
        "facets": {},
    }

    dataset_info = parser_with_mappings._extract_dataset_info(dataset_dict, "output")

    assert dataset_info is not None
    assert dataset_info.platform == "snowflake"
    assert dataset_info.platform_instance == "snowflake_prod"
    assert dataset_info.env == "PROD"


def test_extract_dataset_info_missing_namespace(parser):
    dataset_dict = {
        "name": "TABLE",
        "facets": {},
    }

    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is None


def test_extract_dataset_info_missing_name(parser):
    dataset_dict = {
        "namespace": "snowflake://account",
        "facets": {},
    }

    dataset_info = parser._extract_dataset_info(dataset_dict, "output")
    assert dataset_info is None


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

    assert len(column_lineages) == 0  # No column lineage in this event


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

    # Check column lineage mappings
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
    assert (
        "source_col" in fine_grained.upstreams[0]
    )  # Upstream is a schema field URN string


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
    assert platform == "postgres"  # Custom mapping


def test_parse_lineage_event_empty_inputs():
    parser = OpenLineageParser()
    event = {
        "eventType": "COMPLETE",
        "job": {"namespace": "matillion://account.project", "name": "pipeline"},
        "inputs": [],
        "outputs": [
            {
                "namespace": "snowflake://account",
                "name": "DB.SCHEMA.TABLE",
                "facets": {},
            }
        ],
    }

    inputs, outputs, column_lineages = parser.parse_lineage_event(event)

    assert len(inputs) == 0
    assert len(outputs) == 1
    assert len(column_lineages) == 0


def test_parse_lineage_event_multiple_inputs_outputs():
    parser = OpenLineageParser()
    event = {
        "eventType": "COMPLETE",
        "job": {"namespace": "matillion://account.project", "name": "pipeline"},
        "inputs": [
            {
                "namespace": "postgresql://db:5432",
                "name": "schema.table1",
                "facets": {},
            },
            {
                "namespace": "postgresql://db:5432",
                "name": "schema.table2",
                "facets": {},
            },
        ],
        "outputs": [
            {
                "namespace": "snowflake://account",
                "name": "DB.SCHEMA.OUTPUT1",
                "facets": {},
            },
            {
                "namespace": "snowflake://account",
                "name": "DB.SCHEMA.OUTPUT2",
                "facets": {},
            },
        ],
    }

    inputs, outputs, column_lineages = parser.parse_lineage_event(event)

    assert len(inputs) == 2
    assert len(outputs) == 2


def test_normalize_name_single_part():
    """Test normalization of single-part table names."""
    # 3-tier platform (Snowflake): both database and schema
    assert (
        MatillionDatasetInfo.normalize_name(
            "table", "snowflake", database="db", schema="sch"
        )
        == "db.sch.table"
    )

    # 3-tier with only schema
    assert (
        MatillionDatasetInfo.normalize_name("table", "snowflake", schema="sch")
        == "sch.table"
    )

    # 2-tier platform (MySQL): prepend schema/database
    assert (
        MatillionDatasetInfo.normalize_name("table", "mysql", database="db")
        == "db.table"
    )

    # With neither
    assert MatillionDatasetInfo.normalize_name("table", "snowflake") == "table"


def test_normalize_name_two_parts():
    """Test normalization of two-part names on different platform types."""
    # 3-tier platform (Snowflake): prepend database if available
    assert (
        MatillionDatasetInfo.normalize_name(
            "schema.table", "snowflake", database="db", schema="public"
        )
        == "db.schema.table"
    )

    # 2-tier platform (MySQL): already qualified, keep as-is
    assert (
        MatillionDatasetInfo.normalize_name(
            "database.table", "mysql", database="prod", schema="public"
        )
        == "database.table"
    )

    # 3-tier without database configured
    assert (
        MatillionDatasetInfo.normalize_name("schema.table", "postgres", schema="public")
        == "schema.table"
    )


def test_normalize_name_three_parts():
    """Test that 3-part names are never modified."""
    # 3-tier platform - already fully qualified
    assert (
        MatillionDatasetInfo.normalize_name(
            "db.schema.table", "snowflake", database="other_db", schema="other_sch"
        )
        == "db.schema.table"
    )

    # 2-tier platform - already has 3 parts (unusual but should not be modified)
    assert (
        MatillionDatasetInfo.normalize_name("db.schema.table", "mysql")
        == "db.schema.table"
    )


def test_normalize_field_name_snowflake():
    """Test that Snowflake field names are lowercased."""
    parser = OpenLineageParser()

    # Snowflake should lowercase
    assert parser._normalize_field_name("CUSTOMER_ID", "snowflake") == "customer_id"
    assert parser._normalize_field_name("OrderDate", "snowflake") == "orderdate"
    assert parser._normalize_field_name("already_lower", "snowflake") == "already_lower"


def test_normalize_field_name_other_platforms():
    """Test that non-Snowflake platforms preserve field name casing."""
    parser = OpenLineageParser()

    # Other platforms should preserve case
    assert parser._normalize_field_name("customer_id", "postgres") == "customer_id"
    assert parser._normalize_field_name("CUSTOMER_ID", "mysql") == "CUSTOMER_ID"
    assert parser._normalize_field_name("OrderDate", "bigquery") == "OrderDate"


def test_column_lineage_with_snowflake_lowercasing():
    """Test that column lineage for Snowflake uses lowercase field names."""
    parser = OpenLineageParser()

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
                            "TARGET_COL": {  # Uppercase in OpenLineage
                                "inputFields": [
                                    {
                                        "namespace": "snowflake://account.region",
                                        "name": "SOURCE_DB.SOURCE_SCHEMA.SOURCE_TABLE",
                                        "field": "SOURCE_COL",  # Uppercase in OpenLineage
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

    # Create upstream lineage
    upstream_lineage = parser.create_upstream_lineage(
        inputs, output_dataset, column_lineages
    )

    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) == 1

    fine_grained = upstream_lineage.fineGrainedLineages[0]

    # Field names should be lowercased for Snowflake
    assert fine_grained.downstreams is not None
    assert fine_grained.upstreams is not None
    assert "target_col" in fine_grained.downstreams[0].lower()
    assert "source_col" in fine_grained.upstreams[0].lower()


def test_convert_urns_to_lowercase():
    """Test that dataset URNs are lowercased when convert_urns_to_lowercase is True."""
    namespace_mappings = {
        "snowflake://prod": NamespacePlatformMapping(
            platform_instance="snowflake_prod",
            env="PROD",
            convert_urns_to_lowercase=True,
        ),
        "postgresql://staging": NamespacePlatformMapping(
            platform_instance="postgres_staging",
            env="DEV",
            convert_urns_to_lowercase=False,  # Don't lowercase for this namespace
        ),
    }
    parser = OpenLineageParser(namespace_to_platform_instance=namespace_mappings)

    # Snowflake with lowercase enabled - dataset name should be lowercased
    dataset_dict = {
        "namespace": "snowflake://prod",
        "name": "PROD_DB.PUBLIC.MY_TABLE",  # Uppercase
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "prod_db.public.my_table"  # Lowercased

    # PostgreSQL with lowercase disabled - dataset name should preserve case
    dataset_dict = {
        "namespace": "postgresql://staging",
        "name": "MyDatabase.MySchema.MyTable",  # Mixed case
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "MyDatabase.MySchema.MyTable"  # Preserved


def test_normalize_name_with_namespace_mapping():
    """Test that dataset names are normalized when using namespace mappings.

    When platform_instance is configured, the database is omitted from the normalized
    name because DataHub prepends platform_instance to create the final URN.
    """
    namespace_mappings = {
        "snowflake://prod": NamespacePlatformMapping(
            platform_instance="snowflake_prod",
            env="PROD",
            database="PROD_DB",
            schema="PUBLIC",  # Use alias name
        ),
        "mysql://prod": NamespacePlatformMapping(
            platform_instance="mysql_prod", env="PROD", database="prod_db", schema=None
        ),
    }
    parser = OpenLineageParser(namespace_to_platform_instance=namespace_mappings)

    # Snowflake with platform_instance: database omitted from normalized name
    # Final URN will be: ...snowflake_prod.PUBLIC.my_table...
    dataset_dict = {
        "namespace": "snowflake://prod",
        "name": "my_table",  # Single part
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "PUBLIC.my_table"  # No PROD_DB prefix

    # MySQL (2-tier) with platform_instance: just table name
    # Final URN will be: ...mysql_prod.my_table...
    dataset_dict = {
        "namespace": "mysql://prod",
        "name": "my_table",  # Single part
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "my_table"  # No prod_db prefix

    # 2-part name on MySQL stays as-is (already qualified for 2-tier)
    # Final URN will be: ...mysql_prod.other_db.my_table...
    dataset_dict = {
        "namespace": "mysql://prod",
        "name": "other_db.my_table",
        "facets": {},
    }
    dataset_info = parser._extract_dataset_info(dataset_dict, "input")
    assert dataset_info is not None
    assert dataset_info.name == "other_db.my_table"


def test_unmapped_namespace_extracts_platform_with_defaults(parser):
    """
    Test that even without an explicit NamespacePlatformMapping, we can still:
    1. Extract the platform from the namespace URI
    2. Use sensible defaults (env=PROD, no platform_instance, no db/schema overrides)
    3. Generate valid lineage
    """
    # Create an event with a namespace that has NO explicit mapping
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

    # Parse the event
    input_datasets, output_datasets, column_lineages = parser.parse_lineage_event(event)

    # Verify lineage was extracted
    assert len(input_datasets) == 1
    assert len(output_datasets) == 1

    # Verify input dataset info - platform extraction and defaults
    input_dataset = input_datasets[0]
    assert input_dataset.platform == "postgres"  # Mapped from "postgresql"
    assert input_dataset.env == "PROD"  # Default
    assert input_dataset.platform_instance is None  # No explicit mapping
    assert input_dataset.name == "my_schema.my_table"  # No db/schema prepending

    # Verify output dataset info
    output_dataset = output_datasets[0]
    assert output_dataset.platform == "bigquery"
    assert output_dataset.env == "PROD"  # Default
    assert output_dataset.platform_instance is None  # No explicit mapping

    # Verify URNs are generated correctly
    input_urn = parser._make_dataset_urn(input_dataset)
    output_urn = parser._make_dataset_urn(output_dataset)

    assert "urn:li:dataset:(urn:li:dataPlatform:postgres" in input_urn
    assert "my_schema.my_table" in input_urn
    assert "PROD" in input_urn

    assert "urn:li:dataset:(urn:li:dataPlatform:bigquery" in output_urn
    assert "my_project.my_dataset.output_table" in output_urn


def test_extract_sql_from_event(parser):
    """Test extracting SQL from OpenLineage event's sql facet."""
    event_with_sql = {
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
    }

    sql = parser.extract_sql_from_event(event_with_sql)
    assert sql == "SELECT col1, col2 FROM input_table WHERE col1 > 100"


def test_extract_sql_from_event_no_sql(parser):
    """Test event without SQL facet returns None."""
    event_no_sql = {
        "job": {
            "namespace": "matillion://account.project",
            "name": "test_pipeline",
            "facets": {"jobType": {"jobType": "ORCHESTRATION"}},
        }
    }

    sql = parser.extract_sql_from_event(event_no_sql)
    assert sql is None


def test_extract_sql_from_event_null_sql(parser):
    """Test event with null SQL facet returns None."""
    event_null_sql = {
        "job": {
            "namespace": "matillion://account.project",
            "name": "test_pipeline",
            "facets": {"sql": None},
        }
    }

    sql = parser.extract_sql_from_event(event_null_sql)
    assert sql is None
