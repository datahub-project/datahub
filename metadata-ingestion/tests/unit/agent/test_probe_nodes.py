import pytest

from datahub.ingestion.agent.probe import column_nodes, probe_hierarchy
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def test_column_nodes_have_no_pattern_field():
    nodes, _ = column_nodes(
        [{"name": "id"}, {"name": "amount"}], limit=10, fqn_prefix="db.s.orders"
    )
    assert [n.name for n in nodes] == ["id", "amount"]
    assert nodes[0].fqn == "db.s.orders.id"
    assert all(n.pattern_field is None for n in nodes)


def test_snowflake_declares_database_aware_hierarchy():
    pytest.importorskip("snowflake.connector")
    assert probe_hierarchy("snowflake") == [
        DatasetContainerSubTypes.DATABASE,
        DatasetContainerSubTypes.SCHEMA,
        DatasetSubTypes.TABLE,
        "Column",
    ]


def test_bigquery_declares_project_dataset_hierarchy():
    pytest.importorskip("google.cloud.bigquery")
    assert probe_hierarchy("bigquery") == [
        DatasetContainerSubTypes.BIGQUERY_PROJECT,
        DatasetContainerSubTypes.BIGQUERY_DATASET,
        DatasetSubTypes.TABLE,
        "Column",
    ]


def test_generic_sql_declares_two_level_hierarchy():
    assert probe_hierarchy("sqlalchemy") == [
        DatasetContainerSubTypes.SCHEMA,
        DatasetSubTypes.TABLE,
        "Column",
    ]


def test_two_tier_source_top_container_is_database():
    # MySQL/Hive/etc. have no schema layer and filter by database_pattern.
    assert probe_hierarchy("mysql") == [
        DatasetContainerSubTypes.DATABASE,
        DatasetSubTypes.TABLE,
        "Column",
    ]


def test_unsupported_source_has_no_hierarchy():
    # `file` is registered but implements no probe contract.
    assert probe_hierarchy("file") is None


def test_snowflake_identifier_escaping_prevents_injection():
    pytest.importorskip("snowflake.connector")
    from datahub.ingestion.source.snowflake.snowflake_probe import _quote_identifier

    assert _quote_identifier("DEMO_DB") == "DEMO_DB"
    # An embedded double quote is doubled so it cannot break out of the identifier.
    assert _quote_identifier('EVIL" ; DROP TABLE x --') == 'EVIL"" ; DROP TABLE x --'


def test_snowflake_default_schema_predicate():
    pytest.importorskip("snowflake.connector")
    from datahub.ingestion.source.snowflake.snowflake_utils import (
        is_snowflake_default_schema,
    )

    assert is_snowflake_default_schema("INFORMATION_SCHEMA")
    assert is_snowflake_default_schema("information_schema")  # case-insensitive
    assert not is_snowflake_default_schema("PUBLIC")


def test_redshift_default_schemas_shared_by_sql_and_probe():
    # The probe's default_schemas() must reuse the exact list the SQL excludes.
    from datahub.ingestion.source.redshift.query import (
        _DEFAULT_SCHEMA_EXCLUSION,
        REDSHIFT_DEFAULT_SCHEMAS,
    )

    assert set(REDSHIFT_DEFAULT_SCHEMAS) == {"pg_catalog", "information_schema"}
    # The SQL exclusion clause is generated from the same constant.
    for schema in REDSHIFT_DEFAULT_SCHEMAS:
        assert f"schema_name != '{schema}'" in _DEFAULT_SCHEMA_EXCLUSION
