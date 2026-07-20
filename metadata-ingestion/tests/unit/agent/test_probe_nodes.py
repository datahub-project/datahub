import pytest

from datahub.ingestion.agent.probe import (
    column_nodes,
    container_nodes,
    probe_hierarchy,
    table_nodes,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def test_container_nodes_database_level():
    nodes, truncated = container_nodes(
        ["DEMO_DB", "SMOKE_TEST_DB"],
        limit=10,
        kind=DatasetContainerSubTypes.DATABASE,
        pattern_field="database_pattern",
    )
    assert truncated is False
    assert [n.name for n in nodes] == ["DEMO_DB", "SMOKE_TEST_DB"]
    assert all(n.kind == DatasetContainerSubTypes.DATABASE for n in nodes)
    assert all(n.pattern_field == "database_pattern" for n in nodes)
    # A top-level container has no parent, so name and fqn match.
    assert nodes[0].fqn == "DEMO_DB"


def test_container_nodes_bare_vs_qualified_fqn():
    bare, _ = container_nodes(
        ["PUBLIC"], 10, DatasetContainerSubTypes.SCHEMA, "schema_pattern"
    )
    assert bare[0].fqn == "PUBLIC"

    qualified, _ = container_nodes(
        ["PUBLIC"],
        10,
        DatasetContainerSubTypes.SCHEMA,
        "schema_pattern",
        fqn_prefix="DEMO_DB",
    )
    # Under match_fully_qualified_names, schema_pattern matches DATABASE.SCHEMA.
    assert qualified[0].name == "PUBLIC"
    assert qualified[0].fqn == "DEMO_DB.PUBLIC"


def test_container_nodes_respect_source_specific_labels():
    # BigQuery datasets: kind Dataset, tagged dataset_pattern, fqn PROJECT.DATASET.
    nodes, _ = container_nodes(
        ["town_hall_demo"],
        10,
        DatasetContainerSubTypes.BIGQUERY_DATASET,
        "dataset_pattern",
        fqn_prefix="calm-pagoda-323403",
    )
    assert nodes[0].kind == DatasetContainerSubTypes.BIGQUERY_DATASET
    assert nodes[0].pattern_field == "dataset_pattern"
    assert nodes[0].fqn == "calm-pagoda-323403.town_hall_demo"


def test_table_nodes_distinguish_views_and_qualify_fqn():
    nodes, truncated = table_nodes(
        tables=["orders", "customers"],
        views=["v_orders"],
        limit=10,
        fqn_prefix="DEMO_DB.PUBLIC",
    )
    assert truncated is False
    by_name = {n.name: n for n in nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].pattern_field == "table_pattern"
    assert by_name["orders"].fqn == "DEMO_DB.PUBLIC.orders"
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW
    assert by_name["v_orders"].pattern_field == "view_pattern"


def test_table_nodes_dedupe_view_also_listed_as_table():
    # Some dialects report a view in both listings; it must appear once, as a view.
    nodes, _ = table_nodes(
        tables=["shared"], views=["shared"], limit=10, fqn_prefix="db.s"
    )
    assert len(nodes) == 1
    assert nodes[0].kind == DatasetSubTypes.VIEW


def test_column_nodes_have_no_pattern_field():
    nodes, _ = column_nodes(
        [{"name": "id"}, {"name": "amount"}], limit=10, fqn_prefix="db.s.orders"
    )
    assert [n.name for n in nodes] == ["id", "amount"]
    assert nodes[0].fqn == "db.s.orders.id"
    assert all(n.pattern_field is None for n in nodes)


def test_truncation_flag_and_limit():
    nodes, truncated = container_nodes(
        ["a", "b", "c"], 2, DatasetContainerSubTypes.DATABASE, "database_pattern"
    )
    assert len(nodes) == 2
    assert truncated is True


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
    assert probe_hierarchy("kafka") is None


def test_container_nodes_without_classifier_default_included():
    nodes, _ = container_nodes(
        ["PUBLIC"], 10, DatasetContainerSubTypes.SCHEMA, "schema_pattern"
    )
    assert nodes[0].included is True
    assert nodes[0].excluded_by is None


def test_container_nodes_classifier_records_verdict():
    def classify(name, node_fqn):
        if name == "INFORMATION_SCHEMA":
            return (False, "default_schema")
        if name == "DENIED":
            return (False, "schema_pattern")
        return (True, None)

    nodes, _ = container_nodes(
        ["PUBLIC", "INFORMATION_SCHEMA", "DENIED"],
        10,
        DatasetContainerSubTypes.SCHEMA,
        "schema_pattern",
        classify=classify,
    )
    verdicts = {n.name: (n.included, n.excluded_by) for n in nodes}
    assert verdicts["PUBLIC"] == (True, None)
    assert verdicts["INFORMATION_SCHEMA"] == (False, "default_schema")
    assert verdicts["DENIED"] == (False, "schema_pattern")


def test_table_nodes_classifier_sees_is_view():
    def classify(name, node_fqn, is_view):
        return (not is_view, "view_pattern" if is_view else None)

    nodes, _ = table_nodes(
        tables=["orders"],
        views=["v_orders"],
        limit=10,
        fqn_prefix="s",
        classify=classify,
    )
    verdicts = {n.name: (n.included, n.excluded_by) for n in nodes}
    assert verdicts["orders"] == (True, None)
    assert verdicts["v_orders"] == (False, "view_pattern")


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
