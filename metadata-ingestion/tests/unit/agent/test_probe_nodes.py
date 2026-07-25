import pytest

from datahub.ingestion.agent.probe import (
    ProbeBranchesError,
    ProbeShapeNode,
    column_nodes,
    probe_hierarchy,
    probe_shape,
)
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


def test_probe_shape_derives_a_chain_for_a_linear_source():
    # No connector declares a probe_shape() classmethod today, so this proves
    # the derived-from-hierarchy() path (case 2 in probe_shape's docstring).
    shape = probe_shape("sqlalchemy")
    assert shape is not None
    assert shape.kind == DatasetContainerSubTypes.SCHEMA
    assert [c.kind for c in shape.children] == [DatasetSubTypes.TABLE]


def test_probe_shape_prefers_the_connectors_own_classmethod(monkeypatch):
    import datahub.ingestion.agent.probe as probe_mod

    tree = ProbeShapeNode("Workspace", [])

    class FakeConfig:
        @classmethod
        def probe_shape(cls) -> ProbeShapeNode:
            return tree

    monkeypatch.setattr(probe_mod, "_config_class", lambda source_type: FakeConfig)
    assert probe_shape("bi-thing") is tree


def test_probe_shape_raises_for_a_branching_probe_with_no_classmethod(monkeypatch):
    # A branching connector that hasn't (yet) added its own probe_shape()
    # classmethod is a connector bug -- probe_shape() must say so, not report
    # the source as unsupported (supported: false would be a wrong answer:
    # the source *is* probe-capable, just not derivable from hierarchy()).
    import datahub.ingestion.agent.probe as probe_mod

    class FakeConfig:
        @classmethod
        def probe_hierarchy(cls):
            raise ProbeBranchesError(
                "this probe branches, so its shape is a tree, not a chain; "
                "use shape() instead of hierarchy()"
            )

    monkeypatch.setattr(probe_mod, "_config_class", lambda source_type: FakeConfig)
    with pytest.raises(ValueError, match="probe_shape"):
        probe_shape("bi-thing")


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
