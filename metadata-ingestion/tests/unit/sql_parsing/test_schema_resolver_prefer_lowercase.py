"""
Unit tests for SchemaResolver prefer_lowercase parameter.

Tests the fix for issue #13792: MSSQL View Ingestion Causes Lowercased Lineage Table URNs
"""

import pytest

from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import SchemaResolver


def test_schema_resolver_default_behavior():
    """Test that default behavior (prefer_lowercase=None) uses platform-based defaults."""
    # Test with a case-insensitive platform (mssql)
    resolver_mssql = SchemaResolver(platform="mssql", env="PROD")
    assert (
        resolver_mssql._prefers_urn_lower() is True
    )  # MSSQL not in PLATFORMS_WITH_CASE_SENSITIVE_TABLES

    # Test with a case-sensitive platform (bigquery)
    resolver_bq = SchemaResolver(platform="bigquery", env="PROD")
    assert (
        resolver_bq._prefers_urn_lower() is False
    )  # BigQuery is in PLATFORMS_WITH_CASE_SENSITIVE_TABLES

    resolver_mssql.close()
    resolver_bq.close()


def test_schema_resolver_prefer_lowercase_true():
    """Test that prefer_lowercase=True forces lowercase URNs."""
    # Even for case-sensitive platforms
    resolver_bq = SchemaResolver(platform="bigquery", env="PROD", prefer_lowercase=True)
    assert resolver_bq._prefers_urn_lower() is True

    # And for case-insensitive platforms
    resolver_mssql = SchemaResolver(platform="mssql", env="PROD", prefer_lowercase=True)
    assert resolver_mssql._prefers_urn_lower() is True

    resolver_bq.close()
    resolver_mssql.close()


def test_schema_resolver_prefer_lowercase_false():
    """Test that prefer_lowercase=False forces preserving original case."""
    # Even for case-insensitive platforms
    resolver_mssql = SchemaResolver(
        platform="mssql", env="PROD", prefer_lowercase=False
    )
    assert resolver_mssql._prefers_urn_lower() is False

    # And for case-sensitive platforms
    resolver_bq = SchemaResolver(
        platform="bigquery", env="PROD", prefer_lowercase=False
    )
    assert resolver_bq._prefers_urn_lower() is False

    resolver_mssql.close()
    resolver_bq.close()


def test_schema_resolver_urn_generation_with_prefer_lowercase():
    """Test that URN generation respects prefer_lowercase setting."""
    table = _TableName(database="TestDB", db_schema="TestSchema", table="TestTable")

    # Test with prefer_lowercase=False (preserve case)
    resolver_preserve = SchemaResolver(
        platform="mssql", env="PROD", prefer_lowercase=False
    )
    urn_preserve, _ = resolver_preserve.resolve_table(table)
    assert "TestDB.TestSchema.TestTable" in urn_preserve
    assert (
        "testdb.testschema.testtable" not in urn_preserve.lower()
        or "TestDB" in urn_preserve
    )

    # Test with prefer_lowercase=True (force lowercase)
    resolver_lower = SchemaResolver(platform="mssql", env="PROD", prefer_lowercase=True)
    urn_lower, _ = resolver_lower.resolve_table(table)
    assert "testdb.testschema.testtable" in urn_lower
    assert "TestDB" not in urn_lower

    resolver_preserve.close()
    resolver_lower.close()


def test_schema_resolver_backward_compatibility():
    """Test that existing code without prefer_lowercase parameter still works."""
    # Create resolver without prefer_lowercase parameter (should use platform defaults)
    resolver = SchemaResolver(
        platform="postgres",
        platform_instance="my_instance",
        env="PROD",
    )

    # Should use platform-based default (postgres is case-insensitive)
    assert resolver._prefers_urn_lower() is True

    resolver.close()


@pytest.mark.parametrize(
    "platform,prefer_lowercase,expected",
    [
        # Case-insensitive platforms (mssql, postgres, mysql, etc.) default to lowercase
        ("mssql", None, True),
        ("mssql", True, True),
        ("mssql", False, False),
        ("postgres", None, True),
        ("postgres", False, False),
        ("mysql", None, True),
        ("mysql", False, False),
        # Case-sensitive platforms (bigquery, db2) default to preserving case
        ("bigquery", None, False),
        ("bigquery", True, True),
        ("bigquery", False, False),
        ("db2", None, False),
        ("db2", True, True),
        ("db2", False, False),
    ],
)
def test_prefer_lowercase_override_parametrized(
    platform: str, prefer_lowercase: bool, expected: bool
) -> None:
    """Test that prefer_lowercase override works correctly for various platforms."""
    resolver = SchemaResolver(
        platform=platform, env="PROD", prefer_lowercase=prefer_lowercase
    )
    assert resolver._prefers_urn_lower() is expected
    resolver.close()


def test_mssql_issue_13792_scenario():
    """
    Test the exact scenario from issue #13792.

    MSSQL view referencing a mixed-case table should generate matching URNs
    when convert_urns_to_lowercase=False.
    """
    # Simulate MSSQL with convert_urns_to_lowercase=False
    resolver = SchemaResolver(platform="mssql", env="PROD", prefer_lowercase=False)

    # Table name from the issue: Data Governance.MDM.sales.MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER
    table = _TableName(
        database="Data Governance",
        db_schema="MDM.sales",
        table="MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER",
    )

    urn, _ = resolver.resolve_table(table)

    # URN should preserve the original case
    assert "Data Governance.MDM.sales.MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER" in urn
    assert "data governance.mdm.sales.mdm_connectionrole_eco_partners_master" not in urn

    resolver.close()


def test_mssql_with_convert_urns_to_lowercase_true():
    """
    Test MSSQL with convert_urns_to_lowercase=True (the workaround).

    This should continue to work as before (backward compatibility).
    """
    # Simulate MSSQL with convert_urns_to_lowercase=True
    resolver = SchemaResolver(platform="mssql", env="PROD", prefer_lowercase=True)

    table = _TableName(
        database="Data Governance",
        db_schema="MDM.sales",
        table="MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER",
    )

    urn, _ = resolver.resolve_table(table)

    # URN should be lowercase
    assert "data governance.mdm.sales.mdm_connectionrole_eco_partners_master" in urn
    assert "Data Governance" not in urn

    resolver.close()


def test_urn_consistency_table_and_view_lineage():
    """
    Test that table URN and view lineage URN are consistent when using same resolver.

    This is the core issue from #13792: view lineage URNs must match table URNs
    for lineage to resolve correctly in DataHub.
    """
    resolver = SchemaResolver(platform="mssql", env="PROD", prefer_lowercase=False)

    # Simulate table ingestion
    table = _TableName(database="MyDB", db_schema="dbo", table="CustomerTable")
    table_urn, _ = resolver.resolve_table(table)

    # Simulate view lineage resolution (same table referenced from a view)
    view_upstream_table = _TableName(
        database="MyDB", db_schema="dbo", table="CustomerTable"
    )
    view_upstream_urn, _ = resolver.resolve_table(view_upstream_table)

    # URNs must match exactly for lineage to work
    assert table_urn == view_upstream_urn, (
        f"Table URN and view lineage URN must match: {table_urn} != {view_upstream_urn}"
    )

    resolver.close()


def test_db2_case_sensitive_platform():
    """Test that db2 (case-sensitive platform) defaults to preserving case."""
    resolver = SchemaResolver(platform="db2", env="PROD")

    # Default should preserve case for db2
    assert resolver._prefers_urn_lower() is False

    table = _TableName(database="MYDB", db_schema="MYSCHEMA", table="MyTable")
    urn, _ = resolver.resolve_table(table)

    # URN should preserve case
    assert "MYDB.MYSCHEMA.MyTable" in urn

    resolver.close()


def test_schema_resolver_with_platform_instance():
    """Test that prefer_lowercase works correctly with platform_instance."""
    resolver = SchemaResolver(
        platform="mssql",
        platform_instance="my_instance",
        env="PROD",
        prefer_lowercase=False,
    )

    table = _TableName(database="TestDB", db_schema="dbo", table="TestTable")
    urn, _ = resolver.resolve_table(table)

    # Should preserve case and include platform instance
    assert "TestDB.dbo.TestTable" in urn
    assert resolver._prefers_urn_lower() is False

    resolver.close()
