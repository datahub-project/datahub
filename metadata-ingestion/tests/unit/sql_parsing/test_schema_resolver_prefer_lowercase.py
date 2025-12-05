"""
Unit tests for SchemaResolver prefer_lowercase parameter.

Tests the fix for issue #13792: MSSQL View Ingestion Causes Lowercased Lineage Table URNs
"""

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_common import PLATFORMS_WITH_CASE_SENSITIVE_TABLES
from datahub.sql_parsing.sqlglot_lineage import _TableName


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


def test_all_platforms_with_prefer_lowercase_override():
    """Test that prefer_lowercase override works for all platforms."""
    test_platforms = ["mssql", "postgres", "mysql", "oracle", "snowflake", "bigquery"]

    for platform in test_platforms:
        # Test with prefer_lowercase=False
        resolver_false = SchemaResolver(
            platform=platform, env="PROD", prefer_lowercase=False
        )
        assert resolver_false._prefers_urn_lower() is False, (
            f"Failed for {platform} with prefer_lowercase=False"
        )
        resolver_false.close()

        # Test with prefer_lowercase=True
        resolver_true = SchemaResolver(
            platform=platform, env="PROD", prefer_lowercase=True
        )
        assert resolver_true._prefers_urn_lower() is True, (
            f"Failed for {platform} with prefer_lowercase=True"
        )
        resolver_true.close()

        # Test with prefer_lowercase=None (default)
        resolver_default = SchemaResolver(
            platform=platform, env="PROD", prefer_lowercase=None
        )
        expected = platform not in PLATFORMS_WITH_CASE_SENSITIVE_TABLES
        assert resolver_default._prefers_urn_lower() == expected, (
            f"Failed for {platform} with prefer_lowercase=None"
        )
        resolver_default.close()


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
