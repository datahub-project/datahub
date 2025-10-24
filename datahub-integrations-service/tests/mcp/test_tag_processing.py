"""Tests for GraphQL tag processing functions."""

from datahub_integrations.mcp.mcp_server import (
    _disable_cloud_fields,
    _disable_newer_gms_fields,
    _enable_cloud_fields,
    _enable_newer_gms_fields,
)


def test_enable_cloud_fields():
    """Test that CLOUD tags are stripped when enabling."""
    query = """
    field1
    field2  #[CLOUD]
    field3 {  #[CLOUD]
      subfield  #[CLOUD]
    }  #[CLOUD]
    field4
    """
    result = _enable_cloud_fields(query)

    assert "#[CLOUD]" not in result
    assert "field1" in result
    assert "field2" in result
    assert "field3 {" in result


def test_disable_cloud_fields():
    """Test that CLOUD-tagged lines are commented out when disabling."""
    query = """
    field1
    field2  #[CLOUD]
    field3 {  #[CLOUD]
      subfield  #[CLOUD]
    }  #[CLOUD]
    field4
    """
    result = _disable_cloud_fields(query)

    assert "#     field2  #[CLOUD]" in result  # Note: leading spaces preserved
    assert "#     field3 {  #[CLOUD]" in result
    assert "#       subfield  #[CLOUD]" in result
    assert "    field1" in result  # field1 should NOT be commented
    assert "    field4" in result  # field4 should NOT be commented


def test_enable_newer_gms_fields():
    """Test that NEWER_GMS tags are stripped when enabling."""
    query = """
    field1
    field2  #[NEWER_GMS]
    field3 {  #[NEWER_GMS]
      subfield  #[NEWER_GMS]
    }  #[NEWER_GMS]
    field4
    """
    result = _enable_newer_gms_fields(query)

    assert "#[NEWER_GMS]" not in result
    assert "field2" in result
    assert "field3 {" in result


def test_disable_newer_gms_fields():
    """Test that NEWER_GMS-tagged lines are commented out when disabling."""
    query = """
    field1
    field2  #[NEWER_GMS]
    field3 {  #[NEWER_GMS]
      subfield  #[NEWER_GMS]
    }  #[NEWER_GMS]
    field4
    """
    result = _disable_newer_gms_fields(query)

    assert "#     field2  #[NEWER_GMS]" in result  # Note: leading spaces preserved
    assert "#     field3 {  #[NEWER_GMS]" in result
    assert "#       subfield  #[NEWER_GMS]" in result
    assert "    field1" in result  # field1 should NOT be commented
    assert "    field4" in result  # field4 should NOT be commented


def test_combined_tags_scenario():
    """Test a realistic scenario with combined CLOUD and NEWER_GMS tags."""
    query = """
    query {
      dataset {
        name
        statsSummary {  #[CLOUD]
          queryCount  #[CLOUD]
          rowCount  #[CLOUD]
        }  #[CLOUD]
        documentation {  #[CLOUD]
          text  #[CLOUD]
        }  #[CLOUD]
        otherField  #[NEWER_GMS]
      }
    }
    """

    # Scenario 1: Cloud instance (CLOUD fields shown, NEWER_GMS shown)
    result = _enable_cloud_fields(query)
    result = _enable_newer_gms_fields(result)

    assert "#[CLOUD]" not in result
    assert "#[NEWER_GMS]" not in result
    assert "statsSummary {" in result
    assert "queryCount" in result
    assert "documentation {" in result
    assert "otherField" in result

    # Scenario 2: OSS instance (CLOUD fields hidden, NEWER_GMS shown)
    result = _disable_cloud_fields(query)
    result = _enable_newer_gms_fields(result)

    # CLOUD-tagged lines should be commented
    assert "#         statsSummary {  #[CLOUD]" in result
    assert "#           queryCount  #[CLOUD]" in result
    assert "#         documentation {  #[CLOUD]" in result
    # But NEWER_GMS fields should be visible
    assert "otherField" in result
    assert "#[NEWER_GMS]" not in result
