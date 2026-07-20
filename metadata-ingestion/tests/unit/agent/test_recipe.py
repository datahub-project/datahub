import pytest

from datahub.ingestion.agent.recipe import scaffold, validate_recipe


def test_scaffold_uses_secret_refs():
    pytest.importorskip("snowflake.connector")
    recipe = scaffold("snowflake")
    assert recipe["source"]["type"] == "snowflake"
    # every secret field is a ${...} ref, never a literal
    config_text = str(recipe["source"]["config"])
    assert "${" in config_text


def test_validate_flags_inline_secret():
    pytest.importorskip("snowflake.connector")
    recipe = scaffold("snowflake")
    # Force a plaintext secret into a known secret field.
    recipe["source"]["config"]["password"] = "hunter2"
    result = validate_recipe(recipe)
    assert any("plaintext" in w.lower() for w in result["warnings"])


def test_validate_ref_secret_no_warning():
    pytest.importorskip("snowflake.connector")
    recipe = scaffold("snowflake")
    recipe["source"]["config"]["password"] = "${SNOWFLAKE_PASSWORD}"
    result = validate_recipe(recipe)
    assert not any("plaintext" in w.lower() for w in result["warnings"])


def test_validate_bad_config_reports_errors():
    # A recipe missing required fields must be reported invalid, not crash.
    pytest.importorskip("snowflake.connector")
    result = validate_recipe({"source": {"type": "snowflake", "config": {}}})
    assert result["valid"] is False
    assert result["errors"]
