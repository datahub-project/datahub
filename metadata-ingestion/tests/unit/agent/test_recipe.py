import pytest

from datahub.ingestion.agent.recipe import scaffold, validate_recipe


def test_scaffold_uses_secret_refs():
    pytest.importorskip("snowflake.connector")
    recipe = scaffold("snowflake")
    source = recipe["source"]
    assert isinstance(source, dict)
    assert source["type"] == "snowflake"
    # every secret field is a ${...} ref, never a literal
    config_text = str(source["config"])
    assert "${" in config_text


def test_validate_flags_inline_secret():
    pytest.importorskip("snowflake.connector")
    recipe = scaffold("snowflake")
    source = recipe["source"]
    assert isinstance(source, dict)
    config = source["config"]
    assert isinstance(config, dict)
    # Force a plaintext secret into a known secret field.
    config["password"] = "hunter2"
    result = validate_recipe(recipe)
    warnings = result["warnings"]
    assert isinstance(warnings, list)
    assert any("plaintext" in w.lower() for w in warnings)


def test_validate_ref_secret_no_warning():
    pytest.importorskip("snowflake.connector")
    recipe = scaffold("snowflake")
    source = recipe["source"]
    assert isinstance(source, dict)
    config = source["config"]
    assert isinstance(config, dict)
    config["password"] = "${SNOWFLAKE_PASSWORD}"
    result = validate_recipe(recipe)
    warnings = result["warnings"]
    assert isinstance(warnings, list)
    assert not any("plaintext" in w.lower() for w in warnings)


def test_validate_bad_config_reports_errors():
    # A recipe missing required fields must be reported invalid, not crash.
    pytest.importorskip("snowflake.connector")
    result = validate_recipe({"source": {"type": "snowflake", "config": {}}})
    assert result["valid"] is False
    assert result["errors"]


def test_validate_unknown_source_type_no_crash():
    # An unknown source type must not crash; instead, degrade to invalid recipe.
    result = validate_recipe(
        {"source": {"type": "this-source-does-not-exist", "config": {}}}
    )
    assert result["valid"] is False
    assert result["errors"]


@pytest.mark.parametrize("bad", [[], "", 0])
def test_a_non_mapping_config_is_named_as_such(bad):
    """`or {}` swallowed every falsey value, so the error named whichever field
    happened to be required rather than the config's shape -- telling an agent
    to add host_port to a config that is a list."""
    result = validate_recipe({"source": {"type": "postgres", "config": bad}})
    assert result["valid"] is False
    assert result["errors"] == ["recipe.source.config must be a mapping"]


@pytest.mark.parametrize("recipe_config", [{"config": None}, {}])
def test_an_absent_or_null_config_still_means_no_config(recipe_config):
    """A bare "config:" is YAML null and is how a source needing no config is
    written, so it must not be rejected as malformed."""
    result = validate_recipe({"source": {"type": "demo-data", **recipe_config}})
    assert "must be a mapping" not in str(result["errors"])
