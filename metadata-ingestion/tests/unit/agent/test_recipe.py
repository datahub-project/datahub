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


def test_scaffold_does_not_overwrite_a_connectors_deny_defaults():
    """It emitted {"allow": [".*"], "deny": []} for every pattern field, which
    is not a helpful default but an override: the scaffolded recipe ingested
    MORE than the same recipe without the line. Snowflake stopped denying
    ^SNOWFLAKE_SAMPLE_DATA$, Kafka stopped denying ^_.* so __consumer_offsets
    became a dataset. This is the first command an agent runs."""
    from datahub.ingestion.agent.probe_methods import config_class_for
    from datahub.ingestion.agent.recipe import scaffold

    for source_type, field in (
        ("snowflake", "database_pattern"),
        ("kafka", "topic_patterns"),
    ):
        source = scaffold(source_type)["source"]
        assert isinstance(source, dict)
        config = source["config"]
        assert isinstance(config, dict)
        assert field not in config, (
            f"{source_type}.{field} in the scaffold overrides the connector's "
            f"own deny list"
        )
        # And the connector's default really does carry denies worth keeping,
        # so the assertion above is protecting something.
        model_field = config_class_for(source_type).model_fields[field]
        default = model_field.get_default(call_default_factory=True)
        assert default.deny, f"{source_type}.{field} has no deny default"


def test_scaffold_still_emits_secrets_and_required_fields():
    """The control: omitting pattern fields must not turn into omitting
    everything."""
    from datahub.ingestion.agent.recipe import scaffold

    source = scaffold("snowflake")["source"]
    assert isinstance(source, dict)
    config = source["config"]
    assert isinstance(config, dict) and config, "scaffold produced nothing"
    assert any(str(v).startswith("${") for v in config.values()), (
        "no secret placeholders emitted"
    )


def test_validate_accepts_an_env_ref_in_a_non_string_field(monkeypatch):
    """`${VAR}` is a string wherever it appears, so validating the RAW recipe
    failed pydantic with "Input should be a valid boolean" and called a
    working recipe invalid -- while `datahub ingest` ran it. validate's own
    warning text tells the author to use ${...} references, so it was
    advising the thing it then rejected."""
    from datahub.ingestion.agent.recipe import validate_recipe

    monkeypatch.setenv("PROFILING_ENABLED", "true")
    result = validate_recipe(
        {
            "source": {
                "type": "mysql",
                "config": {
                    "host_port": "h:3306",
                    "username": "u",
                    "password": "p",
                    "profiling": {"enabled": "${PROFILING_ENABLED}"},
                },
            }
        }
    )
    assert result["valid"] is True, result["errors"]


def test_validate_names_a_reference_it_cannot_resolve(monkeypatch):
    """Still an error -- a recipe pointing at an unset variable does not work
    here -- but named, which beats a type complaint about the literal
    "${VAR}"."""
    from datahub.ingestion.agent.recipe import validate_recipe

    monkeypatch.delenv("NOPE_UNSET_VAR", raising=False)
    result = validate_recipe(
        {
            "source": {
                "type": "mysql",
                "config": {
                    "host_port": "h:3306",
                    "username": "u",
                    "password": "${NOPE_UNSET_VAR}",
                },
            }
        }
    )
    assert result["valid"] is False
    errors = result["errors"]
    assert isinstance(errors, list)
    assert any("NOPE_UNSET_VAR" in e for e in errors), errors


def test_validate_sees_a_plaintext_secret_nested_in_a_free_form_dict():
    """The top-level sweep reads only fields describe_source classifies as
    SECRET, so kafka's connection.consumer_config['sasl.password'] came back
    as a clean recipe. The redactor already treats it as a secret -- the one
    command whose job is to say so was the only thing not asking."""
    from datahub.ingestion.agent.recipe import validate_recipe

    result = validate_recipe(
        {
            "source": {
                "type": "kafka",
                "config": {
                    "connection": {
                        "bootstrap": "localhost:9092",
                        "consumer_config": {"sasl.password": "hunter2-plaintext"},
                    }
                },
            }
        }
    )
    found = result["warnings"]
    assert isinstance(found, list)
    assert any("plaintext secret" in w for w in found), found
    # And the value itself never appears -- this warning exists to keep it out
    # of the transcript.
    assert not any("hunter2-plaintext" in w for w in found)


def test_a_referenced_nested_secret_is_not_warned_about():
    """The control: a ${REF} is the thing the warning asks for, so warning
    about it would train the reader to ignore the warning."""
    from datahub.ingestion.agent.recipe import validate_recipe

    result = validate_recipe(
        {
            "source": {
                "type": "kafka",
                "config": {
                    "connection": {
                        "bootstrap": "localhost:9092",
                        "consumer_config": {"sasl.password": "${KAFKA_PASSWORD}"},
                    }
                },
            }
        }
    )
    found = result["warnings"]
    assert isinstance(found, list)
    assert not any("plaintext secret" in w for w in found)
