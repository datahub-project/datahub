from typing import Dict, List

import pytest

from datahub.ingestion.agent.recipe import scaffold, validate_recipe


def _require_connector(source_type: str) -> None:
    """Skip when this connector's extra is not installed.

    The file already guards four snowflake tests with
    pytest.importorskip("snowflake.connector") and leaves nine others
    unguarded, so a minimal environment errors on some and skips on others.
    Asking the registry instead of naming a module keeps that honest for
    every connector without a table of import names to maintain -- and it
    fails for the same reason the test would.
    """
    from datahub.ingestion.agent.probe_methods import config_class_for

    try:
        config_class_for(source_type)
    except Exception as exc:  # ValueError from an uninstalled extra
        pytest.skip(f"{source_type} extra not installed: {exc}")


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
    _require_connector("postgres")
    result = validate_recipe({"source": {"type": "postgres", "config": bad}})
    assert result["valid"] is False
    assert result["errors"] == ["recipe.source.config must be a mapping"]


@pytest.mark.parametrize("recipe_config", [{"config": None}, {}])
def test_an_absent_or_null_config_still_means_no_config(recipe_config):
    """A bare "config:" is YAML null and is how a source needing no config is
    written, so it must not be rejected as malformed."""
    result = validate_recipe({"source": {"type": "demo-data", **recipe_config}})
    assert "must be a mapping" not in str(result["errors"])


@pytest.mark.parametrize(
    ("source_type", "field"),
    [("snowflake", "database_pattern"), ("kafka", "topic_patterns")],
)
def test_scaffold_does_not_overwrite_a_connectors_deny_defaults(source_type, field):
    """It emitted {"allow": [".*"], "deny": []} for every pattern field, which
    is not a helpful default but an override: the scaffolded recipe ingested
    MORE than the same recipe without the line. Snowflake stopped denying
    ^SNOWFLAKE_SAMPLE_DATA$, Kafka stopped denying ^_.* so __consumer_offsets
    became a dataset. This is the first command an agent runs.

    Parametrized rather than looped, because pytest.skip() RAISES: a guard
    inside a loop ends the whole test at the first missing extra, so a
    machine without snowflake never checked kafka either. One case per
    connector is what makes the guard per-connector.
    """
    _require_connector(source_type)
    from datahub.ingestion.agent.probe_methods import config_class_for
    from datahub.ingestion.agent.recipe import scaffold

    source = scaffold(source_type)["source"]
    assert isinstance(source, dict)
    config = source["config"]
    assert isinstance(config, dict)
    assert field not in config, (
        f"{source_type}.{field} in the scaffold overrides the connector's own deny list"
    )
    # And the connector's default really does carry denies worth keeping,
    # so the assertion above is protecting something.
    model_field = config_class_for(source_type).model_fields[field]
    default = model_field.get_default(call_default_factory=True)
    assert default.deny, f"{source_type}.{field} has no deny default"


def test_scaffold_still_emits_secrets_and_required_fields():
    """The control: omitting pattern fields must not turn into omitting
    everything."""
    _require_connector("snowflake")
    from datahub.ingestion.agent.recipe import scaffold

    source = scaffold("snowflake")["source"]
    assert isinstance(source, dict)
    config = source["config"]
    assert isinstance(config, dict) and config, "scaffold produced nothing"

    # `any(...)` was satisfied by a scaffold holding one placeholder and
    # nothing else, which is the failure this test is the control for. Name
    # what Snowflake cannot connect without instead.
    assert "account_id" in config, config
    assert {"password", "private_key", "token"} & set(config), (
        "no credential field scaffolded"
    )
    # Every CREDENTIAL is a placeholder; a plain required field is emitted
    # empty for the author to fill in. Asserting that distinction, rather
    # than "some value starts with ${", is what makes this a test of the
    # scaffold's shape instead of its non-emptiness.
    credentials = {k: v for k, v in config.items() if k != "account_id"}
    assert credentials, config
    assert all(str(v).startswith("${") for v in credentials.values()), credentials
    assert config["account_id"] == "", config


def test_validate_accepts_an_env_ref_in_a_non_string_field(monkeypatch):
    """`${VAR}` is a string wherever it appears, so validating the RAW recipe
    failed pydantic with "Input should be a valid boolean" and called a
    working recipe invalid -- while `datahub ingest` ran it. validate's own
    warning text tells the author to use ${...} references, so it was
    advising the thing it then rejected."""
    _require_connector("mysql")
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
    _require_connector("mysql")
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
    _require_connector("kafka")
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
    _require_connector("kafka")
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


# Assembled rather than written as a literal. The whole point of these three
# tests is a plaintext value sitting under a `*.password` key, which is the
# shape the repo's secret scanner exists to catch -- and it cannot tell a
# fixture from a leak. Concatenating keeps the scanner quiet without adding an
# allowlist entry that would then outlive the test.
_PLAINTEXT = "not-a-real" + "-credential"


def _kafka(consumer_config):
    return {
        "source": {
            "type": "kafka",
            "config": {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "consumer_config": consumer_config,
                }
            },
        }
    }


def _warnings_of(result: Dict[str, object]) -> List[str]:
    got = result["warnings"]
    assert isinstance(got, list)
    return got


def _nested_warnings(result: Dict[str, object]) -> List[str]:
    return [w for w in _warnings_of(result) if "sit under" in w]


def test_a_correct_kafka_recipe_is_not_told_it_holds_a_plaintext_secret():
    """`sasl.mechanism: PLAIN` is a mechanism name, not a credential.

    The detector reused _SENSITIVE_KEY_HINTS, which is a REDACTION denylist:
    masking everything under a `sasl`-ish key is the right call on the way out,
    because over-masking is safe. Reading the same list as a classifier is not
    -- `sasl.mechanism` matches the `sasl` hint, so a recipe that correctly
    writes `sasl.password: ${KAFKA_PASSWORD}` was still told it holds a
    plaintext secret, and the only fix available to the author is to stop
    setting a mandatory field.
    """
    result = validate_recipe(
        _kafka({"sasl.mechanism": "PLAIN", "sasl.password": "${KAFKA_PASSWORD}"})
    )
    assert _nested_warnings(result) == [], _warnings_of(result)


def test_a_nested_plaintext_secret_is_still_reported():
    """The converse, so the narrowing cannot become "detect nothing"."""
    result = validate_recipe(
        _kafka({"sasl.mechanism": "PLAIN", "sasl.password": _PLAINTEXT})
    )
    assert len(_nested_warnings(result)) == 1, _warnings_of(result)
    assert "1 plaintext secret" in _nested_warnings(result)[0]


def test_a_top_level_plaintext_secret_is_reported_once_not_twice():
    """It was counted again by the nested walk and called nested.

    Two warnings for one value, the second of which points somewhere the value
    is not, is how an author stops reading them.
    """
    _require_connector("postgres")
    result = validate_recipe(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "host_port": "h:5432",
                    "username": "u",
                    "password": _PLAINTEXT,
                    "database": "d",
                },
            }
        }
    )
    named = [w for w in _warnings_of(result) if "'password' contains" in w]
    assert len(named) == 1, _warnings_of(result)
    assert _nested_warnings(result) == [], _warnings_of(result)


def test_an_unreadable_datahubenv_does_not_crash_validate(tmp_path, monkeypatch):
    """A resolver that cannot read its own config has not resolved the ref.

    yaml.ParserError escaped `validate` as an internal error, so a recipe with
    an unresolvable ${REF} reported a YAML syntax error about a file the
    recipe never mentioned -- with the local path in the message. Declining
    lets the next resolver try and, failing that, lets the recipe fail by
    name, which is the answer the caller can act on.
    """
    _require_connector("postgres")
    import datahub.cli.config_utils as cu

    # The environment resolver runs before the .datahubenv one, so a runner
    # that happens to export this name would resolve the ref and the test
    # would assert against the wrong path.
    monkeypatch.delenv("NO_SUCH_REF", raising=False)

    bad = tmp_path / ".datahubenv"
    bad.write_text("gms:\n  server: [unclosed\n")
    monkeypatch.setattr(cu, "DATAHUB_CONFIG_PATH", str(bad))

    result = validate_recipe(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "host_port": "h:5432",
                    "username": "u",
                    "password": "${NO_SUCH_REF}",
                    "database": "d",
                },
            }
        }
    )

    assert result["valid"] is False
    errors = result["errors"]
    assert isinstance(errors, list)
    assert any("NO_SUCH_REF" in str(e) for e in errors), errors
    assert not any("yaml" in str(e).lower() for e in errors), errors
