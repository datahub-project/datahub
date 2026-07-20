import json

import pytest
from click.testing import CliRunner

from datahub.cli.recipe_cli import recipe


def test_describe_outputs_json():
    pytest.importorskip("snowflake.connector")
    result = CliRunner().invoke(recipe, ["describe", "snowflake"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["source_type"] == "snowflake"
    assert any(f["kind"] == "secret" for f in payload["fields"])


def test_describe_unknown_source_exit_2():
    result = CliRunner().invoke(recipe, ["describe", "nope-not-real"])
    assert result.exit_code == 2


def test_missing_recipe_file_exit_2():
    result = CliRunner().invoke(recipe, ["validate", "/no/such/recipe.yml"])
    assert result.exit_code == 2


def test_recipe_validate_reports_json(tmp_path):
    pytest.importorskip("snowflake.connector")
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: snowflake\n  config: {}\n")
    result = CliRunner().invoke(recipe, ["validate", str(recipe_file)])
    payload = json.loads(result.output)
    assert payload["valid"] is False


def test_probe_error_output_redacts_secret(tmp_path, monkeypatch):
    pytest.importorskip("snowflake.connector")
    monkeypatch.setenv("MY_PW", "s3cr3t")
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text(
        "source:\n"
        "  type: snowflake\n"
        "  config:\n"
        "    account_id: my-account\n"
        "    password: '${MY_PW}'\n"
    )

    import datahub.cli.recipe_cli as mod

    def boom(*a, **k):
        raise RuntimeError("connection failed for account with password s3cr3t")

    monkeypatch.setattr(mod, "probe", boom)
    result = CliRunner().invoke(
        recipe, ["probe", "schemas", "--recipe", str(recipe_file)]
    )
    assert "s3cr3t" not in result.output
    assert "s3cr3t" not in (result.stderr or "")


def test_probe_error_output_redacts_nested_secret(tmp_path, monkeypatch):
    pytest.importorskip("snowflake.connector")
    monkeypatch.setenv("NESTED_PW", "nestedsecret")
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text(
        "source:\n"
        "  type: snowflake\n"
        "  config:\n"
        "    account_id: my-account\n"
        "    oauth_config:\n"
        "      client_secret: '${NESTED_PW}'\n"
    )

    import datahub.cli.recipe_cli as mod

    def boom(*a, **k):
        raise RuntimeError("connection failed with client_secret nestedsecret")

    monkeypatch.setattr(mod, "probe", boom)
    result = CliRunner().invoke(
        recipe, ["probe", "schemas", "--recipe", str(recipe_file)]
    )
    assert "nestedsecret" not in result.output
    assert "nestedsecret" not in (result.stderr or "")


def test_json_default_masks_secret_str():
    from pydantic import SecretStr

    from datahub.cli.recipe_cli import _json_default

    report = {"password": SecretStr("topsecret"), "host": "example"}
    serialized = json.dumps(report, default=_json_default)
    assert "topsecret" not in serialized
    assert "***" in serialized
