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


def _snowflake_recipe(tmp_path):
    recipe_file = tmp_path / "sf.yml"
    recipe_file.write_text(
        "source:\n  type: snowflake\n  config:\n    account_id: acct\n"
    )
    return recipe_file


def test_probe_databases_builds_empty_parent_path(tmp_path, monkeypatch):
    pytest.importorskip("snowflake.connector")
    import datahub.cli.recipe_cli as mod
    from datahub.ingestion.agent.models import ProbeResult

    captured = {}

    def fake_probe(source_type, config, parent_path, limit):
        captured["parent_path"] = parent_path
        return ProbeResult(
            source_type=source_type, supported=True, parent_path=parent_path
        )

    monkeypatch.setattr(mod, "probe", fake_probe)
    result = CliRunner().invoke(
        recipe, ["probe", "databases", "--recipe", str(_snowflake_recipe(tmp_path))]
    )
    assert result.exit_code == 0
    assert captured["parent_path"] == []


def test_probe_schemas_on_snowflake_requires_database(tmp_path):
    pytest.importorskip("snowflake.connector")
    result = CliRunner().invoke(
        recipe, ["probe", "schemas", "--recipe", str(_snowflake_recipe(tmp_path))]
    )
    # Snowflake's schema level sits under a database, so --database is mandatory.
    assert result.exit_code == 2
    assert "--database" in (result.output + (result.stderr or ""))


def test_probe_tables_on_snowflake_qualifies_parent_path(tmp_path, monkeypatch):
    pytest.importorskip("snowflake.connector")
    import datahub.cli.recipe_cli as mod
    from datahub.ingestion.agent.models import ProbeResult

    captured = {}

    def fake_probe(source_type, config, parent_path, limit):
        captured["parent_path"] = parent_path
        return ProbeResult(
            source_type=source_type, supported=True, parent_path=parent_path
        )

    monkeypatch.setattr(mod, "probe", fake_probe)
    result = CliRunner().invoke(
        recipe,
        [
            "probe",
            "tables",
            "--recipe",
            str(_snowflake_recipe(tmp_path)),
            "--database",
            "DEMO_DB",
            "--schema",
            "PUBLIC",
        ],
    )
    assert result.exit_code == 0
    assert captured["parent_path"] == ["DEMO_DB", "PUBLIC"]


def _bigquery_recipe(tmp_path):
    recipe_file = tmp_path / "bq.yml"
    recipe_file.write_text(
        "source:\n  type: bigquery\n  config:\n    project_ids: [my-project]\n"
    )
    return recipe_file


def test_probe_databases_on_bigquery_lists_projects(tmp_path, monkeypatch):
    pytest.importorskip("google.cloud.bigquery")
    import datahub.cli.recipe_cli as mod
    from datahub.ingestion.agent.models import ProbeResult

    captured = {}

    def fake_probe(source_type, config, parent_path, limit):
        captured["parent_path"] = parent_path
        return ProbeResult(
            source_type=source_type, supported=True, parent_path=parent_path
        )

    monkeypatch.setattr(mod, "probe", fake_probe)
    result = CliRunner().invoke(
        recipe, ["probe", "databases", "--recipe", str(_bigquery_recipe(tmp_path))]
    )
    # BigQuery's top container is a project, which `probe databases` accepts.
    assert result.exit_code == 0
    assert captured["parent_path"] == []


def test_probe_schemas_on_bigquery_requires_database(tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    result = CliRunner().invoke(
        recipe, ["probe", "schemas", "--recipe", str(_bigquery_recipe(tmp_path))]
    )
    # A BigQuery dataset sits under a project, so --database is mandatory.
    assert result.exit_code == 2
    assert "--database" in (result.output + (result.stderr or ""))


def _rest_recipe(tmp_path):
    r = tmp_path / "rest.yml"
    r.write_text(
        "probe:\n"
        "  kind: rest\n"
        "  base_url: https://api.example.com\n"
        "  headers:\n"
        '    Authorization: "Bearer ${API_TOKEN}"\n'
        "  endpoints: [/v1/orders]\n"
    )
    return r


def test_probe_api_resolves_secret_and_hides_it(tmp_path, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "topsecret")
    import datahub.cli.recipe_cli as mod
    from datahub.ingestion.agent.api_probe import ApiEndpointProbe, ApiProbeResult

    captured = {}

    def fake_probe_api(base_url, endpoints, headers=None, budget=10, verify_ssl=True):
        captured["headers"] = headers
        return ApiProbeResult(
            base_url=base_url,
            endpoints=[ApiEndpointProbe(endpoint=endpoints[0], status=200)],
        )

    monkeypatch.setattr(mod, "probe_api", fake_probe_api)
    result = CliRunner().invoke(
        recipe, ["probe", "api", "--recipe", str(_rest_recipe(tmp_path))]
    )
    assert result.exit_code == 0
    # The ${API_TOKEN} ref was resolved in-process before reaching probe_api...
    assert captured["headers"]["Authorization"] == "Bearer topsecret"
    # ...but the resolved value never appears in the agent-visible output.
    assert "topsecret" not in result.output


def test_probe_api_error_output_redacts_secret(tmp_path, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "topsecret")
    import datahub.cli.recipe_cli as mod

    def boom(*a, **k):
        raise ValueError("auth failed for Bearer topsecret")

    monkeypatch.setattr(mod, "probe_api", boom)
    result = CliRunner().invoke(
        recipe, ["probe", "api", "--recipe", str(_rest_recipe(tmp_path))]
    )
    assert result.exit_code == 2
    assert "topsecret" not in result.output
    assert "topsecret" not in (result.stderr or "")


def test_probe_api_rejects_non_rest_kind(tmp_path):
    r = tmp_path / "bad.yml"
    r.write_text("probe:\n  kind: graphql\n  base_url: https://x\n  endpoints: [/a]\n")
    result = CliRunner().invoke(recipe, ["probe", "api", "--recipe", str(r)])
    assert result.exit_code == 2
