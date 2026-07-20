import json

import pytest
from click.testing import CliRunner

from datahub.cli.agent_cli import agent


def test_describe_outputs_json():
    pytest.importorskip("snowflake.connector")
    result = CliRunner().invoke(agent, ["describe", "snowflake"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["source_type"] == "snowflake"
    assert any(f["kind"] == "secret" for f in payload["fields"])


def test_describe_unknown_source_exit_2():
    result = CliRunner().invoke(agent, ["describe", "nope-not-real"])
    assert result.exit_code == 2


def test_recipe_validate_reports_json(tmp_path):
    pytest.importorskip("snowflake.connector")
    recipe = tmp_path / "r.yml"
    recipe.write_text("source:\n  type: snowflake\n  config: {}\n")
    result = CliRunner().invoke(agent, ["recipe", "validate", str(recipe)])
    payload = json.loads(result.output)
    assert payload["valid"] is False
