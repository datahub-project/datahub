import json

from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent.filter_check import FilterCheckResult, FilterVerdict
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    ProbeMethodSpec,
    ProbeParam,
)
from datahub.ingestion.agent.redact import collect_nested_secret_values
from datahub.ingestion.agent.verdicts import ProbeSoftError


def _recipe_file(tmp_path):
    p = tmp_path / "r.yml"
    p.write_text("source:\n  type: postgres\n  config: {}\n")
    return str(p)


def test_parse_extra_params():
    assert rc._parse_extra_params(("--schema", "sales", "--table", "orders")) == {
        "schema": "sales",
        "table": "orders",
    }
    assert rc._parse_extra_params(("--limit=10",)) == {"limit": "10"}
    assert rc._parse_extra_params(("--verbose",)) == {"verbose": "true"}


def test_probe_run(monkeypatch, tmp_path):
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    seen: dict = {}

    def fake_run(st, cfg, cmd, kwargs):
        seen.update(cmd=cmd, kwargs=kwargs)
        return ProbeMethodResult(st, cmd, kwargs, [{"ok": True}])

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "run",
            "foreign_keys",
            "--recipe",
            _recipe_file(tmp_path),
            "--schema",
            "sales",
            "--table",
            "orders",
        ],
    )
    assert res.exit_code == 0, res.output
    assert seen == {
        "cmd": "foreign_keys",
        "kwargs": {"schema": "sales", "table": "orders"},
    }


def test_probe_methods_lists(monkeypatch, tmp_path):

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        rc,
        "list_probe_methods",
        lambda st: [
            ProbeMethodSpec("foreign_keys", [ProbeParam("table", "str", True)], "FKs.")
        ],
    )
    res = CliRunner().invoke(
        recipe, ["probe", "methods", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 0
    assert "foreign_keys" in res.output and "FKs." in res.output


def test_collect_nested_secret_values():
    cfg = {"connection": {"consumer_config": {"sasl.password": "hunter2", "x": "ok"}}}
    vals = collect_nested_secret_values(cfg, ("password", "sasl"))
    assert "hunter2" in vals and "ok" not in vals


def test_probe_methods_redacts_error(monkeypatch, tmp_path):
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda r: ("kafka", {}, {"topsecret"})
    )

    def fake_list(st):
        raise ValueError("boom topsecret")

    monkeypatch.setattr(rc, "list_probe_methods", fake_list)
    res = CliRunner().invoke(
        recipe, ["probe", "methods", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code != 0
    assert "topsecret" not in res.output
    assert "***" in res.output


def test_probe_run_normalizes_then_redacts(monkeypatch, tmp_path):
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda r: ("postgres", {}, {"topsecret"})
    )

    def fake_run(st, cfg, cmd, kwargs):
        return ProbeMethodResult(st, cmd, kwargs, {"value": "has topsecret inside"})

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "run",
            "foreign_keys",
            "--recipe",
            _recipe_file(tmp_path),
        ],
    )
    assert res.exit_code == 0, res.output
    assert "topsecret" not in res.output
    assert "***" in res.output


def test_malformed_yaml_is_a_bad_argument_not_a_connection_failure(tmp_path):
    """YAMLError is not a ValueError, so it reached the connection handler."""
    p = tmp_path / "bad.yml"
    p.write_text("source:\n  type: postgres\n   config: [unclosed\n")
    res = CliRunner().invoke(recipe, ["validate", str(p)])
    assert res.exit_code == 2, res.output
    assert "cannot parse recipe file" in res.output


def test_an_unwritable_report_path_is_a_bad_argument(monkeypatch, tmp_path):
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        rc,
        "run_probe_method",
        lambda st, cfg, cmd, kwargs: ProbeMethodResult(st, cmd, kwargs, {"a": 1}),
    )
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "run",
            "tables",
            "--recipe",
            _recipe_file(tmp_path),
            "--report-to",
            str(tmp_path / "no_such_dir" / "r.json"),
        ],
    )
    assert res.exit_code == 2, res.output
    assert "cannot write report" in res.output


def test_a_soft_error_exits_on_the_bad_argument_code(monkeypatch, tmp_path):
    """A soft error reports that the caller named something absent, so it is a
    bad argument (2), not an unreachable source (3). Reported as 3, an agent
    retries the connection instead of fixing the name it passed."""
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))

    def fake_run(st, cfg, cmd, kwargs):
        raise ProbeSoftError("no report named 'nope' found in space 's'")

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(
        recipe, ["probe", "run", "reports", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 2, res.output
    assert "no report named" in res.output


def test_report_to_writes_the_redacted_payload(monkeypatch, tmp_path):
    # The report file exists for a caller that captures a structured result
    # instead of parsing stdout, so it must carry no more than stdout does --
    # in particular the same redaction.

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("mysql", {}, {"s3cr3t"}))
    monkeypatch.setattr(
        rc,
        "check_filters",
        lambda **kw: FilterCheckResult(
            source_type="mysql",
            kind="Table",
            parent_path=["s3cr3t"],
            pattern_field="table_pattern",
            results=[
                FilterVerdict(
                    name="orders",
                    target="s3cr3t.orders",
                    included=False,
                    excluded_by="table_pattern",
                )
            ],
        ),
    )
    out_file = tmp_path / "report.json"
    result = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--kind",
            "Table",
            "--name",
            "orders",
            "--report-to",
            str(out_file),
        ],
    )
    assert result.exit_code == 0
    written = json.loads(out_file.read_text())
    assert "s3cr3t" not in json.dumps(written)
    assert written["results"][0]["target"] == "***.orders"


def test_a_name_containing_a_comma_is_judged_whole(monkeypatch, tmp_path):
    """--name is exact, not comma-separated.

    Mode collections are human-named and a quoted SQL identifier may contain a
    comma, so splitting on it would judge two names that do not exist and report
    both as excluded -- a wrong answer that looks like a real verdict. The
    executor previously skipped such names with a warning because the CLI could
    not express them.
    """
    seen = {}

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("mysql", {}, set()))

    def fake_check(**kwargs):
        seen.update(kwargs)
        from datahub.ingestion.agent.filter_check import FilterCheckResult

        return FilterCheckResult(
            source_type="mysql",
            kind="Space",
            parent_path=[],
            pattern_field="space_pattern",
            results=[],
        )

    monkeypatch.setattr(rc, "check_filters", fake_check)
    result = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--kind",
            "Space",
            "--name",
            "Finance, EMEA",
            "--name",
            "Sales",
        ],
    )
    assert result.exit_code == 0, result.output
    assert seen["names"] == ["Finance, EMEA", "Sales"]
