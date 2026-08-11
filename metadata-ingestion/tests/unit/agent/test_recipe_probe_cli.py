from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent.probe_methods import ProbeMethodResult
from datahub.ingestion.agent.redact import collect_nested_secret_values


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
    from datahub.ingestion.agent.probe_methods import ProbeMethodSpec, ProbeParam

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


def test_report_to_writes_the_redacted_payload(monkeypatch, tmp_path):
    # The report file exists for a caller that captures a structured result
    # instead of parsing stdout, so it must carry no more than stdout does --
    # in particular the same redaction.
    import json

    from datahub.ingestion.agent.filter_check import FilterCheckResult, FilterVerdict

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
            "--names",
            "orders",
            "--report-to",
            str(out_file),
        ],
    )
    assert result.exit_code == 0
    written = json.loads(out_file.read_text())
    assert "s3cr3t" not in json.dumps(written)
    assert written["results"][0]["target"] == "***.orders"
