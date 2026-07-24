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
