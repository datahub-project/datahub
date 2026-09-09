import json
import pathlib

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


def test_a_read_the_provider_could_not_complete_does_not_exit_zero(
    monkeypatch, tmp_path
):
    """The cardinal case: an empty result that is not an empty source.

    A connector reusing its ingestion fetchers records an unreadable endpoint
    with report.failure(), which nothing used to read -- so a 403 on Mode's
    data_sources came back as {"result": {}, "warnings": []} at exit 0, exactly
    what a workspace with no data sources returns. The partial result is still
    emitted; what changes is that the command no longer claims success.
    """
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("mode", {}, set()))

    def fake_run(st, cfg, cmd, kwargs):
        return ProbeMethodResult(
            st,
            cmd,
            kwargs,
            {},
            failures=["Failed to retrieve Data Sources: 403 Forbidden"],
        )

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(
        recipe, ["probe", "run", "data_sources", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code != 0, res.output
    # The result and the reason both reach the caller.
    assert "403 Forbidden" in res.output
    assert '"failures"' in res.output


def test_an_unreachable_source_exits_on_the_connection_code(monkeypatch, tmp_path):
    """The other half of the exit-code contract, which nothing asserted.

    Every fix in this area moved a case from 3 to 2, so only the 2 side was
    pinned -- widening the ValueError family to `except Exception` made exit 3
    unreachable with the whole suite still green, which would tell an agent
    that every connection failure was its own fault.
    """
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))

    def fake_run(st, cfg, cmd, kwargs):
        raise RuntimeError("could not connect to the server")

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(
        recipe, ["probe", "run", "tables", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 3, res.output
    assert "could not connect" in res.output


def test_a_missing_plugin_extra_is_a_bad_argument_not_a_traceback(tmp_path):
    """ConfigurationError is MetaError, not ValueError, so it escaped every
    ladder -- and it is the likeliest first-contact failure there is. Its
    message carries the `pip install` hint, which was being dropped."""
    res = CliRunner().invoke(recipe, ["describe", "definitely-not-a-real-source"])
    assert res.exit_code == 2, res.output
    assert '"error"' in res.output


def test_a_malformed_try_pattern_is_a_bad_argument_not_a_traceback(tmp_path):
    """AllowDenyPattern compiles lazily inside .allowed(), and re.error is not a
    ValueError -- so a bad pattern crashed the command whose whole job is
    diagnosing patterns."""
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--kind",
            "Table",
            "--name",
            "t",
            "--try-allow",
            "[",
        ],
    )
    assert res.exit_code == 2, res.output
    assert '"error"' in res.output


def test_a_connection_free_command_never_reports_an_unreachable_source(
    monkeypatch, tmp_path
):
    """probe filter opens no connection, so EXIT_CONNECTION would send an agent
    to retry something it never attempted. Unclassifiable failures there are
    EXIT_INTERNAL."""

    def boom(**kwargs):
        raise RuntimeError("something unexpected")

    monkeypatch.setattr(rc, "check_filters", boom)
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--kind",
            "Table",
            "--name",
            "t",
        ],
    )
    assert res.exit_code == rc.EXIT_INTERNAL, res.output
    assert res.exit_code != rc.EXIT_CONNECTION


def test_validate_redacts_a_resolved_secret(monkeypatch, tmp_path):
    """validate was the one command with no redaction at all -- while being the
    command whose job is warning about plaintext secrets. A pydantic
    ValidationError embeds input_value=, so it could echo the secret back."""
    monkeypatch.setenv("PROBE_TEST_PW", "s3cr3t-value")
    p = tmp_path / "r.yml"
    p.write_text(
        "source:\n  type: postgres\n  config:\n"
        "    host_port: localhost:5432\n    username: u\n"
        "    password: ${PROBE_TEST_PW}\n"
    )
    res = CliRunner().invoke(recipe, ["validate", str(p)])
    assert "s3cr3t-value" not in res.output


def test_a_failed_connection_test_does_not_exit_zero(monkeypatch, tmp_path):
    """The report was emitted but never consulted, so a failed test exited 0 --
    in a CLI whose contract is that the caller reads the exit code, the one
    command named after reaching the source did not use it."""
    from datahub.ingestion.api.source import (
        CapabilityReport,
        TestConnectionReport,
    )

    class _Failing:
        @staticmethod
        def test_connection(config_dict):
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason="bad credentials"
                )
            )

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _Failing,
    )
    monkeypatch.setattr(
        "datahub.ingestion.api.source.TestableSource", _Failing, raising=False
    )
    res = CliRunner().invoke(
        recipe, ["test-connection", "--recipe", _recipe_file(tmp_path)]
    )
    # The report still reaches stdout; only the exit code changes.
    assert res.exit_code == 3, res.output
    assert "bad credentials" in res.output


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


def test_the_failure_message_is_redacted_like_everything_else(monkeypatch, tmp_path):
    """The payload was masked and then the raw failure strings were joined onto
    stderr -- and those come from driver and report text, the very channel this
    CLI treats as leaky. A secret masked on stdout reached the agent on the
    error line."""
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("mode", {}, {"s3cr3t-pw"}))
    monkeypatch.setattr(
        rc,
        "run_probe_method",
        lambda st, cfg, cmd, kwargs: ProbeMethodResult(
            st,
            cmd,
            kwargs,
            {},
            failures=["auth failed for postgresql://u:s3cr3t-pw@db/x"],
        ),
    )
    res = CliRunner().invoke(
        recipe, ["probe", "run", "data_sources", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code != 0
    assert "s3cr3t-pw" not in res.output
    assert "***" in res.output


def test_an_internal_failure_with_no_connectivity_report_does_not_exit_zero(
    monkeypatch, tmp_path
):
    """A connector can fail before it reaches basic_connectivity, leaving
    capable None and only internal_failure set -- which exited 0, the very
    thing the capable check was added to stop."""
    from datahub.ingestion.api.source import TestConnectionReport

    class _Failing:
        @staticmethod
        def test_connection(config_dict):
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason="client blew up before connecting",
            )

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _Failing,
    )
    monkeypatch.setattr(
        "datahub.ingestion.api.source.TestableSource", _Failing, raising=False
    )
    res = CliRunner().invoke(
        recipe, ["test-connection", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 3, res.output
    # And it names the field the reason is in. Pointing at basic_connectivity
    # here sent the caller after a key this report does not carry.
    assert "internal_failure_reason" in res.output


# --- when redaction eats the answer ------------------------------------------


def _colliding_recipe(tmp_path: pathlib.Path, secret: str) -> str:
    """A recipe whose password equals its database name."""
    path = tmp_path / "collide.yml"
    path.write_text(
        "source:\n"
        "  type: mysql\n"
        "  config:\n"
        "    host_port: localhost:3306\n"
        "    username: probe_user\n"
        f"    password: {secret}\n"
        f"    database: {secret}\n"
    )
    return str(path)


def test_a_masked_target_says_it_was_masked(tmp_path):
    """`probe filter` exists to report the target a pattern was matched against.

    A password equal to a schema or table name masks that name everywhere it
    occurs -- correctly, since the two are the same string and nothing can tell
    them apart -- so `target` reads "***.orders". Over-masking is the safe
    failure and stays. Silently over-masking is not: an unexplained "***" in the
    one field the command exists to produce is exactly the unreadable answer
    this interface is meant to avoid.
    """
    recipe_path = _colliding_recipe(tmp_path, "shared_name_value")
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            recipe_path,
            "--kind",
            "Table",
            "--parent",
            "shared_name_value",
            "--name",
            "orders",
        ],
    )
    assert res.exit_code == 0, res.output
    payload = json.loads(res.output)

    # The masking itself is unchanged -- this is not a licence to leak.
    assert "shared_name_value" not in res.output
    assert any("***" in v.get("target", "") for v in payload["results"])

    # ...but the caller is told why.
    assert any("redacted" in w for w in payload["warnings"]), payload["warnings"]


def test_no_notice_when_nothing_was_masked(tmp_path):
    """The notice must not cry wolf: it fires on actual redaction, not on the
    mere presence of a secret in the recipe."""
    path = tmp_path / "clean.yml"
    path.write_text(
        "source:\n"
        "  type: mysql\n"
        "  config:\n"
        "    host_port: localhost:3306\n"
        "    username: probe_user\n"
        "    password: a_distinct_password_value\n"
        "    database: my_db\n"
    )
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            str(path),
            "--kind",
            "Table",
            "--parent",
            "my_db",
            "--name",
            "orders",
        ],
    )
    assert res.exit_code == 0, res.output
    payload = json.loads(res.output)
    assert payload["results"][0]["target"] == "my_db.orders"
    assert not any("redacted" in w for w in payload["warnings"]), payload["warnings"]


def test_the_notice_does_not_say_which_secret_collided(tmp_path):
    """Naming the field would tell a caller who cannot see a ${ENV_VAR} secret
    that it equals an identifier they can see."""
    recipe_path = _colliding_recipe(tmp_path, "shared_name_value")
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            recipe_path,
            "--kind",
            "Table",
            "--parent",
            "shared_name_value",
            "--name",
            "orders",
        ],
    )
    assert res.exit_code == 0, res.output
    notice = next(w for w in json.loads(res.output)["warnings"] if "redacted" in w)
    assert "password" not in notice.split("a password the same as")[0]
    assert "shared_name_value" not in notice
