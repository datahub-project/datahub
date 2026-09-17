import io
import json
import pathlib
import sys

import pytest
from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent.filter_check import FilterCheckResult, FilterVerdict
from datahub.ingestion.agent.probe_methods import (
    BARE_FLAG,
    ProbeMethodResult,
    ProbeMethodSpec,
    ProbeParam,
    _coerce,
)
from datahub.ingestion.agent.redact import collect_nested_secret_values, redact
from datahub.ingestion.agent.verdicts import ProbeSoftError


@pytest.fixture(autouse=True)
def _isolate_secret_registry(monkeypatch):
    """The registry is a process-global singleton and masking is opt-out, so a
    secret registered by one test stays registered for the rest of the session
    and would silently mask it out of a later test's output. Loading a stdin
    envelope now registers, so contain it here rather than leave an
    order-dependent flake for someone to find."""
    from datahub.masking.secret_registry import SecretRegistry

    # Cleared IN PLACE rather than replaced. SecretMaskingFilter caches the
    # registry it was built with (masking_filter.py: `self._registry =
    # secret_registry or SecretRegistry.get_instance()`), and the `recipe`
    # group installs those filters on process-global handlers that outlive
    # this fixture. reset_instance() swaps the singleton underneath them, so
    # every handler installed by an earlier test goes on masking against a
    # dead registry and a later test's secrets reach the output unmasked --
    # the very leak this fixture exists to prevent, arriving by a different
    # door. Verified: with reset_instance(), an already-built filter masks
    # nothing registered afterwards; with clear(), it picks up the new
    # secrets and forgets the old ones.
    SecretRegistry.get_instance().clear()

    # The stdin envelope globals, for the same reason and in the same place.
    # Every test here used to reset _stdin_secrets in its own body -- 21
    # copies of one line, which is how one of them ends up forgotten. Tests
    # that dispatch the CLI get this from the `recipe` group callback too;
    # the ones calling rc._load_recipe("-") directly bypass the group, so the
    # fixture is what covers them.
    monkeypatch.setattr(rc, "_stdin_secrets", {}, raising=False)
    monkeypatch.setattr(rc, "_disclosed_recipe_values", set())

    yield
    SecretRegistry.get_instance().clear()


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
    # A bare flag is a sentinel, not the string "true": the parser does not
    # know the parameter's declared type, and guessing sent `--schema --table x`
    # to the driver as schema="true". _coerce holds the spec and decides.
    assert rc._parse_extra_params(("--verbose",)) == {"verbose": BARE_FLAG}


def test_a_bare_flag_is_refused_for_a_non_boolean_parameter():
    """`probe run columns --schema --table orders` used to parse as
    schema="true" and reach the driver as a real name -- on MySQL as
    SHOW CREATE TABLE `true`.`orders`, and on a dialect whose listing filters
    by name rather than erroring, as an empty result at exit 0."""
    with pytest.raises(ValueError, match="expects a str value but was given none"):
        _coerce(ProbeParam(name="schema", type="str", required=True), BARE_FLAG)


def test_a_bare_flag_is_still_true_for_a_boolean_parameter():
    param = ProbeParam(name="verbose", type="bool", required=False)
    assert _coerce(param, BARE_FLAG) is True


def test_an_unrecognised_boolean_value_is_refused_rather_than_read_as_false():
    """`--flag ture` returned a narrower listing and called it the answer,
    while the int branch beside it surfaced bad input as exit 2."""
    param = ProbeParam(name="include_system", type="bool", required=False)
    assert _coerce(param, "no") is False
    assert _coerce(param, "on") is True
    with pytest.raises(ValueError, match="expects a boolean"):
        _coerce(param, "ture")


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
        # list_probe_methods also takes the recipe config, so a
        # recipe-dependent kind can be reported without running the command.
        lambda st, config_dict=None: [
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

    def fake_list(st, config_dict=None):
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


# `--recipe -` accepts the same JSON envelope `datahub ingest -c -` does, so
# the executor can hand the probe its resolved credentials without writing
# them to the environment -- where they are readable from /proc/<pid>/environ
# and `ps e`, and inherited by every process the CLI spawns.


def _envelope(secrets):
    return json.dumps(
        {
            "__recipe_yaml__": (
                "source:\n"
                "  type: mysql\n"
                "  config:\n"
                "    host_port: h:3306\n"
                "    username: u\n"
                "    password: ${PROBE_TEST_REF}\n"
            ),
            "__secrets__": secrets,
        }
    )


def test_a_second_invocation_does_not_inherit_the_first_envelope(monkeypatch, tmp_path):
    """_stdin_secrets is module state, and only ever updated.

    Its comment called that safe because the value "is set at most once per
    process". That is true of `datahub recipe ...` as a one-shot CLI and
    false of every other way the group is dispatched -- this test file alone
    resets it by hand in 21 places, which is the workaround, not the
    contract.

    The consequence is not just staleness: a later recipe resolving ${REF}
    gets the EARLIER caller's credential, and registers it for masking as
    though it had been handed it. Cleared in the group callback, which runs
    exactly once per invocation.
    """
    monkeypatch.delenv("PROBE_TEST_REF", raising=False)
    runner = CliRunner()

    # First invocation: an envelope arrives on stdin.
    first = runner.invoke(
        recipe,
        ["validate", "-"],
        input=_envelope({"PROBE_TEST_REF": "from-the-first-run"}),
    )
    assert first.exit_code == 0, first.output
    # It really was populated, so the second half is testing something.
    assert rc._stdin_secrets == {"PROBE_TEST_REF": "from-the-first-run"}

    # Second invocation, same interpreter: a recipe from a FILE, no envelope
    # and no environment variable to resolve from.
    # Deliberately different plain values from _envelope()'s recipe, so the
    # disclosure assertion below can tell "cleared, then repopulated by THIS
    # recipe" from "still holding the previous one". _disclosed_recipe_values
    # is populated on both input paths now, so an emptiness check could not.
    path = tmp_path / "second.yml"
    path.write_text(
        "source:\n"
        "  type: mysql\n"
        "  config:\n"
        "    host_port: second-host:3307\n"
        "    username: second_user\n"
        "    password: ${PROBE_TEST_REF}\n"
    )
    runner.invoke(recipe, ["validate", str(path)])

    assert rc._stdin_secrets == {}, (
        "the second invocation can resolve ${PROBE_TEST_REF} from the first "
        "caller's envelope"
    )
    assert rc._disclosed_recipe_values == {"second-host:3307", "second_user"}, (
        "the second invocation is still holding what the first recipe disclosed"
    )


def test_a_ref_resolves_from_the_stdin_envelope_not_the_environment(monkeypatch):
    monkeypatch.delenv("PROBE_TEST_REF", raising=False)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(_envelope({"PROBE_TEST_REF": "resolved-from-envelope"})),
    )

    loaded = rc._load_recipe("-")
    source_type, config, secret_values = rc._resolve_for_probe(loaded)

    assert source_type == "mysql"
    assert config["password"] == "resolved-from-envelope"
    # and it is collected for masking, so it cannot reach the caller's output
    assert "resolved-from-envelope" in secret_values


def test_the_envelope_wins_over_a_same_named_environment_variable(monkeypatch):
    monkeypatch.setenv("PROBE_TEST_REF", "from-env")
    monkeypatch.setattr(
        "sys.stdin", io.StringIO(_envelope({"PROBE_TEST_REF": "from-stdin"}))
    )

    _t, config, _s = rc._resolve_for_probe(rc._load_recipe("-"))
    assert config["password"] == "from-stdin"


def test_an_envelope_secret_is_masked_even_if_the_recipe_never_uses_it(monkeypatch):
    """A value may arrive already substituted, so 'the recipe references it' is
    not a sound test for whether it must be masked. Mirrors load_config_file."""
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            _envelope({"PROBE_TEST_REF": "used", "UNREFERENCED": "also-secret"})
        ),
    )

    _t, _c, secret_values = rc._resolve_for_probe(rc._load_recipe("-"))
    assert "also-secret" in secret_values


def test_a_plain_recipe_on_stdin_still_works(monkeypatch):
    monkeypatch.setattr(
        "sys.stdin", io.StringIO("source:\n  type: mysql\n  config:\n    a: 1\n")
    )
    source = rc._load_recipe("-")["source"]
    assert isinstance(source, dict)
    assert source["type"] == "mysql"


def test_empty_stdin_is_a_user_error_not_a_traceback(monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO("   "))
    with pytest.raises(ValueError, match="no recipe received on stdin"):
        rc._load_recipe("-")


def test_a_non_mapping_recipe_on_stdin_is_refused(monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO("- just\n- a list\n"))
    with pytest.raises(ValueError, match="must be a YAML mapping"):
        rc._load_recipe("-")


# The four findings the review raised on the envelope work. Each asserts the
# observable behaviour -- what reaches stdout, what the verdict says -- rather
# than that a particular resolver was consulted.


def test_the_envelope_secret_does_not_reach_stdout(monkeypatch):
    """Membership in secret_values is not the claim worth pinning.

    The claim is that the value cannot be read off the command's output, which
    only the real CLI path -- resolve, run, redact, emit -- can demonstrate.
    """

    def fake_run(st, cfg, cmd, kwargs):
        # A connector echoing the resolved credential back in its payload is
        # exactly the leak the masking exists to stop.
        return ProbeMethodResult(
            st, cmd, kwargs, {"note": f"connected as {cfg['password']}"}
        )

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(
        recipe,
        ["probe", "run", "tables", "--recipe", "-"],
        input=_envelope({"PROBE_TEST_REF": "resolved-from-envelope"}),
    )
    assert "resolved-from-envelope" not in res.output
    assert "***" in res.output


def test_validate_accepts_a_ref_supplied_only_in_the_envelope(monkeypatch):
    """`validate -` and `probe run -` must agree about the same envelope.

    validate resolved with the environment chain only, so a ${REF} whose value
    was piped in was reported unresolvable while probe run accepted it.
    """
    monkeypatch.delenv("PROBE_TEST_REF", raising=False)
    res = CliRunner().invoke(
        recipe,
        ["validate", "-"],
        input=_envelope({"PROBE_TEST_REF": "resolved-from-envelope"}),
    )
    assert res.exit_code == 0, res.output
    assert "Could not resolve secret reference" not in res.output


def test_validate_masks_an_envelope_secret_it_never_resolved(monkeypatch):
    """A value can arrive already substituted, under a key no hint recognises.

    Nothing else in _secrets_in_recipe would collect it, so without the stdin
    floor a pydantic error quoting input_value= emits it in the clear.
    """
    envelope = json.dumps(
        {
            # `env` is checked against a fixed set and its error quotes the
            # value it rejected, so a secret landing there is echoed verbatim.
            "__recipe_yaml__": (
                "source:\n"
                "  type: mysql\n"
                "  config:\n"
                "    host_port: h:3306\n"
                "    env: leaky-value-here\n"
            ),
            "__secrets__": {"PROBE_TEST_REF": "leaky-value-here"},
        }
    )
    res = CliRunner().invoke(recipe, ["validate", "-"], input=envelope)
    assert "leaky-value-here" not in res.output
    assert "***" in res.output


def test_malformed_yaml_in_the_envelope_cannot_echo_the_credential(monkeypatch):
    """The parse fails before the caller has collected anything to mask against.

    A YAML error quotes the offending line, so the redaction has to read the
    envelope's secrets at raise time rather than at block entry.
    """
    envelope = json.dumps(
        {
            # Unclosed quote: the parser reports the line, which carries the value.
            "__recipe_yaml__": 'source:\n  type: mysql\n  bad: "leaky-value-here\n',
            "__secrets__": {"PROBE_TEST_REF": "leaky-value-here"},
        }
    )
    res = CliRunner().invoke(recipe, ["validate", "-"], input=envelope)
    assert res.exit_code != 0
    assert "leaky-value-here" not in res.output


def test_envelope_secrets_reach_the_masking_registry(monkeypatch):
    """The `recipe` group installs the masking backstop -- excepthook, logging
    handlers, stdout wrapper -- whose whole job is catching what the per-command
    redaction misses. Nothing ever registered a secret with it, so it had an
    empty pattern and masked nothing. `load_config_file` registers the envelope
    for `ingest -c -`; this path has to as well.
    """
    from datahub.masking.masking_filter import SecretMaskingFilter

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(_envelope({"PROBE_TEST_REF": "resolved-from-envelope"})),
    )
    rc._load_recipe("-")

    masked = SecretMaskingFilter().mask_text("connected as resolved-from-envelope")
    assert "resolved-from-envelope" not in masked


def test_a_null_envelope_secret_fails_to_resolve_instead_of_becoming_None(monkeypatch):
    """A caller that could not resolve a secret must not get a password of "None".

    str(v) turned JSON null into the string "None", so the probe connected with
    that as the credential and reported whatever the server said, instead of
    naming the reference it could not resolve. The registry will not mask that
    value either -- "none" is on its unmaskable-literals list -- so it would
    also have reached the output.
    """
    monkeypatch.delenv("PROBE_TEST_REF", raising=False)
    monkeypatch.setattr("sys.stdin", io.StringIO(_envelope({"PROBE_TEST_REF": None})))

    loaded = rc._load_recipe("-")
    with pytest.raises(ValueError, match=r"PROBE_TEST_REF"):
        rc._resolve_for_probe(loaded)


def test_an_empty_envelope_secret_does_not_fall_through_to_the_environment(
    monkeypatch,
):
    """An empty string is a value the caller chose, not a missing entry.

    The strings-only filter also dropped falsey ones, so `{"REF": ""}` left
    nothing for MappingResolver and EnvVarResolver went on to read the ambient
    variable -- the exact fall-through the envelope exists to prevent. A caller
    piping an empty credential must get an empty credential.
    """
    monkeypatch.setenv("PROBE_TEST_REF", "from-env")
    monkeypatch.setattr("sys.stdin", io.StringIO(_envelope({"PROBE_TEST_REF": ""})))

    _t, config, _s = rc._resolve_for_probe(rc._load_recipe("-"))
    assert config["password"] == ""


# A secret whose value collides with a plain, non-secret config value is not
# protectable: the recipe already states it, the report legitimately has to
# print it (`target` is a qualified identifier), and masking it is what tells
# a reader the secret equals the identifier they can already see.


def test_a_secret_equal_to_a_plain_config_value_is_not_masked(monkeypatch):
    """A password that happens to equal the database name must not blank the
    database name out of every verdict.

    `target` exists to report the string a pattern matched. Masking the
    container turns "datahub.orders" into "***.orders", which is unreadable
    AND discloses the collision -- the recipe plainly says `database: datahub`,
    so the mask is the only new information in the output.
    """
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "__recipe_yaml__": (
                        "source:\n"
                        "  type: mysql\n"
                        "  config:\n"
                        "    host_port: h:3306\n"
                        "    database: datahub\n"
                        "    username: u\n"
                        "    password: ${PROBE_TEST_REF}\n"
                    ),
                    "__secrets__": {"PROBE_TEST_REF": "datahub"},
                }
            )
        ),
    )

    _t, config, secret_values = rc._resolve_for_probe(rc._load_recipe("-"))

    assert config["password"] == "datahub"
    assert "datahub" not in secret_values
    # and so a verdict can still name what it matched
    assert redact({"target": "datahub.orders"}, secret_values) == {
        "target": "datahub.orders"
    }


def test_a_secret_matching_an_inline_secret_field_is_still_masked(monkeypatch):
    """The exemption is for NON-secret config values only.

    The trap: a recipe with `password: p` AND `database: p` makes the value
    both an inline secret and a plain config value. An unconditional
    exemption unmasks the credential -- the report travels further than the
    recipe does, to GMS, the logs and an LLM. Only a value the raw recipe does
    not carry under a sensitive key is exempt.
    """
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "__recipe_yaml__": (
                        "source:\n"
                        "  type: mysql\n"
                        "  config:\n"
                        "    host_port: h:3306\n"
                        "    username: u\n"
                        "    password: hunter2\n"
                        "    database: hunter2\n"
                    ),
                    "__secrets__": {},
                }
            )
        ),
    )

    _t, _config, secret_values = rc._resolve_for_probe(rc._load_recipe("-"))
    assert "hunter2" in secret_values


def test_a_ref_under_an_unrecognised_key_is_still_masked(monkeypatch):
    """The exemption reads the RAW recipe, not the resolved config.

    Reading the resolved config would collect a ${ref}-sourced secret living
    under a key no sensitivity hint matches, and then exempt it from masking --
    unmasking the very secret the ${ref} collection exists to catch. A raw
    plain literal cannot be a resolved secret, so raw-only is safe.
    """
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "__recipe_yaml__": (
                        "source:\n"
                        "  type: mysql\n"
                        "  config:\n"
                        "    host_port: h:3306\n"
                        "    username: u\n"
                        "    password: p\n"
                        "    options:\n"
                        "      some_odd_key: ${PROBE_TEST_REF}\n"
                    ),
                    "__secrets__": {"PROBE_TEST_REF": "not-a-database-name"},
                }
            )
        ),
    )

    _t, _config, secret_values = rc._resolve_for_probe(rc._load_recipe("-"))
    assert "not-a-database-name" in secret_values


def test_an_envelope_secret_the_recipe_discloses_is_not_registered(monkeypatch):
    """The masking backstop is the third place this value gets blanked.

    Even with redaction exempting it, registering it with the registry masks
    the child's whole stdout stream -- so `datahub.ingestion.source.sql` logs
    as `***REDACTED:PW***.ingestion.source.sql`, and those lines become the
    task's operator-visible logs. Same rule, same reason: the recipe states
    the identifier in the clear, so masking it protects nothing.
    """
    from datahub.masking.masking_filter import SecretMaskingFilter

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "__recipe_yaml__": (
                        "source:\n"
                        "  type: mysql\n"
                        "  config:\n"
                        "    host_port: h:3306\n"
                        "    database: probe_db\n"
                        "    password: ${PROBE_TEST_REF}\n"
                    ),
                    "__secrets__": {"PROBE_TEST_REF": "probe_db"},
                }
            )
        ),
    )
    rc._load_recipe("-")

    assert SecretMaskingFilter().mask_text("probe_db.orders") == "probe_db.orders"


def test_an_envelope_secret_the_recipe_does_not_disclose_is_still_registered(
    monkeypatch,
):
    """The exemption must not become a hole in the backstop."""
    from datahub.masking.masking_filter import SecretMaskingFilter

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "__recipe_yaml__": (
                        "source:\n"
                        "  type: mysql\n"
                        "  config:\n"
                        "    host_port: h:3306\n"
                        "    database: probe_db\n"
                        "    password: ${PROBE_TEST_REF}\n"
                    ),
                    "__secrets__": {"PROBE_TEST_REF": "an-actual-password"},
                }
            )
        ),
    )
    rc._load_recipe("-")

    masked = SecretMaskingFilter().mask_text("connected as an-actual-password")
    assert "an-actual-password" not in masked


def test_a_malformed_envelope_recipe_still_registers_its_secrets(monkeypatch):
    """Registration happens before the YAML is parsed on purpose, so a parse
    error quoting the offending document is already covered. Computing the
    exemption must not disturb that: an unparseable recipe discloses nothing.
    """
    from datahub.masking.masking_filter import SecretMaskingFilter

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "__recipe_yaml__": 'source:\n  bad: "unterminated\n',
                    "__secrets__": {"PROBE_TEST_REF": "still-a-secret"},
                }
            )
        ),
    )
    with pytest.raises(ValueError):
        rc._load_recipe("-")

    masked = SecretMaskingFilter().mask_text("leaked still-a-secret")
    assert "still-a-secret" not in masked


def test_the_error_path_honours_the_same_disclosure_exemption(monkeypatch):
    """The exemption has to hold on the failure path too, or it half-works.

    A secret equal to a plain recipe identifier is dropped from the redaction
    set so `probe filter` can still print `target` -- masking a value the
    recipe states in the clear only corrupts output and announces the
    collision. The error path rebuilt its set with _with_stdin_secrets, which
    unions every envelope value straight back in, so `could not connect to
    analytics` came back with the database name blanked after all.
    """
    monkeypatch.setattr(rc, "_stdin_secrets", {"PW": "analytics"}, raising=False)
    monkeypatch.setattr(rc, "_disclosed_recipe_values", {"analytics"})

    assert rc._with_stdin_secrets(set()) == set()

    # A genuine envelope secret is still added.
    monkeypatch.setattr(
        rc, "_stdin_secrets", {"PW": "analytics", "TOK": "t0k3nvalue"}, raising=False
    )
    assert rc._with_stdin_secrets(set()) == {"t0k3nvalue"}


def test_a_malformed_envelope_is_a_user_error_not_an_internal_one(monkeypatch):
    """Exit codes are the agent's control flow, so the wrong one misroutes it.

    A `__recipe_yaml__` that is not a string fell through to yaml.safe_load
    and surfaced as `'dict' object has no attribute 'read'` at exit 1 --
    an internal-error code, and an implementation detail as the message. The
    agent reads exit 1 as "something broke, maybe retry" when the answer is
    "you built the envelope wrong, fix it", which is exit 2.
    """
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(json.dumps({"__recipe_yaml__": {"source": {}}, "__secrets__": {}})),
    )

    with pytest.raises(ValueError, match="__recipe_yaml__"):
        rc._recipe_from_stdin()


def test_the_cli_hands_over_a_valueless_flag_without_guessing(monkeypatch, tmp_path):
    """The CLI's half of the bare-flag contract.

    `--schema` with no value is a token the parser cannot type-check: it sees
    tokens, not the declared parameter. Its job is therefore to hand over the
    BARE_FLAG sentinel and let the spec decide, rather than guess "true" and
    send a plausible-looking schema name to the driver. _coerce's refusal of
    that sentinel is tested directly above; this pins the half that would
    otherwise regress silently -- a CLI that guessed would still satisfy the
    _coerce test, because _coerce would never see a sentinel.

    Note what this test must NOT do: stub run_probe_method and then assert
    exit 2. Coercion happens inside run_probe_method, so stubbing it removes
    the very refusal being checked -- the first draft of this test did that
    and "failed", which was the stub talking, not the code.
    """
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    seen: dict = {}

    def fake_run(st, cfg, cmd, kwargs):
        seen.update(kwargs)
        return ProbeMethodResult(st, cmd, kwargs, [])

    monkeypatch.setattr(rc, "run_probe_method", fake_run)

    CliRunner().invoke(
        recipe,
        [
            "probe",
            "run",
            "foreign_keys",
            "--recipe",
            _recipe_file(tmp_path),
            "--schema",
            "--table",
            "orders",
        ],
    )

    assert seen["schema"] is BARE_FLAG, seen
    assert seen["schema"] != "true"
    # The neighbouring value is untouched -- the sentinel is for the flag that
    # was given none, not for everything after it.
    assert seen["table"] == "orders", seen


def test_clearing_the_registry_keeps_installed_filters_working():
    """Why the fixture above clears instead of resetting.

    A filter caches the registry it was constructed with, and the `recipe`
    group installs filters on process-global handlers. Swapping the singleton
    leaves those handlers masking against the old one, so secrets registered
    by a later test are not masked at all -- a leak that looks like a passing
    test, because the assertion "the secret is absent from the output" is
    also satisfied when nothing was ever there to mask.
    """
    from datahub.masking.masking_filter import SecretMaskingFilter
    from datahub.masking.secret_registry import SecretRegistry

    registry = SecretRegistry.get_instance()
    registry.register_secret("PW", "earlier-secret-value")
    installed = SecretMaskingFilter()

    registry.clear()
    SecretRegistry.get_instance().register_secret("PW", "later-secret-value")

    assert "later-secret-value" not in installed.mask_text("saw later-secret-value")
    # ...and the previous test's secret is genuinely gone, which is the
    # isolation the fixture is for.
    assert "earlier-secret-value" in installed.mask_text("saw earlier-secret-value")


def test_an_unserializable_value_becomes_a_bounded_string_not_its_internals():
    """_json_default used to return o.__dict__.

    json.dumps then walked whatever the library hung on the object -- a
    driver error carries connection state, and redaction afterwards only
    knows the values it collected, so an unregistered credential nested a
    few attributes deep went out in the clear. str(o) is bounded and is
    what the caller can act on.
    """
    from pydantic import SecretStr

    class _DriverError(Exception):
        def __init__(self) -> None:
            super().__init__("connection failed")
            self.conn = "postgresql://u:" + "hunter2" + "@h/db"

    rendered = json.dumps({"err": _DriverError()}, default=rc._json_default)
    assert "hunter2" not in rendered, rendered
    assert "connection failed" in rendered

    # SecretStr is still handled first: its __dict__ holds _secret_value.
    assert "s3kret" not in json.dumps(
        {"pw": SecretStr("s3kret")}, default=rc._json_default
    )


def test_a_report_file_is_never_left_half_written(tmp_path):
    """json.dump truncates on open, so a serialization failure partway
    through leaves a file that reads as valid output. The default= makes
    that unreachable."""

    class _Odd:
        def __str__(self) -> str:
            return "odd-value"

    target = tmp_path / "report.json"
    rc._write_report(str(target), {"a": 1, "b": _Odd()})

    written = json.loads(target.read_text())
    assert written == {"a": 1, "b": "odd-value"}


def test_the_report_file_is_masked_like_stdout(tmp_path, monkeypatch):
    """stdout and --report-to must not disagree about a secret.

    `_emit` goes through the registry-backed stdout wrapper bootstrap
    installs; a file write does not touch it. With a secret registered but
    not collected into a command's `secret_values` -- an envelope secret the
    command never referenced -- stdout masked it and the file carried it in
    the clear.

    Per-command redaction does not cover this: `probe methods` writes its
    payload with no _redacted_payload at all.
    """
    from datahub.masking.secret_registry import SecretRegistry

    secret = "envelope" + "-only-credential"
    SecretRegistry.get_instance().register_secrets_batch({"ENV_PW": secret})

    target = tmp_path / "report.json"
    rc._write_report(str(target), {"note": f"value is {secret}"})

    written = target.read_text()
    assert secret not in written, "the report file carried an unmasked secret"
    assert "REDACTED" in written


def test_a_malformed_envelope_still_registers_its_secrets(monkeypatch):
    """The failure path is where unmasked values do the most damage.

    recipe_cli registers envelope secrets before parsing the YAML on
    purpose. Extracting the parser reversed that -- it raised on a bad
    `__recipe_yaml__` before reading `__secrets__`, so the caller never saw
    the secrets it was about to need while reporting the failure.
    """
    from datahub.masking.masking_filter import SecretMaskingFilter

    secret = "malformed" + "-envelope-secret"
    envelope = json.dumps(
        {"__recipe_yaml__": {"not": "a string"}, "__secrets__": {"PW": secret}}
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(envelope))

    with pytest.raises(ValueError, match="__recipe_yaml__"):
        rc._recipe_from_stdin()

    assert secret not in SecretMaskingFilter().mask_text(f"leaked {secret}"), (
        "the envelope's secrets were not registered before it failed"
    )
