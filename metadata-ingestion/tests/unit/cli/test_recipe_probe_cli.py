import io
import json
import pathlib
import sys
from typing import (
    Annotated,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Sequence,
    Set,
    Tuple,
)

import click
import pytest
from click.testing import CliRunner, Result
from pydantic import Field, SecretStr, field_validator
from sqlalchemy import create_engine

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent.filter_check import FilterCheckResult, FilterVerdict
from datahub.ingestion.agent.probe_methods import (
    BARE_FLAG,
    ProbeMethodResult,
    ProbeMethodSpec,
    ProbeParam,
    _coerce,
    probe_method,
)
from datahub.ingestion.agent.redact import collect_nested_secret_values, redact
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)

# Defined in tests/unit/conftest.py, for any test driving `datahub recipe`.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")


@pytest.fixture(autouse=True)
def _verbose_logs_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests pin what the CLI withholds, which an exported
    DATAHUB_PROBE_VERBOSE_LOGS (the local-debugging switch) turns off. A test
    of the switch sets it itself."""
    monkeypatch.delenv("DATAHUB_PROBE_VERBOSE_LOGS", raising=False)


def _recipe_file(tmp_path):
    p = tmp_path / "r.yml"
    p.write_text("source:\n  type: postgres\n  config: {}\n")
    return str(p)


def _invoke_probe_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    provider: type,
    config: type,
    command: str = "tables",
    *params: str,
    cli: click.Command = recipe,
    before: Sequence[str] = (),
    source_type: str = "leaky",
    secrets: FrozenSet[str] = frozenset(),
) -> Result:
    """`probe run <command> --recipe <file> <params>` over a stand-in
    source: the recipe resolves to `secrets`, the provider and config are the
    classes given, and telemetry is off. `before` goes ahead of `probe`, for
    a `cli` above the `recipe` group (`datahub --debug recipe`)."""
    from datahub.ingestion.agent import probe_methods

    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: (source_type, {}, set(secrets))
    )
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    monkeypatch.setattr(probe_methods, "_provider_class", lambda _st: provider)
    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: config)
    return CliRunner().invoke(
        cli,
        [
            *before,
            "probe",
            "run",
            command,
            "--recipe",
            _recipe_file(tmp_path),
            *params,
        ],
    )


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
    """Parsed as schema="true", `probe run columns --schema --table orders`
    would reach the driver as a real name -- on MySQL as
    SHOW CREATE TABLE `true`.`orders`, and on a dialect whose listing filters
    by name rather than erroring, as an empty result at exit 0."""
    with pytest.raises(ValueError, match="expects a str value but was given none"):
        _coerce(ProbeParam(name="schema", type="str", required=True), BARE_FLAG)


def test_a_bare_flag_is_still_true_for_a_boolean_parameter():
    param = ProbeParam(name="verbose", type="bool", required=False)
    assert _coerce(param, BARE_FLAG)


def test_an_unrecognised_boolean_value_is_refused_rather_than_read_as_false():
    """`--flag ture` returned a narrower listing and called it the answer,
    while the int branch beside it surfaced bad input as exit 2."""
    param = ProbeParam(name="include_system", type="bool", required=False)
    assert not _coerce(param, "no")
    assert _coerce(param, "on")
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
        lambda st: [
            ProbeMethodSpec("foreign_keys", [ProbeParam("table", "str", True)], "FKs.")
        ],
    )
    res = CliRunner().invoke(
        recipe, ["probe", "methods", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 0
    assert "foreign_keys" in res.output and "FKs." in res.output


def test_probe_methods_reports_the_container_kind_of_an_incomplete_recipe(tmp_path):
    # The recipe names no host or credentials; the kind comes from the config
    # class, so it is still reported.
    res = CliRunner().invoke(
        recipe, ["probe", "methods", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 0, res.output
    kinds = {m["command"]: m["kind"] for m in json.loads(res.output)["methods"]}
    assert kinds["containers"] == "Schema"
    assert kinds["tables"] == "Table"


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
    with report.failure(): unread, a 403 on Mode's data_sources comes back as
    {"result": {}, "warnings": []} at exit 0, exactly what a workspace with no
    data sources returns. The partial result is still emitted; the exit code
    says it is not the whole answer.
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


class _DoubledFilters(ConfigModel):
    a_pattern: Annotated[AllowDenyPattern, Filters("Table")] = Field(
        default=AllowDenyPattern.allow_all()
    )
    b_pattern: Annotated[AllowDenyPattern, Filters("Table")] = Field(
        default=AllowDenyPattern.allow_all()
    )

    @classmethod
    def probe_kind_overrides(cls) -> Mapping[str, str]:
        return {"tables": "Table"}


class _FiltersOnAString(ConfigModel):
    table_name: Annotated[str, Filters("Table")] = "orders"

    @classmethod
    def probe_kind_overrides(cls) -> Mapping[str, str]:
        return {"tables": "Table"}


@pytest.mark.parametrize(
    "config_cls",
    [_DoubledFilters, _FiltersOnAString],
    ids=["on-two-fields", "on-a-non-pattern"],
)
def test_a_misdeclared_filters_is_the_connectors_defect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, config_cls: type
) -> None:
    """Nothing the caller passes can fix a connector declaring Filters wrong,
    so both commands that resolve it exit 1, not 2."""
    from datahub.ingestion.agent import filter_check, introspect

    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: config_cls)
    monkeypatch.setattr(introspect, "require_config_class", lambda _st: config_cls)
    monkeypatch.setattr(introspect, "list_probe_methods", lambda _st: [])
    filtered = CliRunner().invoke(
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
    described = CliRunner().invoke(recipe, ["describe", "postgres"])
    assert filtered.exit_code == rc.EXIT_INTERNAL, filtered.output
    assert described.exit_code == rc.EXIT_INTERNAL, described.output


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
    monkeypatch.setattr(rc, "TestableSource", _Failing)
    res = CliRunner().invoke(
        recipe, ["test-connection", "--recipe", _recipe_file(tmp_path)]
    )
    # The report still reaches stdout; only the exit code changes.
    assert res.exit_code == 3, res.output
    # As a structured object, not the report's repr: the exit-3 message sends
    # the caller to basic_connectivity, so that key has to be in the payload.
    report = json.loads(res.stdout)
    assert report["basic_connectivity"] == {
        "capable": False,
        "failure_reason": "bad credentials",
    }


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
    both as excluded -- a wrong answer that looks like a real verdict.
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
    monkeypatch.setattr(rc, "TestableSource", _Failing)
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
    """_stdin_secrets is module state. Set once per process holds for
    `datahub recipe ...` as a one-shot CLI and for no other way the group is
    dispatched, and a later recipe resolving ${REF} would get the EARLIER
    caller's credential, registered for masking as though it had been handed
    it. Cleared in the group callback, which runs once per invocation.
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


def test_resolve_probe_recipe_resolves_only_from_the_secrets_it_is_given(
    monkeypatch,
):
    """The entry point for callers outside the CLI never runs the `recipe`
    group callback that clears _stdin_secrets, so it must not read them: a
    CLI invocation earlier in the process would otherwise hand its ${REF}s
    the earlier caller's credential."""
    monkeypatch.setenv("PROBE_TEST_REF", "from-env")
    first = CliRunner().invoke(
        recipe,
        ["validate", "-"],
        input=_envelope({"PROBE_TEST_REF": "from-the-earlier-cli-call"}),
    )
    assert first.exit_code == 0, first.output
    assert rc._stdin_secrets, "the earlier call left nothing behind to inherit"
    loaded: Dict[str, object] = {
        "source": {
            "type": "mysql",
            "config": {
                "host_port": "h:3306",
                "username": "u",
                "password": "${PROBE_TEST_REF}",
            },
        }
    }

    _t, config, secret_values = rc.resolve_probe_recipe(loaded)
    assert config["password"] == "from-env"
    assert "from-the-earlier-cli-call" not in secret_values

    _t, config, secret_values = rc.resolve_probe_recipe(
        loaded, stdin_secrets={"PROBE_TEST_REF": "handed-in"}
    )
    assert config["password"] == "handed-in"
    assert "handed-in" in secret_values


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


def test_a_secret_equal_to_a_plain_config_value_is_still_masked(monkeypatch):
    """A password that happens to equal the database name is masked wherever
    it appears, the identifier included.

    Masking matches strings, not meanings: hiding the value where it is the
    password means hiding it everywhere. `target` then reads
    `***REDACTED:...***.orders`, which over-masks the identifier -- the safe
    side, and the redaction notice tells the caller why.
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
    assert "datahub" in secret_values
    assert "datahub" not in json.dumps(
        redact({"note": "the password is: datahub"}, secret_values)
    )


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


_NESTED_SENTINEL = "nested-inline-sentinel-7Q2"


def _require_connector(source_type: str) -> None:
    from datahub.ingestion.agent.probe_methods import config_class_for

    try:
        config_class_for(source_type)
    except ValueError as exc:
        pytest.skip(f"{source_type} extra not installed: {exc}")


def _probe_secrets(recipe_doc: Dict[str, object]) -> Set[str]:
    return rc.resolve_probe_recipe(recipe_doc)[2]


@pytest.mark.parametrize(
    "collect",
    [_probe_secrets, rc._secrets_in_recipe],
    ids=["probe", "validate"],
)
@pytest.mark.parametrize(
    "source_type, config",
    [
        ("abs", {"azure_config": {"connection_string": _NESTED_SENTINEL}}),
        ("excel", {"azure_config": {"connection_string": _NESTED_SENTINEL}}),
        ("lookml", {"git_info": {"repo": "o/r", "deploy_key": _NESTED_SENTINEL}}),
        (
            "lookml",
            {
                "project_dependencies": {
                    "dep": {"repo": "o/dep", "deploy_key": _NESTED_SENTINEL}
                }
            },
        ),
        ("odcs", {"git_info": {"repo": "o/r", "deploy_key": _NESTED_SENTINEL}}),
        ("sqlmesh", {"git_info": {"repo": "o/r", "deploy_key": _NESTED_SENTINEL}}),
        # The deprecated name pydantic_renamed_field moves to git_info: only
        # the validated config holds it under a SecretStr field.
        (
            "lookml",
            {
                "github_info": {"repo": "o/r", "deploy_key": _NESTED_SENTINEL},
                "connection_to_platform_map": {"c": "postgres"},
                "project_name": "p",
            },
        ),
    ],
)
def test_an_inline_secret_in_a_nested_config_block_is_collected(
    collect: Callable[[Dict[str, object]], Set[str]],
    source_type: str,
    config: Dict[str, object],
) -> None:
    """SecretStr fields in nested config blocks, under keys no name hint
    matches, so only walking the config class finds them -- in both the probe
    path and the never-raising one validate and the error handler use."""
    _require_connector(source_type)
    recipe_doc: Dict[str, object] = {"source": {"type": source_type, "config": config}}
    assert _NESTED_SENTINEL in collect(recipe_doc)


@pytest.mark.parametrize(
    "collect",
    [_probe_secrets, rc._secrets_in_recipe],
    ids=["probe", "validate"],
)
@pytest.mark.parametrize(
    "source_type, config, registered, not_registered",
    [
        # A typed block of request settings under a key holding "token".
        (
            "openapi",
            {
                "name": "n",
                "url": "https://api.example",
                "swagger_file": "s.json",
                "get_token": {"request_type": "post", "url_complement": "api/login"},
            },
            set(),
            {"post", "api/login"},
        ),
        # A free-form client config: beneath a sensitive key, all of it.
        (
            "kafka",
            {
                "connection": {
                    "bootstrap": "b:9092",
                    "consumer_config": {"sasl": {"username": "PLANTED-sasl-user"}},
                }
            },
            {"PLANTED-sasl-user"},
            set(),
        ),
        # A config block's field named for a credential, typed plain str.
        (
            "snowflake",
            {
                "account_id": "a",
                "oauth_config": {
                    "provider": "okta",
                    "authority_url": "https://idp.example",
                    "client_id": "PLANTED-client-id",
                    "scopes": ["s"],
                },
            },
            {"PLANTED-client-id"},
            set(),
        ),
    ],
)
def test_a_keys_secret_verdict_reaches_only_free_form_values(
    collect: Callable[[Dict[str, object]], Set[str]],
    source_type: str,
    config: Dict[str, object],
    registered: Set[str],
    not_registered: Set[str],
) -> None:
    _require_connector(source_type)
    found = collect({"source": {"type": source_type, "config": config}})
    assert registered <= found, found
    assert not_registered.isdisjoint(found), found


_TOKEN_REQUEST_RECIPE = (
    "source:\n  type: openapi\n  config:\n    name: n\n"
    "    url: https://api.example\n    swagger_file: s.json\n"
    "    username: u\n    password: ${PROBE_TEST_PW}\n"
    "    get_token:\n      request_type: post\n      url_complement: api/login\n"
)


def test_a_token_request_setting_leaves_ordinary_output_intact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """`post` registered as a secret masked every word holding it, and
    validate told the author two request settings were plaintext secrets."""
    _require_connector("openapi")
    monkeypatch.setenv("PROBE_TEST_PW", "PLANTED-api-pw")
    p = tmp_path / "r.yml"
    p.write_text(_TOKEN_REQUEST_RECIPE)
    text = "postgresql://h/x; got posts"

    def fake_run(st, cfg, cmd, kwargs):
        return ProbeMethodResult(
            st, cmd, kwargs, {"note": text, "auth": f"as {cfg['password']}"}
        )

    monkeypatch.setattr(rc, "run_probe_method", fake_run)
    res = CliRunner().invoke(recipe, ["probe", "run", "tables", "--recipe", str(p)])
    assert res.exit_code == 0, res.output
    assert text in res.output
    assert "PLANTED-api-pw" not in res.output

    res = CliRunner().invoke(recipe, ["validate", str(p)])
    assert res.exit_code == 0, res.output
    assert json.loads(res.output)["warnings"] == []


class _RepositoryBlock(ConfigModel):
    repo: str
    deploy_key: SecretStr


class _NestedSecretConfig(ConfigModel):
    repository: _RepositoryBlock


def test_test_connection_masks_an_inline_secret_in_a_nested_block(
    monkeypatch, tmp_path
):
    from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport

    class _EchoingSource:
        @staticmethod
        def get_config_class() -> type:
            return _NestedSecretConfig

        @staticmethod
        def test_connection(config_dict):
            key = config_dict["repository"]["deploy_key"]
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=f"clone refused key {key}"
                )
            )

    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _EchoingSource,
    )
    monkeypatch.setattr(rc, "TestableSource", _EchoingSource)
    p = tmp_path / "r.yml"
    p.write_text(
        "source:\n  type: echoing\n  config:\n    repository:\n"
        f"      repo: o/r\n      deploy_key: {_NESTED_SENTINEL}\n"
    )
    res = CliRunner().invoke(recipe, ["test-connection", "--recipe", str(p)])
    assert res.exit_code == 3, res.output
    assert "clone refused key" in res.stdout
    assert _NESTED_SENTINEL not in res.output


def test_an_envelope_secret_equal_to_a_plain_value_is_still_registered(monkeypatch):
    """The masking backstop too: an envelope secret is registered whatever
    else the recipe states, so `the password is: probe_db` never prints it."""
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

    masked = SecretMaskingFilter().mask_text("the password is: probe_db")
    assert "probe_db" not in masked


def test_an_envelope_secret_is_registered_for_masking(monkeypatch):
    """The registry is the backstop for anything a command prints."""
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
    error quoting the offending document is already covered."""
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
    """_json_default returns str(o), not o.__dict__.

    Given __dict__, json.dumps walks whatever the library hung on the object
    -- a driver error carries connection state, and redaction afterwards only
    knows the values it collected, so an unregistered credential nested a
    few attributes deep goes out in the clear. str(o) is bounded and is
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


@pytest.mark.parametrize(
    "raised, code",
    [
        ("connection", 3),
        ("internal", 1),
        ("value", 2),
    ],
)
def test_probe_errors_map_to_the_documented_exit_codes(raised: str, code: int) -> None:
    from datahub.cli.recipe_cli import EXIT_CONNECTION, _exit_codes
    from datahub.ingestion.agent.verdicts import (
        ProbeConnectionError,
        ProbeInternalError,
    )

    exc: Exception = {
        "connection": ProbeConnectionError("unreachable"),
        "internal": ProbeInternalError("KeyError"),
        "value": ValueError("bad argument"),
    }[raised]
    with pytest.raises(SystemExit) as exit_info, _exit_codes(fallback=EXIT_CONNECTION):
        raise exc
    assert exit_info.value.code == code


def test_the_report_file_masks_before_serializing(tmp_path: pathlib.Path) -> None:
    """JSON escaping changes how a secret renders; masking the serialized
    text can then miss it."""
    from datahub.masking.secret_registry import SecretRegistry

    secret = 'quote"and\\back' + "slash-secret"
    SecretRegistry.get_instance().register_secrets_batch({"PW": secret})
    out = tmp_path / "report.json"
    rc._write_report(str(out), {"error": f"driver said {secret}"})

    written = out.read_text()
    assert secret not in json.loads(written)["error"]
    assert "slash-secret" not in written


def _run_file(tmp_path, envelope):
    p = tmp_path / "run.json"
    p.write_text(json.dumps(envelope))
    return str(p)


def _capturing_check_filters(monkeypatch):
    seen = {}

    def fake(**kwargs):
        seen.update(kwargs)
        return FilterCheckResult(
            source_type="mysql",
            kind=kwargs["kind"],
            parent_path=list(kwargs["parent_path"]),
            pattern_field=None,
            results=[],
        )

    monkeypatch.setattr(rc, "check_filters", fake)
    return seen


def test_from_run_takes_names_attributes_kind_and_parent_from_the_listing(
    monkeypatch, tmp_path
):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path,
        {
            "kind": "Table",
            "parent_path": ["db"],
            "result": [{"name": "t1", "rows_hint": 3}, "t2"],
        },
    )
    res = CliRunner().invoke(
        recipe,
        ["probe", "filter", "--recipe", _recipe_file(tmp_path), "--from-run", run],
    )
    assert res.exit_code == 0, res.output
    assert seen["kind"] == "Table"
    assert seen["parent_path"] == ["db"]
    assert seen["names"] == ["t1", "t2"]
    assert seen["attributes"] == [{"rows_hint": "3"}, {}]


def test_from_run_and_name_together_are_a_bad_argument(monkeypatch, tmp_path):
    _capturing_check_filters(monkeypatch)
    run = _run_file(tmp_path, {"kind": "Table", "result": ["t1"]})
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--from-run",
            run,
            "--name",
            "t2",
        ],
    )
    assert res.exit_code == 2, res.output
    assert "both name the objects to judge" in res.output


def test_a_kind_contradicting_the_listing_is_a_bad_argument(monkeypatch, tmp_path):
    _capturing_check_filters(monkeypatch)
    run = _run_file(tmp_path, {"kind": "Table", "result": ["t1"]})
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--from-run",
            run,
            "--kind",
            "View",
        ],
    )
    assert res.exit_code == 2, res.output
    assert "contradicts the listing" in res.output


def test_a_listing_from_another_source_is_a_bad_argument(monkeypatch, tmp_path):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path, {"source_type": "mysql", "kind": "Table", "result": ["t1"]}
    )
    res = CliRunner().invoke(
        recipe,
        ["probe", "filter", "--recipe", _recipe_file(tmp_path), "--from-run", run],
    )
    assert res.exit_code == 2, res.output
    assert "this listing came from mysql" in res.output
    assert seen == {}


def test_a_listing_from_the_recipes_source_is_judged(monkeypatch, tmp_path):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path, {"source_type": "postgres", "kind": "Table", "result": ["t1"]}
    )
    res = CliRunner().invoke(
        recipe,
        ["probe", "filter", "--recipe", _recipe_file(tmp_path), "--from-run", run],
    )
    assert res.exit_code == 0, res.output
    assert seen["names"] == ["t1"]


def test_neither_names_nor_a_run_is_a_bad_argument(tmp_path):
    res = CliRunner().invoke(
        recipe,
        ["probe", "filter", "--recipe", _recipe_file(tmp_path), "--kind", "Table"],
    )
    assert res.exit_code == 2, res.output
    assert "nothing to judge" in res.output


def _filter_from_run(tmp_path, run, *extra):
    return CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--from-run",
            run,
            *extra,
        ],
    )


def test_a_kind_differing_only_in_case_is_accepted_and_passed_on(monkeypatch, tmp_path):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(tmp_path, {"kind": "Table", "result": ["t1"]})
    res = _filter_from_run(tmp_path, run, "--kind", "table")
    assert res.exit_code == 0, res.output
    # The caller's spelling reaches check_filters, which canonicalises it.
    assert seen["kind"] == "table"


def test_a_listing_without_a_kind_asks_for_kind(monkeypatch, tmp_path):
    _capturing_check_filters(monkeypatch)
    run = _run_file(tmp_path, {"result": ["t1"]})
    res = _filter_from_run(tmp_path, run)
    assert res.exit_code == 2, res.output
    assert "the listing does not say what kind it holds" in res.output
    assert "--kind" in res.output


def test_names_without_a_kind_ask_for_kind(monkeypatch, tmp_path):
    _capturing_check_filters(monkeypatch)
    res = CliRunner().invoke(
        recipe,
        ["probe", "filter", "--recipe", _recipe_file(tmp_path), "--name", "t1"],
    )
    assert res.exit_code == 2, res.output
    assert "pass --kind" in res.output
    # No listing was given, so blaming one would send the caller looking for it.
    assert "listing" not in res.output


def _warnings_of(res):
    return json.loads(res.stdout)["warnings"]


def test_a_truncated_or_failed_listing_is_judged_with_warnings(monkeypatch, tmp_path):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path,
        {
            "kind": "Table",
            "result": ["t1"],
            "truncated": True,
            "failures": ["could not read schema x"],
        },
    )
    res = _filter_from_run(tmp_path, run)
    assert res.exit_code == 0, res.output
    assert seen["names"] == ["t1"]
    warnings = _warnings_of(res)
    assert any("truncated" in w for w in warnings), warnings
    assert any("incomplete" in w for w in warnings), warnings


def test_a_listing_whose_run_warned_is_judged_with_those_warnings(
    monkeypatch, tmp_path
):
    _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path,
        {"kind": "Table", "result": ["t1"], "warnings": ["could not read x"]},
    )
    res = _filter_from_run(tmp_path, run)
    assert res.exit_code == 0, res.output
    warnings = _warnings_of(res)
    assert any("could not read x" in w for w in warnings), warnings


def test_a_redacted_listing_kind_asks_for_kind(monkeypatch, tmp_path):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(tmp_path, {"kind": "***", "result": ["t1"]})
    refused = _filter_from_run(tmp_path, run)
    assert refused.exit_code == 2, refused.output
    assert "--kind" in refused.output
    assert seen == {}

    accepted = _filter_from_run(tmp_path, run, "--kind", "table")
    assert accepted.exit_code == 0, accepted.output
    assert seen["kind"] == "table"


def test_redacted_entries_are_skipped_with_a_warning(monkeypatch, tmp_path):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path,
        {
            "kind": "Workspace",
            "result": ["***", {"name": "Sales", "id": "***"}],
        },
    )
    res = _filter_from_run(tmp_path, run)
    assert res.exit_code == 0, res.output
    assert seen["names"] == ["Sales"]
    assert seen["attributes"] == [{}]
    warnings = _warnings_of(res)
    assert any("listing entries 0" in w for w in warnings), warnings
    assert any("`id`" in w for w in warnings), warnings


def test_an_unreadable_run_file_is_a_bad_argument(tmp_path):
    import os

    run = pathlib.Path(_run_file(tmp_path, {"kind": "Table", "result": ["t1"]}))
    run.chmod(0)
    try:
        if os.access(run, os.R_OK):
            pytest.skip("this user reads files regardless of their mode")
        # Called directly: click's own readable check would otherwise answer
        # first, and the reader must not depend on it (the file can change
        # between that check and the read).
        with pytest.raises(ValueError, match="cannot read --from-run file"):
            rc._read_run_file(str(run))
    finally:
        run.chmod(0o600)


def test_a_run_file_over_the_size_limit_is_a_bad_argument(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    run = _run_file(tmp_path, {"kind": "Table", "result": ["t1"]})
    size = pathlib.Path(run).stat().st_size
    monkeypatch.setattr(rc, "MAX_RUN_FILE_BYTES", size)
    assert rc._read_run_file(run) == {"kind": "Table", "result": ["t1"]}
    monkeypatch.setattr(rc, "MAX_RUN_FILE_BYTES", size - 1)
    res = _filter_from_run(tmp_path, run)
    assert res.exit_code == 2, res.output


def test_a_run_file_that_is_not_a_regular_file_is_a_bad_argument(
    tmp_path: pathlib.Path,
) -> None:
    if not pathlib.Path("/dev/zero").exists():
        pytest.skip("no /dev/zero here")
    res = _filter_from_run(tmp_path, "/dev/zero")
    assert res.exit_code == 2, res.output


def test_a_named_pipe_run_file_is_refused_without_waiting_for_a_writer(
    tmp_path: pathlib.Path,
) -> None:
    """Opening a FIFO for reading blocks until something writes to it, so
    the regular-file check must come without that wait."""
    import os
    import threading

    if not hasattr(os, "mkfifo"):
        pytest.skip("no named pipes here")
    fifo = tmp_path / "run.json"
    os.mkfifo(fifo)
    outcome: List[BaseException] = []

    def read() -> None:
        try:
            rc._read_run_file(str(fifo))
        except BaseException as exc:
            outcome.append(exc)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(10)
    if reader.is_alive():
        # Opening the write end releases the blocked reader.
        os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))
        reader.join(5)
        pytest.fail("reading a named pipe waited for a writer")
    assert len(outcome) == 1 and isinstance(outcome[0], ValueError), outcome


def test_a_redacted_listing_parent_is_refused_unless_parent_is_given(
    monkeypatch, tmp_path
):
    seen = _capturing_check_filters(monkeypatch)
    run = _run_file(
        tmp_path, {"kind": "Table", "parent_path": ["***"], "result": ["t1"]}
    )
    refused = _filter_from_run(tmp_path, run)
    assert refused.exit_code == 2, refused.output
    assert "parent_path was redacted" in refused.output
    assert seen == {}

    accepted = _filter_from_run(tmp_path, run, "--parent", "real_db")
    assert accepted.exit_code == 0, accepted.output
    assert seen["parent_path"] == ["real_db"]


def test_a_masked_parent_copied_from_output_is_a_bad_argument(monkeypatch, tmp_path):
    # A secret equal to a schema name masks the schema in every output, and a
    # caller copying it back must be stopped, not handed a verdict on `***`.
    seen = _capturing_check_filters(monkeypatch)
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe_file(tmp_path),
            "--kind",
            "Table",
            "--parent",
            "***",
            "--name",
            "orders",
        ],
    )
    assert res.exit_code == 2, res.output
    assert "--parent" in res.output
    assert seen == {}


def test_test_connection_scrubs_credential_shapes_from_driver_text(
    monkeypatch, tmp_path
):
    from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport

    class _Failing:
        @staticmethod
        def test_connection(config_dict):
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason="GET http://admin:"
                    + "PLANTED-pw@host:8083/x failed",
                    mitigation_message="check client_secret=PLANTED-cs",
                )
            )

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _Failing,
    )
    monkeypatch.setattr(rc, "TestableSource", _Failing)
    res = CliRunner().invoke(
        recipe, ["test-connection", "--recipe", _recipe_file(tmp_path)]
    )
    assert "PLANTED" not in res.stdout


def test_redacted_payload_scrubs_without_mutating_the_input_and_says_so():
    payload = {"result": {"x": "y"}, "warnings": ["token=abcdef1"], "failures": []}
    out = rc._redacted_payload(payload, set())
    assert isinstance(out, dict)
    assert payload["warnings"] == ["token=abcdef1"]
    assert "abcdef1" not in str(out)
    assert len(out["warnings"]) == 2


_LOG_SENTINEL = "PLANTED-cli-log-secret"
_REGISTERED_SENTINEL = "plainregisteredvalue42"


class _DebugLeakingProvider:
    @classmethod
    def for_config(cls, config):
        return cls()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    @probe_method()
    def tables(self) -> list:
        "Tables."
        import logging

        log = logging.getLogger("datahub.ingestion.source.leaky_cli.fetcher")
        try:
            raise ConnectionError(f"GET http://connect/?token={_LOG_SENTINEL}")
        except ConnectionError:
            log.debug("fetch failed", exc_info=True)
        log.warning("retrying with %s", _REGISTERED_SENTINEL)
        return [{"name": "t"}]


class _DebugLeakingConfig:
    @classmethod
    def probe_provider_class(cls):
        return _DebugLeakingProvider

    @classmethod
    def model_validate(cls, d):
        return cls()


@pytest.fixture
def _real_cli_logging(monkeypatch):
    """Run the real `configure_logging` that `datahub --debug` installs, then
    put the process-wide logging state back for the tests after this one."""
    import logging

    import datahub.entrypoints as entrypoints
    from datahub.utilities.logging_manager import DATAHUB_PACKAGES

    monkeypatch.setenv("DATAHUB_SUPPRESS_LOGGING_MANAGER", "0")
    root = logging.getLogger()
    saved_root = (root.level, list(root.handlers))
    saved_libs = {
        lib: (
            logging.getLogger(lib).level,
            logging.getLogger(lib).propagate,
            list(logging.getLogger(lib).handlers),
        )
        for lib in DATAHUB_PACKAGES
    }
    yield entrypoints.datahub
    if entrypoints._logging_configured is not None:
        entrypoints._logging_configured.__exit__(None, None, None)
        entrypoints._logging_configured = None
    root.setLevel(saved_root[0])
    root.handlers[:] = saved_root[1]
    for lib, (level, propagate, handlers) in saved_libs.items():
        lib_logger = logging.getLogger(lib)
        lib_logger.setLevel(level)
        lib_logger.propagate = propagate
        lib_logger.handlers[:] = handlers


def test_reused_code_debug_tracebacks_are_dropped_under_the_debug_flag(
    monkeypatch, tmp_path, _real_cli_logging
):
    """`datahub --debug` routes every datahub.* DEBUG record, tracebacks
    included, to stderr. A connector's reused fetcher logging its failed
    request URL must still not reach it while a probe runs -- and a registered
    secret with no credential shape is masked because the CLI hands the
    recipe's secrets to the guard."""
    res = _invoke_probe_run(
        monkeypatch,
        tmp_path,
        _DebugLeakingProvider,
        _DebugLeakingConfig,
        cli=_real_cli_logging,
        before=["--debug", "recipe"],
        secrets=frozenset({_REGISTERED_SENTINEL}),
    )
    assert res.exit_code == 0, res.output
    assert "retrying with" in res.stderr
    assert _LOG_SENTINEL not in res.output
    assert _LOG_SENTINEL not in res.stderr
    assert _REGISTERED_SENTINEL not in res.output
    assert _REGISTERED_SENTINEL not in res.stderr


class _UtilityLoggingProvider(_DebugLeakingProvider):
    @probe_method()
    def tables(self) -> list:
        "Tables."
        import logging

        logging.getLogger("datahub.utilities.some_helper").debug(
            "helper saw password=%s", _LOG_SENTINEL
        )
        return [{"name": "t"}]


class _UtilityLoggingConfig(_DebugLeakingConfig):
    @classmethod
    def probe_provider_class(cls):
        return _UtilityLoggingProvider


def test_a_shared_datahub_module_debug_line_is_scrubbed_under_the_debug_flag(
    monkeypatch, tmp_path, _real_cli_logging
):
    res = _invoke_probe_run(
        monkeypatch,
        tmp_path,
        _UtilityLoggingProvider,
        _UtilityLoggingConfig,
        cli=_real_cli_logging,
        before=["--debug", "recipe"],
    )
    assert res.exit_code == 0, res.output
    assert "helper saw" in res.stderr
    assert _LOG_SENTINEL not in res.stderr


class _LoggingTestableSource:
    @staticmethod
    def test_connection(config_dict):
        import logging

        from datahub.ingestion.api.source import (
            CapabilityReport,
            TestConnectionReport,
        )

        logging.getLogger("datahub.ingestion.source.leaky_tc").warning(
            "connect retry password=%s", _LOG_SENTINEL
        )
        return TestConnectionReport(basic_connectivity=CapabilityReport(capable=True))


@pytest.mark.parametrize("debug", [False, True])
def test_test_connection_keeps_reused_logs_scrubbed(
    monkeypatch, tmp_path, _real_cli_logging, debug
):
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _LoggingTestableSource,
    )
    monkeypatch.setattr(rc, "TestableSource", _LoggingTestableSource)
    if not debug:
        # tests/conftest.py turns DATAHUB_DEBUG on for the whole run.
        monkeypatch.delenv("DATAHUB_DEBUG", raising=False)
    args = ["--debug"] if debug else []
    res = CliRunner().invoke(
        _real_cli_logging,
        [*args, "recipe", "test-connection", "--recipe", _recipe_file(tmp_path)],
    )
    assert res.exit_code == 0, res.output
    assert _LOG_SENTINEL not in res.stderr
    assert _LOG_SENTINEL not in res.output


def test_test_connection_scrubs_logs_from_resolving_the_source(
    monkeypatch, tmp_path, _real_cli_logging
):
    """Looking the source up imports its module, and a module can log at
    import time; `probe run`'s guard covers that step too."""
    import logging

    def _importing_get(source_type):
        logging.getLogger("some_driver.imported_by_the_source").warning(
            "imported with password=%s", _LOG_SENTINEL
        )
        return _LoggingTestableSource

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        _importing_get,
    )
    monkeypatch.setattr(rc, "TestableSource", _LoggingTestableSource)
    res = CliRunner().invoke(
        _real_cli_logging,
        ["recipe", "test-connection", "--recipe", _recipe_file(tmp_path)],
    )
    assert res.exit_code == 0, res.output
    assert "imported with" in res.stderr
    assert _LOG_SENTINEL not in res.output


def _log_like_reused_code(side: io.StringIO) -> None:
    """Every way reused code reaches a log, each carrying a credential-shaped
    or a registered shape-free secret: its own DEBUG and WARNING,
    logger.exception, a handler and a non-propagating logger it sets up
    mid-call, logging.lastResort, and warnings.warn."""
    import logging
    import warnings

    source_log = logging.getLogger("datahub.ingestion.source.leaky_cli.fetcher")
    source_log.debug("debug fetch with %s", _REGISTERED_SENTINEL)
    try:
        raise ConnectionError(f"GET http://connect/?token={_LOG_SENTINEL}")
    except ConnectionError:
        source_log.exception("fetch failed")
    own = logging.getLogger("some_sdk.cli_own_handler")
    handler = logging.StreamHandler(side)
    own.addHandler(handler)
    own.propagate = False
    lonely = logging.getLogger("some_sdk.cli_last_resort")
    lonely.propagate = False
    try:
        own.warning("own handler login password=%s", _LOG_SENTINEL)
        lonely.warning("last resort with %s", _REGISTERED_SENTINEL)
    finally:
        own.removeHandler(handler)
        own.propagate = True
        lonely.propagate = True
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        warnings.warn(f"sdk warning token={_LOG_SENTINEL}", UserWarning, stacklevel=1)


@pytest.fixture
def _fresh_warning_capture():
    """Capture off, so the `recipe` group's masking bootstrap turns it on over
    pytest's own warning recorder rather than finding it on from an earlier
    test (when pytest's recorder would take the warning instead)."""
    import logging

    was_capturing = getattr(logging, "_warnings_showwarning", None) is not None
    logging.captureWarnings(False)
    yield
    logging.captureWarnings(False)
    logging.captureWarnings(was_capturing)


def _assert_scrubbed_everywhere(res: Result, side: io.StringIO, debug: bool) -> None:
    assert res.exit_code == 0, res.output
    for text in (res.output, res.stderr, side.getvalue()):
        assert _LOG_SENTINEL not in text
        assert _REGISTERED_SENTINEL not in text
    assert "Traceback" not in res.stderr
    assert "fetch failed" in res.stderr
    assert "own handler login" in side.getvalue()
    assert "last resort with" in res.stderr
    assert "sdk warning" in res.stderr
    assert ("debug fetch with" in res.stderr) is debug


@pytest.mark.usefixtures("_fresh_warning_capture")
@pytest.mark.parametrize("debug", [False, True])
def test_probe_run_scrubs_every_reused_log_channel(
    monkeypatch, tmp_path, _real_cli_logging, debug
):
    side = io.StringIO()

    class _Provider(_DebugLeakingProvider):
        @probe_method()
        def tables(self) -> list:
            "Tables."
            _log_like_reused_code(side)
            return [{"name": "t"}]

    if not debug:
        # tests/conftest.py turns DATAHUB_DEBUG on for the whole run.
        monkeypatch.delenv("DATAHUB_DEBUG", raising=False)
    res = _invoke_probe_run(
        monkeypatch,
        tmp_path,
        _Provider,
        _DebugLeakingConfig,
        cli=_real_cli_logging,
        before=["--debug", "recipe"] if debug else ["recipe"],
        secrets=frozenset({_REGISTERED_SENTINEL}),
    )
    _assert_scrubbed_everywhere(res, side, debug)


@pytest.mark.usefixtures("_fresh_warning_capture")
@pytest.mark.parametrize("debug", [False, True])
def test_test_connection_scrubs_every_reused_log_channel(
    monkeypatch, tmp_path, _real_cli_logging, debug
):
    from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport

    side = io.StringIO()

    class _Source:
        @staticmethod
        def test_connection(config_dict):
            _log_like_reused_code(side)
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )

    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda r: ("postgres", {}, {_REGISTERED_SENTINEL})
    )
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _Source,
    )
    monkeypatch.setattr(rc, "TestableSource", _Source)
    if not debug:
        monkeypatch.delenv("DATAHUB_DEBUG", raising=False)
    args = ["--debug"] if debug else []
    res = CliRunner().invoke(
        _real_cli_logging,
        [*args, "recipe", "test-connection", "--recipe", _recipe_file(tmp_path)],
    )
    _assert_scrubbed_everywhere(res, side, debug)


def test_test_connection_withholds_a_pydantic_input_echo(monkeypatch, tmp_path):
    """A source's own test_connection parses its config itself and reports
    str(ValidationError); under DATAHUB_DEBUG that quotes input_value, and a
    long value's truncated repr matches no registered secret."""
    from pydantic import BaseModel, ConfigDict, ValidationError

    from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport

    long_secret = "PLANTEDhead" + "q" * 120 + "PLANTEDtail"

    class _Echoing(BaseModel):
        model_config = ConfigDict(hide_input_in_errors=False)
        port: int

    class _Failing:
        @staticmethod
        def test_connection(config_dict):
            try:
                _Echoing.model_validate({"port": long_secret})
            except ValidationError as exc:
                reason = str(exc)
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=reason
                )
            )

    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda r: ("postgres", {}, {long_secret})
    )
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _Failing,
    )
    monkeypatch.setattr(rc, "TestableSource", _Failing)
    res = CliRunner().invoke(
        recipe, ["test-connection", "--recipe", _recipe_file(tmp_path)]
    )
    assert res.exit_code == 3, res.output
    assert "PLANTED" not in res.output
    assert "int_parsing" in res.stdout


_CRASH_SENTINEL = "PLANTED-crash-text"


def _test_connection_of(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, source_cls: type
) -> Result:
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda r: ("postgres", {}, set()))
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: source_cls,
    )
    monkeypatch.setattr(rc, "TestableSource", source_cls)
    return CliRunner().invoke(
        recipe, ["test-connection", "--recipe", _recipe_file(tmp_path)]
    )


def _crashing_test_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, raised: BaseException
) -> Result:
    class _Crashing:
        @staticmethod
        def test_connection(config_dict):
            raise raised

    return _test_connection_of(monkeypatch, tmp_path, _Crashing)


class _ConnectorError(Exception):
    pass


class _ConnectorAbort(BaseException):
    pass


@pytest.mark.parametrize(
    "raised, exit_code, label",
    [
        (
            _ConnectorError(f"login to db://u:{_CRASH_SENTINEL}@h refused"),
            3,
            "_ConnectorError",
        ),
        (ValueError(f"bad host {_CRASH_SENTINEL}"), 2, "ValueError"),
        (KeyError(f"no key {_CRASH_SENTINEL}"), 1, "KeyError"),
        # An exit status carries no text, and is still the source giving up.
        (SystemExit(0), 3, "SystemExit"),
        (SystemExit(None), 3, "SystemExit"),
        (SystemExit(f"fatal: {_CRASH_SENTINEL}"), 3, "SystemExit"),
        (_ConnectorAbort(f"aborted with {_CRASH_SENTINEL}"), 3, "_ConnectorAbort"),
    ],
)
def test_a_crashed_test_connection_is_named_by_label_on_its_own_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    raised: BaseException,
    exit_code: int,
    label: str,
) -> None:
    """The source's own connect code raised: its text is the source's, and
    is where a connection string comes from, so it is named by label, on the
    exit code `probe run` gives the same exception."""
    res = _crashing_test_connection(monkeypatch, tmp_path, raised)
    assert res.exit_code == exit_code, res.output
    assert _CRASH_SENTINEL not in res.output
    assert json.loads(res.stderr)["error"] == (
        f"source 'postgres' test_connection failed ({label})"
    )


_TRUSTED_EXIT_CODES = [
    (ProbeArgumentError, 2),
    (ProbeSoftError, 2),
    (ProbeReadFailed, 3),
    (ProbeConnectionError, 3),
    (ProbeInternalError, 1),
]


@pytest.mark.parametrize("how", ["from", "implicit"])
@pytest.mark.parametrize(
    "trusted, exit_code", _TRUSTED_EXIT_CODES, ids=lambda v: getattr(v, "__name__", v)
)
def test_a_trusted_test_connection_error_quoting_foreign_text_keeps_its_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    trusted: type,
    exit_code: int,
    how: str,
) -> None:
    """A source's test_connection may raise a framework type around its
    driver's error. The type is the source's choice and keeps its exit code;
    the driver's text it quotes is withheld, named by label."""

    class _Wrapping:
        @staticmethod
        def test_connection(config_dict):
            try:
                raise _ConnectorError(f"login refused for {_CRASH_SENTINEL}")
            except _ConnectorError as exc:
                if how == "from":
                    raise trusted(f"cannot connect: {exc}") from exc
                raise trusted(f"cannot connect: {exc!r}")  # noqa: B904

    res = _test_connection_of(monkeypatch, tmp_path, _Wrapping)
    assert res.exit_code == exit_code, res.output
    assert _CRASH_SENTINEL not in res.output
    assert "_ConnectorError" in json.loads(res.stderr)["error"]


class _PortConfig(ConfigModel):
    port: int


def test_a_test_connection_refusing_the_recipe_exits_on_the_bad_argument_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """test_connection is handed the recipe's config unvalidated, so a
    ValidationError it raises is the recipe failing the source's model."""
    from pydantic import ValidationError

    try:
        _PortConfig.model_validate({"port": _CRASH_SENTINEL})
    except ValidationError as exc:
        raised = exc
    res = _crashing_test_connection(monkeypatch, tmp_path, raised)
    assert res.exit_code == 2, res.output
    assert _CRASH_SENTINEL not in res.output
    # The failing field is named, so the recipe author knows what to fix.
    assert "port" in json.loads(res.stderr)["error"]


class _UnrenderableReport:
    def as_obj(self) -> object:
        raise _ConnectorError(f"report broke on db://u:{_CRASH_SENTINEL}@h")


class _ReturnsAnUnrenderableReport:
    @staticmethod
    def test_connection(config_dict):
        return _UnrenderableReport()


def test_a_report_that_cannot_render_itself_is_named_by_label(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """as_obj() is the source's own report code, so its crash is labelled
    like test_connection's."""
    res = _test_connection_of(monkeypatch, tmp_path, _ReturnsAnUnrenderableReport)
    assert res.exit_code == 3, res.output
    assert _CRASH_SENTINEL not in res.output
    assert json.loads(res.stderr)["error"] == (
        "source 'postgres' test_connection failed (_ConnectorError)"
    )


def test_the_verbose_switch_shows_a_crashed_test_connections_scrubbed_text(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    res = _crashing_test_connection(
        monkeypatch, tmp_path, _ConnectorError("login refused; password=hunter2")
    )
    assert res.exit_code == 3, res.output
    assert json.loads(res.stderr)["error"] == (
        "source 'postgres' test_connection failed (_ConnectorError): "
        "login refused; password=***"
    )


def test_a_hostile_schema_exits_on_the_bad_argument_code(
    tmp_path: pathlib.Path,
) -> None:
    """Before identifiers were resolved against the catalog listing, this
    payload reached sqlite's reflection SQL and the call exited 3 with the
    driver's OperationalError -- telling an agent the source was unreachable
    when its argument was the problem."""
    db = tmp_path / "t.db"
    seed = create_engine(f"sqlite:///{db}")
    with seed.begin() as c:
        c.exec_driver_sql("CREATE TABLE orders (id INTEGER)")
    seed.dispose()
    recipe_path = tmp_path / "r.yml"
    recipe_path.write_text(
        "source:\n"
        "  type: sqlalchemy\n"
        "  config:\n"
        "    platform: sqlite\n"
        f"    connect_uri: sqlite:///{db}\n"
    )
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "run",
            "tables",
            "--recipe",
            str(recipe_path),
            "--schema",
            "x' UNION SELECT 1 --",
        ],
    )
    assert res.exit_code == 2, res.output
    assert "containers" in res.output


_TRUST_SENTINEL = "PLANTED-trust-by-type"


class _TrustProvider:
    """Takes SQL and API paths but declares neither sql_dialect nor
    api_allowlist, and raises each kind of error the trust rule sorts."""

    @classmethod
    def for_config(cls, config: object) -> "_TrustProvider":
        return cls()

    def __enter__(self) -> "_TrustProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method(name="sql", scoped_sql_param="query")
    def sql(self, query: str) -> List[str]:
        """Run SQL."""
        return []

    @probe_method(name="api", scoped_path_param="path")
    def api(self, path: str) -> List[str]:
        """Call an API path."""
        return []

    @probe_method(name="plain_value")
    def plain_value(self) -> List[str]:
        """Raise a plain ValueError."""
        raise ValueError(f"no widget named {_TRUST_SENTINEL}")

    @probe_method(name="argument")
    def argument(self) -> List[str]:
        """Raise a ProbeArgumentError."""
        raise ProbeArgumentError("no widget named 'w'; run `widgets`")

    @probe_method(name="foreign")
    def foreign(self) -> List[str]:
        """Raise an untrusted error carrying a credential."""
        raise RuntimeError(
            f"fetcher gave up on https://user:{_TRUST_SENTINEL}@host/api"
        )


def _invoke_trust(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    command: str,
    *params: str,
) -> Result:
    class _Config:
        @classmethod
        def model_validate(cls, d: object) -> "_Config":
            return cls()

    return _invoke_probe_run(
        monkeypatch,
        tmp_path,
        _TrustProvider,
        _Config,
        command,
        *params,
        source_type="fake",
    )


@pytest.mark.parametrize(
    "command, params, declaration",
    [
        ("sql", ("--query", "SELECT 1"), "sql_dialect"),
        ("api", ("--path", "/widgets"), "api_allowlist"),
    ],
)
def test_a_scoped_command_without_its_declaration_is_a_provider_defect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    command: str,
    params: Tuple[str, ...],
    declaration: str,
) -> None:
    """A provider that takes SQL or a path but declares no dialect or
    allowlist cannot be fixed by the caller, so it exits 1, not 2."""
    res = _invoke_trust(monkeypatch, tmp_path, command, *params)
    assert res.exit_code == rc.EXIT_INTERNAL, res.output
    assert declaration in res.stderr


def test_a_providers_plain_value_error_exits_2_by_class_name(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _invoke_trust(monkeypatch, tmp_path, "plain_value")
    assert res.exit_code == rc.EXIT_USER, res.output
    assert "'plain_value' failed (ValueError)" in res.stderr
    assert _TRUST_SENTINEL not in res.output


def test_a_probe_argument_error_exits_2_with_its_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _invoke_trust(monkeypatch, tmp_path, "argument")
    assert res.exit_code == rc.EXIT_USER, res.output
    assert "no widget named 'w'; run `widgets`" in res.stderr


def test_untrusted_text_is_withheld_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _invoke_trust(monkeypatch, tmp_path, "foreign")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output
    assert "'foreign' failed (RuntimeError)" in res.stderr
    assert "fetcher gave up" not in res.output
    assert _TRUST_SENTINEL not in res.output


def test_the_verbose_switch_shows_untrusted_text_scrubbed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    res = _invoke_trust(monkeypatch, tmp_path, "foreign")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output
    assert "'foreign' failed (RuntimeError): fetcher gave up on https://" in res.stderr
    assert _TRUST_SENTINEL not in res.output


_HOOK_SENTINEL = "PLANTED-hook-text"


class _UrlCheckingConfig(ConfigModel):
    uri: str

    @field_validator("uri")
    @classmethod
    def _reachable(cls, value: str) -> str:
        raise ValueError(f"cannot reach {value}")


class _UrlCheckingSource:
    @classmethod
    def get_config_class(cls) -> type:
        return _UrlCheckingConfig


def test_validate_scrubs_credential_shapes_from_a_validator_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """A validator's message is kept, since it is the diagnostic, and may
    quote a value under a key no hint marks secret: masked by its shape."""
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _UrlCheckingSource,
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text(
        "source:\n  type: checking\n  config:\n"
        f"    uri: http://admin:{_HOOK_SENTINEL}@db.example/x\n"
    )
    res = CliRunner().invoke(recipe, ["validate", str(recipe_file)])
    assert res.exit_code == 0, res.output
    assert _HOOK_SENTINEL not in res.output
    errors = json.loads(res.stdout)["errors"]
    assert any("cannot reach http://***@db.example/x" in e for e in errors), errors


def test_the_recipes_secrets_reach_the_masking_registry_for_the_verbose_switch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    _real_cli_logging: click.Command,
) -> None:
    """DATAHUB_PROBE_VERBOSE_LOGS turns the log guard off, which leaves the
    registry's masking of every log handler: a value the recipe resolved, with
    no credential shape, is masked there only if it was registered."""
    from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport
    from datahub.ingestion.source.sql.postgres.source import PostgresConfig

    class _Source:
        @classmethod
        def get_config_class(cls) -> type:
            return PostgresConfig

        @staticmethod
        def test_connection(config_dict):
            import logging

            logging.getLogger("some_driver.session").warning(
                "session opened for %s", config_dict["username"]
            )
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )

    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    monkeypatch.setenv("PROBE_SESSION_USER", _REGISTERED_SENTINEL)
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _Source,
    )
    monkeypatch.setattr(rc, "TestableSource", _Source)
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text(
        "source:\n  type: postgres\n  config:\n    host_port: h:5432\n"
        "    username: ${PROBE_SESSION_USER}\n    database: d\n"
    )
    res = CliRunner().invoke(
        _real_cli_logging,
        ["recipe", "test-connection", "--recipe", str(recipe_file)],
    )
    assert res.exit_code == 0, res.output
    assert "session opened for" in res.stderr
    assert _REGISTERED_SENTINEL not in res.stderr
    assert _REGISTERED_SENTINEL not in res.output


class _HookFailure:
    """What a config hook raises, set per test."""

    raised: BaseException = KeyError(f"no rule for {_HOOK_SENTINEL}")


class _OverrideRaisingConfig(ConfigModel):
    table_pattern: Annotated[AllowDenyPattern, Filters("Table")] = (
        AllowDenyPattern.allow_all()
    )

    def probe_verdict_override(self, ctx: object) -> None:
        raise _HookFailure.raised


@pytest.mark.parametrize(
    "raised, exit_code, error",
    [
        (
            KeyError(f"no rule for {_HOOK_SENTINEL}"),
            1,
            "the connector is defective: "
            "_OverrideRaisingConfig.probe_verdict_override failed (KeyError)",
        ),
        (
            TypeError(f"bad ctx {_HOOK_SENTINEL}"),
            1,
            "the connector is defective: "
            "_OverrideRaisingConfig.probe_verdict_override failed (TypeError)",
        ),
        (
            ProbeArgumentError("pass the database as the first --parent"),
            2,
            "pass the database as the first --parent",
        ),
    ],
)
def test_a_config_hook_raising_in_probe_filter_is_policed_like_a_provider(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    raised: BaseException,
    exit_code: int,
    error: str,
) -> None:
    """A hook is the connector's code: an untrusted exception from it is the
    connector's defect, named by label; a trusted one keeps its type and text."""
    from datahub.ingestion.agent import probe_methods

    monkeypatch.setattr(_HookFailure, "raised", raised)
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda _r: ("fake", {}, set()))
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    monkeypatch.setattr(
        probe_methods, "config_class_for", lambda _st: _OverrideRaisingConfig
    )
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
            "orders",
        ],
    )
    assert res.exit_code == exit_code, res.output
    assert _HOOK_SENTINEL not in res.output
    assert json.loads(res.stderr)["error"] == error


class _PatternReadingOverrideConfig(ConfigModel):
    table_pattern: Annotated[AllowDenyPattern, Filters("Table")] = (
        AllowDenyPattern.allow_all()
    )

    def probe_verdict_override(self, ctx: object) -> None:
        # Only --try-allow reaches here (a malformed --try-deny is refused
        # before any hook runs), and the hook must see that hypothetical, not
        # the recipe's allow-all; otherwise exit 1 fails the test.
        if list(self.table_pattern.allow) != ["^ord"]:
            raise AssertionError("the --try-allow pattern did not reach the hook")
        # Compiles the hypothetical pattern lazily, inside the hook.
        self.table_pattern.allowed("orders")


@pytest.mark.parametrize(
    "flag, regex, exit_code", [("--try-allow", "^ord", 0), ("--try-deny", "(", 2)]
)
def test_a_malformed_try_regex_is_the_callers_input_even_inside_a_hook(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    flag: str,
    regex: str,
    exit_code: int,
) -> None:
    from datahub.ingestion.agent import probe_methods

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda _r: ("fake", {}, set()))
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    monkeypatch.setattr(
        probe_methods, "config_class_for", lambda _st: _PatternReadingOverrideConfig
    )
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
            "orders",
            flag,
            regex,
        ],
    )
    assert res.exit_code == exit_code, res.output


class _UnfilteredRaisingConfig(ConfigModel):
    @classmethod
    def probe_unfiltered_kinds(cls) -> Set[str]:
        raise KeyError(f"no kinds in {_HOOK_SENTINEL}")


class _UnfilteredRaisingSource:
    @classmethod
    def get_config_class(cls) -> type:
        return _UnfilteredRaisingConfig


def test_a_config_hook_raising_in_describe_is_the_connectors_defect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    monkeypatch.setattr(
        "datahub.ingestion.source.source_registry.source_registry.get",
        lambda st: _UnfilteredRaisingSource,
    )
    res = CliRunner().invoke(recipe, ["describe", "fake"])
    assert res.exit_code == 1, res.output
    assert _HOOK_SENTINEL not in res.output
    assert json.loads(res.stderr)["error"] == (
        "the connector is defective: "
        "_UnfilteredRaisingConfig.probe_unfiltered_kinds failed (KeyError)"
    )


def test_a_renamed_fields_secret_is_registered_before_a_command_prints(
    tmp_path: pathlib.Path,
) -> None:
    """The registry learns it when the CLI collects the recipe's secrets,
    before any command runs, not only when the source validates later."""
    from datahub.masking.masking_filter import SecretMaskingFilter

    _require_connector("lookml")
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text(
        "source:\n  type: lookml\n  config:\n"
        "    project_name: p\n    connection_to_platform_map: {c: postgres}\n"
        f"    github_info: {{repo: o/r, deploy_key: {_NESTED_SENTINEL}}}\n"
    )
    _t, _c, secret_values = rc._resolve_for_probe(rc._load_recipe(str(recipe_file)))
    assert _NESTED_SENTINEL in secret_values
    assert _NESTED_SENTINEL not in SecretMaskingFilter().mask_text(
        f"clone refused key {_NESTED_SENTINEL}"
    )


# --- validate's secret collection when the recipe cannot be read fully --------

_INLINE_SECRET = "planted-inline-value"
_REF_SECRET = "planted-resolved-value"


def _validate_quoting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    config_yaml: str,
    quoted: Sequence[str],
) -> Result:
    """Run `validate` on a recipe of an unregistered source type, with a
    validator that quotes `quoted` back, as a pydantic error quotes the input
    it rejected."""

    def _quoting_validator(*_args: object, **_kwargs: object) -> Dict[str, object]:
        return {
            "valid": False,
            "errors": [f"rejected '{value}'" for value in quoted],
            "warnings": [],
        }

    monkeypatch.setattr(rc, "validate_recipe", _quoting_validator)
    path = tmp_path / "r.yml"
    path.write_text("source:\n  type: no-such-source-type\n  config:\n" + config_yaml)
    return CliRunner().invoke(recipe, ["validate", str(path)])


def test_validate_masks_secrets_of_a_source_type_it_cannot_resolve(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """No config class and no secret fields for an unknown type: the inline
    secret found by key and the value a ${REF} resolved to are still masked."""
    monkeypatch.setenv("PROBE_T_HOST_REF", _REF_SECRET)
    res = _validate_quoting(
        monkeypatch,
        tmp_path,
        f"    password: {_INLINE_SECRET}\n    host_port: ${{PROBE_T_HOST_REF}}\n",
        [_INLINE_SECRET, _REF_SECRET],
    )
    assert res.exit_code == 0, res.output
    assert _INLINE_SECRET not in res.output
    assert _REF_SECRET not in res.output
    errors = json.loads(res.stdout)["errors"]
    assert len(errors) == 2
    assert all("***" in error for error in errors)


def test_validate_masks_inline_secrets_when_a_reference_cannot_resolve(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """Resolution fails on an unset ${REF}; the inline secret collected before
    it is still masked."""
    monkeypatch.delenv("PROBE_T_UNSET_REF", raising=False)
    res = _validate_quoting(
        monkeypatch,
        tmp_path,
        f"    password: {_INLINE_SECRET}\n    host_port: ${{PROBE_T_UNSET_REF}}\n",
        [_INLINE_SECRET],
    )
    assert res.exit_code == 0, res.output
    assert _INLINE_SECRET not in res.output
    assert all("***" in error for error in json.loads(res.stdout)["errors"])
