import pathlib
from typing import Callable, List

import pytest
from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.error_policy import classify_foreign, foreign_label
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
)
from tests.unit.agent import _foreign_coded_errors as coded
from tests.unit.agent._foreign_coded_errors import SENTINEL


def _caught(raiser: Callable[[], object]) -> BaseException:
    try:
        raiser()
    except BaseException as exc:
        return exc
    raise AssertionError("did not raise")


@pytest.mark.parametrize(
    "raiser, label",
    [
        (coded.pg, "PgError; SQLSTATE 42P01"),
        (coded.snowflake, "SnowflakeProgrammingError; SQLSTATE 42S02; errno 2003"),
        (coded.odbc, "OdbcProgrammingError; SQLSTATE 42S02"),
        (coded.mysql, "MySqlProgrammingError; errno 1146"),
        (coded.sqlalchemy_wrapping_pg, "ProgrammingError; SQLSTATE 42P01"),
        (coded.http, "HTTPError; HTTP 403"),
        (coded.google, "PermissionDenied; HTTP 403"),
        (coded.azure, "HttpResponseError; HTTP 404"),
        (coded.aws, "ClientError; AccessDenied"),
        (coded.chained_from_pg, "RuntimeError; SQLSTATE 42P01"),
    ],
)
def test_a_foreign_error_is_labelled_with_its_code(
    raiser: Callable[[], object], label: str
) -> None:
    assert foreign_label(_caught(raiser)) == label


@pytest.mark.parametrize(
    "raiser",
    [
        lambda: coded.pg(pgcode=f"42P01 {SENTINEL}"),
        lambda: coded.pg(pgcode="hello"),
        lambda: coded.snowflake(errno=-1, sqlstate=SENTINEL),
        lambda: coded.snowflake(errno=f"2003 {SENTINEL}", sqlstate=None),
        lambda: coded.snowflake(errno=True, sqlstate=None),
        lambda: coded.azure(status_code=f"403 {SENTINEL}"),
        lambda: coded.azure(status_code=700),
        lambda: coded.aws(code=f"AccessDenied; user={SENTINEL}"),
        lambda: coded.aws(code=SENTINEL * 10),
        coded.raising_code,
        coded.broken_getattr,
        # Only botocore's errors are read for an AWS code: elsewhere an
        # alphanumeric token in that shape could be anything.
        coded.not_aws,
        coded.connection_error_while_handling_429,
        coded.suppressed_429,
    ],
)
def test_a_code_that_is_not_a_bare_code_is_dropped(
    raiser: Callable[[], object],
) -> None:
    exc = _caught(raiser)
    label = foreign_label(exc)
    assert label == type(exc).__name__
    assert SENTINEL not in label


def test_a_string_subclass_cannot_render_its_own_text() -> None:
    label = foreign_label(_caught(coded.sneaky_str))
    assert label == "PgError; SQLSTATE 42P01"


def test_a_plain_error_with_a_code_shaped_argument_gets_no_code() -> None:
    """args[0] is read as a code only for drivers known to put one there:
    `ValueError("HY000")` could be any five letters of caller data."""
    assert foreign_label(ValueError("HY000")) == "ValueError"
    assert foreign_label(KeyError(1146)) == "KeyError"


def test_the_code_does_not_move_the_exit_family() -> None:
    exc = _caught(lambda: coded.pg())
    assert isinstance(classify_foreign(exc, "'tables'"), ProbeConnectionError)
    assert str(classify_foreign(exc, "'tables'")) == (
        "'tables' failed (PgError; SQLSTATE 42P01)"
    )
    value = ValueError("x")
    assert isinstance(classify_foreign(value, "'tables'"), ProbeArgumentError)


class _Provider:
    def __init__(self, mode: str) -> None:
        self.mode = mode

    @classmethod
    def for_config(cls, config: object) -> "_Provider":
        mode = getattr(config, "mode", "")
        if mode == "open":
            coded.aws()
        return cls(mode)

    def __enter__(self) -> "_Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        if self.mode == "close":
            coded.azure()

    @probe_method(name="things")
    def things(self) -> List[str]:
        """List things."""
        if self.mode == "call":
            coded.sqlalchemy_wrapping_pg()
        if self.mode == "cause-property":
            coded.cause_property()
        if self.mode == "hostile-getattribute":
            coded.hostile_getattribute()
        if self.mode == "wraps-cause-property":
            try:
                coded.cause_property()
            except Exception as exc:
                raise ProbeConnectionError("listing failed") from exc
        if self.mode == "wrapped":
            try:
                coded.http()
            except Exception as exc:
                raise ProbeConnectionError(f"listing failed: {exc}") from exc
        return []


RunFn = Callable[[str], ProbeMethodResult]


@pytest.fixture
def run(monkeypatch: pytest.MonkeyPatch) -> RunFn:
    from datahub.configuration.common import ConfigModel

    class _Config(ConfigModel):
        mode: str = ""

        @classmethod
        def probe_provider_class(cls) -> type:
            return _Provider

    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)

    def _run(mode: str) -> ProbeMethodResult:
        return run_probe_method("fake", {"mode": mode}, "things", {})

    return _run


@pytest.mark.parametrize(
    "mode, message",
    [
        ("call", "'things' failed (ProgrammingError; SQLSTATE 42P01)"),
        ("open", "opening source 'fake' failed (ClientError; AccessDenied)"),
        ("close", "closing source 'fake' failed (HttpResponseError; HTTP 404)"),
        ("wrapped", "listing failed: (HTTPError; HTTP 403)"),
    ],
)
def test_every_foreign_failure_path_shows_the_code(
    run: RunFn, mode: str, message: str
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run(mode)
    assert str(info.value) == message


@pytest.mark.parametrize(
    "mode, code",
    [
        ("call", "SQLSTATE 42P01"),
        ("open", "AccessDenied"),
        ("close", "HTTP 404"),
        ("wrapped", "HTTP 403"),
    ],
)
def test_the_cli_shows_the_code_and_keeps_exit_3(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    code: str,
) -> None:
    monkeypatch.setattr(rc, "_stdin_secrets", {}, raising=False)
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": mode}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )
    assert res.exit_code == 3, res.output
    assert code in res.stderr
    assert SENTINEL not in res.output


@pytest.mark.parametrize(
    "mode, label",
    [
        ("cause-property", "'things' failed (_CauseProperty)"),
        ("hostile-getattribute", "'things' failed (_HostileGetattribute)"),
        # Authored, so the backstop walks the chain looking for foreign text.
        ("wraps-cause-property", "listing failed"),
    ],
)
def test_reading_the_chain_runs_no_foreign_code(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    label: str,
) -> None:
    """A chain link or code attribute read through a property or an
    overridden __getattribute__ is foreign code, and its exception text
    would reach stderr through the CLI's catch-all."""
    monkeypatch.setattr(rc, "_stdin_secrets", {}, raising=False)
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": mode}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )
    assert res.exit_code == 3, res.output
    assert label in res.stderr
    assert SENTINEL not in res.output
