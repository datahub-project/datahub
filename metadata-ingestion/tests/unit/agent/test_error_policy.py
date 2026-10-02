import pathlib
from typing import Callable, List

import pytest
from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.error_policy import (
    TRUSTED_TYPES,
    is_trusted,
    police_trusted,
    withhold_foreign_text,
)
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.sql_gate import SqlScopeError
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)
from tests.unit.agent import _foreign_errors
from tests.unit.agent._foreign_errors import SENTINEL


@pytest.fixture(autouse=True)
def _quiet_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAHUB_PROBE_VERBOSE_LOGS", raising=False)


def test_probe_argument_error_is_a_value_error_but_not_a_soft_error() -> None:
    err = ProbeArgumentError("no project titled 'x'")
    assert isinstance(err, ValueError)
    assert not isinstance(err, ProbeSoftError)


def test_the_gate_refusals_are_trusted_argument_errors() -> None:
    assert issubclass(SqlScopeError, ProbeArgumentError)
    assert issubclass(ApiScopeError, ProbeArgumentError)
    for trusted in (*TRUSTED_TYPES, SqlScopeError, ApiScopeError):
        assert is_trusted(trusted("x"))


@pytest.mark.parametrize(
    "exc", [ValueError("x"), RuntimeError("x"), TypeError("x"), KeyError("x")]
)
def test_any_other_type_is_not_trusted_wherever_it_was_raised(
    exc: BaseException,
) -> None:
    assert not is_trusted(exc)


class _RebuildRefusing(ProbeConnectionError):
    """A trusted type whose message is not its args: police_authored cannot
    rebuild it in place, so it falls back to the framework type."""

    def __init__(self, code: int, detail: str) -> None:
        super().__init__(code, detail)
        self.code = code
        self.detail = detail

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}"


class _Provider:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.failures: List[str] = []

    @classmethod
    def for_config(cls, config: object) -> "_Provider":
        mode = getattr(config, "mode", "")
        if mode == "open-foreign":
            _foreign_errors.connect()
        if mode == "open-foreign-value":
            _foreign_errors.parse_url(f"jdbc:mysql://db?password={SENTINEL}")
        if mode == "open-foreign-runtime":
            _foreign_errors.fetch()
        if mode == "open-plain-value":
            raise ValueError(f"database '{SENTINEL}' is not listed")
        if mode == "open-argument":
            raise ProbeArgumentError("database 'x' is not listed; run `databases`")
        if mode == "open-wraps-foreign":
            try:
                _foreign_errors.connect()
            except Exception as exc:
                raise ProbeConnectionError(f"login failed: {exc}") from exc
        return cls(mode)

    def __enter__(self) -> "_Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        if self.mode.startswith("exit-foreign"):
            _foreign_errors.close()
        if self.mode == "exit-trusted":
            raise ProbeConnectionError("closing the session timed out")
        if self.mode == "exit-plain-defect":
            raise TypeError(f"close bookkeeping broke on {SENTINEL}")
        if self.mode == "exit-plain-key":
            raise KeyError(f"no session {SENTINEL}")
        if self.mode == "exit-plain-value":
            raise ValueError(f"session {SENTINEL} was already closed")
        return None

    def _raise_wrapped(self, name: str) -> None:
        """Trusted exceptions whose message quotes a foreign one."""
        if self.mode == "wraps-foreign-connection":
            try:
                _foreign_errors.connect()
            except Exception as exc:
                raise ProbeConnectionError(f"listing failed: {exc}") from exc
        if self.mode == "wraps-foreign-value-implicitly":
            try:
                _foreign_errors.parse_name(name)
            except ValueError as exc:
                raise ProbeArgumentError(f"bad url: {exc!r}")  # noqa: B904
        if self.mode == "wraps-foreign-rebuild-refusing":
            try:
                _foreign_errors.query()
            except Exception as exc:
                raise _RebuildRefusing(500, f"fetch said {exc}") from exc
        if self.mode == "wraps-foreign-unprintable":
            try:
                _foreign_errors.unprintable()
            except Exception as exc:
                raise ProbeConnectionError("listing failed") from exc
        if self.mode == "recorded-wraps-foreign":
            self.failures = ["GET /things returned 401"]
            try:
                _foreign_errors.connect()
            except Exception as exc:
                raise ProbeArgumentError(f"no things: {exc}") from exc

    def _raise_after_lookup(self, name: str) -> None:
        """Trusted refusals raised while handling the provider's own lookup."""
        known = {"a": 1}
        rows = {1: "a", 2: "b"}
        if self.mode == "lookup-from-none":
            try:
                known[name]
            except KeyError:
                raise ProbeArgumentError(
                    f"no thing named '{name}'; run `things`"
                ) from None
        if self.mode == "lookup-from-exc":
            try:
                known[name]
            except KeyError as exc:
                raise ProbeArgumentError(
                    f"no thing named '{name}'; run `things`"
                ) from exc
        if self.mode == "lookup-implicit":
            try:
                known[name]
            except KeyError:
                raise ProbeArgumentError(  # noqa: B904
                    f"no thing named '{name}'; run `things`"
                )
        if self.mode == "lookup-short-key":
            try:
                rows[3]
            except KeyError:
                raise ProbeArgumentError(  # noqa: B904
                    "row 3 is not one of the 2 rows listed; rows 1 to 2 exist"
                )
        if self.mode == "lookup-foreign-repr":
            try:
                _foreign_errors.lookup()
            except KeyError as exc:
                raise ProbeConnectionError(f"lookup said {exc!r}") from exc

    @probe_method(name="things")
    def things(self, name: str = "") -> List[str]:
        """List things."""
        if self.mode.startswith("lookup-"):
            self._raise_after_lookup(name)
        if self.mode == "exit-foreign-after-argument":
            raise ProbeArgumentError(f"no thing named '{name}'")
        if self.mode == "exit-foreign-after-foreign":
            _foreign_errors.fetch()
        if (
            self.mode.startswith("wraps-foreign")
            or self.mode == "recorded-wraps-foreign"
        ):
            self._raise_wrapped(name)
        if self.mode == "plain-index":
            raise IndexError(f"row 3 of 2 in {SENTINEL}")
        if self.mode == "plain-name":
            raise NameError(f"undefined_helper {SENTINEL}")
        if self.mode == "plain-value":
            raise ValueError(f"no thing named '{name}'")
        if self.mode == "plain-runtime":
            raise RuntimeError(f"gave up on {name}")
        if self.mode == "foreign-value":
            _foreign_errors.parse_url(f"jdbc:mysql://db?password={SENTINEL}")
        if self.mode == "foreign-transport":
            _foreign_errors.connect()
        if self.mode == "foreign-runtime":
            _foreign_errors.fetch()
        if self.mode == "foreign-permission":
            _foreign_errors.read_file()
        if self.mode == "foreign-programming":
            _foreign_errors.query()
        if self.mode == "foreign-key":
            _foreign_errors.lookup()
        if self.mode == "recorded-foreign":
            self.failures = ["GET /things returned 401"]
            _foreign_errors.parse_url(f"jdbc:mysql://db?password={SENTINEL}")
        if self.mode == "argument":
            raise ProbeArgumentError(f"no thing named '{name}'")
        if self.mode == "soft":
            raise ProbeSoftError(f"thing '{name}' was deleted mid-listing")
        if self.mode == "not-implemented":
            raise NotImplementedError(f"dialect quoted {SENTINEL}")
        return []


RunFn = Callable[..., ProbeMethodResult]


@pytest.fixture
def run(monkeypatch: pytest.MonkeyPatch) -> RunFn:
    from datahub.configuration.common import ConfigModel

    class _Config(ConfigModel):
        mode: str = ""

        @classmethod
        def probe_provider_class(cls) -> type:
            return _Provider

    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)

    def _run(mode: str, **kwargs: object) -> ProbeMethodResult:
        return run_probe_method("fake", {"mode": mode}, "things", dict(kwargs))

    return _run


def test_a_providers_plain_value_error_keeps_exit_2_but_not_its_text(
    run: RunFn,
) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        run("plain-value", name="widget")
    assert str(info.value) == "'things' failed (ValueError)"


@pytest.mark.parametrize("mode", ["argument", "soft"])
def test_a_trusted_type_keeps_its_message(run: RunFn, mode: str) -> None:
    with pytest.raises(ValueError) as info:
        run(mode, name="widget")
    assert isinstance(info.value, (ProbeArgumentError, ProbeSoftError))
    assert "'widget'" in str(info.value)


def test_a_foreign_value_error_reports_only_its_class(run: RunFn) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        run("foreign-value")
    assert str(info.value) == "'things' failed (ValueError)"


@pytest.mark.parametrize(
    "mode, expected, label",
    [
        ("foreign-runtime", ProbeConnectionError, "RuntimeError"),
        ("foreign-permission", ProbeConnectionError, "PermissionError"),
        ("foreign-programming", ProbeConnectionError, "ProgrammingError"),
        ("foreign-transport", ProbeConnectionError, "TransportError"),
        ("plain-runtime", ProbeConnectionError, "RuntimeError"),
        ("foreign-key", ProbeInternalError, "KeyError"),
        ("plain-index", ProbeInternalError, "IndexError"),
        ("plain-name", ProbeInternalError, "NameError"),
    ],
)
def test_an_untrusted_call_failure_keeps_its_exit_family_and_loses_its_text(
    run: RunFn, mode: str, expected: type, label: str
) -> None:
    with pytest.raises(expected) as info:
        run(mode, name="widget")
    assert str(info.value) == f"'things' failed ({label})"


def test_a_recorded_failure_withholds_the_foreign_text(run: RunFn) -> None:
    with pytest.raises(ProbeReadFailed) as info:
        run("recorded-foreign")
    assert str(info.value) == (
        "ValueError; the connector recorded: GET /things returned 401"
    )


def test_an_unsupported_command_is_a_trusted_argument_error(run: RunFn) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        run("not-implemented")
    assert "does not support the 'things' command" in str(info.value)
    assert SENTINEL not in str(info.value)


@pytest.mark.parametrize(
    "mode, class_name",
    [
        ("open-foreign", "TransportError"),
        ("open-foreign-value", "ValueError"),
        ("open-foreign-runtime", "RuntimeError"),
        ("open-plain-value", "ValueError"),
    ],
)
def test_any_untrusted_error_while_opening_is_a_connection_error(
    run: RunFn, mode: str, class_name: str
) -> None:
    # The caller's input was all checked before the provider is built, so an
    # untrusted failure there is the source's (exit 3), whatever its type.
    with pytest.raises(ProbeConnectionError) as info:
        run(mode)
    assert str(info.value) == f"opening source 'fake' failed ({class_name})"


def test_a_trusted_argument_error_while_opening_exits_2(run: RunFn) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        run("open-argument")
    assert "is not listed" in str(info.value)


@pytest.mark.parametrize(
    "mode, class_name",
    [
        ("exit-foreign", "TypeError"),
        ("exit-plain-defect", "TypeError"),
        ("exit-plain-key", "KeyError"),
        ("exit-plain-value", "ValueError"),
    ],
)
def test_an_untrusted_failure_closing_the_source_is_a_connection_error(
    run: RunFn, mode: str, class_name: str
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run(mode)
    assert str(info.value) == f"closing source 'fake' failed ({class_name})"


def test_a_trusted_failure_closing_the_source_keeps_its_message(run: RunFn) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("exit-trusted")
    assert str(info.value) == "closing the session timed out"


def test_a_close_failure_does_not_mask_the_commands_own_failure(run: RunFn) -> None:
    # contextlib semantics: the close failure would replace the body's, so the
    # caller would read "closing failed" for what was a wrong argument.
    with pytest.raises(ProbeArgumentError) as info:
        run("exit-foreign-after-argument", name="widget")
    assert str(info.value) == "no thing named 'widget'"


def test_a_close_failure_keeps_the_commands_policed_foreign_failure(
    run: RunFn,
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("exit-foreign-after-foreign")
    assert str(info.value) == "'things' failed (RuntimeError)"


@pytest.mark.parametrize(
    "mode, expected, message",
    [
        ("open-wraps-foreign", ProbeConnectionError, "login failed: (TransportError)"),
        (
            "wraps-foreign-connection",
            ProbeConnectionError,
            "listing failed: (TransportError)",
        ),
        ("wraps-foreign-value-implicitly", ProbeArgumentError, "bad url: (ValueError)"),
        (
            "wraps-foreign-rebuild-refusing",
            ProbeConnectionError,
            "500: fetch said (ProgrammingError)",
        ),
        (
            "recorded-wraps-foreign",
            ProbeReadFailed,
            "no things: (TransportError); the connector recorded: "
            "GET /things returned 401",
        ),
        ("wraps-foreign-unprintable", ProbeConnectionError, "listing failed"),
    ],
)
def test_foreign_text_wrapped_in_a_trusted_message_is_withheld(
    run: RunFn, mode: str, expected: type, message: str
) -> None:
    # Trusted types are trusted wherever raised, so f"...: {exc}" around a
    # foreign exception would otherwise carry its text out. The exit family
    # stays; the foreign text becomes its label.
    with pytest.raises(expected) as info:
        run(mode, name="widget")
    assert str(info.value) == message


@pytest.mark.parametrize(
    "mode, expected, message",
    [
        (
            "lookup-from-none",
            ProbeArgumentError,
            "no thing named 'widget'; run `things`",
        ),
        (
            "lookup-from-exc",
            ProbeArgumentError,
            "no thing named 'widget'; run `things`",
        ),
        (
            "lookup-implicit",
            ProbeArgumentError,
            "no thing named 'widget'; run `things`",
        ),
        (
            "lookup-short-key",
            ProbeArgumentError,
            "row 3 is not one of the 2 rows listed; rows 1 to 2 exist",
        ),
        ("lookup-foreign-repr", ProbeConnectionError, "lookup said (KeyError)"),
    ],
)
def test_a_missed_key_is_not_mistaken_for_foreign_text(
    run: RunFn, mode: str, expected: type, message: str
) -> None:
    # A lookup error's str is the key it missed -- the caller's own argument --
    # so only its repr counts as its text; and a rendering of a few characters
    # would match innocently anywhere in the message.
    with pytest.raises(expected) as info:
        run(mode, name="widget")
    assert str(info.value) == message


def test_a_short_foreign_text_is_not_searched_for() -> None:
    try:
        try:
            raise RuntimeError("x")
        except RuntimeError:
            raise ProbeArgumentError("no thing named 'x'")  # noqa: B904
    except ProbeArgumentError as wrapper:
        assert withhold_foreign_text(wrapper) == "no thing named 'x'"


def test_the_backstop_reads_the_context_as_well_as_the_cause() -> None:
    try:
        try:
            _foreign_errors.fetch()
        except RuntimeError as exc:
            raise ProbeConnectionError(f"fetch said {exc}")  # noqa: B904
    except ProbeConnectionError as wrapper:
        assert withhold_foreign_text(wrapper) == "fetch said (RuntimeError)"
        replacement = police_trusted(wrapper)
    assert isinstance(replacement, ProbeConnectionError)
    assert str(replacement) == "fetch said (RuntimeError)"


class _SoftRebuildRefusing(ProbeSoftError):
    def __init__(self, endpoint: str, detail: str) -> None:
        super().__init__(endpoint, detail)
        self.endpoint = endpoint
        self.detail = detail

    def __str__(self) -> str:
        return f"{self.endpoint}: {self.detail}"


def test_an_unrebuildable_trusted_type_falls_back_to_its_own_family() -> None:
    try:
        try:
            _foreign_errors.fetch()
        except RuntimeError as exc:
            raise _SoftRebuildRefusing("/reports", f"said {exc}") from exc
    except _SoftRebuildRefusing as wrapper:
        replacement = police_trusted(wrapper)
    assert type(replacement) is ProbeSoftError
    assert str(replacement) == "/reports: said (RuntimeError)"


def test_a_trusted_exception_quoting_nothing_foreign_is_left_alone() -> None:
    assert police_trusted(ProbeArgumentError("no thing named 'x'")) is None


def test_the_backstop_shows_the_scrubbed_text_under_verbose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    try:
        try:
            _foreign_errors.fetch()
        except RuntimeError as exc:
            raise ProbeConnectionError(f"fetch said {exc}") from exc
    except ProbeConnectionError as wrapper:
        message = withhold_foreign_text(wrapper)
    assert message.startswith("fetch said (RuntimeError: fetcher gave up on https://")
    assert SENTINEL not in message


@pytest.mark.parametrize(
    "mode, exit_code",
    [
        ("foreign-value", 2),
        ("foreign-runtime", 3),
        ("foreign-permission", 3),
        ("foreign-programming", 3),
        ("foreign-key", 1),
        ("foreign-transport", 3),
        ("recorded-foreign", 3),
        ("open-foreign", 3),
        ("open-foreign-value", 3),
        ("open-foreign-runtime", 3),
        ("open-plain-value", 3),
        ("plain-value", 2),
        ("plain-index", 1),
        ("plain-name", 1),
        ("argument", 2),
        ("soft", 2),
        ("not-implemented", 2),
        ("open-argument", 2),
        ("exit-foreign", 3),
        ("exit-plain-defect", 3),
        ("exit-plain-key", 3),
        ("exit-plain-value", 3),
        ("exit-foreign-after-argument", 2),
        ("exit-foreign-after-foreign", 3),
        ("exit-trusted", 3),
        ("open-wraps-foreign", 3),
        ("wraps-foreign-connection", 3),
        ("wraps-foreign-value-implicitly", 2),
        ("wraps-foreign-rebuild-refusing", 3),
        ("recorded-wraps-foreign", 3),
        ("wraps-foreign-unprintable", 3),
        ("lookup-from-none", 2),
        ("lookup-foreign-repr", 3),
    ],
)
def test_the_cli_never_prints_foreign_text_from_the_exception_chain(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    exit_code: int,
) -> None:
    # The CLI renders str(exc) only; this pins that neither __cause__ nor
    # __context__ (still set under `from None`) reaches the output either,
    # and that withholding the text never moves the exit code.
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": mode}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )
    assert res.exit_code == exit_code, res.output
    assert SENTINEL not in res.output
    assert "Traceback" not in res.output
