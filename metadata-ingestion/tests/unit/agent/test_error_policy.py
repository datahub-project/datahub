import base64
import builtins
import io
import json
import pathlib
from typing import (
    Annotated,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Type,
    cast,
)

import pytest
from click.testing import CliRunner
from pydantic import BaseModel, Field

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent import filter_check, probe_methods
from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.error_policy import (
    _MAX_CHAIN_LINKS,
    TRUSTED_TYPES,
    _foreign_in_chain,
    classify_foreign,
    is_trusted,
    label_foreign_text,
    police_trusted,
)
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import (
    BARE_FLAG,
    ProbeMethodResult,
    ProbeParam,
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
    Verdict,
    VerdictContext,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from tests.unit.agent import _foreign_errors
from tests.unit.agent._foreign_errors import SENTINEL


def test_probe_argument_error_is_a_value_error_but_not_a_soft_error() -> None:
    err = ProbeArgumentError("no project titled 'x'")
    assert isinstance(err, ValueError)
    assert not isinstance(err, ProbeSoftError)


def test_the_gate_refusals_are_trusted_argument_errors() -> None:
    assert issubclass(SqlScopeError, ProbeArgumentError)
    assert issubclass(ApiScopeError, ProbeArgumentError)
    for trusted in (*TRUSTED_TYPES, SqlScopeError, ApiScopeError):
        assert is_trusted(trusted("x"))


def _raise_here() -> None:
    raise ValueError(f"no thing named '{SENTINEL}'")


@pytest.mark.parametrize(
    "raiser",
    [
        _raise_here,
        _foreign_errors.fetch,
        _foreign_errors.lookup,
        # A plain ValueError from inside the framework package.
        lambda: probe_methods.clamp_item_limit(cast(int, "many")),
    ],
)
def test_an_untrusted_type_is_untrusted_wherever_it_was_raised(
    raiser: Callable[[], object],
) -> None:
    with pytest.raises(Exception) as info:
        raiser()
    assert not is_trusted(info.value)


class _RebuildRefusing(ProbeConnectionError):
    """A trusted type whose message is not its args: police_trusted cannot
    rebuild it in place, so it falls back to the framework type."""

    def __init__(self, code: int, detail: str) -> None:
        super().__init__(code, detail)
        self.code = code
        self.detail = detail

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}"


# Call modes that only run one foreign raiser.
_FOREIGN_CALLS: Dict[str, Callable[[], None]] = {
    "foreign-value": lambda: _foreign_errors.parse_url(
        f"jdbc:mysql://db?password={SENTINEL}"
    ),
    "foreign-transport": _foreign_errors.connect,
    "foreign-runtime": _foreign_errors.fetch,
    "foreign-permission": _foreign_errors.read_file,
    "foreign-programming": _foreign_errors.query,
    "foreign-key": _foreign_errors.lookup,
    "call-exit": _foreign_errors.exit_process,
    "call-abort": _foreign_errors.abort,
}


class _RaisesWhenIterated:
    """A provider value that reads fine and fails only once iterated, the way
    a lazy listing does: past the attribute read the framework polices."""

    def __init__(self, raiser: Callable[[], object]) -> None:
        self._raiser = raiser

    def __iter__(self) -> Iterator[str]:
        self._raiser()
        return iter(())


class _EntryWithUnreadableTitle:
    """A report entry whose title raises when the read-back renders it."""

    @property
    def title(self) -> str:
        _foreign_errors.fetch()
        return "unreachable"


class _ReportWithUnreadableEntry:
    def __init__(self) -> None:
        self.warnings = [_EntryWithUnreadableTitle()]
        self.failures: List[object] = []


_ITERATION_FAILURES: Dict[str, Callable[[], object]] = {
    "foreign": _foreign_errors.fetch,
    "key": _foreign_errors.lookup,
}


class _Provider:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self._failures: List[str] = []

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
        if mode == "open-exit":
            _foreign_errors.exit_process()
        if mode == "open-abort":
            _foreign_errors.abort()
        if mode == "open-interrupt":
            raise KeyboardInterrupt
        return cls(mode)

    @property
    def sql_dialect(self) -> str:
        # Read by the SQL gate after the provider is built, before the call.
        if self.mode == "dialect-foreign":
            _foreign_errors.fetch()
        if self.mode == "dialect-key":
            _foreign_errors.lookup()
        if self.mode == "dialect-exit":
            _foreign_errors.exit_process()
        if self.mode == "dialect-wraps-foreign":
            try:
                _foreign_errors.fetch()
            except RuntimeError as exc:
                raise ProbeConnectionError(f"dialect lookup failed: {exc}") from exc
        return "postgres"

    @property
    def api_allowlist(self) -> Iterable[str]:
        # Read and iterated by the path gate, before the call.
        if self.mode.startswith("allowlist-iter-"):
            return _RaisesWhenIterated(
                _ITERATION_FAILURES[self.mode.removeprefix("allowlist-iter-")]
            )
        return ("/things",)

    @property
    def warnings(self) -> Iterable[str]:
        # Read back after the call returns.
        if self.mode.startswith("warnings-iter-"):
            return _RaisesWhenIterated(
                _ITERATION_FAILURES[self.mode.removeprefix("warnings-iter-")]
            )
        if self.mode == "warnings-foreign":
            _foreign_errors.fetch()
        if self.mode == "warnings-wraps-foreign":
            try:
                _foreign_errors.connect()
            except Exception as exc:
                raise ProbeConnectionError(f"warnings unreadable: {exc}") from exc
        return []

    @property
    def failures(self) -> List[str]:
        # Read back after the call, and while a call failure is reported.
        if self.mode == "failures-foreign":
            _foreign_errors.fetch()
        return self._failures

    @failures.setter
    def failures(self, value: List[str]) -> None:
        self._failures = value

    @property
    def probe_report(self) -> object:
        if self.mode == "report-foreign":
            _foreign_errors.fetch()
        if self.mode == "report-entry-foreign":
            return _ReportWithUnreadableEntry()
        return None

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
        if self.mode == "exit-exit":
            _foreign_errors.exit_process()
        if self.mode == "exit-abort":
            _foreign_errors.abort()
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
        if self.mode == "plain-login":
            raise RuntimeError(f"login refused; password={SENTINEL}")
        foreign = _FOREIGN_CALLS.get(self.mode)
        if foreign is not None:
            foreign()
        if self.mode == "recorded-foreign":
            self.failures = ["GET /things returned 401"]
            _foreign_errors.parse_url(f"jdbc:mysql://db?password={SENTINEL}")
        if self.mode == "argument":
            raise ProbeArgumentError(f"no thing named '{name}'")
        if self.mode == "soft":
            raise ProbeSoftError(f"thing '{name}' was deleted mid-listing")
        if self.mode == "not-implemented":
            raise NotImplementedError(f"dialect quoted {SENTINEL}")
        if self.mode == "recorded-exit":
            self.failures = ["GET /things returned 401"]
            _foreign_errors.exit_process()
        if self.mode == "failures-foreign":
            raise ProbeArgumentError(f"no thing named '{name}'")
        return []

    @probe_method(name="sql", scoped_sql_param="query")
    def sql(self, query: str) -> List[str]:
        """Run a catalog query."""
        return []

    @probe_method(name="api", scoped_path_param="path")
    def api(self, path: str) -> List[str]:
        """Fetch one listed endpoint."""
        return []


RunFn = Callable[..., ProbeMethodResult]


@pytest.fixture
def run(monkeypatch: pytest.MonkeyPatch) -> RunFn:
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


class _Response(BaseModel):
    id: int


def _raised_by(call: Callable[[], object]) -> BaseException:
    try:
        call()
    except Exception as exc:
        return exc
    raise AssertionError("nothing was raised")


@pytest.mark.parametrize(
    "call, label",
    [
        # An SSO login page served with status 200 where JSON was expected.
        (lambda: json.loads("<html>sign in</html>"), "JSONDecodeError"),
        # A response failing the model it is parsed into.
        (lambda: _Response.model_validate({"id": "n/a"}), "ValidationError"),
        (lambda: b"\xff".decode("utf-8"), "UnicodeDecodeError"),
        # A base64 body that does not decode (binascii.Error).
        (lambda: base64.b64decode("abc", validate=True), "Error"),
        # An OSError that is also a ValueError.
        (lambda: io.StringIO().fileno(), "UnsupportedOperation"),
    ],
)
def test_a_value_error_reading_what_the_source_sent_is_a_connection_error(
    call: Callable[[], object], label: str
) -> None:
    exc = _raised_by(call)
    assert isinstance(exc, ValueError)
    classified = classify_foreign(exc, "'things'")
    assert isinstance(classified, ProbeConnectionError)
    assert str(classified) == f"'things' failed ({label})"


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
    # Any source can lack a command; the wording names no kind of source.
    assert "SQL" not in str(info.value)
    assert SENTINEL not in str(info.value)
    assert info.value.__cause__ is None


@pytest.mark.parametrize(
    "command, kwargs, env",
    [
        ("things", {}, {"DATAHUB_PROBE_DISABLED": "true"}),
        ("nosuch", {}, {}),
        ("things", {"bogus": "1"}, {}),
        ("sql", {}, {}),
        ("sql", {"query": "SELECT 1"}, {"DATAHUB_PROBE_DISABLE_RAW_ACCESS": "true"}),
    ],
)
def test_the_frameworks_own_refusals_are_trusted_argument_errors(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    kwargs: Dict[str, object],
    env: Dict[str, str],
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ProbeArgumentError):
        run_probe_method("fake", {"mode": ""}, command, kwargs)


class _Unbuildable:
    @probe_method(name="things")
    def things(self) -> List[str]:
        """List things."""
        return []


@pytest.mark.parametrize("provider_cls", [None, _Unbuildable])
def test_a_source_the_probe_cannot_build_is_refused_as_an_argument_error(
    run: RunFn, monkeypatch: pytest.MonkeyPatch, provider_cls: Optional[type]
) -> None:
    monkeypatch.setattr(probe_methods, "_provider_class", lambda _st: provider_cls)
    with pytest.raises(ProbeArgumentError):
        run_probe_method("fake", {"mode": ""}, "things", {})


@pytest.mark.parametrize(
    "param, value",
    [
        (ProbeParam("name", "str", True), BARE_FLAG),
        (ProbeParam("limit", "int", True), "many"),
        (ProbeParam("limit", "int", True), ["1"]),
        (ProbeParam("deep", "bool", True), "maybe"),
    ],
)
def test_a_value_its_parameter_cannot_take_is_a_trusted_argument_error(
    param: ProbeParam, value: object
) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        probe_methods._coerce(param, value)
    assert param.name in str(info.value)


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


def _unreadable(attribute: str, label: str) -> str:
    return (
        f"the probe provider is defective: reading _Provider.{attribute} "
        f"failed ({label})"
    )


@pytest.mark.parametrize(
    "mode, label",
    [
        ("dialect-foreign", "RuntimeError"),
        ("dialect-key", "KeyError"),
        ("dialect-exit", "SystemExit"),
    ],
)
def test_an_attribute_the_gate_reads_that_raises_is_the_providers_defect(
    run: RunFn, mode: str, label: str
) -> None:
    # Whatever it raised, an attribute the framework reads by name failing to
    # be read is a defect in the provider (exit 1), named by class and
    # attribute and never by the exception's text.
    with pytest.raises(ProbeInternalError) as info:
        run_probe_method("fake", {"mode": mode}, "sql", {"query": "SELECT 1"})
    assert str(info.value) == _unreadable("sql_dialect", label)


@pytest.mark.parametrize(
    "mode, attribute",
    [
        ("warnings-foreign", "warnings"),
        ("report-foreign", "probe_report"),
        # Read while the call's own refusal is reported.
        ("failures-foreign", "failures"),
    ],
)
def test_an_attribute_read_back_after_the_call_that_raises_is_the_providers_defect(
    run: RunFn, mode: str, attribute: str
) -> None:
    with pytest.raises(ProbeInternalError) as info:
        run(mode, name="widget")
    assert str(info.value) == _unreadable(attribute, "RuntimeError")


@pytest.mark.parametrize(
    "mode, command, kwargs, message",
    [
        (
            "dialect-wraps-foreign",
            "sql",
            {"query": "SELECT 1"},
            "dialect lookup failed: (RuntimeError)",
        ),
        (
            "warnings-wraps-foreign",
            "things",
            {},
            "warnings unreadable: (TransportError)",
        ),
    ],
)
def test_a_trusted_error_raised_by_an_attribute_is_policed_and_keeps_its_type(
    run: RunFn,
    mode: str,
    command: str,
    kwargs: Dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method("fake", {"mode": mode}, command, kwargs)
    assert str(info.value) == message


@pytest.mark.parametrize(
    "mode, command, kwargs, label",
    [
        ("dialect-wraps-foreign", "sql", {"query": "SELECT 1"}, "(RuntimeError)"),
        ("wraps-foreign-connection", "things", {}, "(TransportError)"),
        ("open-wraps-foreign", "things", {}, "(TransportError)"),
    ],
)
def test_a_policed_trusted_error_is_labelled_once_under_verbose(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    command: str,
    kwargs: Dict[str, object],
    label: str,
) -> None:
    # Policed where it is raised and nowhere else: a second pass would find
    # the shown text again and put a second label in front of it.
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method("fake", {"mode": mode}, command, kwargs)
    message = str(info.value)
    assert message.count(label) == 1, message
    assert f"{label}: " in message


@pytest.mark.parametrize(
    "mode, expected, message",
    [
        (
            "open-exit",
            ProbeConnectionError,
            "opening source 'fake' failed (SystemExit)",
        ),
        ("open-abort", ProbeConnectionError, "opening source 'fake' failed (Abort)"),
        ("call-exit", ProbeConnectionError, "'things' failed (SystemExit)"),
        ("call-abort", ProbeConnectionError, "'things' failed (Abort)"),
        (
            "recorded-exit",
            ProbeReadFailed,
            "SystemExit; the connector recorded: GET /things returned 401",
        ),
        (
            "exit-exit",
            ProbeConnectionError,
            "closing source 'fake' failed (SystemExit)",
        ),
        ("exit-abort", ProbeConnectionError, "closing source 'fake' failed (Abort)"),
    ],
)
def test_a_foreign_exit_is_named_by_class_not_printed(
    run: RunFn, mode: str, expected: type, message: str
) -> None:
    # A library calling sys.exit(reason), or raising its own BaseException,
    # would otherwise escape every handler and print its text.
    with pytest.raises(expected) as info:
        run(mode)
    assert str(info.value) == message


def test_an_interrupt_passes_through(run: RunFn) -> None:
    with pytest.raises(KeyboardInterrupt):
        run("open-interrupt")


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


def _quoting(raiser: Callable[[], object], own_values: Set[str]) -> str:
    """The backstop's rendering of a refusal that quotes str() of whatever
    `raiser` raised, in a call whose own argument values are `own_values`."""
    try:
        try:
            raiser()
        except LookupError as exc:
            raise ProbeArgumentError(f"lookup said {exc}") from exc
    except ProbeArgumentError as wrapper:
        return label_foreign_text(wrapper, own_values=own_values)
    raise AssertionError("raiser raised nothing")


def _missed_key() -> object:
    return {"a": 1}["widget-name"]


def _key_with_message() -> object:
    raise KeyError(f"key {SENTINEL}")


class _KeyRenderingElse(KeyError):
    def __str__(self) -> str:
        return f"held {SENTINEL}"


def _key_rendering_something_else() -> object:
    raise _KeyRenderingElse("widget-name")


def _index_with_text() -> object:
    raise IndexError(f"row 3 of 2 in {SENTINEL}")


def _key_subclass_with_message() -> object:
    from sqlalchemy.exc import NoSuchColumnError

    raise NoSuchColumnError(f"Could not locate column in row for column '{SENTINEL}'")


@pytest.mark.parametrize(
    "raiser, own_values, expected",
    [
        # str(KeyError(k)) is repr(k): the caller's own argument, kept.
        (_missed_key, {"widget-name"}, "lookup said 'widget-name'"),
        # The same key when the caller did not pass it: foreign text.
        (_missed_key, set(), "lookup said (KeyError)"),
        # A lookup error carrying a message of its own is foreign text, even
        # in a call that passed other values.
        (_key_with_message, {"widget-name"}, "lookup said (KeyError)"),
        (_key_rendering_something_else, {"widget-name"}, "lookup said (KeyError)"),
        (_index_with_text, set(), "lookup said (IndexError)"),
        (_key_subclass_with_message, set(), "lookup said (NoSuchColumnError)"),
    ],
)
def test_only_the_callers_own_missed_key_is_exempt_from_the_backstop(
    raiser: Callable[[], object], own_values: Set[str], expected: str
) -> None:
    rendered = _quoting(raiser, own_values)
    assert rendered.replace("_KeyRenderingElse", "KeyError") == expected
    assert SENTINEL not in rendered


class _WrapsForeignKeyConfig(ConfigModel):
    phase: str = ""
    how: str = ""
    trusted: str = "connection"


_TRUSTED_BY_NAME: Dict[str, Type[BaseException]] = {
    "argument": ProbeArgumentError,
    "soft": ProbeSoftError,
    "read-failed": ProbeReadFailed,
    "connection": ProbeConnectionError,
    "internal": ProbeInternalError,
    "sql-scope": SqlScopeError,
    "api-scope": ApiScopeError,
}


def _wrap_foreign_key(config: _WrapsForeignKeyConfig) -> None:
    """A trusted refusal quoting a foreign KeyError that carries text."""
    trusted = _TRUSTED_BY_NAME[config.trusted]
    try:
        _foreign_errors.lookup()
    except KeyError as exc:
        if config.how == "from":
            raise trusted(f"listing failed: {exc}") from exc
        if config.how == "none":
            raise trusted(f"listing failed: {exc}") from None
        raise trusted(f"listing failed: {exc}")  # noqa: B904


class _WrapsForeignKey:
    def __init__(self, config: _WrapsForeignKeyConfig) -> None:
        self._config = config

    @classmethod
    def for_config(cls, config: _WrapsForeignKeyConfig) -> "_WrapsForeignKey":
        if config.phase == "open":
            _wrap_foreign_key(config)
        return cls(config)

    def __enter__(self) -> "_WrapsForeignKey":
        if self._config.phase == "enter":
            _wrap_foreign_key(self._config)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._config.phase == "exit":
            _wrap_foreign_key(self._config)

    @probe_method(name="things")
    def things(self, name: str = "") -> List[str]:
        """List things."""
        if self._config.phase == "call":
            _wrap_foreign_key(self._config)
        return []


@pytest.fixture
def run_wrapping_a_foreign_key(monkeypatch: pytest.MonkeyPatch) -> RunFn:
    class _Config(_WrapsForeignKeyConfig):
        @classmethod
        def probe_provider_class(cls) -> type:
            return _WrapsForeignKey

    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)

    def _run(**config: object) -> ProbeMethodResult:
        # The caller passes a name, so the call has argument values of its own.
        return run_probe_method("fake", dict(config), "things", {"name": "widget"})

    return _run


@pytest.mark.parametrize("phase", ["open", "enter", "call", "exit"])
@pytest.mark.parametrize("how", ["from", "none", "implicit"])
def test_a_foreign_key_errors_text_is_withheld_in_every_phase(
    run_wrapping_a_foreign_key: RunFn, phase: str, how: str
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run_wrapping_a_foreign_key(phase=phase, how=how)
    assert str(info.value) == "listing failed: (KeyError)"


@pytest.mark.parametrize("trusted", sorted(_TRUSTED_BY_NAME))
def test_a_foreign_key_errors_text_is_withheld_under_every_trusted_type(
    run_wrapping_a_foreign_key: RunFn, trusted: str
) -> None:
    with pytest.raises(TRUSTED_TYPES) as info:
        run_wrapping_a_foreign_key(phase="call", how="from", trusted=trusted)
    assert isinstance(info.value, _TRUSTED_BY_NAME[trusted])
    assert str(info.value) == "listing failed: (KeyError)"


def test_a_short_foreign_text_is_not_searched_for() -> None:
    try:
        try:
            raise RuntimeError("x")
        except RuntimeError:
            raise ProbeArgumentError("no thing named 'x'")  # noqa: B904
    except ProbeArgumentError as wrapper:
        assert label_foreign_text(wrapper) == "no thing named 'x'"


def test_the_backstop_reads_the_context_as_well_as_the_cause() -> None:
    try:
        try:
            _foreign_errors.fetch()
        except RuntimeError as exc:
            raise ProbeConnectionError(f"fetch said {exc}")  # noqa: B904
    except ProbeConnectionError as wrapper:
        assert label_foreign_text(wrapper) == "fetch said (RuntimeError)"
        replacement = police_trusted(wrapper)
    assert isinstance(replacement, ProbeConnectionError)
    assert str(replacement) == "fetch said (RuntimeError)"


def test_the_backstop_reads_an_exception_groups_children() -> None:
    # Looked up, not named: the package still supports Python 3.10.
    group_type = getattr(builtins, "ExceptionGroup", None)
    if group_type is None:
        pytest.skip("ExceptionGroup is new in Python 3.11")
    child = RuntimeError(f"fetcher gave up on https://user:{SENTINEL}@host/api")
    try:
        try:
            raise group_type("connect", [child, OSError("refused")])
        except Exception:
            raise ProbeConnectionError(f"login failed: {child}")  # noqa: B904
    except ProbeConnectionError as wrapper:
        assert label_foreign_text(wrapper) == "login failed: (RuntimeError)"


def test_the_backstop_walks_a_bounded_number_of_an_exception_groups_children() -> None:
    group_type = getattr(builtins, "ExceptionGroup", None)
    if group_type is None:
        pytest.skip("ExceptionGroup is new in Python 3.11")
    children = [RuntimeError(f"child {i} of {SENTINEL}") for i in range(200)]
    try:
        try:
            raise group_type("fan-out", children)
        except Exception:
            raise ProbeConnectionError("fan-out failed")  # noqa: B904
    except ProbeConnectionError as wrapper:
        found = _foreign_in_chain(wrapper)
    # The group and the children read before the bound, out of 201 links.
    assert len(found) == _MAX_CHAIN_LINKS


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
        message = label_foreign_text(wrapper)
    assert message.startswith("fetch said (RuntimeError): fetcher gave up on https://")
    assert SENTINEL not in message


def test_the_verbose_text_follows_the_label_so_the_cli_scrub_keeps_it_closed(
    run: RunFn, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # The CLI scrubs the message again, and a masked value runs to the next
    # space or separator: inside the parenthesis it would take the `)`.
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": "plain-login"}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )
    assert res.exit_code == 3, res.output
    assert json.loads(res.stderr)["error"] == (
        "'things' failed (RuntimeError): login refused; password=***"
    )


@pytest.mark.parametrize(
    "mode, exit_code",
    [
        ("dialect-foreign", 1),
        ("dialect-key", 1),
        ("dialect-exit", 1),
        ("dialect-wraps-foreign", 3),
    ],
)
def test_the_cli_polices_a_provider_attribute_the_gate_reads(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    exit_code: int,
) -> None:
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": mode}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe,
        ["probe", "run", "sql", "--recipe", str(recipe_file), "--query", "SELECT 1"],
    )
    assert res.exit_code == exit_code, res.output
    assert SENTINEL not in res.output
    assert "Traceback" not in res.output
    if exit_code == 1:
        assert json.loads(res.stderr)["error"].startswith(
            "the probe provider is defective: reading _Provider.sql_dialect failed"
        )


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
        ("warnings-foreign", 1),
        ("warnings-wraps-foreign", 3),
        ("report-foreign", 1),
        ("failures-foreign", 1),
        ("open-exit", 3),
        ("open-abort", 3),
        ("call-exit", 3),
        ("call-abort", 3),
        ("exit-exit", 3),
        ("exit-abort", 3),
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


# --- an untrusted error from the gate or the read-back ------------------------
#
# Both run outside the handlers that police the open, the call and each
# attribute read: the gate iterates the allowlist it read, and the read-back
# iterates and renders what the provider handed back. What either raises is
# classified like a call failure, so its text is withheld and its type alone
# picks the exit code.


@pytest.mark.parametrize(
    "mode, command, kwargs, expected, exit_code",
    [
        ("allowlist-iter-foreign", "api", {"path": "/things"}, ProbeConnectionError, 3),
        ("allowlist-iter-key", "api", {"path": "/things"}, ProbeInternalError, 1),
        ("warnings-iter-foreign", "things", {}, ProbeConnectionError, 3),
        ("warnings-iter-key", "things", {}, ProbeInternalError, 1),
        ("report-entry-foreign", "things", {}, ProbeConnectionError, 3),
    ],
)
def test_an_untrusted_error_from_the_gate_or_the_read_back_is_withheld(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    command: str,
    kwargs: Dict[str, str],
    expected: Type[Exception],
    exit_code: int,
) -> None:
    with pytest.raises(expected) as info:
        run_probe_method("fake", {"mode": mode}, command, dict(kwargs))
    assert SENTINEL not in str(info.value)
    assert f"'{command}'" in str(info.value)

    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("fake", {"mode": mode}, set())
    )
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    flags = [arg for name, value in kwargs.items() for arg in (f"--{name}", value)]
    res = CliRunner().invoke(
        recipe, ["probe", "run", command, "--recipe", str(recipe_file), *flags]
    )
    assert res.exit_code == exit_code, res.output
    assert SENTINEL not in res.output
    assert "Traceback" not in res.output


# --- a config hook's trusted error quoting foreign text ------------------------


class _CatalogError(Exception):
    """Stands in for a reused library's error, quoting what it was given."""


def _overriding_config(trusted: Type[Exception], how: str) -> Type[ConfigModel]:
    class _Overrides(ConfigModel):
        database_pattern: Annotated[
            AllowDenyPattern, Filters(DatasetContainerSubTypes.DATABASE)
        ] = Field(default=AllowDenyPattern.allow_all())

        def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
            try:
                raise _CatalogError(f"catalog lookup refused for {SENTINEL}")
            except _CatalogError as exc:
                if how == "from":
                    raise trusted(f"cannot judge {ctx.name}: {exc}") from exc
                raise trusted(f"cannot judge {ctx.name}: {exc!r}")  # noqa: B904

    return _Overrides


_EXIT_CODE_OF: Dict[Type[Exception], int] = {
    ProbeArgumentError: 2,
    ProbeSoftError: 2,
    ProbeReadFailed: 1,
    ProbeConnectionError: 3,
    ProbeInternalError: 1,
}


@pytest.mark.parametrize("how", ["from", "implicit"])
@pytest.mark.parametrize("trusted", TRUSTED_TYPES, ids=lambda t: t.__name__)
def test_a_config_hooks_trusted_error_keeps_its_type_but_not_the_foreign_text(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    trusted: Type[Exception],
    how: str,
) -> None:
    """call_config_hook re-polices a trusted error: the connector chose its
    type, so the exit code stands, but the foreign text it quotes is the
    reused library's and is replaced by that error's label."""
    config_cls = _overriding_config(trusted, how)
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: config_cls)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])
    kind = str(DatasetContainerSubTypes.DATABASE)

    with pytest.raises(trusted) as info:
        check_filters(
            source_type="fake",
            config_dict={},
            kind=kind,
            parent_path=[],
            names=["sales"],
        )
    assert type(info.value) is trusted
    assert SENTINEL not in str(info.value)
    assert "_CatalogError" in str(info.value)

    monkeypatch.setattr(rc, "_resolve_for_probe", lambda _r: ("fake", {}, set()))
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            str(recipe_file),
            "--kind",
            kind,
            "--name",
            "sales",
        ],
    )
    assert res.exit_code == _EXIT_CODE_OF[trusted], res.output
    assert SENTINEL not in res.output
    assert "Traceback" not in res.output
