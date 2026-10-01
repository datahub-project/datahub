import pathlib
from typing import Callable, List

import pytest
from click.testing import CliRunner, Result

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.error_policy import is_authored
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)
from tests.unit.agent import _base_provider, _foreign_errors
from tests.unit.agent._foreign_errors import SENTINEL


def test_probe_argument_error_is_a_value_error_but_not_a_soft_error() -> None:
    err = ProbeArgumentError("no project titled 'x'")
    assert isinstance(err, ValueError)
    assert not isinstance(err, ProbeSoftError)


class _OddError(Exception):
    """An authored type whose constructor is not (message,)."""

    def __init__(self, code: int, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


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
        if mode == "open-authored":
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
        if self.mode == "exit-authored":
            raise ProbeConnectionError("closing the session timed out")
        if self.mode == "exit-authored-defect":
            raise TypeError("close bookkeeping broke")
        return None

    def _raise_wrapped(self, name: str) -> None:
        """Authored exceptions whose message quotes a foreign one."""
        if self.mode == "wraps-foreign-connection":
            try:
                _foreign_errors.connect()
            except Exception as exc:
                raise ProbeConnectionError(f"listing failed: {exc}") from exc
        if self.mode == "wraps-foreign-value-implicitly":
            try:
                _foreign_errors.parse_name(name)
            except ValueError as exc:
                raise ValueError(f"bad url: {exc!r}")  # noqa: B904
        if self.mode == "wraps-foreign-odd-ctor":
            try:
                _foreign_errors.query()
            except Exception as exc:
                raise _OddError(500, f"fetch said {exc}") from exc
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
                raise ValueError(f"no things: {exc}") from exc

    @probe_method(name="things")
    def things(self, name: str = "") -> List[str]:
        """List things."""
        if self.mode == "exit-foreign-after-authored":
            raise ProbeArgumentError(f"no thing named '{name}'")
        if self.mode == "exit-foreign-after-foreign":
            _foreign_errors.fetch()
        if (
            self.mode.startswith("wraps-foreign")
            or self.mode == "recorded-wraps-foreign"
        ):
            self._raise_wrapped(name)
        if self.mode == "authored-index":
            raise IndexError("row 3 of 2")
        if self.mode == "authored-name":
            raise NameError("undefined_helper")
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
        if self.mode == "authored-value":
            raise ValueError(f"no thing named '{name}'")
        if self.mode == "authored-arg":
            raise ProbeArgumentError(f"no thing named '{name}'")
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


def test_a_foreign_value_error_reports_only_its_class(run: RunFn) -> None:
    # Still the bad-argument family (exit 2): withholding the text must not
    # also move the exit code.
    with pytest.raises(ProbeArgumentError) as info:
        run("foreign-value")
    assert SENTINEL not in str(info.value)
    assert "ValueError" in str(info.value)


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("foreign-runtime", ProbeConnectionError),
        ("foreign-permission", ProbeConnectionError),
        ("foreign-programming", ProbeConnectionError),
        ("foreign-key", ProbeInternalError),
    ],
)
def test_a_foreign_call_failure_keeps_its_exit_family(
    run: RunFn, mode: str, expected: type
) -> None:
    with pytest.raises(expected) as info:
        run(mode)
    assert SENTINEL not in str(info.value)


def test_a_recorded_failure_withholds_the_foreign_text(run: RunFn) -> None:
    with pytest.raises(ProbeReadFailed) as info:
        run("recorded-foreign")
    assert SENTINEL not in str(info.value)
    assert "ValueError" in str(info.value)
    assert "returned 401" in str(info.value)


def test_a_provider_without_a_source_file_authors_nothing_by_file(
    run: RunFn, monkeypatch: pytest.MonkeyPatch
) -> None:
    # inspect.getsourcefile raises TypeError for a class whose module has no
    # __file__; the probe must still run, treating nothing as authored by file.
    def _no_file(obj: object) -> str:
        raise TypeError("built-in class")

    monkeypatch.setattr(probe_methods.inspect, "getsourcefile", _no_file)
    with pytest.raises(ProbeArgumentError) as info:
        run("authored-value", name="widget")
    assert "widget" not in str(info.value)
    assert "ValueError" in str(info.value)


def test_an_exception_with_no_traceback_is_not_authored() -> None:
    assert not is_authored(RuntimeError("never raised"), {__file__})


def test_a_raise_in_the_provider_file_is_authored() -> None:
    try:
        raise RuntimeError("here")
    except RuntimeError as exc:
        assert is_authored(exc, {__file__})


def test_a_foreign_transport_error_is_unreachable_with_no_text(run: RunFn) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("foreign-transport")
    assert SENTINEL not in str(info.value)


def test_an_authored_value_error_keeps_its_message_and_exits_2(run: RunFn) -> None:
    with pytest.raises(ValueError) as info:
        run("authored-value", name="widget")
    assert not isinstance(info.value, (ProbeInternalError, ProbeConnectionError))
    assert "widget" in str(info.value)


def test_a_probe_argument_error_keeps_its_message(run: RunFn) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        run("authored-arg", name="widget")
    assert "widget" in str(info.value)


def test_a_foreign_error_while_opening_reports_only_its_class(run: RunFn) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("open-foreign")
    assert SENTINEL not in str(info.value)


@pytest.mark.parametrize(
    "mode, class_name",
    [("open-foreign-value", "ValueError"), ("open-foreign-runtime", "RuntimeError")],
)
def test_any_foreign_error_while_opening_is_a_connection_error(
    run: RunFn, mode: str, class_name: str
) -> None:
    # Not only the transport/auth names: the provider is being built, so any
    # foreign failure there is the source's (exit 3), never "fix your input".
    with pytest.raises(ProbeConnectionError) as info:
        run(mode)
    assert not isinstance(info.value, ValueError)
    assert SENTINEL not in str(info.value)
    assert class_name in str(info.value)


def test_an_authored_argument_error_while_opening_exits_2(run: RunFn) -> None:
    with pytest.raises(ProbeArgumentError):
        run("open-authored")


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
        ("authored-value", 2),
        ("authored-arg", 2),
        ("open-authored", 2),
        ("exit-foreign", 3),
        ("exit-foreign-after-authored", 2),
        ("exit-foreign-after-foreign", 3),
        ("exit-authored", 3),
        ("authored-index", 1),
        ("authored-name", 1),
        ("open-wraps-foreign", 3),
        ("wraps-foreign-connection", 3),
        ("wraps-foreign-value-implicitly", 2),
        ("wraps-foreign-odd-ctor", 3),
        ("recorded-wraps-foreign", 3),
        ("exit-authored-defect", 1),
        ("wraps-foreign-unprintable", 3),
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
    monkeypatch.setattr(rc, "_stdin_secrets", {}, raising=False)
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


def test_a_foreign_failure_closing_the_source_reports_only_its_class(
    run: RunFn,
) -> None:
    # Raised by the provider's __exit__, after the command returned: still the
    # source's failure (exit 3), never "fix your input", and never its text.
    with pytest.raises(ProbeConnectionError) as info:
        run("exit-foreign")
    assert SENTINEL not in str(info.value)
    assert "closing" in str(info.value)
    assert "TypeError" in str(info.value)


def test_an_authored_failure_closing_the_source_keeps_its_message(
    run: RunFn,
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("exit-authored")
    assert "timed out" in str(info.value)


def test_a_close_failure_does_not_mask_the_commands_own_failure(
    run: RunFn,
) -> None:
    # contextlib semantics: the close failure would replace the body's, so the
    # caller would read "closing failed" for what was a wrong argument.
    with pytest.raises(ProbeArgumentError) as info:
        run("exit-foreign-after-authored", name="widget")
    assert "widget" in str(info.value)
    assert SENTINEL not in str(info.value)


def test_a_close_failure_keeps_the_commands_policed_foreign_failure(
    run: RunFn,
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("exit-foreign-after-foreign")
    assert "RuntimeError" in str(info.value)
    assert "TypeError" not in str(info.value)
    assert SENTINEL not in str(info.value)


@pytest.mark.parametrize("mode", ["authored-index", "authored-name"])
def test_an_authored_index_or_name_error_is_a_defect(run: RunFn, mode: str) -> None:
    with pytest.raises(ProbeInternalError):
        run(mode)


def _invoke_cli(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    provider_cls: type,
    command: str,
    *params: str,
) -> Result:
    monkeypatch.setattr(rc, "_stdin_secrets", {}, raising=False)
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda _r: ("fake", {}, set()))
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    monkeypatch.setattr(probe_methods, "_provider_class", lambda _st: provider_cls)

    class _Config:
        @classmethod
        def model_validate(cls, d: object) -> "_Config":
            return cls()

    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    return CliRunner().invoke(
        recipe,
        ["probe", "run", command, "--recipe", str(recipe_file), *params],
    )


class _SubclassedProvider(_base_provider.BaseThingsProvider):
    """Adds nothing: its commands and their errors live in the base's file."""


def test_a_provider_base_class_in_another_file_keeps_its_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _invoke_cli(
        monkeypatch, tmp_path, _SubclassedProvider, "things", "--name", "widget"
    )
    assert res.exit_code == 2, res.output
    assert "no thing named 'widget'" in res.output


class _SourceBackedProvider(_base_provider.SourceWithProbeMethod):
    @classmethod
    def for_config(cls, config: object) -> "_SourceBackedProvider":
        return cls.__new__(cls)

    def __enter__(self) -> "_SourceBackedProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_an_ingestion_source_base_class_does_not_vouch_for_its_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _invoke_cli(monkeypatch, tmp_path, _SourceBackedProvider, "widgets")
    assert res.exit_code == 2, res.output
    assert _base_provider.SOURCE_SENTINEL not in res.output
    assert "ValueError" in res.output


@pytest.mark.parametrize(
    "mode, expected, kept",
    [
        ("open-wraps-foreign", ProbeConnectionError, "login failed"),
        ("wraps-foreign-connection", ProbeConnectionError, "listing failed"),
        ("wraps-foreign-value-implicitly", ValueError, "bad url"),
        ("wraps-foreign-odd-ctor", ProbeConnectionError, "fetch said"),
        ("recorded-wraps-foreign", ProbeReadFailed, "no things"),
    ],
)
def test_foreign_text_wrapped_in_an_authored_message_is_withheld(
    run: RunFn, mode: str, expected: type, kept: str
) -> None:
    # Framework and provider types are trusted wherever raised, so
    # f"...: {exc}" around a foreign exception would otherwise carry its text
    # out. The type (and so the exit code) stays; the foreign text becomes
    # its class name.
    with pytest.raises(expected) as info:
        run(mode)
    assert SENTINEL not in str(info.value)
    assert kept in str(info.value)
    assert "Error)" in str(info.value)


def test_an_authored_defect_closing_the_source_is_internal(run: RunFn) -> None:
    # As on the call path: a TypeError the provider raised itself is a defect
    # (exit 1), not the CLI's "your input was wrong" family.
    with pytest.raises(ProbeInternalError) as info:
        run("exit-authored-defect")
    assert "close bookkeeping broke" in str(info.value)


def test_a_foreign_error_that_cannot_render_is_skipped(run: RunFn) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("wraps-foreign-unprintable")
    assert str(info.value) == "listing failed"
