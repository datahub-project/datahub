import pathlib
from typing import Callable, List

import pytest
from click.testing import CliRunner

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
from tests.unit.agent import _foreign_errors
from tests.unit.agent._foreign_errors import SENTINEL


def test_probe_argument_error_is_a_value_error_but_not_a_soft_error() -> None:
    err = ProbeArgumentError("no project titled 'x'")
    assert isinstance(err, ValueError)
    assert not isinstance(err, ProbeSoftError)


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
        return cls(mode)

    def __enter__(self) -> "_Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method(name="things")
    def things(self, name: str = "") -> List[str]:
        """List things."""
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
    assert not is_authored(RuntimeError("never raised"), __file__)


def test_a_raise_in_the_provider_file_is_authored() -> None:
    try:
        raise RuntimeError("here")
    except RuntimeError as exc:
        assert is_authored(exc, __file__)


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
