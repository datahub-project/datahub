import pathlib
from typing import Callable, List

import pytest
from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
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

    @classmethod
    def for_config(cls, config: object) -> "_Provider":
        mode = getattr(config, "mode", "")
        if mode == "open-foreign":
            _foreign_errors.connect()
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
    with pytest.raises(ProbeInternalError) as info:
        run("foreign-value")
    assert SENTINEL not in str(info.value)
    assert "ValueError" in str(info.value)


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


def test_an_authored_argument_error_while_opening_exits_2(run: RunFn) -> None:
    with pytest.raises(ProbeArgumentError):
        run("open-authored")


@pytest.mark.parametrize(
    "mode, exit_code",
    [("foreign-value", 1), ("foreign-transport", 3), ("open-foreign", 3)],
)
def test_the_cli_never_prints_foreign_text_from_the_exception_chain(
    run: RunFn,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mode: str,
    exit_code: int,
) -> None:
    # The CLI renders str(exc) only; this pins that neither __cause__ nor
    # __context__ (still set under `from None`) reaches the output either.
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
