"""A source that cannot be opened is named by the stdlib network exception its
driver keeps in the chain: refused, unresolved, timed out, unreachable or an
untrusted certificate. Types and errno only, never text.

The chains are built the way each driver raises them, as recorded against
real servers and closed ports.
"""

import errno
import pathlib
import socket
import ssl
from typing import Callable, Dict, Optional, Type

import pymysql.err
import pytest
from click.testing import CliRunner

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe
from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.error_policy import NETWORK_REASON_HINTS, network_reason
from datahub.ingestion.agent.probe_methods import probe_method, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe

SECRET = "PLANTED-network-secret"


def _pymysql(inner: OSError) -> BaseException:
    """PyMySQL raises its OperationalError while handling the socket's error,
    without `from`, so the socket error is on __context__ only."""
    try:
        try:
            raise inner
        except OSError:
            raise pymysql.err.OperationalError(  # noqa: B904
                2003, f"Can't connect to MySQL server on 'h' ({SECRET})"
            )
    except BaseException as exc:
        return exc
    raise AssertionError("unreachable")


def _pytds_refused() -> BaseException:
    """pytds raises a TimeoutError from the ConnectionRefusedError: the
    outermost type is the wrong one."""
    try:
        try:
            raise ConnectionRefusedError(errno.ECONNREFUSED, f"refused {SECRET}")
        except ConnectionRefusedError as refused:
            raise TimeoutError(f"login to {SECRET} failed") from refused
    except BaseException as exc:
        return exc
    raise AssertionError("unreachable")


_CHAINS: Dict[str, Callable[[], BaseException]] = {
    "pymysql-refused": lambda: _pymysql(
        ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
    ),
    "pymysql-dns": lambda: _pymysql(
        socket.gaierror(socket.EAI_NONAME, f"cannot resolve {SECRET}")
    ),
    "pymysql-timeout": lambda: _pymysql(TimeoutError("timed out")),
    "pymysql-tls": lambda: _pymysql(
        ssl.SSLCertVerificationError(1, f"[SSL: CERTIFICATE_VERIFY_FAILED] {SECRET}")
    ),
    "pymysql-unreachable": lambda: _pymysql(
        OSError(errno.EHOSTUNREACH, f"No route to host {SECRET}")
    ),
    "pytds-refused": _pytds_refused,
    "bare-gaierror": lambda: socket.gaierror(
        socket.EAI_NONAME, f"nodename {SECRET} not known"
    ),
    "pymysql-auth": lambda: pymysql.err.OperationalError(
        1045, f"Access denied for user 'u' (using password: {SECRET})"
    ),
}

_REASONS = {
    "pymysql-refused": "ConnectionRefused",
    "pymysql-dns": "HostNotResolved",
    "pymysql-timeout": "Timeout",
    "pymysql-tls": "TlsVerifyFailed",
    "pymysql-unreachable": "HostUnreachable",
    "pytds-refused": "ConnectionRefused",
    "bare-gaierror": "HostNotResolved",
    "pymysql-auth": None,
}


@pytest.mark.parametrize("chain", sorted(_CHAINS))
def test_the_innermost_network_exception_names_the_reason(chain: str) -> None:
    assert network_reason(_CHAINS[chain]()) == _REASONS[chain]


def test_a_network_error_handled_long_before_is_out_of_reach() -> None:
    exc: BaseException = ConnectionRefusedError(errno.ECONNREFUSED, "refused")
    for _ in range(40):
        outer = RuntimeError("wrapper")
        outer.__context__ = exc
        exc = outer
    assert network_reason(exc) is None


def test_a_chain_cycle_ends() -> None:
    first, second = RuntimeError("a"), RuntimeError("b")
    first.__context__ = second
    second.__cause__ = first
    assert network_reason(first) is None


class _Provider:
    probe_error_code = staticmethod(SqlAlchemyMetadataProbe.probe_error_code)
    raises_on_open: Optional[BaseException] = None
    raises_on_close: Optional[BaseException] = None

    @classmethod
    def for_config(cls, config: object) -> "_Provider":
        if cls.raises_on_open is not None:
            raise cls.raises_on_open
        return cls()

    def __enter__(self) -> "_Provider":
        return self

    def __exit__(self, *exc: object) -> None:
        if self.raises_on_close is not None:
            raise self.raises_on_close

    @probe_method(name="things")
    def things(self) -> list:
        """List things."""
        return []


class _Config(ConfigModel):
    @classmethod
    def probe_provider_class(cls) -> type:
        return _Provider


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> Type[_Provider]:
    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)
    monkeypatch.setattr(_Provider, "raises_on_open", None)
    monkeypatch.setattr(_Provider, "raises_on_close", None)
    return _Provider


def _open_failure(provider: Type[_Provider], chain: str) -> str:
    provider.raises_on_open = _CHAINS[chain]()
    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method("fake", {}, "things", {})
    return str(info.value)


@pytest.mark.parametrize("chain", sorted(c for c in _CHAINS if _REASONS[c]))
def test_opening_names_the_reason_after_the_label(
    provider: Type[_Provider], chain: str
) -> None:
    message = _open_failure(provider, chain)
    reason = _REASONS[chain]
    assert reason is not None
    assert message.startswith("opening source 'fake' failed (")
    assert f"): {reason} - {NETWORK_REASON_HINTS[reason]}" in message
    assert SECRET not in message


def test_the_label_keeps_the_drivers_own_code(provider: Type[_Provider]) -> None:
    message = _open_failure(provider, "pymysql-refused")
    assert "(OperationalError; errno 2003): ConnectionRefused - " in message


def test_a_failure_with_no_network_cause_is_reported_as_before(
    provider: Type[_Provider],
) -> None:
    message = _open_failure(provider, "pymysql-auth")
    assert message.endswith("failed (OperationalError; errno 1045)")
    assert not any(reason in message for reason in NETWORK_REASON_HINTS)
    assert SECRET not in message


def test_closing_names_no_network_reason(provider: Type[_Provider]) -> None:
    """A context outside the open path can be an unrelated earlier retry."""
    provider.raises_on_close = _CHAINS["pymysql-refused"]()
    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method("fake", {}, "things", {})
    assert info.value.args[0].startswith("closing source 'fake' failed (")
    assert "ConnectionRefused" not in str(info.value)


def test_the_verbose_text_follows_the_hint_scrubbed(
    provider: Type[_Provider], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAHUB_PROBE_VERBOSE_LOGS", "1")
    message = _open_failure(provider, "pymysql-auth")
    assert "Access denied" in message
    message = _open_failure(provider, "pymysql-refused")
    assert f"ConnectionRefused - {NETWORK_REASON_HINTS['ConnectionRefused']}: " in (
        message
    )


def test_the_cli_shows_the_reason_and_keeps_exit_3(
    provider: Type[_Provider], monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    provider.raises_on_open = _CHAINS["pytds-refused"]()
    monkeypatch.setattr(rc, "_resolve_for_probe", lambda _r: ("fake", {}, set()))
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    result = CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )
    assert result.exit_code == 3, result.output
    assert "): ConnectionRefused - " in result.stderr
    assert SECRET not in result.output
