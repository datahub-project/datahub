"""enable_auto_decorators must not wrap a command that is already wrapped.

It could not tell: has_decorator identified a decorator by looking for
"telemetry" in __module__ and "with_telemetry" in __name__, but with_telemetry's
wrapper carries @wraps(func) and so reports the *wrapped* function's identity.
The check never matched, so every explicitly decorated command got a second,
auto-applied wrapper and fired every function-call event twice -- once with the
arg_* dimensions the explicit decorator captured, once with them as None.

Measured on the real CLI before the fix:

    $ datahub ingest run -c /nonexistent.yml
       ('function-call', 'start', 'datahub.cli.ingest_cli.run')
       ('function-call', 'start', 'datahub.cli.ingest_cli.run')
       ('function-call', 'error', 'datahub.cli.ingest_cli.run')
       ('function-call', 'error', 'datahub.cli.ingest_cli.run')

Both pings carry an identical `function`, so they cannot be told apart after the
fact: any invocation count from that event was 2x and any breakdown by arg_* was
half nulls.
"""

from typing import Any, Dict, List, Tuple

import click
import pytest
from click.testing import CliRunner

from datahub.cli.cli_utils import enable_auto_decorators
from datahub.telemetry import telemetry


@pytest.fixture
def pings(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Dict[str, Any]]]:
    captured: List[Tuple[str, Dict[str, Any]]] = []
    monkeypatch.setattr(
        telemetry.telemetry_instance,
        "ping",
        lambda event, props=None, *a, **k: captured.append((event, props or {})),
    )
    monkeypatch.setattr(telemetry.telemetry_instance, "init_tracking", lambda: None)
    monkeypatch.setattr(
        telemetry.telemetry_instance, "init_capture_exception", lambda: None
    )
    monkeypatch.setattr(
        telemetry.telemetry_instance, "capture_exception", lambda e: None
    )
    return captured


def _group() -> click.Group:
    @click.group()
    def root() -> None:
        pass

    @root.command(name="explicit")
    @click.option("--flag", is_flag=True)
    @telemetry.with_telemetry(capture_kwargs=["flag"])
    def explicit(flag: bool) -> None:
        click.echo("ok")

    @root.command(name="plain")
    @click.option("--flag", is_flag=True)
    def plain(flag: bool) -> None:
        click.echo("ok")

    return root


def _starts(pings: List[Tuple[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return [p for e, p in pings if e == "function-call" and p.get("status") == "start"]


def test_an_explicitly_decorated_command_is_not_wrapped_twice(pings):
    root = _group()
    enable_auto_decorators(root)
    res = CliRunner().invoke(root, ["explicit", "--flag"])
    assert res.exit_code == 0, res.output
    assert len(_starts(pings)) == 1


def test_suppressing_the_auto_wrapper_keeps_the_explicit_one_and_its_dimensions(pings):
    """The failure mode to avoid is suppressing the wrong wrapper: that would
    keep one ping and silently drop the arg_* data, which is worse than the
    double-count it replaced."""
    root = _group()
    enable_auto_decorators(root)
    res = CliRunner().invoke(root, ["explicit", "--flag"])
    assert res.exit_code == 0, res.output
    (start,) = _starts(pings)
    assert start["arg_flag"] is True


def test_an_undecorated_command_still_gets_telemetry(pings):
    """The auto-wrapper is why recipe_cli needs no decorator of its own."""
    root = _group()
    enable_auto_decorators(root)
    res = CliRunner().invoke(root, ["plain"])
    assert res.exit_code == 0, res.output
    assert len(_starts(pings)) == 1


def test_running_the_auto_wrapper_twice_does_not_stack(pings):
    """Idempotence falls out of marking the wrapper, and is worth pinning: the
    old name-based check made a second pass wrap everything again."""
    root = _group()
    enable_auto_decorators(root)
    enable_auto_decorators(root)
    res = CliRunner().invoke(root, ["plain"])
    assert res.exit_code == 0, res.output
    assert len(_starts(pings)) == 1


def test_is_telemetry_wrapped_sees_through_wraps():
    """The property the fix rests on. @wraps makes the wrapper advertise the
    wrapped function's __module__ and __name__, which is why the old check
    could not work."""

    def target() -> None:
        pass

    wrapped = telemetry.with_telemetry()(target)

    assert wrapped.__name__ == target.__name__
    assert wrapped.__module__ == target.__module__
    assert "telemetry" not in wrapped.__module__
    assert telemetry.is_telemetry_wrapped(wrapped)
    assert not telemetry.is_telemetry_wrapped(target)


def test_an_already_wrapped_callback_is_left_entirely_alone(pings):
    """The metadata wrapper used to run unconditionally, so on a callback that
    needed no telemetry wrapper it became wraps(f)(f) -- setting f.__wrapped__
    to f. inspect.unwrap and inspect.signature then raise "wrapper loop" on
    exactly the commands that instrument themselves.

    Unreachable while the detection never fired; making the detection work is
    what made it live.
    """
    import inspect

    root = _group()
    before = root.commands["explicit"].callback
    enable_auto_decorators(root)
    after = root.commands["explicit"].callback

    assert after is before, "an already-wrapped callback should be untouched"
    assert after is not None
    assert getattr(after, "__wrapped__", None) is not after
    inspect.unwrap(after)
    inspect.signature(after)
