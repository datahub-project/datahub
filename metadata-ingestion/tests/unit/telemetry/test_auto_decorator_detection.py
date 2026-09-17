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

    @root.command(name="explicit-boom")
    @telemetry.with_telemetry()
    def explicit_boom() -> None:
        raise RuntimeError("boom")

    return root


def _of_status(
    pings: List[Tuple[str, Dict[str, Any]]], status: str
) -> List[Dict[str, Any]]:
    return [p for e, p in pings if e == "function-call" and p.get("status") == status]


def _starts(pings: List[Tuple[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return _of_status(pings, "start")


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


def test_a_failing_command_fires_one_error_event_not_two(pings):
    """The other half of the double-wrapping symptom, and the untested one.

    This module's docstring records what the real CLI emitted before the
    fix -- two 'start' pings AND two 'error' pings for one invocation -- but
    every test here counted only the starts. The error path runs through a
    different arm of with_telemetry (the except branch, status="error"), so
    a regression that duplicated it while leaving starts alone would have
    gone unnoticed.
    """
    root = _group()
    enable_auto_decorators(root)

    res = CliRunner().invoke(root, ["explicit-boom"])
    assert res.exit_code != 0

    assert len(_of_status(pings, "error")) == 1, _of_status(pings, "error")
    # And the start it pairs with is still single, so the two counts agree.
    assert len(_starts(pings)) == 1


def test_is_telemetry_wrapped_sees_through_wraps():
    """The property the fix rests on. @wraps makes the wrapper advertise the
    wrapped function's __module__ and __name__, which is why the old check
    could not work."""

    def target() -> None:
        pass

    # Pinned rather than inherited. This function's __module__ is
    # "test_auto_decorator_detection" only because tests/unit/telemetry has no
    # __init__.py; add one, or move the file, and it becomes
    # "tests.unit.telemetry.test_auto_decorator_detection" -- which contains
    # "telemetry" and breaks the assertion below for a reason that has nothing
    # to do with the code under test. The assertion is about what @wraps
    # copies, so the module it copies should be one this test chose.
    target.__module__ = "datahub.ingestion.source.example"

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


def test_a_deep_decorator_chain_does_not_hide_the_telemetry_wrapper():
    """A depth cap that gives up says "not wrapped", which is the answer that
    causes the bug.

    is_telemetry_wrapped stopped after 20 links. A command wrapped more
    deeply than that reported False, and enable_auto_decorators reads False
    as "needs one" -- so it would add a second wrapper and every
    function-call event for that command would fire twice again, which is
    the whole defect this detection exists to prevent.
    """

    def target() -> None:
        pass

    # Deliberately NOT @wraps: it copies __dict__, so the marker propagates
    # to every wrapper above and is found at depth 0 whatever the length --
    # a @wraps chain passes this test without ever reaching the cap, which
    # is how the first draft of it passed against the unfixed code.
    fn = telemetry.with_telemetry()(target)
    for _ in range(40):

        def outer(f=fn):
            def inner(*a, **k):
                return f(*a, **k)

            # setattr: __wrapped__ is a dynamic attribute, and a plain
            # function has no such declared member.
            # Through an Any-typed name: __wrapped__ is a dynamic
            # attribute, which a plain function does not declare.
            link: Any = inner
            link.__wrapped__ = f  # the link, without the dict copy
            return inner

        fn = outer()

    assert not getattr(fn, telemetry.TELEMETRY_WRAPPED_ATTR, False), (
        "the marker propagated, so this chain does not exercise the walk"
    )
    assert telemetry.is_telemetry_wrapped(fn)


def test_a_self_referential_wrapper_chain_terminates():
    """Why the cap was there, and what has to survive removing it.

    wraps(f)(f) sets f.__wrapped__ = f -- a cycle this branch has already met
    once, where it made inspect.unwrap raise "wrapper loop" on exactly the
    commands that instrument themselves. Identity, not depth, is what ends
    the walk.
    """
    from functools import wraps

    def lonely() -> None:
        pass

    wraps(lonely)(lonely)  # lonely.__wrapped__ is lonely
    # Same Any-typed read as above: the cycle is real, the attribute is
    # dynamic, and a plain function does not declare it.
    cyclic: Any = lonely
    assert cyclic.__wrapped__ is lonely

    assert telemetry.is_telemetry_wrapped(lonely) is False
