import os
import time

import pytest

from datahub.testing.pytest_hooks import pytest_configure

pytest_plugins = ["pytester"]


def test_pin_timezone_sets_utc(pytestconfig: pytest.Config) -> None:
    # POSIX TZ form: a fixed +5:30 offset, so the test does not depend on host
    # tzdata. A zone name silently resolves to UTC where tzdata is absent, which
    # would make this precondition fail and the assertions below vacuous.
    os.environ["TZ"] = "XXX-5:30"
    time.tzset()
    assert time.tzname[0] != "UTC"

    pytest_configure(pytestconfig)

    assert os.environ["TZ"] == "UTC"
    assert time.tzname == ("UTC", "UTC")


def test_pin_timezone_is_inert_without_tzset(
    monkeypatch: pytest.MonkeyPatch, pytestconfig: pytest.Config
) -> None:
    """Platforms without tzset must be left alone entirely.

    Setting TZ there would not affect this process -- nothing reads it -- but it
    is still inherited by every subprocess the suite spawns, and by libraries
    that consult the variable directly. A half-applied pin is worse than none,
    so the assignment is guarded rather than unconditional.
    """
    monkeypatch.delattr(time, "tzset", raising=False)
    os.environ.pop("TZ", None)

    pytest_configure(pytestconfig)

    assert "TZ" not in os.environ


@pytest.mark.timezone("XXX-5:30")
def test_timezone_marker_applies_the_requested_zone() -> None:
    assert time.tzname[0] == "XXX"
    assert -time.timezone / 3600 == 5.5


def test_pin_holds_for_unmarked_tests() -> None:
    """An unmarked test runs under the session pin, not whatever ran before it.

    tests/unit runs with --random-order, so this cannot be relied on to land after
    the marked test above; when it does, it also catches a failure to restore.
    """
    assert time.tzname == ("UTC", "UTC")
    assert os.environ["TZ"] == "UTC"


# Each inner run is a full pytest session and re-emits the outer session's
# plugin warnings. asyncio is unused here.
INLINE_ARGS = ("-p", "no:asyncio")

CONFTEST = (
    "from datahub.testing.pytest_hooks import (  # noqa: F401\n"
    "    local_timezone,\n"
    "    pytest_configure,\n"
    ")\n"
)


@pytest.mark.parametrize(
    "marker",
    [
        "@pytest.mark.timezone",
        '@pytest.mark.timezone("")',
        "@pytest.mark.timezone(5)",
        '@pytest.mark.timezone(zone="XXX-5:30")',
    ],
    ids=["missing", "empty", "non-string", "keyword-form"],
)
def test_marker_rejects_a_zone_it_cannot_apply(
    pytester: pytest.Pytester, marker: str
) -> None:
    """A marker that cannot be applied must fail loudly, not run on the pin.

    libc reads TZ="" as UTC, so an empty zone would otherwise run on the session
    pin while the test asserts as though it were somewhere else. The keyword
    form is a usage error rather than a wrong answer, and takes the same message.
    """
    pytester.makeconftest(CONFTEST)
    pytester.makepyfile(f"import pytest\n\n{marker}\ndef test_marked(): pass\n")

    # Inline rather than subprocess so coverage sees the guard execute.
    result = pytester.runpytest(*INLINE_ARGS)

    # Anchored to the raised-error line: pytest echoes the fixture source into
    # every traceback through it, so an unanchored match also hits the
    # `pytest.fail(...)` call and passes even when the guard never fires.
    result.stdout.fnmatch_lines(
        ["E*Failed: @pytest.mark.timezone requires a non-empty zone string*"]
    )
    result.assert_outcomes(errors=1)


def test_marker_skips_where_tzset_is_absent(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without tzset the zone cannot be applied, so the test must not run at all.

    Running it would assert against the local zone while claiming to be in
    another one. The inline run shares this process, so tzset is removed with
    monkeypatch to keep the removal from outliving the test.
    """
    monkeypatch.delattr(time, "tzset", raising=False)
    pytester.makeconftest(CONFTEST)
    pytester.makepyfile(
        'import pytest\n\n@pytest.mark.timezone("XXX-5:30")\ndef test_marked(): pass\n'
    )

    pytester.runpytest(*INLINE_ARGS).assert_outcomes(skipped=1)


def test_unmarked_test_leaves_an_unset_tz_unset(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Windows path: no tzset, so no pin and TZ stays unset end to end.

    The fixture's restore branch must drop TZ when it was never set, not write
    a value back -- otherwise the first marked test on such a host would leak
    its zone into every later test.
    """
    monkeypatch.delattr(time, "tzset", raising=False)
    monkeypatch.delenv("TZ", raising=False)
    pytester.makeconftest(CONFTEST)
    pytester.makepyfile(
        'import os\n\ndef test_unmarked():\n    assert "TZ" not in os.environ\n'
    )

    pytester.runpytest(*INLINE_ARGS).assert_outcomes(passed=1)

    # Out here rather than in the inner test: the restore runs at teardown,
    # after the inner body, and the inline run shares this process.
    assert "TZ" not in os.environ
