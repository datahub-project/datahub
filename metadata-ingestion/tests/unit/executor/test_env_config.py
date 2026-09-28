"""Parsing of the executor's venv-cache knobs.

These are read from inside the exclusive-lock region of _acquire_cache_entry,
which has no handler above it before setup_venv's own try. Anything they raise
escapes with the entry still locked, so that cache key could never be built,
hit or evicted again for the life of the process -- and every ingestion on the
node would fail at venv setup. None of them may raise, for any string an
operator might plausibly write.
"""

from typing import Callable

import pytest

from datahub.executor.common.env_config import (
    DEFAULT_VENV_CACHE_LATEST_TTL_HOURS,
    DEFAULT_VENV_CACHE_MAX_AGE_HOURS,
    DEFAULT_VENV_CACHE_MAX_ENTRIES,
    get_venv_cache_latest_ttl_sec,
    get_venv_cache_max_age_sec,
    get_venv_cache_max_entries,
)

# (env var, reader, value when unset)
KNOBS = [
    pytest.param(
        "DATAHUB_VENV_CACHE_MAX_ENTRIES",
        get_venv_cache_max_entries,
        DEFAULT_VENV_CACHE_MAX_ENTRIES,
        id="max-entries",
    ),
    pytest.param(
        "DATAHUB_VENV_CACHE_MAX_AGE_HOURS",
        get_venv_cache_max_age_sec,
        DEFAULT_VENV_CACHE_MAX_AGE_HOURS * 3600,
        id="max-age",
    ),
    pytest.param(
        "DATAHUB_VENV_CACHE_LATEST_TTL_HOURS",
        get_venv_cache_latest_ttl_sec,
        DEFAULT_VENV_CACHE_LATEST_TTL_HOURS * 3600,
        id="latest-ttl",
    ),
]

UNUSABLE = [
    # Every plausible spelling of "no limit". float() accepts all of these and
    # int() then raises OverflowError -- an ArithmeticError, not the ValueError
    # a narrower guard catches.
    "inf",
    "Infinity",
    "-inf",
    "1e400",
    "nan",
    # Meaningless. A negative bound is the dangerous one: it parses cleanly and
    # makes every entry look over the limit, so eviction would delete the whole
    # cache on every single build.
    "-1",
    "0",
    "",
    "ten",
    "10 entries",
    # Absurd but FINITE. Rejecting only inf/nan lets these through and
    # silently removes the bound the knob exists to impose: int(1e300) makes
    # `remaining <= max_entries` always true, and 1e308 hours overflows to
    # inf when multiplied into seconds -- without raising, and with no log
    # line, against docs promising a bound an operator can size a volume by.
    "1e300",
    "1e308",
]


@pytest.mark.parametrize(("var", "read", "default"), KNOBS)
@pytest.mark.parametrize("raw", UNUSABLE)
def test_an_unusable_value_falls_back_to_the_default(
    var: str,
    read: Callable[[], float],
    default: float,
    raw: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(var, raw)

    assert read() == default


@pytest.mark.parametrize(("var", "read", "default"), KNOBS)
def test_an_unset_value_is_the_default(
    var: str,
    read: Callable[[], float],
    default: float,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(var, raising=False)

    assert read() == default


def test_max_entries_is_a_whole_number(monkeypatch: pytest.MonkeyPatch) -> None:
    """A count has to be usable as one -- `len(entries) > 2.5` is not a bound
    an operator can reason about, and slicing with it raises."""
    monkeypatch.setenv("DATAHUB_VENV_CACHE_MAX_ENTRIES", "4")
    assert get_venv_cache_max_entries() == 4

    monkeypatch.setenv("DATAHUB_VENV_CACHE_MAX_ENTRIES", "2.5")
    value = get_venv_cache_max_entries()
    assert isinstance(value, int)
    assert value == 2, "a fractional count must round DOWN, never up past the bound"


@pytest.mark.parametrize(
    ("var", "read", "hours", "expected_sec"),
    [
        ("DATAHUB_VENV_CACHE_MAX_AGE_HOURS", get_venv_cache_max_age_sec, "48", 172800),
        (
            "DATAHUB_VENV_CACHE_LATEST_TTL_HOURS",
            get_venv_cache_latest_ttl_sec,
            "6",
            21600,
        ),
        # A sub-hour TTL is the reasonable way to ask for "re-resolve often"
        # during an incident, so fractions must survive.
        (
            "DATAHUB_VENV_CACHE_LATEST_TTL_HOURS",
            get_venv_cache_latest_ttl_sec,
            "0.5",
            1800,
        ),
        # Whitespace is what a Helm values file produces often enough to matter.
        (
            "DATAHUB_VENV_CACHE_MAX_AGE_HOURS",
            get_venv_cache_max_age_sec,
            "  12  ",
            43200,
        ),
    ],
)
def test_hour_knobs_are_converted_to_seconds(
    var: str,
    read: Callable[[], float],
    hours: str,
    expected_sec: float,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(var, hours)

    assert read() == expected_sec
