# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import logging
import math
import os
import pathlib

logger = logging.getLogger(__name__)

# ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH keeps its original name on purpose:
# renaming it would silently change behaviour for deployments that already set it.
DEFAULT_GMS_PAYLOAD_MAX_LENGTH = 15368520


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


def get_payload_max_length() -> int:
    val = os.environ.get("ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH")
    return int(val) if val and val.isdigit() else DEFAULT_GMS_PAYLOAD_MAX_LENGTH


def get_bundled_venv_path() -> str:
    return os.environ.get("DATAHUB_BUNDLED_VENV_PATH", "/opt/datahub/venvs")


def get_dependency_resolution_enabled() -> bool:
    return string_to_bool(
        os.environ.get("INGESTION_DEPENDENCY_RESOLUTION_ENABLED", "true")
    )


def get_print_subprocess_logs() -> bool:
    return string_to_bool(
        os.environ.get("DATAHUB_EXECUTOR_PRINT_SUBPROCESS_LOGS", "true")
    )


def get_venv_cache_enabled() -> bool:
    """Whether named venvs are reused from a node-local cache.

    Defaults on: the speedup is the point, and a cache that ships off gets no
    soak. The switch exists because this changes behaviour for ingestion in
    customer deployments, and an operator who suspects the cache must be able
    to take it out of the picture without shipping new code.
    """
    return string_to_bool(os.environ.get("DATAHUB_VENV_CACHE_ENABLED", "true"))


def get_venv_cache_path(exec_out_dir: str) -> str:
    """Root for reusable venvs: a SIBLING of the per-execution directories.

    Deliberately not inside `exec_out_dir`. That is the directory
    finalize_task_output removes when a task ends, which is the very reason
    venvs are rebuilt every run. Both callers construct it as
    `{config.tmp_dir}/{exec_id}`, so its parent is the executor's configured
    tmp_dir -- the same volume, which is what keeps uv's hardlinking working.
    """
    override = os.environ.get("DATAHUB_VENV_CACHE_PATH")
    if override:
        return override
    return str(pathlib.Path(exec_out_dir).parent / "_venv_cache")


# How many entries the cache keeps. Chosen over a byte budget deliberately:
# sizing the cache in bytes means measuring it, and measuring a venv means
# walking tens of thousands of files per entry on every build -- for a number
# that does not correspond to disk anyway, because DataHub defaults uv to
# UV_LINK_MODE=hardlink and most of a venv is links into uv's package cache.
# A count is one stat per entry and an operator can see it with `ls`.
DEFAULT_VENV_CACHE_MAX_ENTRIES = 10

# Drop an entry nothing has used in this long, even when the cache is under
# its entry count. Bounds the cache on a pod that runs one recipe for weeks.
# Equal to DEFAULT_VENV_CACHE_LATEST_TTL_HOURS on purpose: most entries are
# built from `latest`, and one idle for longer than the TTL can never be
# served again as-is -- its next use discards and rebuilds it -- so keeping it
# past that point only holds disk. The cache pays off in the short bursts of a
# test connection or probe, well inside a day.
DEFAULT_VENV_CACHE_MAX_AGE_HOURS = 24

# How long a venv built from a MOVING version -- `latest`, or a dev-build
# branch alias -- may be served before it is rebuilt. Unlike the two above
# this is not about disk: it is what stops a long-lived pod serving the build
# it resolved on the day it started, forever.
DEFAULT_VENV_CACHE_LATEST_TTL_HOURS = 24

# Ceilings, so an absurd value is rejected loudly rather than silently
# removing the bound. 10k entries is far past any real node; 10 years of
# hours is past any pod lifetime and stays well clear of the float overflow
# that turns hours-to-seconds into inf.
MAX_VENV_CACHE_ENTRIES = 10_000
MAX_VENV_CACHE_HOURS = 24 * 365 * 10


@functools.lru_cache(maxsize=None)
def _warn_unusable_value_once(var: str, raw: str, fallback: float) -> None:
    """Once per distinct value -- these are read on every build path."""
    logger.warning(
        "%s=%r is not usable; falling back to %s. Set a positive, finite number.",
        var,
        raw,
        fallback,
    )


def _positive_number(var: str, default: float, *, maximum: float) -> float:
    """Read a positive, finite number from the environment, or the default.

    Never raises, and that is a requirement rather than politeness: these are
    read from inside the exclusive-lock region of _acquire_cache_entry, which
    has no handler above it before setup_venv's own try. An exception would
    escape with the entry still locked, so that cache key could never be
    built, hit or evicted again for the life of the process, and every
    ingestion and test-connection on the node would fail at venv setup.

    `float(raw)` alone is not enough. It accepts "inf", "Infinity", "-inf"
    and "1e400" -- exactly how an operator writes "no limit" -- and int() on
    those raises OverflowError, an ArithmeticError rather than the ValueError
    a narrower guard catches.

    Non-positive values are rejected rather than honoured. A negative bound
    parses cleanly and makes every entry look over the limit, so eviction
    would delete the entire cache on every single build; zero says the same
    thing, and DATAHUB_VENV_CACHE_ENABLED already exists for operators who
    want no cache at all.
    """
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        _warn_unusable_value_once(var, raw, default)
        return default
    if not math.isfinite(value) or value <= 0 or value > maximum:
        # An upper bound as well as a lower one. Rejecting only inf/nan lets
        # an absurd-but-finite value through and silently removes the bound
        # the knob exists to impose: MAX_ENTRIES=1e300 makes
        # `remaining <= max_entries` always true, and MAX_AGE_HOURS=1e308
        # overflows to inf when multiplied into seconds, without raising.
        # Both give an unbounded cache with no log line, against docs that
        # promise a bound an operator can size a volume against.
        _warn_unusable_value_once(var, raw, default)
        return default
    return value


def get_venv_cache_max_entries() -> int:
    """Most entries the cache keeps, oldest-used evicted first.

    Rounded DOWN, so a fractional value can never widen the bound past what
    the operator asked for -- and a value that floors to zero falls back to
    the default rather than emptying the cache on every build.
    """
    entries = int(
        _positive_number(
            "DATAHUB_VENV_CACHE_MAX_ENTRIES",
            DEFAULT_VENV_CACHE_MAX_ENTRIES,
            maximum=MAX_VENV_CACHE_ENTRIES,
        )
    )
    return entries if entries >= 1 else DEFAULT_VENV_CACHE_MAX_ENTRIES


def get_venv_cache_max_age_sec() -> float:
    """Evict an entry nothing has used in this long, regardless of count."""
    return (
        _positive_number(
            "DATAHUB_VENV_CACHE_MAX_AGE_HOURS",
            DEFAULT_VENV_CACHE_MAX_AGE_HOURS,
            maximum=MAX_VENV_CACHE_HOURS,
        )
        * 3600
    )


def get_venv_cache_latest_ttl_sec() -> float:
    """How long a venv built from a moving version may be reused before rebuild."""
    return (
        _positive_number(
            "DATAHUB_VENV_CACHE_LATEST_TTL_HOURS",
            DEFAULT_VENV_CACHE_LATEST_TTL_HOURS,
            maximum=MAX_VENV_CACHE_HOURS,
        )
        * 3600
    )
