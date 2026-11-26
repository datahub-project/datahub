"""CPU-only tests for the Docker vs Docker-free env drift check (check_env_parity.py)."""

from __future__ import annotations

from pathlib import Path

import pytest

from check_env_parity import AllowEntry, compare, job_sets_pinned, read_allowlist
from utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.PLATFORM_INTERNAL)


def _envs(**gms: str) -> dict[str, dict[str, str]]:
    return {"gms": gms}


def test_differences_must_match_an_allowlist_glob() -> None:
    docker = _envs(KAFKA_BOOTSTRAP_SERVER="broker:29092", FLAG="true", SAME="True")
    docker_free = _envs(FLAG="false", SAME="true")
    allow = [AllowEntry("gms", "KAFKA_*", "no Kafka", 1)]
    unlisted, allowed, stale, pins = compare(docker, docker_free, allow, _envs())
    assert unlisted == ["gms FLAG: docker='true' docker-free='false'"]
    assert allowed == [
        "gms KAFKA_BOOTSTRAP_SERVER: docker='broker:29092' docker-free=None"
    ]
    assert stale == []
    assert pins == []


def test_allowlist_line_matching_nothing_is_stale() -> None:
    entry = AllowEntry("gms", "KAFKA_*", "no Kafka", 3)
    unlisted, _, stale, _ = compare(_envs(), _envs(), [entry], _envs())
    assert unlisted == []
    assert stale == [entry]


def test_pinned_keys_are_reported_but_never_compared() -> None:
    docker = _envs(INDEX_PREFIX="", SERVER_PORT="8080")
    pinned = _envs(INDEX_PREFIX="stack_a", SERVER_PORT="8080")
    # An allowlist line for a pinned key matches nothing, so it is stale.
    entry = AllowEntry("gms", "INDEX_PREFIX", "per stack", 1)
    unlisted, allowed, stale, pins = compare(docker, _envs(), [entry], pinned)
    assert unlisted == [] and allowed == []
    assert stale == [entry]
    assert pins == ["gms INDEX_PREFIX: docker='' docker-free='stack_a'"]


def test_job_layer_must_not_set_a_pinned_key() -> None:
    keys = {"gms": {"SERVER_PORT", "FLAG"}}
    problems = job_sets_pinned(keys, _envs(SERVER_PORT="8080"))
    assert len(problems) == 1 and problems[0].startswith("gms SERVER_PORT:")


def test_allowlist_requires_a_reason(tmp_path: Path) -> None:
    path = tmp_path / "allow.txt"
    path.write_text("gms KAFKA_*  # no Kafka\n")
    assert read_allowlist(path) == [AllowEntry("gms", "KAFKA_*", "no Kafka", 1)]
    path.write_text("gms KAFKA_*\n")
    with pytest.raises(SystemExit):
        read_allowlist(path)
