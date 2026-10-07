"""Check every API path the connector uses against the captured Qualytics spec.

Qualytics is single-tenant and each deployment serves its own OpenAPI spec, so the
committed fixture is the contract we develop against. This test is the tripwire for
API surface churn: refresh the fixture from a newer deployment with
`tests/unit/qualytics/capture_openapi.py` and anything the connector reads that has moved or been
removed fails here, rather than at 3am against a customer's deployment.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from datahub.ingestion.source.qualytics.constants import (
    CONSUMED_PATHS,
    DEFAULT_API_ROOT_PATH,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "openapi.json"


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    # Fail, never skip: a skipped contract test reports green, and a fixture lost in a
    # move would quietly switch off the API-churn tripwire.
    assert FIXTURE.exists(), (
        f"{FIXTURE} is missing; run tests/unit/qualytics/capture_openapi.py"
    )
    loaded: dict[str, Any] = json.loads(FIXTURE.read_text())
    return loaded


def test_spec_paths_all_carry_the_api_root_path(spec: dict[str, Any]) -> None:
    # The spec's keys include the deployment's API_ROOT_PATH, while our constants are
    # relative to `base_url` (which already ends in it). If this ever stops holding,
    # every path constant needs revisiting -- so assert the assumption rather than
    # letting it rot silently.
    offenders = [
        p for p in spec["paths"] if not p.startswith(f"{DEFAULT_API_ROOT_PATH}/")
    ]
    assert offenders == [], offenders


@pytest.mark.parametrize("path", CONSUMED_PATHS)
def test_consumed_path_exists_in_spec(spec: dict[str, Any], path: str) -> None:
    full = f"{DEFAULT_API_ROOT_PATH}{path}"
    assert full in spec["paths"], (
        f"{full} is not in the captured Qualytics spec "
        f"({spec['info']['version']}). Either the endpoint moved, or the constant is "
        f"wrong -- read the spec, do not guess."
    )


@pytest.mark.parametrize("path", CONSUMED_PATHS)
def test_consumed_path_supports_get(spec: dict[str, Any], path: str) -> None:
    # The connector is read-only. A path that exists but has no GET is not usable by us.
    full = f"{DEFAULT_API_ROOT_PATH}{path}"
    assert "get" in spec["paths"][full], f"{full} has no GET operation"
