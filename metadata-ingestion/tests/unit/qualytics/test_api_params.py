"""Validate outgoing query parameters against the captured OpenAPI spec.

This exists because of a bug the rest of the suite could not see. The client sent
`archived=False` on every quality-check and anomaly listing; the spec types that
parameter as `Literal["include", "only"]`, so a real deployment answered 422 and the
connector emitted zero assertions while reporting success. `requests_mock` accepts any
parameter, so every mock-based test passed -- and one of them asserted the wrong value,
locking the bug in.

Mocks verify that we call what we meant to. Only the spec verifies that what we meant
to call is real.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from requests_mock import Mocker

from datahub.ingestion.source.qualytics.client import QualyticsClient
from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig
from datahub.ingestion.source.qualytics.constants import DEFAULT_API_ROOT_PATH

BASE = "https://acme.qualytics.io/api"
SPEC = Path(__file__).resolve().parent / "fixtures" / "openapi.json"


def _spec_params(path: str) -> dict[str, dict[str, Any]]:
    spec = json.loads(SPEC.read_text())
    operation = spec["paths"][f"{DEFAULT_API_ROOT_PATH}{path}"]["get"]
    return {p["name"]: p for p in operation.get("parameters", [])}


def _allowed_values(param: dict[str, Any]) -> set[str] | None:
    """Enum members a parameter accepts, or None if it is not an enum."""
    schema = param.get("schema", {})
    for variant in schema.get("anyOf", [schema]):
        if "enum" in variant:
            return set(variant["enum"])
    return None


def _client() -> QualyticsClient:
    return QualyticsClient(
        QualyticsSourceConfig.model_validate({"base_url": BASE, "token": "t"})
    )


def _empty_page() -> dict[str, Any]:
    return {"items": [], "page": 1, "pages": 1, "size": 100, "total": 0}


@pytest.mark.parametrize(
    ("path", "call", "expected"),
    [
        ("/datastores", lambda c: list(c.list_datastores()), set()),
        (
            "/containers",
            lambda c: list(c.list_containers(datastore_id=1)),
            {"datastore"},
        ),
        (
            "/quality-checks",
            lambda c: list(c.list_quality_checks(container_id=10)),
            {"container"},
        ),
        (
            "/anomalies",
            lambda c: list(
                c.list_anomalies(
                    container_id=10, start_date="2026-08-01", end_date="2026-09-09"
                )
            ),
            {"container", "start_date", "end_date"},
        ),
    ],
)
def test_outgoing_query_params_exist_in_the_spec(
    path: str, call: Any, expected: set[str]
) -> None:
    declared = _spec_params(path)

    with Mocker() as m:
        m.get(f"{BASE}{path}", json=_empty_page())
        call(_client())
        sent = m.last_request.qs

    unknown = set(sent) - {name.lower() for name in declared}
    assert unknown == set(), (
        f"{path} sent parameters the spec does not declare: {sorted(unknown)}"
    )
    # And nothing dropped: a listing that loses its filter returns every container's
    # checks for each container, and still passes the check above.
    assert set(sent) >= {"page", "size", *expected}


@pytest.mark.parametrize(
    ("path", "call"),
    [
        ("/quality-checks", lambda c: list(c.list_quality_checks(container_id=10))),
        ("/anomalies", lambda c: list(c.list_anomalies(container_id=10))),
    ],
)
def test_enum_query_params_are_sent_with_values_the_spec_allows(
    path: str, call: Any
) -> None:
    # The `archived` bug in one assertion: a bool where an enum was required.
    declared = {name.lower(): p for name, p in _spec_params(path).items()}

    with Mocker() as m:
        m.get(f"{BASE}{path}", json=_empty_page())
        call(_client())
        sent = m.last_request.qs

    for name, values in sent.items():
        allowed = _allowed_values(declared.get(name, {}))
        if allowed is None:
            continue
        lowered = {v.lower() for v in allowed}
        assert set(values) <= lowered, (
            f"{path} sent {name}={values}, but the spec allows only {sorted(allowed)}"
        )
