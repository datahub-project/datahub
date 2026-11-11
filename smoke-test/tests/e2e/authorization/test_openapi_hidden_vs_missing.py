"""Every OpenAPI read keyed by a urn answers for an entity the caller can't view exactly as for one
that doesn't exist.

The endpoints come from the live OpenAPI spec, so a new endpoint is covered without being listed
here. Requires ``VIEW_AUTHORIZATION_ENABLED=true``; REST API authorization is on by default.
"""

from __future__ import annotations

import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import pytest

from tests.e2e.authorization.hidden_vs_missing import (
    HiddenPair,
    HiddenWorld,
    differences,
    hidden_world,
    normalize,
)
from tests.e2e.authorization.utils import is_view_authorization_enabled
from tests.e2e.utils import get_frontend_url
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.global_policy_mutator,
    pytest.mark.domain(Domain.PLATFORM),
]

_PATH_PARAM = re.compile(r"\{([^}]+)\}")
# Path segments that name an entity type, e.g. /v3/entity/dataset/{urn}.
_ENTITY_SEGMENT = re.compile(r"/(?:entity|timeline/v1|relationship)/([a-zA-Z]+)/")
_FILLERS: Dict[str, Any] = {
    "aspectName": "status",
    "aspect": "status",
    "version": "0",
    "count": "10",
    "start": "0",
}


@dataclass(frozen=True)
class Endpoint:
    method: str
    path: str
    parameters: List[Dict[str, Any]]


@pytest.fixture(scope="module")
def world(graph_client, auth_session) -> Iterator[HiddenWorld]:
    if not is_view_authorization_enabled(auth_session):
        pytest.skip("VIEW_AUTHORIZATION_ENABLED is false")
    yield from hidden_world(graph_client)


def _specs(auth_session) -> List[Dict[str, Any]]:
    gms = auth_session.gms_url()
    config = auth_session.get(f"{gms}/openapi/v3/api-docs/swagger-config")
    config.raise_for_status()
    specs = []
    for entry in config.json().get("urls", []):
        response = auth_session.get(
            urllib.parse.urljoin(gms + "/", entry["url"].lstrip("/"))
        )
        response.raise_for_status()
        specs.append(response.json())
    return specs


def _is_urn_param(name: str) -> bool:
    return "urn" in name.lower()


def _endpoints(specs: List[Dict[str, Any]]) -> List[Endpoint]:
    endpoints = []
    for spec in specs:
        for path, operations in spec.get("paths", {}).items():
            shared = operations.get("parameters", [])
            for method in ("get", "head"):
                operation = operations.get(method)
                if operation is None:
                    continue
                parameters = shared + operation.get("parameters", [])
                if any(
                    p.get("in") in ("path", "query") and _is_urn_param(p["name"])
                    for p in parameters
                ):
                    endpoints.append(Endpoint(method.upper(), path, parameters))
    return endpoints


def _applies_to(endpoint: Endpoint, pair: HiddenPair) -> bool:
    match = _ENTITY_SEGMENT.search(endpoint.path)
    return match is None or match.group(1).lower() == pair.entity_type.lower()


def _filler(parameter: Dict[str, Any], pair: HiddenPair) -> Optional[str]:
    name = parameter["name"]
    if name in _FILLERS:
        return _FILLERS[name]
    if name in ("entityName", "entityType"):
        return pair.entity_type
    schema = parameter.get("schema", {})
    if "default" in schema:
        return (
            str(schema["default"]).lower()
            if isinstance(schema["default"], bool)
            else str(schema["default"])
        )
    if schema.get("enum"):
        return str(schema["enum"][0])
    if schema.get("type") == "boolean":
        return "false"
    if schema.get("type") in ("integer", "number"):
        return "0"
    if parameter.get("required"):
        return "x"
    return None


def _request(session, endpoint: Endpoint, pair: HiddenPair, urn: str) -> Any:
    path = endpoint.path
    query: Dict[str, str] = {}
    for parameter in endpoint.parameters:
        location = parameter.get("in")
        if location not in ("path", "query"):
            continue
        value = urn if _is_urn_param(parameter["name"]) else _filler(parameter, pair)
        if value is None:
            continue
        if location == "path":
            path = path.replace(
                "{" + parameter["name"] + "}", urllib.parse.quote(value, safe="")
            )
        else:
            query[parameter["name"]] = value
    if _PATH_PARAM.search(path):
        return None
    response = session.request(
        endpoint.method,
        f"{get_frontend_url()}/openapi{path}",
        params=query,
        allow_redirects=False,
    )
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text
    return {"status": response.status_code, "body": body}


def test_openapi_reads_answer_hidden_entities_like_missing_ones(world, auth_session):
    endpoints = _endpoints(_specs(auth_session))
    assert len(endpoints) > 20, (
        f"only {len(endpoints)} urn-keyed endpoints found in the spec"
    )

    session = world.user_session()
    failures: List[str] = []
    compared = 0
    for pair in world.pairs:
        for endpoint in endpoints:
            if not _applies_to(endpoint, pair):
                continue
            hidden = _request(session, endpoint, pair, pair.hidden_urn)
            if hidden is None:
                continue
            absent = _request(session, endpoint, pair, pair.absent_urn)
            absent_again = _request(session, endpoint, pair, pair.absent_urn)
            compared += 1
            diffs = differences(
                normalize(hidden, pair),
                normalize(absent, pair),
                normalize(absent_again, pair),
            )
            if diffs:
                failures.append(
                    f"{endpoint.method} {endpoint.path} [{pair.entity_type}]:\n  "
                    + "\n  ".join(diffs[:5])
                )
    logger.info("compared %d hidden/missing request pairs", compared)
    assert compared > 0
    assert not failures, (
        f"{len(failures)} OpenAPI reads tell a hidden entity from a missing one:\n"
        + "\n".join(failures)
    )
