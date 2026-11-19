"""Version-selecting reads of raw aspects over DataHub's OpenAPI v3 ``batchGet``.

GraphQL serves only an aspect's current value. OpenAPI v3
``POST /openapi/v3/entity/{entityName}/batchGet`` (served by GMS 1.7.0 and later)
takes an ``If-Version-Match`` header per aspect, so one request can
read a different stored version of each (urn, aspect) pair.
"""

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.graph.client import DataHubGraph
from pydantic import JsonValue

# Spelled out rather than imported from read_backend, which imports this module.
JsonObject = dict[str, JsonValue]

_JSON_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}
_VERSION_HEADER = "If-Version-Match"


class VersionedBatchGetUnavailable(RuntimeError):
    """The instance does not serve versioned OpenAPI v3 ``batchGet``.

    Raised on a 404 so the specific diagnostic survives instead of being folded
    into the generic transport error, which would hide the one actionable cause.
    """


@dataclass(frozen=True)
class VersionedAspectPair:
    urn: str
    entity_name: str
    aspect_name: str
    version: int


@dataclass(frozen=True)
class RawVersionedBatch:
    """Envelopes (``value``, ``systemMetadata``, ``auditStamp``) by (urn, aspect).

    A pair GMS returned nothing for is absent from ``aspects``; a pair whose request
    failed is in ``errors`` with a message safe to show the caller.
    """

    aspects: dict[tuple[str, str], JsonObject]
    errors: dict[tuple[str, str], str]
    http_calls: int


class VersionedOpenApiClient:
    """Reads one stored version per (urn, aspect) pair, one request per entity type.

    The SDK's typed ``OpenApiAPI.get_entities`` drops ``auditStamp`` and cannot send a
    per-aspect version header, so this posts through the caller's authenticated
    ``graph._session`` itself. It returns the raw envelope the history tool needs.
    """

    def __init__(self, graph: DataHubGraph):
        self._graph = graph

    def get_entities(
        self,
        pairs: Iterable[VersionedAspectPair],
        *,
        with_system_metadata: bool = True,
    ) -> RawVersionedBatch:
        grouped: dict[str, dict[str, list[tuple[str, int]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for pair in pairs:
            grouped[pair.entity_name][pair.urn].append((pair.aspect_name, pair.version))

        aspects: dict[tuple[str, str], JsonObject] = {}
        errors: dict[tuple[str, str], str] = {}
        http_calls = 0
        for entity_name, entities in grouped.items():
            request_payload: list[JsonObject] = []
            group_keys: list[tuple[str, str]] = []
            for urn, aspect_versions in entities.items():
                entity_request: JsonObject = {"urn": urn}
                for aspect_name, version in aspect_versions:
                    entity_request[aspect_name] = {
                        "headers": {_VERSION_HEADER: str(version)}
                    }
                    group_keys.append((urn, aspect_name))
                request_payload.append(entity_request)

            url = f"{self._graph._gms_server}/openapi/v3/entity/{entity_name}/batchGet"
            try:
                http_calls += 1
                response = self._graph._session.post(
                    url,
                    params={"systemMetadata": str(with_system_metadata).lower()},
                    data=json.dumps(request_payload),
                    headers=_JSON_HEADERS,
                )
                if response.status_code == 404:
                    raise VersionedBatchGetUnavailable(
                        "versioned OpenAPI v3 batchGet is unavailable"
                    )
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, list):
                    raise RuntimeError("invalid OpenAPI v3 batchGet response")
            except VersionedBatchGetUnavailable as exc:
                # Our own message, so it is safe to surface verbatim; unlike the
                # generic branch it carries no provider-supplied text.
                message = f"DataHub batchGet failed: {exc}"
                for key in group_keys:
                    errors[key] = message
                continue
            except Exception as exc:
                message = f"DataHub batchGet failed ({type(exc).__name__})"
                for key in group_keys:
                    errors[key] = message
                continue

            for entity in body:
                if not isinstance(entity, dict) or not isinstance(
                    entity.get("urn"), str
                ):
                    continue
                urn = entity["urn"]
                if urn not in entities:
                    continue
                for aspect_name, _version in entities[urn]:
                    envelope = entity.get(aspect_name)
                    if isinstance(envelope, dict) and isinstance(
                        envelope.get("value"), dict
                    ):
                        aspects[(urn, aspect_name)] = envelope

        return RawVersionedBatch(aspects=aspects, errors=errors, http_calls=http_calls)
