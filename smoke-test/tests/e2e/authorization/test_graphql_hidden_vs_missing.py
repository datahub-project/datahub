"""Every GraphQL read answers for an entity the caller can't view exactly as for one that doesn't
exist.

Queries are generated from the live schema, so a new query field, or a new field on an entity
type, is covered without being listed here:

- every ``Query`` field that takes arguments, with the hidden urn in each string argument;
- every field of each hidden entity, reached through its lookup (``dataset(urn:)`` and so on).

Requires ``VIEW_AUTHORIZATION_ENABLED=true``.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

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

_URN = object()
_SELECTION_DEPTH = 2
_MAX_ENUM_VARIANTS = 3
_RELATIONSHIP_TYPES = [
    "IsPartOf",
    "Contains",
    "DownstreamOf",
    "Consumes",
    "Produces",
    "TaggedWith",
    "TermedWith",
    "AssociatedWith",
    "OwnedBy",
    "DataProductContains",
    "IsA",
    "HasA",
    "Asserts",
]
_NAMED_ARGUMENTS: Dict[str, Any] = {
    "types": _RELATIONSHIP_TYPES,
    "query": "*",
    "start": 0,
    "count": 10,
    "limit": 10,
}

INTROSPECTION = """
query {
  __schema {
    types {
      kind name
      fields(includeDeprecated: true) {
        name
        args { name type { ...T } defaultValue }
        type { ...T }
      }
      inputFields { name type { ...T } defaultValue }
      enumValues(includeDeprecated: true) { name }
      possibleTypes { name }
    }
  }
}
fragment T on __Type {
  kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } }
}
"""


class Schema:
    def __init__(self, types: List[Dict[str, Any]]):
        self.types = {t["name"]: t for t in types}

    @staticmethod
    def unwrap(type_ref: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, bool]:
        """(named type, required, is list)."""
        required = type_ref["kind"] == "NON_NULL"
        is_list = False
        while type_ref["kind"] in ("NON_NULL", "LIST"):
            if type_ref["kind"] == "LIST":
                is_list = True
            type_ref = type_ref["ofType"]
        return type_ref, required, is_list

    def selection(self, type_name: str, depth: int) -> Optional[str]:
        named = self.types[type_name]
        if named["kind"] in ("SCALAR", "ENUM"):
            return ""
        parts = ["__typename"]
        if named["kind"] == "UNION":
            for possible in named["possibleTypes"]:
                inner = self.selection(possible["name"], depth)
                if inner:
                    parts.append(f"... on {possible['name']} {inner}")
            return "{ " + " ".join(parts) + " }"
        for field in named.get("fields") or []:
            if any(self.unwrap(a["type"])[1] for a in field["args"]):
                continue
            field_type, _, _ = self.unwrap(field["type"])
            if self.types[field_type["name"]]["kind"] in ("SCALAR", "ENUM"):
                parts.append(field["name"])
            elif depth > 0:
                inner = self.selection(field_type["name"], depth - 1)
                if inner:
                    parts.append(f"{field['name']} {inner}")
        return "{ " + " ".join(parts) + " }"

    def literal(
        self, type_ref: Dict[str, Any], name: str, urn: str, enum_choice: int
    ) -> Any:
        named, _, is_list = self.unwrap(type_ref)
        value = self._value(named, name, urn, enum_choice)
        if value is None:
            return None
        return f"[{value}]" if is_list and not value.startswith("[") else value

    def _value(
        self, named: Dict[str, Any], name: str, urn: str, enum_choice: int
    ) -> Any:
        kind = self.types[named["name"]]["kind"]
        if name in _NAMED_ARGUMENTS and kind == "SCALAR":
            return json.dumps(_NAMED_ARGUMENTS[name])
        if kind == "ENUM":
            values = self.types[named["name"]]["enumValues"]
            return values[min(enum_choice, len(values) - 1)]["name"]
        if kind == "SCALAR":
            return {
                "Int": "0" if name in ("start", "offset") else "10",
                "Long": "0",
                "Float": "0",
                "Boolean": "false",
            }.get(named["name"], json.dumps(urn))
        if kind == "INPUT_OBJECT":
            fields = []
            for input_field in self.types[named["name"]]["inputFields"]:
                _, required, _ = self.unwrap(input_field["type"])
                if not required and not _carries_urn(input_field["name"]):
                    continue
                value = self.literal(
                    input_field["type"], input_field["name"], urn, enum_choice
                )
                if value is not None:
                    fields.append(f"{input_field['name']}: {value}")
            return "{ " + ", ".join(fields) + " }"
        return None

    def arguments(
        self, args: List[Dict[str, Any]], urn: str, enum_choice: int
    ) -> Optional[str]:
        parts = []
        for arg in args:
            _, required, _ = self.unwrap(arg["type"])
            if (
                not required
                and not _carries_urn(arg["name"])
                and arg["name"] != "input"
            ):
                continue
            value = self.literal(arg["type"], arg["name"], urn, enum_choice)
            if value is None:
                if required:
                    return None
                continue
            parts.append(f"{arg['name']}: {value}")
        return "(" + ", ".join(parts) + ")" if parts else ""

    def enum_variants(self, args: List[Dict[str, Any]]) -> int:
        """How many enum choices to try: enough to walk lineage both ways, and no more."""
        counts = [1]
        for arg in args:
            named, _, _ = self.unwrap(arg["type"])
            if self.types[named["name"]]["kind"] == "ENUM":
                counts.append(len(self.types[named["name"]]["enumValues"]))
            if self.types[named["name"]]["kind"] == "INPUT_OBJECT":
                for input_field in self.types[named["name"]]["inputFields"]:
                    inner, required, _ = self.unwrap(input_field["type"])
                    if required and self.types[inner["name"]]["kind"] == "ENUM":
                        counts.append(len(self.types[inner["name"]]["enumValues"]))
        return min(max(counts), _MAX_ENUM_VARIANTS)


def _carries_urn(name: str) -> bool:
    lowered = name.lower()
    return "urn" in lowered or lowered in ("filters", "orfilters", "values", "value")


@dataclass(frozen=True)
class Probe:
    label: str
    pair: HiddenPair
    hidden: str
    absent: str


def _root_probes(schema: Schema, pair: HiddenPair) -> List[Probe]:
    probes = []
    for field in schema.types["Query"]["fields"]:
        if not field["args"]:
            continue
        field_type, _, _ = schema.unwrap(field["type"])
        selection = schema.selection(field_type["name"], _SELECTION_DEPTH) or ""
        for choice in range(schema.enum_variants(field["args"])):
            queries = []
            for urn in (pair.hidden_urn, pair.absent_urn):
                args = schema.arguments(field["args"], urn, choice)
                if args is None:
                    break
                queries.append(f"query {{ {field['name']}{args} {selection} }}")
            if len(queries) == 2:
                probes.append(
                    Probe(
                        f"Query.{field['name']}#{choice}", pair, queries[0], queries[1]
                    )
                )
    return probes


def _field_probes(schema: Schema, pair: HiddenPair) -> List[Probe]:
    lookup = next(
        (
            f
            for f in schema.types["Query"]["fields"]
            if f["name"] == pair.entity_type
            and [a["name"] for a in f["args"]] == ["urn"]
        ),
        None,
    )
    if lookup is None:
        return []
    entity_type, _, _ = schema.unwrap(lookup["type"])
    probes = []
    for field in schema.types[entity_type["name"]]["fields"]:
        field_type, _, _ = schema.unwrap(field["type"])
        selection = schema.selection(field_type["name"], _SELECTION_DEPTH) or ""
        for choice in range(schema.enum_variants(field["args"])):
            queries = []
            for urn in (pair.hidden_urn, pair.absent_urn):
                args = schema.arguments(field["args"], urn, choice)
                if args is None:
                    break
                queries.append(
                    f"query {{ {lookup['name']}(urn: {json.dumps(urn)}) "
                    f"{{ urn {field['name']}{args} {selection} }} }}"
                )
            if len(queries) == 2:
                probes.append(
                    Probe(
                        f"{entity_type['name']}.{field['name']}#{choice}",
                        pair,
                        queries[0],
                        queries[1],
                    )
                )
    return probes


def _execute(session, query: str) -> Dict[str, Any]:
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql", json={"query": query}
    )
    try:
        body = response.json()
    except ValueError:
        return {"status": response.status_code, "body": response.text}
    errors = [
        {
            "message": e.get("message"),
            "path": e.get("path"),
            "extensions": e.get("extensions"),
        }
        for e in body.get("errors") or []
    ]
    return {"status": response.status_code, "data": body.get("data"), "errors": errors}


def _check(session, probe: Probe) -> Optional[str]:
    hidden = _execute(session, probe.hidden)
    absent = _execute(session, probe.absent)
    absent_again = _execute(session, probe.absent)
    diffs = differences(
        normalize(hidden, probe.pair),
        normalize(absent, probe.pair),
        normalize(absent_again, probe.pair),
    )
    if not diffs:
        return None
    return f"{probe.label} [{probe.pair.entity_type}]:\n  " + "\n  ".join(diffs[:5])


@pytest.fixture(scope="module")
def world(graph_client, auth_session) -> Iterator[HiddenWorld]:
    if not is_view_authorization_enabled(auth_session):
        pytest.skip("VIEW_AUTHORIZATION_ENABLED is false")
    yield from hidden_world(graph_client)


@pytest.fixture(scope="module")
def schema(auth_session) -> Schema:
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json={"query": INTROSPECTION}
    )
    response.raise_for_status()
    return Schema(response.json()["data"]["__schema"]["types"])


def _run(world: HiddenWorld, probes: List[Probe]) -> None:
    session = world.user_session()
    with ThreadPoolExecutor(max_workers=8) as pool:
        failures = [f for f in pool.map(lambda p: _check(session, p), probes) if f]
    logger.info("compared %d hidden/missing GraphQL probes", len(probes))
    assert not failures, (
        f"{len(failures)} GraphQL reads tell a hidden entity from a missing one:\n"
        + "\n".join(failures)
    )


def test_query_fields_answer_hidden_entities_like_missing_ones(world, schema):
    probes = [p for pair in world.pairs for p in _root_probes(schema, pair)]
    assert len(probes) > 100, f"only {len(probes)} query probes generated"
    _run(world, probes)


def test_entity_fields_answer_hidden_entities_like_missing_ones(world, schema):
    probes = [p for pair in world.pairs for p in _field_probes(schema, pair)]
    covered = {p.pair.entity_type for p in probes}
    assert covered == {p.entity_type for p in world.pairs}, f"no lookup for {covered}"
    _run(world, probes)
