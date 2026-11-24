"""Every packaged .gql must compile and validate on every pinned GMS schema.

The schemas come from scripts/export_graphql_schemas.py, which the Gradle test
tasks run first.
"""

import functools
import json
import pathlib
from typing import Any

import pytest
from graphql import GraphQLSchema, build_schema, parse, validate

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.gql_compiler import compile_gql
from datahub_core_mcp_tools.graphql_helpers import GQL_DIR

PACKAGE_DIR = pathlib.Path(__file__).resolve().parent.parent
SCHEMA_DIR = PACKAGE_DIR / "build" / "graphql-schemas"
PINS: list[dict[str, Any]] = json.loads(
    (PACKAGE_DIR / "tests" / "graphql_schemas.json").read_text()
)["schemas"]
DOCUMENTS = sorted(GQL_DIR.glob("*.gql"))


@functools.lru_cache(maxsize=None)
def schema(name: str) -> GraphQLSchema:
    path = SCHEMA_DIR / f"{name}.graphql"
    if not path.exists():
        pytest.fail(
            f"{path} is missing: run scripts/export_graphql_schemas.py "
            "(the Gradle test tasks do)"
        )
    return build_schema(path.read_text(encoding="utf-8"))


def server(pin: dict[str, Any]) -> GmsServerInfo:
    # master has no release number: compile as "newest", keeping everything.
    return GmsServerInfo(version=pin["version"], server_env="core")


def errors(document: str, pin: dict[str, Any]) -> list[str]:
    compiled = compile_gql(document, server(pin))
    return [e.message for e in validate(schema(pin["name"]), parse(compiled.query))]


def test_documents_are_packaged() -> None:
    assert DOCUMENTS


@pytest.mark.parametrize("pin", PINS, ids=lambda p: p["name"])
@pytest.mark.parametrize("document", DOCUMENTS, ids=lambda d: d.name)
def test_document_is_valid(pin: dict[str, Any], document: pathlib.Path) -> None:
    assert errors(document.read_text(encoding="utf-8"), pin) == []


NEW_IN_1_8 = """
query q($input: GetRootEntitiesInput!) {
  me { corpUser { urn } }
  getRootDataProducts(input: $input) { #[since oss=1.8.0]
    total
  }
}
"""


def _pin(name: str) -> dict[str, Any]:
    return next(p for p in PINS if p["name"] == name)


@pytest.mark.parametrize("name", ["oss-1.7.0", "oss-1.8.0", "master"])
def test_marked_field_is_valid_everywhere(name: str) -> None:
    assert errors(NEW_IN_1_8, _pin(name)) == []


def test_marked_field_is_dropped_before_its_release() -> None:
    compiled = compile_gql(NEW_IN_1_8, server(_pin("oss-1.7.0"))).query
    kept = compile_gql(NEW_IN_1_8, server(_pin("oss-1.8.0"))).query
    unmarked = NEW_IN_1_8.replace(" #[since oss=1.8.0]", "")

    assert "getRootDataProducts" not in compiled
    assert "getRootDataProducts" in kept
    # Without the marker the query is invalid on 1.7.0: the gate is doing real work.
    assert errors(unmarked, _pin("oss-1.7.0"))
