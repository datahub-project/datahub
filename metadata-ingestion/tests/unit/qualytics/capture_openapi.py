#!/usr/bin/env python3
"""Capture a Qualytics deployment's OpenAPI spec into tests/unit/qualytics/fixtures/openapi.json.

Qualytics is single-tenant, so each deployment serves the spec for whatever version it
runs. That spec is the contract this connector is written against -- refresh it when you
target a newer Qualytics release, and diff the result before committing.

Only the slice the connector uses is kept: the paths in ``CONSUMED_PATHS`` and every
schema those paths reach through ``$ref``. The full spec is
several megabytes of endpoints this connector never calls; the slice is what the tests
check against, and it is small enough to review in a diff. Prose fields are dropped too.

Usage:
    QUALYTICS_BASE_URL=https://acme.qualytics.io/api \\
    QUALYTICS_TOKEN=... \\
    python tests/unit/qualytics/capture_openapi.py

    # Re-trim an already captured spec, e.g. after adding a path to constants.py:
    python tests/unit/qualytics/capture_openapi.py --from-file path/to/full-openapi.json

The captured spec describes endpoints and schemas only -- no customer data. Even so,
skim the diff before committing: example values can carry a tenant hostname.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

from datahub.ingestion.source.qualytics.constants import (
    CONSUMED_PATHS,
    DEFAULT_API_ROOT_PATH,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "openapi.json"
_REF_PREFIX = "#/components/schemas/"
# Prose fields. No test reads them, and a deployment's spec carries its developers'
# working notes in them -- internal ticket numbers and the like -- which have no place
# in a published fixture.
_PROSE_KEYS = frozenset({"description", "summary"})


def _refs(node: Any) -> set[str]:
    """Names of every component schema referenced anywhere under ``node``."""
    found: set[str] = set()
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith(_REF_PREFIX):
            found.add(ref[len(_REF_PREFIX) :])
        for value in node.values():
            found |= _refs(value)
    elif isinstance(node, list):
        for value in node:
            found |= _refs(value)
    return found


def _strip_prose(node: Any) -> Any:
    """Drop string-valued prose keys, recursively.

    Only *string* values go: a schema property that happens to be named ``description``
    has a dict value and is kept, since it is part of the contract.
    """
    if isinstance(node, dict):
        return {
            k: _strip_prose(v)
            for k, v in node.items()
            if not (k in _PROSE_KEYS and isinstance(v, str))
        }
    if isinstance(node, list):
        return [_strip_prose(v) for v in node]
    return node


def trim(spec: dict[str, Any]) -> dict[str, Any]:
    """Keep the connector's paths and the transitive closure of their schemas."""
    wanted = {f"{DEFAULT_API_ROOT_PATH}{p}" for p in CONSUMED_PATHS}
    missing = sorted(wanted - set(spec.get("paths", {})))
    if missing:
        # Not fatal here: test_api_paths.py reports it with context. Say so anyway.
        print(f"Paths not in this spec: {missing}", file=sys.stderr)
    paths = {p: op for p, op in spec.get("paths", {}).items() if p in wanted}

    all_schemas: dict[str, Any] = spec.get("components", {}).get("schemas", {})
    keep: set[str] = set()
    frontier = _refs(paths)
    while frontier:
        name = frontier.pop()
        if name in keep or name not in all_schemas:
            continue
        keep.add(name)
        frontier |= _refs(all_schemas[name])

    return {
        "openapi": spec.get("openapi"),
        # The version is what the tests report; the service's internal title is not
        # needed and is not the product's name.
        "info": {
            "title": "Qualytics API",
            "version": spec.get("info", {}).get("version"),
        },
        "paths": _strip_prose(paths),
        "components": {
            "schemas": _strip_prose({n: all_schemas[n] for n in sorted(keep)})
        },
    }


def _fetch() -> dict[str, Any]:
    base_url = os.environ.get("QUALYTICS_BASE_URL", "").rstrip("/")
    token = os.environ.get("QUALYTICS_TOKEN")
    if not base_url or not token:
        raise SystemExit(
            "Set QUALYTICS_BASE_URL (e.g. https://acme.qualytics.io/api) and QUALYTICS_TOKEN."
        )
    resp = requests.get(
        f"{base_url}/openapi.json",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    spec: dict[str, Any] = resp.json()
    return spec


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--from-file", type=Path, help="Trim a spec on disk instead of fetching."
    )
    args = parser.parse_args()

    spec = json.loads(args.from_file.read_text()) if args.from_file else _fetch()
    trimmed = trim(spec)

    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(trimmed, indent=2, sort_keys=True) + "\n")

    print(
        f"Wrote {FIXTURE}: {len(trimmed['paths'])} of {len(spec.get('paths', {}))} paths, "
        f"{len(trimmed['components']['schemas'])} of "
        f"{len(spec.get('components', {}).get('schemas', {}))} schemas "
        f"(version {trimmed['info'].get('version')})."
    )
    print("Review the diff for tenant hostnames before committing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
