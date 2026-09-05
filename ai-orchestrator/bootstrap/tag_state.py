"""Inspect or clear the taxonomy tags on datasets, for setting up a clean demo.

    python bootstrap/tag_state.py            # show current state
    python bootstrap/tag_state.py --reset    # strip taxonomy tags, leave others alone
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.parse
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from mcp_tools import get_mcp, shutdown_mcp  # noqa: E402
from pii_taxonomy import PROVENANCE_TAG, is_taxonomy_tag  # noqa: E402

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080").rstrip("/")
GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")
PLATFORMS = ("mysql", "snowflake")


def headers() -> dict[str, str]:
    out = {"Content-Type": "application/json"}
    if GMS_TOKEN:
        out["Authorization"] = f"Bearer {GMS_TOKEN}"
    return out


def aspect_url(urn: str) -> str:
    return (
        f"{GMS_URL}/openapi/v3/entity/dataset/"
        f"{urllib.parse.quote(urn, safe='')}/editableschemametadata"
    )


async def datasets(mcp) -> list[str]:
    platforms = ", ".join(PLATFORMS)
    raw = await mcp.execute_tool(
        "search",
        {
            "query": "*",
            "filter": f"entity_type = dataset AND platform IN ({platforms})",
            "num_results": 50,
        },
    )
    payload = json.loads(raw.get("result") or "{}")
    urns = []
    for row in payload.get("searchResults") or []:
        entity = row.get("entity") or row
        urn = entity.get("urn") if isinstance(entity, dict) else None
        if urn:
            urns.append(urn)
    return sorted(urns)


def tagged_columns(client: httpx.Client, urn: str) -> dict[str, list[str]]:
    response = client.get(aspect_url(urn), headers=headers())
    if response.status_code >= 400:
        return {}
    aspect = (response.json() or {}).get("value") or {}
    out = {}
    for entry in aspect.get("editableSchemaFieldInfo") or []:
        names = [
            item["tag"].rsplit(":", 1)[-1]
            for item in (entry.get("globalTags") or {}).get("tags") or []
            if is_taxonomy_tag(item.get("tag", ""))
        ]
        if names:
            out[entry["fieldPath"]] = names
    return out


def strip(client: httpx.Client, urn: str) -> int:
    """Remove only our tags, preserving everything else in the aspect."""
    response = client.get(aspect_url(urn), headers=headers())
    if response.status_code >= 400:
        return 0
    aspect = (response.json() or {}).get("value") or {}
    entries = aspect.get("editableSchemaFieldInfo") or []

    cleared = 0
    for entry in entries:
        global_tags = entry.get("globalTags") or {}
        kept = [
            item
            for item in global_tags.get("tags") or []
            if not is_taxonomy_tag(item.get("tag", ""))
        ]
        if len(kept) != len(global_tags.get("tags") or []):
            cleared += 1
        global_tags["tags"] = kept
        entry["globalTags"] = global_tags

    if cleared:
        client.post(
            aspect_url(urn),
            params={"createIfNotExists": "false"},
            headers=headers(),
            json={"value": {"editableSchemaFieldInfo": entries}},
        )
    return cleared


async def main() -> None:
    reset = "--reset" in sys.argv
    mcp = await get_mcp()
    try:
        urns = await datasets(mcp)
    finally:
        await shutdown_mcp()

    with httpx.Client(timeout=20) as client:
        if reset:
            total = sum(strip(client, urn) for urn in urns)
            print(f"reset: cleared taxonomy tags from {total} column(s)\n")

        for urn in urns:
            state = tagged_columns(client, urn)
            name = urn.split(",")[1] if "," in urn else urn
            if not state:
                print(f"  {name:36} clean")
                continue
            ai = sum(1 for tags in state.values() if PROVENANCE_TAG in tags)
            print(f"  {name:36} {len(state)} tagged ({ai} AI-proposed)")
            for field, tags in state.items():
                print(f"      {field:24} {[t for t in tags if t != PROVENANCE_TAG]}")


if __name__ == "__main__":
    asyncio.run(main())
