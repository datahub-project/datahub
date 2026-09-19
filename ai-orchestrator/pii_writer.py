"""Writes column tags in a single request.

The tags live in the `editableSchemaMetadata` aspect, which the OpenAPI v3 endpoint
replaces wholesale. So this reads it, merges, and writes once. That is both faster than
per-column calls and the only way to be correct: DataHub's per-column write paths each
read-modify-write the same aspect, so a batch of them races and the last write wins,
which is how a run that reported seven tagged columns once left tags on one.

Merging is additive. A steward's own tags, descriptions, and glossary terms on these
fields are preserved, because the aspect carries them too and a replace would drop them.
"""
from __future__ import annotations

import logging
import os
import urllib.parse

import httpx
from pydantic import BaseModel

from pii_taxonomy import PROVENANCE_TAG, tag_urn

logger = logging.getLogger("pii_writer")

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080").rstrip("/")
GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")
TIMEOUT = float(os.environ.get("PII_WRITE_TIMEOUT", "20"))

_ASPECT = "editableschemametadata"


class WriteError(RuntimeError):
    pass


class WriteResult(BaseModel):
    written: list[str] = []
    unchanged: list[str] = []


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if GMS_TOKEN:
        headers["Authorization"] = f"Bearer {GMS_TOKEN}"
    return headers


def _aspect_url(dataset_urn: str) -> str:
    encoded = urllib.parse.quote(dataset_urn, safe="")
    return f"{GMS_URL}/openapi/v3/entity/dataset/{encoded}/{_ASPECT}"


def merge_field_tags(
    aspect: dict, tags_by_field: dict[str, list[str]]
) -> tuple[dict, WriteResult]:
    """Add tags to the aspect without disturbing anything already there.

    Pure, so the merge can be tested without a GMS.
    """
    entries = list(aspect.get("editableSchemaFieldInfo") or [])
    by_path = {entry.get("fieldPath"): entry for entry in entries}
    result = WriteResult()

    for field_path, tag_names in tags_by_field.items():
        entry = by_path.get(field_path)
        if entry is None:
            entry = {"fieldPath": field_path}
            entries.append(entry)
            by_path[field_path] = entry

        global_tags = entry.setdefault("globalTags", {})
        existing = list(global_tags.get("tags") or [])
        present = {item.get("tag") for item in existing}

        added = False
        for name in tag_names:
            urn = tag_urn(name)
            if urn in present:
                continue
            existing.append({"tag": urn})
            present.add(urn)
            added = True

        global_tags["tags"] = existing
        (result.written if added else result.unchanged).append(field_path)

    return {"editableSchemaFieldInfo": entries}, result


async def apply_field_tags(
    dataset_urn: str, tags_by_field: dict[str, list[str]]
) -> WriteResult:
    """Read the aspect, merge every column's tags, write once."""
    if not tags_by_field:
        return WriteResult()

    url = _aspect_url(dataset_urn)
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        response = await client.get(url, headers=_headers())
        if response.status_code == 404:
            aspect: dict = {}
        elif response.status_code >= 400:
            raise WriteError(
                f"Reading {_ASPECT} for {dataset_urn} returned "
                f"{response.status_code}: {response.text[:200]}"
            )
        else:
            aspect = (response.json() or {}).get("value") or {}

        merged, result = merge_field_tags(aspect, tags_by_field)
        if not result.written:
            logger.info("No tag changes needed for %s", dataset_urn)
            return result

        written = await client.post(
            url,
            headers=_headers(),
            params={"createIfNotExists": "false"},
            json={"value": merged},
        )
        if written.status_code >= 400:
            raise WriteError(
                f"Writing {_ASPECT} for {dataset_urn} returned "
                f"{written.status_code}: {written.text[:200]}"
            )

    logger.info(
        "Tagged %d column(s) on %s in one write (%d already current)",
        len(result.written),
        dataset_urn,
        len(result.unchanged),
    )
    return result


def tags_for(label: str) -> list[str]:
    """The label plus the provenance tag, so every machine write stays identifiable."""
    return [label, PROVENANCE_TAG]
