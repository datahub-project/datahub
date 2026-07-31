"""Create the taxonomy tag entities.

The aspect write does not validate tag references, so a tag that does not exist still
attaches — it just renders as a bare URN with no description. Run this once per
environment. Idempotent: `createIfNotExists=false` makes it an upsert.

    python bootstrap/seed_tags.py
"""
from __future__ import annotations

import os
import sys
import urllib.parse
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from pii_taxonomy import LABELS, PROVENANCE_TAG, tag_urn  # noqa: E402

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080").rstrip("/")
GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")


def headers() -> dict[str, str]:
    out = {"Content-Type": "application/json"}
    if GMS_TOKEN:
        out["Authorization"] = f"Bearer {GMS_TOKEN}"
    return out


def upsert(client: httpx.Client, name: str, description: str) -> None:
    encoded = urllib.parse.quote(tag_urn(name), safe="")
    response = client.post(
        f"{GMS_URL}/openapi/v3/entity/tag/{encoded}/tagproperties",
        params={"createIfNotExists": "false"},
        headers=headers(),
        json={"value": {"name": name, "description": description}},
    )
    status = "ok" if response.status_code < 400 else f"FAILED {response.status_code}"
    print(f"  {name:24} {status}")
    if response.status_code >= 400:
        print(f"      {response.text[:200]}")


def main() -> None:
    print(f"Seeding {len(LABELS) + 1} tags into {GMS_URL}")
    with httpx.Client(timeout=20) as client:
        for label in LABELS:
            upsert(client, label.name, label.description)
        upsert(
            client,
            PROVENANCE_TAG,
            "Applied by the DataHub AI Assistant from a reviewed proposal, not by a human.",
        )


if __name__ == "__main__":
    main()
