"""Random Drive trees and recipes: the probe's verdicts and `explain` must agree
with a real ingestion run on every one.

The fixed fixture is small and tidy. Real Drives have deep trees, same-named
sibling folders, names with '/' and quotes, empty and oversized Docs, and folders
the recipe denies part way down; each seed draws a different mix.
"""

import os
import random
import re
from dataclasses import replace
from pathlib import Path
from typing import Dict, List
from unittest import mock

import pytest

from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    make_document_urn,
)
from datahub.ingestion.source.google_drive.google_drive_probe import (
    GoogleDriveMetadataProbe,
)
from tests.test_helpers.google_drive_fakes import (
    EXPORT_HTML,
    EXPORT_MD,
    MIME_DOC,
    MIME_FOLDER,
    MIME_SHEET,
    FakeDriveService,
    drive_file,
    http_error,
)
from tests.test_helpers.google_drive_probe_parity import (
    BUILD_DRIVE_SERVICE,
    LISTINGS,
    emitted_documents,
)
from tests.test_helpers.probe_parity import assert_probe_parity, pipeline_ingestion

# Overlapping on purpose, so siblings share names and patterns hit several levels.
FOLDER_NAMES = ["Eng", "Archive", "Specs", "Q3/Q4", "Old 'stuff'", "Team & Co", "Eng"]
DOC_TITLES = ["Runbook", "Copy of Runbook", "Design", "Draft (old)", "Notes", "Plan"]
SEEDS = int(os.environ.get("GOOGLE_DRIVE_PROBE_SEEDS", "12"))


class _Ids:
    def __init__(self) -> None:
        self._next = 0

    def __call__(self, prefix: str) -> str:
        self._next += 1
        return f"{prefix}{self._next:010d}"


def random_drive(rng: random.Random, *, export_rules: bool) -> FakeDriveService:
    """A random tree. With export_rules off, every Doc exports non-empty text, so
    only folders and titles decide; with it on, some Docs are empty or too large
    to export."""
    svc = FakeDriveService(page_size=rng.choice([1, 2, 3, 50]))
    ids = _Ids()
    roots: List[str] = []
    for index in range(rng.randint(1, 2)):
        if index == 1 and rng.random() < 0.5:
            drive_id = ids("shared")
            svc.add(drive_file(drive_id, "Drive", MIME_FOLDER, None, drive_id=drive_id))
            svc.shared_drives[drive_id] = f"Shared {index}"
        else:
            drive_id = None
            svc.add(drive_file(ids("root"), f"Root {index}", MIME_FOLDER, None))
        roots.append(list(svc.files_by_id)[-1])

    def fill(folder_id: str, drive_id: object, depth: int) -> None:
        for _ in range(rng.randint(1 if depth == 1 else 0, 4)):
            doc_id = ids("doc")
            svc.add(
                drive_file(
                    doc_id,
                    rng.choice(DOC_TITLES),
                    MIME_DOC,
                    folder_id,
                    drive_id=drive_id if isinstance(drive_id, str) else None,
                    owners=[f"user{rng.randint(1, 3)}@example.com"],
                )
            )
            roll = rng.random()
            if export_rules and roll < 0.15:
                svc.exports[(doc_id, EXPORT_MD)] = b"\n"
                svc.exports[(doc_id, EXPORT_HTML)] = b"<html><body></body></html>"
            elif export_rules and roll < 0.25:
                svc.exports[(doc_id, EXPORT_MD)] = http_error(
                    403, "exportSizeLimitExceeded"
                )
            else:
                svc.exports[(doc_id, EXPORT_MD)] = ("x" * rng.randint(1, 80)).encode()
        if rng.random() < 0.3:
            svc.add(drive_file(ids("sheet"), "Budget", MIME_SHEET, folder_id))
        if depth >= 5:
            return
        for _ in range(rng.randint(0, 3 if depth < 2 else 2)):
            child = ids("folder")
            svc.add(
                drive_file(
                    child,
                    rng.choice(FOLDER_NAMES),
                    MIME_FOLDER,
                    folder_id,
                    drive_id=drive_id if isinstance(drive_id, str) else None,
                )
            )
            fill(child, drive_id, depth + 1)

    for root in roots:
        raw = svc.files_by_id[root]
        fill(root, raw.get("driveId"), 1)
    svc.root_ids = roots  # type: ignore[attr-defined]
    return svc


def random_recipe(
    rng: random.Random, roots: List[str], *, export_rules: bool
) -> Dict[str, object]:
    recipe: Dict[str, object] = {
        "root_folder_ids": roots,
        "recursive": rng.random() < 0.8,
        "create_root_documents": rng.random() < 0.7,
    }
    if rng.random() < 0.7:
        recipe["folder_pattern"] = {
            "deny": [f".*/{re.escape(rng.choice(FOLDER_NAMES))}$"]
        }
    if rng.random() < 0.3:
        recipe["folder_pattern"] = {
            "allow": [f"[^/]+(/{re.escape(rng.choice(FOLDER_NAMES))}(/.*)?)?$"]
        }
    if rng.random() < 0.6:
        recipe["document_pattern"] = {"deny": [f"^{re.escape(rng.choice(DOC_TITLES))}"]}
    if export_rules:
        recipe["skip_empty_documents"] = rng.random() < 0.8
        recipe["min_text_length"] = rng.choice([0, 0, 10])
        recipe["max_text_length"] = rng.choice([0, 1_000_000, 50])
    else:
        recipe["skip_empty_documents"] = False
    return recipe


@pytest.mark.parametrize("seed", range(SEEDS))
def test_listing_verdicts_match_ingestion_on_random_drives(
    seed: int, tmp_path: Path
) -> None:
    rng = random.Random(seed)
    svc = random_drive(rng, export_rules=False)
    recipe = random_recipe(rng, svc.root_ids, export_rules=False)  # type: ignore[attr-defined]

    # A random recipe may exclude every folder or Doc; agreement then means the
    # probe excludes them all too, which both directions still check.
    listings = [replace(listing, expect_empty=True) for listing in LISTINGS]

    with mock.patch(BUILD_DRIVE_SERVICE, return_value=svc):
        assert_probe_parity(
            "google-drive",
            recipe,
            pipeline_ingestion("google-drive", tmp_path),
            listings,
        )


@pytest.mark.parametrize("seed", range(SEEDS))
def test_explain_matches_ingestion_on_random_drives(seed: int, tmp_path: Path) -> None:
    rng = random.Random(10_000 + seed)
    svc = random_drive(rng, export_rules=True)
    recipe = random_recipe(rng, svc.root_ids, export_rules=True)  # type: ignore[attr-defined]

    with mock.patch(BUILD_DRIVE_SERVICE, return_value=svc):
        emitted = emitted_documents(
            pipeline_ingestion("google-drive", tmp_path)(recipe), folders=False
        )
        config = GoogleDriveSourceConfig.model_validate(recipe)
        with GoogleDriveMetadataProbe(config) as probe:
            explained = {
                file_id: probe.explain(document=file_id)
                for file_id, raw in svc.files_by_id.items()
                if raw["mimeType"] == MIME_DOC
            }

    ingested = {make_document_urn(i) for i, r in explained.items() if r["ingested"]}
    assert ingested == emitted, {
        urn: explained.get(urn.rsplit(".", 1)[-1]) for urn in ingested ^ emitted
    }
