from pathlib import Path
from typing import Dict, List, Set
from unittest import mock

import pytest

from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    make_document_urn,
)
from datahub.metadata.schema_classes import DocumentInfoClass, SubTypesClass
from tests.test_helpers.google_drive_fakes import (
    ARCHIVE,
    DOC_B2,
    DOC_COPY,
    DOC_HTML,
    DOC_HUGE,
    DOC_IMAGES,
    DOC_OLD,
    DOC_RUNBOOK,
    ENG,
    ROOT_A,
    SHARED_B,
    SPECS,
    TEAM,
    FakeDriveService,
    build_fixture_service,
)
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

BUILD_DRIVE_SERVICE = (
    "datahub.ingestion.source.google_drive.google_drive_client.build_drive_service"
)
FOLDER = "Folder"
LIMIT = {"limit": 1000}


def parity_service() -> FakeDriveService:
    """The shared fixture, less the Doc whose export fails. Probe verdicts judge
    folders and titles; a Doc dropped after export is beyond what they decide."""
    svc = build_fixture_service()
    del svc.files_by_id[DOC_HUGE]
    svc.children[SHARED_B].remove(DOC_HUGE)
    return svc


def emitted_documents(index: EmittedIndex, *, folders: bool) -> Set[str]:
    found: Set[str] = set()
    for urn, aspects in index.aspects.items():
        if not any(isinstance(a, DocumentInfoClass) for a in aspects):
            continue
        is_folder = any(
            isinstance(a, SubTypesClass) and FOLDER in a.typeNames for a in aspects
        )
        if is_folder == folders:
            found.add(urn)
    return found


def by_drive_id(record: JudgedRecord) -> str:
    return make_document_urn(record.attributes["id"])


LISTINGS: List[ParityListing] = [
    ParityListing(
        label="folders",
        command="folders",
        kwargs=LIMIT,
        emitted=lambda index: emitted_documents(index, folders=True),
        identity=by_drive_id,
    ),
    ParityListing(
        label="google docs",
        command="documents",
        kwargs=LIMIT,
        fan_out=FanOut("folders", "folder", parent_kwargs=LIMIT),
        emitted=lambda index: emitted_documents(index, folders=False),
        identity=by_drive_id,
    ),
]

BASE_RECIPE: Dict[str, object] = {
    "root_folder_ids": [ROOT_A, SHARED_B],
    # Keep Docs whose export is empty, so only folders and titles decide.
    "skip_empty_documents": False,
    "min_text_length": 0,
}


@pytest.mark.parametrize(
    "overrides, excluded",
    [
        pytest.param(
            {
                "folder_pattern": {"deny": [".*/Archive$"]},
                "document_pattern": {"deny": ["^Copy of "]},
            },
            {
                "folders": {ARCHIVE: "folder_pattern"},
                "google docs": {
                    DOC_COPY: "document_pattern",
                    # No pattern names Doc Old; the walk never enters Archive.
                    DOC_OLD: "folder_pattern",
                },
            },
            id="patterns",
        ),
        pytest.param(
            {"recursive": False},
            {
                # Every folder below a root, at any depth, and every Doc in one.
                "folders": {
                    ENG: "recursive",
                    TEAM: "recursive",
                    ARCHIVE: "recursive",
                    SPECS: "recursive",
                },
                "google docs": {
                    DOC_RUNBOOK: "recursive",
                    DOC_B2: "recursive",
                    DOC_OLD: "recursive",
                    DOC_IMAGES: "recursive",
                    DOC_HTML: "recursive",
                },
            },
            id="not-recursive",
        ),
        pytest.param(
            {"create_root_documents": False},
            {
                "folders": {
                    ROOT_A: "create_root_documents",
                    SHARED_B: "create_root_documents",
                },
                "google docs": {},
            },
            id="no-root-documents",
        ),
    ],
)
def test_probe_verdicts_match_ingestion(
    tmp_path: Path,
    overrides: Dict[str, object],
    # Every exclusion, by Drive file ID and the rule that decided it.
    excluded: Dict[str, Dict[str, str]],
) -> None:
    svc = parity_service()
    recipe = {**BASE_RECIPE, **overrides}

    with mock.patch(BUILD_DRIVE_SERVICE, return_value=svc):
        report = assert_probe_parity(
            "google-drive",
            recipe,
            pipeline_ingestion("google-drive", tmp_path),
            LISTINGS,
        )

    for label, expected in excluded.items():
        assert report.excluded_by(label) == {
            make_document_urn(file_id): reason for file_id, reason in expected.items()
        }
