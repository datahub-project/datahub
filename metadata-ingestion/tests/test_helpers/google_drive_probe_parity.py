"""Parity listings for the Google Drive probe, shared by its fixed and random
parity tests."""

from typing import List, Set

from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    make_document_urn,
)
from datahub.metadata.schema_classes import DocumentInfoClass, SubTypesClass
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
)

BUILD_DRIVE_SERVICE = (
    "datahub.ingestion.source.google_drive.google_drive_client.build_drive_service"
)
FOLDER = "Folder"
LIMIT = {"limit": 1000}


def emitted_documents(index: EmittedIndex, *, folders: bool) -> Set[str]:
    """Document URNs the run emitted: folder Documents, or Google Docs."""
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
