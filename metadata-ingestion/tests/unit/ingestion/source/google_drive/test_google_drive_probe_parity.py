from pathlib import Path
from typing import Dict
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
    ARCHIVE,
    DOC_B2,
    DOC_COPY,
    DOC_HTML,
    DOC_HUGE,
    DOC_IMAGES,
    DOC_OLD,
    DOC_RUNBOOK,
    ENG,
    MIME_FOLDER,
    ROOT_A,
    SHARED_B,
    SPECS,
    TEAM,
    FakeDriveService,
    build_fixture_service,
)
from tests.test_helpers.google_drive_probe_parity import (
    BUILD_DRIVE_SERVICE,
    LISTINGS,
    emitted_documents,
)
from tests.test_helpers.probe_parity import assert_probe_parity, pipeline_ingestion


def parity_service() -> FakeDriveService:
    """The shared fixture, less the Doc whose export fails. Probe verdicts judge
    folders and titles; a Doc dropped after export is beyond what they decide."""
    svc = build_fixture_service()
    del svc.files_by_id[DOC_HUGE]
    svc.children[SHARED_B].remove(DOC_HUGE)
    return svc


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


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {
                "folder_pattern": {"deny": [".*/Archive$"]},
                "document_pattern": {"deny": ["^Copy of "]},
            },
            id="patterns",
        ),
        pytest.param({"recursive": False, "max_text_length": 30}, id="text-rules"),
    ],
)
def test_explain_agrees_with_ingestion_for_every_doc(
    tmp_path: Path, overrides: Dict[str, object]
) -> None:
    # The whole fixture, including the Docs dropped after export (empty, too
    # large, too long), which only `explain` judges.
    svc = build_fixture_service()
    recipe: Dict[str, object] = {"root_folder_ids": [ROOT_A, SHARED_B], **overrides}

    with mock.patch(BUILD_DRIVE_SERVICE, return_value=svc):
        emitted = emitted_documents(
            pipeline_ingestion("google-drive", tmp_path)(recipe), folders=False
        )
        config = GoogleDriveSourceConfig.model_validate(recipe)
        with GoogleDriveMetadataProbe(config) as probe:
            explained = {
                file_id: probe.explain(document=file_id)
                for file_id, raw in svc.files_by_id.items()
                if raw["mimeType"] != MIME_FOLDER
            }

    ingested = {
        make_document_urn(file_id)
        for file_id, result in explained.items()
        if result["ingested"]
    }
    assert ingested == emitted
    # Every outcome came from a named rule, not a fallthrough.
    assert all(r["ingested"] or r["explanation"] for r in explained.values())
