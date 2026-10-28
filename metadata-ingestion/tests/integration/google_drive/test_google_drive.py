import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Set
from unittest import mock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    make_document_urn,
)
from datahub.ingestion.source.google_drive.google_drive_source import (
    GoogleDriveSource,
)
from datahub.ingestion.source.google_drive.models import StoredDocument
from datahub.metadata.schema_classes import DocumentInfoClass
from datahub.testing import mce_helpers
from tests.test_helpers.google_drive_fakes import (
    DOC_B1,
    DOC_RUNBOOK,
    ENG,
    EXPORT_HTML,
    EXPORT_MD,
    ROOT_A,
    SHARED_B,
    FakeDriveService,
    build_fixture_service,
    http_error,
)
from tests.test_helpers.state_helpers import (
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2026-03-01 12:00:00"
GOLDEN_FILE = "google_drive_mces_golden.json"
BUILD_DRIVE_SERVICE = (
    "datahub.ingestion.source.google_drive.google_drive_client.build_drive_service"
)
CHECKPOINT_GRAPH = (
    "datahub.ingestion.source.state_provider."
    "datahub_ingestion_checkpointing_provider.DataHubGraph"
)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_google_drive_ingest(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/google_drive"
    output_path = tmp_path / "google_drive_mces.json"

    with mock.patch(
        "datahub.ingestion.source.google_drive.google_drive_client.build_drive_service",
        return_value=build_fixture_service(),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "google-drive-test",
                "source": {
                    "type": "google-drive",
                    "config": {
                        "root_folder_ids": [ROOT_A, SHARED_B],
                        "folder_pattern": {"deny": [".*/Archive$"]},
                        "document_pattern": {"deny": ["^Copy of "]},
                        "platform_instance": "test-workspace",
                    },
                },
                "sink": {"type": "file", "config": {"filename": str(output_path)}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status(raise_warnings=False)

    with open(output_path) as f:
        emitted = json.load(f)
    document_urns = {
        mcp["entityUrn"] for mcp in emitted if mcp.get("aspectName") == "documentInfo"
    }
    # 5 folders (2 roots + Eng, Specs, Team) + 6 Google Docs.
    assert len(document_urns) == 11

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / GOLDEN_FILE,
    )


def stateful_pipeline_config(
    output_path: Path, fail_safe_threshold: float = 100.0, **overrides: Any
) -> Dict[str, Any]:
    return {
        "pipeline_name": "google-drive-stateful",
        "run_id": "google-drive-stateful",
        "source": {
            "type": "google-drive",
            "config": {
                "root_folder_ids": [ROOT_A, SHARED_B],
                "folder_pattern": {"deny": [".*/Archive$"]},
                "document_pattern": {"deny": ["^Copy of "]},
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": fail_safe_threshold,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": "http://fake.local"}},
                    },
                },
                **overrides,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


def written_infos(output_path: Path) -> Dict[str, DocumentInfoClass]:
    with open(output_path) as f:
        return {
            mcp["entityUrn"]: DocumentInfoClass.from_obj(mcp["aspect"]["json"])
            for mcp in json.load(f)
            if mcp.get("aspectName") == "documentInfo"
        }


def soft_deleted_urns(output_path: Path) -> Set[str]:
    with open(output_path) as f:
        return {
            mcp["entityUrn"]
            for mcp in json.load(f)
            if mcp.get("aspectName") == "status" and mcp["aspect"]["json"]["removed"]
        }


def remove_doc_b1(svc: FakeDriveService) -> None:
    svc.children[SHARED_B].remove(DOC_B1)


def fail_eng_listing(svc: FakeDriveService) -> None:
    svc.list_errors[ENG] = http_error(500, "backendError")


def runbook_connection_reset(svc: FakeDriveService) -> None:
    svc.exports[(DOC_RUNBOOK, EXPORT_MD)] = ConnectionResetError("reset by peer")


def runbook_export_unavailable(svc: FakeDriveService) -> None:
    svc.exports[(DOC_RUNBOOK, EXPORT_MD)] = http_error(500, "backendError")
    svc.exports[(DOC_RUNBOOK, EXPORT_HTML)] = http_error(500, "backendError")


def runbook_too_large(svc: FakeDriveService) -> None:
    svc.exports[(DOC_RUNBOOK, EXPORT_MD)] = http_error(403, "exportSizeLimitExceeded")


def runbook_deleted_after_listing(svc: FakeDriveService) -> None:
    del svc.exports[(DOC_RUNBOOK, EXPORT_MD)]


def runbook_unparseable(svc: FakeDriveService) -> None:
    del svc.files_by_id[DOC_RUNBOOK]["name"]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize(
    "change, expected_soft_deleted, run_fails",
    [
        (remove_doc_b1, {DOC_B1}, False),
        # A partial listing is a failure, which must suppress stale removal.
        (fail_eng_listing, set(), True),
        # Possibly transient errors keep the existing document.
        (runbook_connection_reset, set(), False),
        (runbook_export_unavailable, set(), False),
        (runbook_unparseable, set(), False),
        # Documents that can never be exported, or no longer exist, are removed.
        (runbook_too_large, {DOC_RUNBOOK}, False),
        (runbook_deleted_after_listing, {DOC_RUNBOOK}, False),
    ],
)
def test_stateful_ingestion_soft_deletes_only_documents_that_are_gone(
    tmp_path: Path,
    mock_datahub_graph: Any,
    change: Callable[[FakeDriveService], None],
    expected_soft_deleted: Set[str],
    run_fails: bool,
) -> None:
    with mock.patch(CHECKPOINT_GRAPH, mock_datahub_graph) as graph:
        graph.return_value = mock_datahub_graph
        with mock.patch(BUILD_DRIVE_SERVICE, return_value=build_fixture_service()):
            first = run_and_get_pipeline(stateful_pipeline_config(tmp_path / "1.json"))
        validate_all_providers_have_committed_successfully(first, 1)

        second_svc = build_fixture_service()
        change(second_svc)
        with mock.patch(BUILD_DRIVE_SERVICE, return_value=second_svc):
            second = Pipeline.create(stateful_pipeline_config(tmp_path / "2.json"))
            second.run()

    if run_fails:
        assert second.source.get_report().failures
    else:
        second.raise_from_status()
        validate_all_providers_have_committed_successfully(second, 1)
    assert soft_deleted_urns(tmp_path / "1.json") == set()
    assert soft_deleted_urns(tmp_path / "2.json") == {
        make_document_urn(file_id) for file_id in expected_soft_deleted
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
@pytest.mark.parametrize(
    "overrides",
    [
        {},
        # Nothing but documents is emitted, so a quiet run has no folder writes either.
        {"create_root_documents": False, "recursive": False},
    ],
)
def test_run_where_nothing_changed_still_removes_deleted_documents(
    tmp_path: Path, mock_datahub_graph: Any, overrides: Dict[str, Any]
) -> None:
    # The default fail_safe_threshold applies, as in a real deployment.
    with mock.patch(CHECKPOINT_GRAPH, mock_datahub_graph) as graph:
        graph.return_value = mock_datahub_graph
        config = stateful_pipeline_config(
            tmp_path / "1.json", fail_safe_threshold=75.0, **overrides
        )
        with mock.patch(BUILD_DRIVE_SERVICE, return_value=build_fixture_service()):
            first = run_and_get_pipeline(config)
        validate_all_providers_have_committed_successfully(first, 1)

        # DataHub now holds what the first run wrote, so everything still in Drive
        # is unchanged.
        infos = written_infos(tmp_path / "1.json")

        def load_stored(file_ids: List[str]) -> Dict[str, StoredDocument]:
            urns = [make_document_urn(file_id) for file_id in file_ids]
            return {
                urn: StoredDocument(info=infos[urn], removed=False)
                for urn in urns
                if urn in infos
            }

        second_svc = build_fixture_service()
        remove_doc_b1(second_svc)
        config = stateful_pipeline_config(
            tmp_path / "2.json", fail_safe_threshold=75.0, **overrides
        )
        with (
            mock.patch(BUILD_DRIVE_SERVICE, return_value=second_svc),
            mock.patch.object(
                GoogleDriveSource, "_load_stored_documents", side_effect=load_stored
            ),
        ):
            second = run_and_get_pipeline(config)
        validate_all_providers_have_committed_successfully(second, 1)

    assert not second.source.get_report().failures
    assert soft_deleted_urns(tmp_path / "2.json") == {make_document_urn(DOC_B1)}
    # Nothing that was unchanged was written again.
    assert written_infos(tmp_path / "2.json") == {}
