from typing import Any, Dict, cast
from unittest import mock

import pytest
from google.oauth2 import service_account
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from datahub.ingestion.source.google_drive.google_drive_client import (
    GoogleDriveClient,
    build_drive_service,
)
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_report import (
    GoogleDriveSourceReport,
)
from tests.test_helpers.google_drive_fakes import (
    ARCHIVE,
    CREDENTIAL,
    DOC_A1,
    DOC_B1,
    DOC_B2,
    DOC_EMPTY,
    DOC_HTML,
    DOC_HUGE,
    DOC_IMAGES,
    DOC_OLD,
    DOC_RUNBOOK,
    ENG,
    MIME_DOC,
    MIME_FOLDER,
    ROOT_A,
    SHARED_B,
    SPECS,
    FakeDriveService,
    build_fixture_service,
    http_error,
)


def make_client(svc: FakeDriveService, **config_overrides: Any) -> GoogleDriveClient:
    config_dict: Dict[str, Any] = {"root_folder_ids": [ROOT_A, SHARED_B]}
    config_dict.update(config_overrides)
    config = GoogleDriveSourceConfig.model_validate(config_dict)
    return GoogleDriveClient(
        config, GoogleDriveSourceReport(), service=cast(Resource, svc)
    )


def walk_all(client: GoogleDriveClient) -> Any:
    roots = [client.get_root(r) for r in client.config.root_folder_ids]
    return client.walk(roots)


def test_walk_discovers_nested_folders_and_docs_across_pages() -> None:
    svc = build_fixture_service()
    client = make_client(
        svc,
        folder_pattern={"deny": [".*/Archive$"]},
        document_pattern={"deny": ["^Copy of "]},
    )
    tree = walk_all(client)

    doc_ids = {ref.file.id for ref in tree.documents}
    assert doc_ids == {
        DOC_A1,
        DOC_EMPTY,
        DOC_RUNBOOK,
        DOC_IMAGES,
        DOC_HTML,
        DOC_B1,
        DOC_HUGE,
        DOC_B2,
    }
    # Root A has 6 children and the fake pages by 2, so pagination must be followed.
    assert svc.calls_for("files.list").count(ROOT_A) == 3
    # Parents always precede children in the folder ordering.
    order = list(tree.folders)
    assert order.index(ROOT_A) < order.index(ENG) < order.index(SPECS)
    assert tree.folders[SPECS].path == "Root A/Eng/Specs"
    assert client.report.shortcuts_skipped == 1
    assert client.report.documents_filtered == 1


def test_denied_folder_subtree_is_never_listed() -> None:
    svc = build_fixture_service()
    client = make_client(svc, folder_pattern={"deny": [".*/Archive$"]})
    tree = walk_all(client)

    assert ARCHIVE not in tree.folders
    assert ARCHIVE not in svc.calls_for("files.list")
    assert DOC_OLD not in {ref.file.id for ref in tree.documents}
    assert client.report.folders_filtered == 1


def test_non_recursive_lists_only_root_children() -> None:
    svc = build_fixture_service()
    client = make_client(svc, recursive=False)
    tree = walk_all(client)

    assert set(tree.folders) == {ROOT_A, SHARED_B}
    assert ENG not in svc.calls_for("files.list")
    assert {ref.file.id for ref in tree.documents} >= {DOC_A1, DOC_B1}
    assert DOC_RUNBOOK not in {ref.file.id for ref in tree.documents}


def test_overlapping_roots_emit_each_document_once() -> None:
    svc = build_fixture_service()
    client = make_client(svc, root_folder_ids=[ROOT_A, ENG])
    tree = walk_all(client)

    ids = [ref.file.id for ref in tree.documents]
    assert ids.count(DOC_RUNBOOK) == 1
    # Eng is a root and also a child of Root A, but is listed only once (2 pages).
    assert svc.calls_for("files.list").count(ENG) == 2


def test_listing_error_is_a_failure_and_walk_continues() -> None:
    svc = build_fixture_service()
    svc.list_errors[ENG] = http_error(500, "backendError")
    client = make_client(svc)
    tree = walk_all(client)

    assert client.report.failures
    assert DOC_B1 in {ref.file.id for ref in tree.documents}
    assert DOC_RUNBOOK not in {ref.file.id for ref in tree.documents}


def test_shared_drive_root_uses_drive_name() -> None:
    client = make_client(build_fixture_service())
    root = client.get_root(SHARED_B)
    assert root.title == "Shared Drive B"
    assert root.drive_id == SHARED_B


def test_shared_drive_not_addressable_by_files_get_falls_back_to_drives_get() -> None:
    svc = build_fixture_service()
    svc.shared_drives["0AteamDriveOnly"] = "Team Drive"
    root = make_client(svc).get_root("0AteamDriveOnly")

    assert root.title == "Team Drive"
    assert root.drive_id == "0AteamDriveOnly"
    assert root.created_time is not None
    # Without a modified time the folder document would be re-stamped every run.
    assert root.modified_time == root.created_time
    assert svc.calls_for("drives.get") == ["0AteamDriveOnly"]


def test_unparseable_folder_is_a_failure_but_unparseable_doc_is_a_warning() -> None:
    svc = build_fixture_service()
    svc.add({"id": "brokenFolder01", "mimeType": MIME_FOLDER, "parents": [ROOT_A]})
    client = make_client(svc)
    walk_all(client)
    assert client.report.failures
    assert not client.report.warnings

    svc = build_fixture_service()
    svc.add({"id": "brokenDocxxx01", "mimeType": MIME_DOC, "parents": [ROOT_A]})
    client = make_client(svc)
    walk_all(client)
    assert client.report.warnings
    assert not client.report.failures


def test_root_that_is_not_a_folder_or_shared_drive_is_rejected() -> None:
    client = make_client(build_fixture_service())
    with pytest.raises(ValueError):
        client.get_root(DOC_A1)
    with pytest.raises(HttpError):
        client.get_root("missingRoot0")


def test_service_account_credentials_honour_delegated_user() -> None:
    config = GoogleDriveSourceConfig.model_validate(
        {
            "root_folder_ids": [ROOT_A],
            "credential": CREDENTIAL,
            "delegated_user": "reader@example.com",
        }
    )
    with (
        mock.patch.object(
            service_account.Credentials, "from_service_account_info"
        ) as from_info,
        mock.patch(
            "datahub.ingestion.source.google_drive.google_drive_client.build"
        ) as build,
    ):
        build_drive_service(config)

    delegated = from_info.return_value.with_subject
    delegated.assert_called_once_with("reader@example.com")
    assert build.call_args.kwargs["credentials"] is delegated.return_value


def test_export_markdown_success() -> None:
    client = make_client(build_fixture_service())
    result = client.export_markdown(DOC_A1)
    assert result.text is not None and "Hello from" in result.text
    assert not result.used_html_fallback


def test_export_falls_back_to_html_on_error_and_on_empty_markdown() -> None:
    client = make_client(build_fixture_service())

    converted = client.export_markdown(DOC_HTML)
    assert converted.used_html_fallback
    assert converted.text is not None
    assert "# Html Spec" in converted.text and "**fallback**" in converted.text

    empty = client.export_markdown(DOC_EMPTY)
    assert empty.used_html_fallback
    assert empty.text is not None and not empty.text.strip()


def test_export_too_large_and_missing_do_not_fall_back() -> None:
    svc = build_fixture_service()
    client = make_client(svc)

    huge = client.export_markdown(DOC_HUGE)
    assert huge.text is None and huge.failure_reason == "too_large"

    missing = client.export_markdown("missingxxxxx")
    assert missing.text is None and missing.failure_reason == "not_found"
    assert not any(
        target.endswith("text/html")
        for target in svc.calls_for("files.export")
        if target.startswith((DOC_HUGE, "missingxxxxx"))
    )
