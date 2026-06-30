"""Unit tests for the Google Drive ingestion source.

Google client libraries are not required — they are lazy-imported inside methods
and mocked via sys.modules injection + method patching in this test module.
"""

import sys
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError

# ---------------------------------------------------------------------------
# Inject stubs for google client libraries before any source imports.
# The source lazy-imports these inside methods, but html2text is referenced
# in __init__ via a try/except, so we stub it here to avoid ImportError.
# ---------------------------------------------------------------------------
_GOOGLE_STUBS = [
    "google",
    "google.oauth2",
    "google.oauth2.service_account",
    "google.auth",
    "googleapiclient",
    "googleapiclient.discovery",
    "googleapiclient.errors",
    "googleapiclient.http",
    "html2text",
]
for _mod in _GOOGLE_STUBS:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# Now it is safe to import our source modules.
from datahub.ingestion.api.common import PipelineContext  # noqa: E402
from datahub.ingestion.source.google_drive.google_drive_config import (  # noqa: E402
    GoogleDriveAuthConfig,
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_source import (  # noqa: E402
    MIME_GOOGLE_DOC,
    MIME_GOOGLE_FOLDER,
    MIME_GOOGLE_SHEETS,
    MIME_GOOGLE_SLIDES,
    GoogleDriveSource,
)

# ---------------------------------------------------------------------------
# Helper: build a GoogleDriveSource without hitting real Google APIs.
# ---------------------------------------------------------------------------


def _make_source(
    config_dict: Dict[str, Any] | None = None,
    mock_drive: Any = None,
) -> GoogleDriveSource:
    """Create a GoogleDriveSource with mocked Drive service and chunking subsystem."""
    import datahub.ingestion.source.unstructured.chunking_config as cc
    import datahub.ingestion.source.unstructured.chunking_source as cs

    if mock_drive is None:
        mock_drive = MagicMock()

    mock_chunking = MagicMock()
    mock_chunking.report.num_documents_limit_reached = False
    mock_chunking.process_elements_inline.return_value = []

    cfg = GoogleDriveSourceConfig.model_validate(config_dict or {})
    ctx = PipelineContext(run_id="test-run")

    with (
        patch.object(
            GoogleDriveSource, "_build_drive_service", return_value=mock_drive
        ),
        patch.object(cc, "DocumentChunkingSourceConfig", return_value=MagicMock()),
        patch.object(cs, "DocumentChunkingSource", return_value=mock_chunking),
    ):
        src = GoogleDriveSource(cfg, ctx)

    return src


# ===========================================================================
# GoogleDriveAuthConfig — model_validator tests
# ===========================================================================


class TestGoogleDriveAuthConfig:
    def test_both_key_file_and_key_json_raises(self) -> None:
        """Providing both service_account_key_file and service_account_key_json is rejected."""
        with pytest.raises(ValidationError):
            GoogleDriveAuthConfig(
                service_account_key_file="/path/to/key.json",
                service_account_key_json=SecretStr('{"type": "service_account"}'),
            )

    def test_key_file_only_is_valid(self) -> None:
        """Providing only service_account_key_file is accepted."""
        cfg = GoogleDriveAuthConfig(service_account_key_file="/path/to/key.json")
        assert cfg.service_account_key_file == "/path/to/key.json"
        assert cfg.service_account_key_json is None

    def test_key_json_only_is_valid(self) -> None:
        """Providing only service_account_key_json is accepted."""
        cfg = GoogleDriveAuthConfig(
            service_account_key_json=SecretStr('{"type": "service_account"}')
        )
        assert cfg.service_account_key_json is not None
        assert (
            cfg.service_account_key_json.get_secret_value()
            == '{"type": "service_account"}'
        )
        assert cfg.service_account_key_file is None

    def test_no_credentials_uses_adc(self) -> None:
        """Providing no credentials is valid — falls through to Application Default Credentials."""
        cfg = GoogleDriveAuthConfig()
        assert cfg.service_account_key_file is None
        assert cfg.service_account_key_json is None


# ===========================================================================
# GoogleDriveSourceConfig — folder_ids URL stripping validator
# ===========================================================================


class TestFolderIdStripValidator:
    def test_full_url_is_stripped_to_bare_id(self) -> None:
        """A full Drive folder URL is reduced to the bare folder ID."""
        cfg = GoogleDriveSourceConfig.model_validate(
            {
                "folder_ids": [
                    "https://drive.google.com/drive/folders/ABC123DEF456?resourcekey=foo"
                ]
            }
        )
        assert cfg.folder_ids == ["ABC123DEF456"]

    def test_bare_id_passes_through_unchanged(self) -> None:
        """A bare ID (no URL path) is stored as-is."""
        cfg = GoogleDriveSourceConfig.model_validate({"folder_ids": ["ABC123DEF456"]})
        assert cfg.folder_ids == ["ABC123DEF456"]

    def test_mixed_urls_and_ids(self) -> None:
        """A list mixing full URLs and bare IDs is normalised correctly."""
        cfg = GoogleDriveSourceConfig.model_validate(
            {
                "folder_ids": [
                    "https://drive.google.com/drive/folders/FOLDER_ONE",
                    "FOLDER_TWO",
                ]
            }
        )
        assert cfg.folder_ids == ["FOLDER_ONE", "FOLDER_TWO"]

    def test_url_without_query_string(self) -> None:
        """A folder URL without a query string still yields the correct ID."""
        cfg = GoogleDriveSourceConfig.model_validate(
            {"folder_ids": ["https://drive.google.com/drive/folders/MY_FOLDER_ID"]}
        )
        assert cfg.folder_ids == ["MY_FOLDER_ID"]

    def test_empty_folder_ids(self) -> None:
        """An empty list is accepted (all-Drive mode)."""
        cfg = GoogleDriveSourceConfig.model_validate({"folder_ids": []})
        assert cfg.folder_ids == []


# ===========================================================================
# GoogleDriveSource._selected_mime_types
# ===========================================================================


class TestSelectedMimeTypes:
    def test_docs_only_by_default(self) -> None:
        src = _make_source()
        assert src._selected_mime_types() == [MIME_GOOGLE_DOC]

    def test_docs_and_slides(self) -> None:
        src = _make_source({"include_slides": True})
        types = src._selected_mime_types()
        assert MIME_GOOGLE_DOC in types
        assert MIME_GOOGLE_SLIDES in types
        assert MIME_GOOGLE_SHEETS not in types

    def test_all_three_types(self) -> None:
        src = _make_source({"include_slides": True, "include_sheets": True})
        types = src._selected_mime_types()
        assert MIME_GOOGLE_DOC in types
        assert MIME_GOOGLE_SLIDES in types
        assert MIME_GOOGLE_SHEETS in types

    def test_no_docs_no_types(self) -> None:
        src = _make_source({"include_docs": False})
        assert src._selected_mime_types() == []


# ===========================================================================
# Deduplication of trashed files in get_workunits_internal
# ===========================================================================


class TestDedupAndTrashedFiles:
    """The source deduplicates by file ID and excludes trashed entries via get_workunits_internal."""

    def _run_workunits(
        self, src: GoogleDriveSource, files: List[Dict[str, Any]]
    ) -> List[str]:
        """Drive _list_all_accessible_files to return *files*, run the main loop,
        return the file IDs that were actually ingested (reached _ingest_file)."""
        ingested_ids: List[str] = []

        def fake_ingest_file(
            file_metadata: Dict[str, Any], already_emitted_folders: Any
        ) -> Any:
            ingested_ids.append(file_metadata["id"])
            return iter([])

        with (
            patch.object(src, "_list_all_accessible_files", return_value=files),
            patch.object(src, "_emit_platform_metadata", return_value=iter([])),
            patch.object(src, "_ingest_file", side_effect=fake_ingest_file),
        ):
            list(src.get_workunits_internal())

        return ingested_ids

    def test_duplicate_ids_are_deduplicated(self) -> None:
        src = _make_source()
        files = [
            {"id": "file1", "mimeType": MIME_GOOGLE_DOC, "trashed": False},
            {"id": "file1", "mimeType": MIME_GOOGLE_DOC, "trashed": False},
        ]
        ingested = self._run_workunits(src, files)
        assert ingested.count("file1") == 1

    def test_trashed_files_are_excluded(self) -> None:
        src = _make_source()
        files = [
            {"id": "file1", "mimeType": MIME_GOOGLE_DOC, "trashed": False},
            {"id": "file2", "mimeType": MIME_GOOGLE_DOC, "trashed": True},
        ]
        ingested = self._run_workunits(src, files)
        assert "file1" in ingested
        assert "file2" not in ingested

    def test_all_trashed_yields_empty(self) -> None:
        src = _make_source()
        files = [
            {"id": "file1", "mimeType": MIME_GOOGLE_DOC, "trashed": True},
            {"id": "file2", "mimeType": MIME_GOOGLE_DOC, "trashed": True},
        ]
        ingested = self._run_workunits(src, files)
        assert ingested == []


# ===========================================================================
# GoogleDriveSource._build_doc_id
# ===========================================================================


class TestBuildDocId:
    def test_doc_id_contains_file_id(self) -> None:
        src = _make_source({"platform_instance": "my-org"})
        doc_id = src._build_doc_id("FILEID123")
        assert "FILEID123" in doc_id
        assert doc_id.startswith("google-drive-")

    def test_doc_id_stable_across_calls(self) -> None:
        src = _make_source({"platform_instance": "stable-instance"})
        assert src._build_doc_id("XYZ") == src._build_doc_id("XYZ")

    def test_doc_id_differs_for_different_files(self) -> None:
        src = _make_source({"platform_instance": "test"})
        assert src._build_doc_id("A") != src._build_doc_id("B")


# ===========================================================================
# GoogleDriveSource._parse_timestamp
# ===========================================================================


class TestParseTimestamp:
    def test_iso_timestamp_with_z_suffix(self) -> None:
        src = _make_source()
        result = src._parse_timestamp("2024-06-15T10:30:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 15

    def test_none_returns_none(self) -> None:
        src = _make_source()
        assert src._parse_timestamp(None) is None

    def test_empty_string_returns_none(self) -> None:
        src = _make_source()
        assert src._parse_timestamp("") is None

    def test_invalid_string_returns_none(self) -> None:
        src = _make_source()
        assert src._parse_timestamp("not-a-date") is None


# ===========================================================================
# GoogleDriveSource._build_custom_properties
# ===========================================================================


class TestBuildCustomProperties:
    def test_returns_expected_keys(self) -> None:
        src = _make_source()
        file_meta: Dict[str, Any] = {
            "id": "file123",
            "mimeType": MIME_GOOGLE_DOC,
            "owners": [{"emailAddress": "owner@example.com", "displayName": "Owner"}],
            "lastModifyingUser": {"emailAddress": "modifier@example.com"},
        }
        props = src._build_custom_properties(file_meta, content_hash="abc123")
        assert props["file_id"] == "file123"
        assert props["owner_email"] == "owner@example.com"
        assert props["content_hash"] == "abc123"
        assert "extraction_algo_version" in props

    def test_missing_owners_yields_empty_strings(self) -> None:
        src = _make_source()
        file_meta: Dict[str, Any] = {"id": "file456", "mimeType": MIME_GOOGLE_DOC}
        props = src._build_custom_properties(file_meta, content_hash="hash")
        assert props["owner_email"] == ""
        assert props["owner_name"] == ""

    def test_missing_last_modifying_user_yields_empty_string(self) -> None:
        src = _make_source()
        file_meta: Dict[str, Any] = {"id": "file789", "mimeType": MIME_GOOGLE_DOC}
        props = src._build_custom_properties(file_meta, content_hash="hash")
        assert props["last_modifying_user_email"] == ""


# ===========================================================================
# GoogleDriveSource._list_files_in_folder — folder discovery
# ===========================================================================


class TestListFilesInFolder:
    def test_folders_discovered_counted_and_not_returned_as_files(self) -> None:
        mock_drive = MagicMock()
        mock_drive.files().list().execute.return_value = {
            "files": [
                {
                    "id": "subfolder1",
                    "mimeType": MIME_GOOGLE_FOLDER,
                    "name": "SubFolder",
                    "trashed": False,
                    "parents": [],
                },
            ],
            "nextPageToken": None,
        }
        src = _make_source(mock_drive=mock_drive)
        # Non-recursive so we don't recurse into subfolder
        files = src._list_files_in_folder("root-folder", recursive=False)
        # Folder itself should NOT appear in the file results
        assert all(f["mimeType"] != MIME_GOOGLE_FOLDER for f in files)
        assert src.report.folders_discovered == 1

    def test_docs_in_folder_are_returned(self) -> None:
        mock_drive = MagicMock()
        mock_drive.files().list().execute.return_value = {
            "files": [
                {
                    "id": "doc1",
                    "mimeType": MIME_GOOGLE_DOC,
                    "name": "My Doc",
                    "trashed": False,
                },
            ],
            "nextPageToken": None,
        }
        src = _make_source(mock_drive=mock_drive)
        files = src._list_files_in_folder("folder-id", recursive=False)
        assert len(files) == 1
        assert files[0]["id"] == "doc1"
        assert src.report.files_discovered == 1

    def test_pagination_fetches_all_pages(self) -> None:
        mock_drive = MagicMock()
        # Two pages: first returns a token, second returns None
        mock_drive.files().list().execute.side_effect = [
            {
                "files": [
                    {
                        "id": "doc1",
                        "mimeType": MIME_GOOGLE_DOC,
                        "name": "Doc1",
                        "trashed": False,
                    }
                ],
                "nextPageToken": "TOKEN",
            },
            {
                "files": [
                    {
                        "id": "doc2",
                        "mimeType": MIME_GOOGLE_DOC,
                        "name": "Doc2",
                        "trashed": False,
                    }
                ],
                "nextPageToken": None,
            },
        ]
        src = _make_source(mock_drive=mock_drive)
        files = src._list_files_in_folder("folder-id", recursive=False)
        assert len(files) == 2
        assert {f["id"] for f in files} == {"doc1", "doc2"}


# ===========================================================================
# GoogleDriveSource._collect_folder_ancestors — mimeType must be present
# ===========================================================================


class TestCollectFolderAncestors:
    """_collect_folder_ancestors must walk up the parent chain.

    This test would FAIL without the fix that adds mimeType to _DRIVE_FOLDER_FIELDS
    because the mimeType gate in _collect_folder_ancestors would never be True.
    """

    def test_ancestors_collected_when_mimetype_is_folder(self) -> None:
        """When the Drive API returns mimeType for folder metadata, ancestors are collected."""
        mock_drive = MagicMock()

        # Simulate a two-level hierarchy: root_folder → parent_folder → file
        # _get_folder_metadata is called with "parent-folder-id" first, then "root-folder-id"
        parent_folder_meta = {
            "id": "parent-folder-id",
            "name": "Parent Folder",
            "mimeType": MIME_GOOGLE_FOLDER,
            "parents": ["root-folder-id"],
            "webViewLink": "https://drive.google.com/drive/folders/parent-folder-id",
        }
        root_folder_meta = {
            "id": "root-folder-id",
            "name": "Root Folder",
            "mimeType": MIME_GOOGLE_FOLDER,
            "parents": [],  # no further parents — stops traversal
            "webViewLink": "https://drive.google.com/drive/folders/root-folder-id",
        }

        def fake_get(fileId: str, fields: str) -> Any:
            result = MagicMock()
            if fileId == "parent-folder-id":
                result.execute.return_value = parent_folder_meta
            elif fileId == "root-folder-id":
                result.execute.return_value = root_folder_meta
            else:
                result.execute.return_value = None
            return result

        mock_drive.files().get.side_effect = fake_get

        src = _make_source(mock_drive=mock_drive)

        file_metadata: Dict[str, Any] = {
            "id": "my-doc-id",
            "name": "My Doc",
            "mimeType": MIME_GOOGLE_DOC,
            "parents": ["parent-folder-id"],
        }
        ancestors = src._collect_folder_ancestors(file_metadata)

        # Must find both ancestors (root first, then parent)
        assert len(ancestors) == 2
        assert ancestors[0]["id"] == "root-folder-id"
        assert ancestors[1]["id"] == "parent-folder-id"

    def test_no_ancestors_when_file_has_no_parents(self) -> None:
        """A file with no parents yields an empty ancestor list."""
        src = _make_source()
        file_metadata: Dict[str, Any] = {
            "id": "orphan-doc",
            "name": "Orphan",
            "mimeType": MIME_GOOGLE_DOC,
            "parents": [],
        }
        ancestors = src._collect_folder_ancestors(file_metadata)
        assert ancestors == []


# ===========================================================================
# GoogleDriveSource._ingest_file — short-text skip behaviour
# ===========================================================================


class TestIngestFileShortText:
    def test_file_shorter_than_minimum_is_skipped(self) -> None:
        src = _make_source({"filtering": {"min_text_length": 100}})
        # _extract_text returns a short string
        with patch.object(src, "_extract_text", return_value="hi"):
            with patch.object(src, "_ingest_folder_chain", return_value=(None, [])):
                list(
                    src._ingest_file(
                        {"id": "f1", "name": "Short", "mimeType": MIME_GOOGLE_DOC},
                        set(),
                    )
                )
        assert src.report.docs_skipped_too_short == 1
        assert src.report.docs_processed == 0

    def test_file_above_minimum_is_processed(self) -> None:
        src = _make_source({"filtering": {"min_text_length": 5}})
        long_text = "x" * 100
        with (
            patch.object(src, "_extract_text", return_value=long_text),
            patch.object(src, "_ingest_folder_chain", return_value=(None, [])),
            patch.object(src, "_build_document") as mock_build,
            patch.object(src, "_add_platform_instance"),
        ):
            mock_doc = MagicMock()
            mock_doc.as_workunits.return_value = []
            mock_build.return_value = mock_doc
            list(
                src._ingest_file(
                    {
                        "id": "f2",
                        "name": "Long",
                        "mimeType": MIME_GOOGLE_DOC,
                        "modifiedTime": "2024-01-01T00:00:00Z",
                    },
                    set(),
                )
            )
        assert src.report.docs_skipped_too_short == 0
        assert src.report.docs_processed == 1

    def test_empty_text_reports_failure(self) -> None:
        src = _make_source()
        with patch.object(src, "_extract_text", return_value=None):
            list(
                src._ingest_file(
                    {"id": "f3", "name": "Empty", "mimeType": MIME_GOOGLE_DOC}, set()
                )
            )
        assert src.report.docs_failed == 1


# ===========================================================================
# GoogleDriveSource._get_instance_id
# ===========================================================================


class TestGetInstanceId:
    def test_explicit_platform_instance_is_used(self) -> None:
        src = _make_source({"platform_instance": "my-custom-instance"})
        assert src._get_instance_id() == "my-custom-instance"

    def test_adc_fallback_is_deterministic(self) -> None:
        """Without credentials, the instance ID is derived from the string 'adc'."""
        src1 = _make_source()
        src2 = _make_source()
        assert src1._get_instance_id() == src2._get_instance_id()

    def test_instance_id_from_key_file_is_deterministic(self) -> None:
        cfg = {"credentials": {"service_account_key_file": "/path/to/key.json"}}
        src1 = _make_source(cfg)
        src2 = _make_source(cfg)
        assert src1._get_instance_id() == src2._get_instance_id()
