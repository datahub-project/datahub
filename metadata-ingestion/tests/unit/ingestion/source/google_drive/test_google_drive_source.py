"""Unit tests for the Google Drive ingestion source.

Google client libraries are not required — they are lazy-imported inside methods.
``googleapiclient`` and ``html2text`` are not installed in the dev venv; they are
provided only for the duration of tests that need them via ``patch.dict(sys.modules,
...)``.  ``google.auth`` and ``google.oauth2`` ARE installed (bigquery/gcs/etc.
depend on ``google-auth``), so we must never replace them in sys.modules.
"""

import sys
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError

# Import source modules — no sys.modules injection needed; the source only
# imports google/googleapiclient/html2text inside methods (lazy imports).
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveAuthConfig,
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_source import (
    MIME_GOOGLE_DOC,
    MIME_GOOGLE_FOLDER,
    MIME_GOOGLE_SHEETS,
    MIME_GOOGLE_SLIDES,
    GoogleDriveSource,
)

# ---------------------------------------------------------------------------
# Module-level stubs for genuinely missing packages.
#
# ``googleapiclient`` and ``html2text`` are NOT installed in the venv.  We
# provide minimal placeholder entries here so that Python's import machinery
# resolves package-parent lookups (e.g. ``googleapiclient.discovery`` requires
# ``googleapiclient`` to already be in sys.modules as a package).  The actual
# mock objects are injected per-test via the ``missing_google_stubs`` fixture.
#
# IMPORTANT: ``google``, ``google.auth``, and ``google.oauth2`` are installed
# and must never be replaced here.
# ---------------------------------------------------------------------------
_MISSING_PACKAGES = [
    "googleapiclient",
    "googleapiclient.discovery",
    "googleapiclient.errors",
    "googleapiclient.http",
    "html2text",
]


def _build_stub_modules() -> Dict[str, MagicMock]:
    """Return a dict of MagicMock stubs for the missing packages."""
    stubs: Dict[str, MagicMock] = {}
    for mod in _MISSING_PACKAGES:
        stubs[mod] = MagicMock()
    return stubs


@pytest.fixture(autouse=True)
def missing_google_stubs():
    """Inject stubs for packages that are not installed, for the duration of each test.

    Uses ``patch.dict`` so sys.modules is fully restored after every test —
    even on failure.  This prevents test-ordering pollution.
    """
    stubs = _build_stub_modules()
    with patch.dict(sys.modules, stubs):
        yield stubs


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


# ===========================================================================
# GoogleDriveSource._get_credentials — authentication paths
# ===========================================================================


class TestGetCredentials:
    def test_service_account_key_file_path(self) -> None:
        """When service_account_key_file is set, from_service_account_file is called."""
        src = _make_source({"credentials": {"service_account_key_file": "/key.json"}})

        mock_creds = MagicMock()
        # Patch the real google.oauth2.service_account.Credentials method directly
        # using a restoring patch — does NOT replace sys.modules["google.oauth2"].
        with patch(
            "google.oauth2.service_account.Credentials.from_service_account_file",
            return_value=mock_creds,
        ) as mock_from_file:
            result = src._get_credentials()

        mock_from_file.assert_called_once_with(
            "/key.json", scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        assert result is mock_creds

    def test_service_account_key_json(self) -> None:
        """When service_account_key_json is set, from_service_account_info is called."""
        import json

        key_data = {"type": "service_account", "project_id": "my-project"}
        key_json_str = json.dumps(key_data)

        src = _make_source({"credentials": {"service_account_key_json": key_json_str}})

        mock_creds = MagicMock()
        with patch(
            "google.oauth2.service_account.Credentials.from_service_account_info",
            return_value=mock_creds,
        ) as mock_from_info:
            result = src._get_credentials()

        mock_from_info.assert_called_once_with(
            key_data, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        assert result is mock_creds

    def test_adc_fallback(self) -> None:
        """When no service account is configured, google.auth.default is called."""
        src = _make_source()  # no credentials configured

        mock_adc_creds = MagicMock()

        # Patch google.auth.default in the real installed module.
        # The source does ``import google.auth`` then ``google.auth.default(...)``.
        with patch(
            "google.auth.default", return_value=(mock_adc_creds, None)
        ) as mock_default:
            result = src._get_credentials()

        mock_default.assert_called_with(
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        assert result is mock_adc_creds


# ===========================================================================
# GoogleDriveSource._build_drive_service
# ===========================================================================


class TestBuildDriveService:
    def test_build_drive_service_calls_build(self) -> None:
        """_build_drive_service calls googleapiclient.discovery.build with drive v3."""
        # We need a source with _build_drive_service NOT pre-mocked so we can test it

        mock_chunking = MagicMock()
        mock_chunking.report.num_documents_limit_reached = False

        mock_build = MagicMock()
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        mock_discovery = MagicMock()
        mock_discovery.build = mock_build

        mock_creds = MagicMock()

        cfg = GoogleDriveSource.__new__(GoogleDriveSource)
        cfg.config = GoogleDriveSourceConfig.model_validate({})

        with patch.dict(sys.modules, {"googleapiclient.discovery": mock_discovery}):
            with patch.object(
                GoogleDriveSource, "_get_credentials", return_value=mock_creds
            ):
                # Call the method directly on a partially-constructed instance
                result = GoogleDriveSource._build_drive_service(cfg)

        mock_build.assert_called_once_with(
            "drive", "v3", credentials=mock_creds, cache_discovery=False
        )
        assert result is mock_service


# ===========================================================================
# GoogleDriveSource._list_all_accessible_files — pagination
# ===========================================================================


class TestListAllAccessibleFiles:
    def test_returns_all_accessible_files_with_pagination(self) -> None:
        mock_drive = MagicMock()
        mock_drive.files().list().execute.side_effect = [
            {
                "files": [{"id": "doc1", "mimeType": MIME_GOOGLE_DOC, "name": "Doc1"}],
                "nextPageToken": "PAGE2",
            },
            {
                "files": [{"id": "doc2", "mimeType": MIME_GOOGLE_DOC, "name": "Doc2"}],
                "nextPageToken": None,
            },
        ]
        src = _make_source(mock_drive=mock_drive)
        files = src._list_all_accessible_files()
        assert len(files) == 2
        assert {f["id"] for f in files} == {"doc1", "doc2"}
        assert src.report.files_discovered == 2

    def test_empty_drive_returns_empty_list(self) -> None:
        mock_drive = MagicMock()
        mock_drive.files().list().execute.return_value = {
            "files": [],
            "nextPageToken": None,
        }
        src = _make_source(mock_drive=mock_drive)
        files = src._list_all_accessible_files()
        assert files == []


# ===========================================================================
# Content export: _export_doc_as_markdown, _export_doc_as_html,
# _export_as_plain_text, _extract_text
# ===========================================================================


class TestContentExport:
    def _make_downloader_mock(self, content: bytes) -> MagicMock:
        """Return a mock MediaIoBaseDownload that fills BytesIO with *content*."""
        mock_downloader_cls = MagicMock()

        def fake_downloader_init(fh: Any, request: Any) -> MagicMock:
            fh.write(content)
            fh.seek(0)
            inst = MagicMock()
            inst.next_chunk.return_value = (None, True)
            return inst

        mock_downloader_cls.side_effect = fake_downloader_init
        return mock_downloader_cls

    def test_export_doc_as_markdown_success(self) -> None:
        """_export_doc_as_markdown returns markdown string on success."""
        markdown_bytes = b"# Hello\n\nThis is markdown."
        mock_drive = MagicMock()
        mock_drive.files().export_media.return_value = MagicMock()

        mock_http = MagicMock()
        mock_http.MediaIoBaseDownload = self._make_downloader_mock(markdown_bytes)

        src = _make_source(mock_drive=mock_drive)
        with patch.dict(sys.modules, {"googleapiclient.http": mock_http}):
            # Also need to patch errors to avoid import issues
            mock_errors = MagicMock()
            mock_errors.HttpError = type("HttpError", (Exception,), {"resp": None})
            with patch.dict(sys.modules, {"googleapiclient.errors": mock_errors}):
                result = src._export_doc_as_markdown("file-id-1")

        assert result == "# Hello\n\nThis is markdown."

    def test_export_doc_as_markdown_http_error_returns_none(self) -> None:
        """_export_doc_as_markdown returns None on HttpError 403."""
        mock_drive = MagicMock()

        class FakeResp:
            status = 403

        class FakeHttpError(Exception):
            resp = FakeResp()

        mock_http = MagicMock()
        mock_errors = MagicMock()
        mock_errors.HttpError = FakeHttpError
        mock_http.MediaIoBaseDownload.side_effect = FakeHttpError("forbidden")

        src = _make_source(mock_drive=mock_drive)
        with patch.dict(
            sys.modules,
            {"googleapiclient.http": mock_http, "googleapiclient.errors": mock_errors},
        ):
            result = src._export_doc_as_markdown("file-id-1")

        assert result is None

    def test_export_doc_as_html_success(self) -> None:
        """_export_doc_as_html returns HTML string on success."""
        html_bytes = b"<html><body><p>Hello</p></body></html>"
        mock_drive = MagicMock()
        mock_drive.files().export_media.return_value = MagicMock()

        mock_http = MagicMock()
        mock_http.MediaIoBaseDownload = self._make_downloader_mock(html_bytes)

        src = _make_source(mock_drive=mock_drive)
        with patch.dict(sys.modules, {"googleapiclient.http": mock_http}):
            result = src._export_doc_as_html("file-id-2")

        assert result == "<html><body><p>Hello</p></body></html>"

    def test_export_doc_as_html_exception_returns_none(self) -> None:
        """_export_doc_as_html returns None when an exception occurs."""
        mock_drive = MagicMock()
        mock_http = MagicMock()
        mock_http.MediaIoBaseDownload.side_effect = Exception("connection error")

        src = _make_source(mock_drive=mock_drive)
        with patch.dict(sys.modules, {"googleapiclient.http": mock_http}):
            result = src._export_doc_as_html("file-id-2")

        assert result is None

    def test_export_as_plain_text_success(self) -> None:
        """_export_as_plain_text returns plain text on success."""
        text_bytes = b"Slide 1\nSlide 2"
        mock_drive = MagicMock()
        mock_drive.files().export_media.return_value = MagicMock()

        mock_http = MagicMock()
        mock_http.MediaIoBaseDownload = self._make_downloader_mock(text_bytes)

        src = _make_source(mock_drive=mock_drive)
        with patch.dict(sys.modules, {"googleapiclient.http": mock_http}):
            result = src._export_as_plain_text("file-id-3")

        assert result == "Slide 1\nSlide 2"

    def test_export_as_plain_text_exception_returns_none(self) -> None:
        """_export_as_plain_text returns None on failure."""
        mock_drive = MagicMock()
        mock_http = MagicMock()
        mock_http.MediaIoBaseDownload.side_effect = Exception("timeout")

        src = _make_source(mock_drive=mock_drive)
        with patch.dict(sys.modules, {"googleapiclient.http": mock_http}):
            result = src._export_as_plain_text("file-id-3")

        assert result is None

    def test_extract_text_doc_returns_markdown(self) -> None:
        """_extract_text for a Doc returns markdown when available."""
        src = _make_source()
        with patch.object(src, "_export_doc_as_markdown", return_value="# Doc content"):
            result = src._extract_text({"id": "doc1", "mimeType": MIME_GOOGLE_DOC})
        assert result == "# Doc content"

    def test_extract_text_doc_falls_back_to_html(self) -> None:
        """_extract_text falls back to HTML→markdown when markdown export fails."""
        src = _make_source()
        # Create a real html2text-like converter
        mock_converter = MagicMock()
        mock_converter.handle.return_value = "Converted markdown"
        src._html_converter = mock_converter

        with (
            patch.object(src, "_export_doc_as_markdown", return_value=None),
            patch.object(src, "_export_doc_as_html", return_value="<p>Hello</p>"),
        ):
            result = src._extract_text({"id": "doc1", "mimeType": MIME_GOOGLE_DOC})

        assert result == "Converted markdown"
        mock_converter.handle.assert_called_once_with("<p>Hello</p>")

    def test_extract_text_doc_falls_back_to_plain_text(self) -> None:
        """_extract_text falls back to plain text when both markdown and html fail."""
        src = _make_source()
        src._html_converter = None  # No HTML converter

        with (
            patch.object(src, "_export_doc_as_markdown", return_value=None),
            patch.object(src, "_export_as_plain_text", return_value="plain text"),
        ):
            result = src._extract_text({"id": "doc1", "mimeType": MIME_GOOGLE_DOC})

        assert result == "plain text"

    def test_extract_text_slides_uses_plain_text(self) -> None:
        """_extract_text for Slides uses _export_as_plain_text."""
        src = _make_source({"include_slides": True})
        with patch.object(src, "_export_as_plain_text", return_value="Slide content"):
            result = src._extract_text({"id": "slide1", "mimeType": MIME_GOOGLE_SLIDES})
        assert result == "Slide content"

    def test_extract_text_sheets_uses_plain_text(self) -> None:
        """_extract_text for Sheets uses _export_as_plain_text."""
        src = _make_source({"include_sheets": True})
        with patch.object(
            src, "_export_as_plain_text", return_value="col1,col2\nval1,val2"
        ):
            result = src._extract_text({"id": "sheet1", "mimeType": MIME_GOOGLE_SHEETS})
        assert result == "col1,col2\nval1,val2"

    def test_extract_text_unknown_mime_returns_none(self) -> None:
        """_extract_text for an unknown MIME type returns None."""
        src = _make_source()
        result = src._extract_text({"id": "other1", "mimeType": "application/pdf"})
        assert result is None


# ===========================================================================
# GoogleDriveSource._build_document — NATIVE vs EXTERNAL modes
# ===========================================================================


class TestBuildDocument:
    def test_build_document_external_mode(self) -> None:
        """_build_document in EXTERNAL mode creates an external document."""
        from datahub.ingestion.source.documents.document_import_mode import (
            DocumentImportMode,
        )

        src = _make_source({"document_import_mode": "EXTERNAL"})
        assert src.config.document_import_mode == DocumentImportMode.EXTERNAL

        doc = src._build_document(
            doc_id="test-doc-id",
            title="Test Doc",
            text="Some content",
            external_url="https://docs.google.com/doc/id/view",
            file_id="drive-file-id",
            custom_properties={"key": "value"},
            parent_urn=None,
            created_time=None,
            last_modified_time=None,
        )
        assert doc is not None

    def test_build_document_native_mode(self) -> None:
        """_build_document in NATIVE mode creates a native document."""
        from datahub.ingestion.source.documents.document_import_mode import (
            DocumentImportMode,
        )

        src = _make_source({"document_import_mode": "NATIVE"})
        assert src.config.document_import_mode == DocumentImportMode.NATIVE

        doc = src._build_document(
            doc_id="test-doc-id-native",
            title="Native Doc",
            text="Some native content",
            external_url="https://docs.google.com/doc/id/view",
            file_id="drive-file-id",
            custom_properties={},
            parent_urn=None,
            created_time=None,
            last_modified_time=None,
        )
        assert doc is not None

    def test_build_folder_document_external_mode(self) -> None:
        """_build_folder_document in EXTERNAL mode creates an external folder doc."""
        from datahub.ingestion.source.documents.document_import_mode import (
            DocumentImportMode,
        )

        src = _make_source({"document_import_mode": "EXTERNAL"})
        assert src.config.document_import_mode == DocumentImportMode.EXTERNAL

        folder_meta = {
            "id": "folder123",
            "name": "My Folder",
            "webViewLink": "https://drive.google.com/drive/folders/folder123",
        }
        doc = src._build_folder_document(
            folder_metadata=folder_meta,
            parent_urn=None,
        )
        assert doc is not None

    def test_build_folder_document_native_mode(self) -> None:
        """_build_folder_document in NATIVE mode creates a native folder doc."""

        src = _make_source({"document_import_mode": "NATIVE"})
        folder_meta = {
            "id": "folder456",
            "name": "Native Folder",
        }
        doc = src._build_folder_document(
            folder_metadata=folder_meta,
            parent_urn=None,
        )
        assert doc is not None


# ===========================================================================
# GoogleDriveSource._emit_platform_metadata
# ===========================================================================


class TestEmitPlatformMetadata:
    def test_emits_platform_workunit(self) -> None:
        """_emit_platform_metadata yields at least one MetadataWorkUnit."""
        from datahub.ingestion.api.workunit import MetadataWorkUnit

        src = _make_source()
        workunits = list(src._emit_platform_metadata())
        assert len(workunits) == 1
        assert isinstance(workunits[0], MetadataWorkUnit)


# ===========================================================================
# GoogleDriveSource._ingest_folder_chain
# ===========================================================================


class TestIngestFolderChain:
    def test_ingest_folders_disabled_returns_none_and_empty(self) -> None:
        """When ingest_folders=False, returns (None, [])."""
        src = _make_source({"ingest_folders": False})
        parent_urn, workunits = src._ingest_folder_chain(
            {"id": "file1", "parents": ["folder1"]}, set()
        )
        assert parent_urn is None
        assert workunits == []

    def test_ingest_folders_no_ancestors_returns_none(self) -> None:
        """When ingest_folders=True but no ancestors found, returns (None, [])."""
        src = _make_source({"ingest_folders": True})
        with patch.object(src, "_collect_folder_ancestors", return_value=[]):
            parent_urn, workunits = src._ingest_folder_chain(
                {"id": "file1", "parents": []}, set()
            )
        assert parent_urn is None
        assert workunits == []

    def test_ingest_folder_chain_emits_folder_workunits(self) -> None:
        """When ancestors exist, workunits are emitted and parent_urn is set correctly."""
        src = _make_source({"ingest_folders": True})

        folder_meta = {
            "id": "folder1",
            "name": "Parent Folder",
            "mimeType": MIME_GOOGLE_FOLDER,
            "webViewLink": "https://drive.google.com/drive/folders/folder1",
        }

        mock_doc = MagicMock()
        mock_wu = MagicMock()
        mock_doc.as_workunits.return_value = [mock_wu]

        with (
            patch.object(src, "_collect_folder_ancestors", return_value=[folder_meta]),
            patch.object(src, "_build_folder_document", return_value=mock_doc),
            patch.object(src, "_add_platform_instance"),
        ):
            parent_urn, workunits = src._ingest_folder_chain(
                {"id": "file1", "parents": ["folder1"]}, set()
            )

        assert parent_urn is not None
        assert "folder1" in parent_urn
        assert len(workunits) == 1
        assert workunits[0] is mock_wu
        assert src.report.folders_ingested == 1

    def test_ingest_folder_chain_skips_already_emitted_folders(self) -> None:
        """Folders already in already_emitted_folders are not re-emitted."""
        src = _make_source({"ingest_folders": True})

        folder_meta = {
            "id": "folder1",
            "name": "Parent Folder",
            "mimeType": MIME_GOOGLE_FOLDER,
        }

        already_emitted: set = {"folder1"}

        with (
            patch.object(src, "_collect_folder_ancestors", return_value=[folder_meta]),
            patch.object(src, "_build_folder_document") as mock_build,
        ):
            parent_urn, workunits = src._ingest_folder_chain(
                {"id": "file1", "parents": ["folder1"]}, already_emitted
            )

        # _build_folder_document should not be called since folder already emitted
        mock_build.assert_not_called()
        assert workunits == []


# ===========================================================================
# GoogleDriveSource.get_workunits_internal — folder_ids path + max_documents
# ===========================================================================


class TestGetWorkunitsInternal:
    def test_folder_ids_path_calls_list_files_in_folder(self) -> None:
        """When folder_ids is set, _list_files_in_folder is called for each folder."""
        src = _make_source({"folder_ids": ["folder-abc", "folder-xyz"]})

        ingested_ids: List[str] = []

        def fake_list_folder(folder_id: str, recursive: bool) -> List[Dict[str, Any]]:
            return [{"id": f"file-from-{folder_id}", "mimeType": MIME_GOOGLE_DOC}]

        def fake_ingest(
            file_metadata: Dict[str, Any], already_emitted_folders: Any
        ) -> Any:
            ingested_ids.append(file_metadata["id"])
            return iter([])

        with (
            patch.object(src, "_list_files_in_folder", side_effect=fake_list_folder),
            patch.object(src, "_emit_platform_metadata", return_value=iter([])),
            patch.object(src, "_ingest_file", side_effect=fake_ingest),
        ):
            list(src.get_workunits_internal())

        assert "file-from-folder-abc" in ingested_ids
        assert "file-from-folder-xyz" in ingested_ids

    def test_max_documents_limits_files_processed(self) -> None:
        """max_documents caps the number of files passed to _ingest_file."""
        src = _make_source({"max_documents": 2})

        files = [{"id": f"file{i}", "mimeType": MIME_GOOGLE_DOC} for i in range(5)]

        ingested_ids: List[str] = []

        def fake_ingest(
            file_metadata: Dict[str, Any], already_emitted_folders: Any
        ) -> Any:
            ingested_ids.append(file_metadata["id"])
            return iter([])

        with (
            patch.object(src, "_list_all_accessible_files", return_value=files),
            patch.object(src, "_emit_platform_metadata", return_value=iter([])),
            patch.object(src, "_ingest_file", side_effect=fake_ingest),
        ):
            list(src.get_workunits_internal())

        assert len(ingested_ids) == 2

    def test_document_limit_reached_stops_ingestion(self) -> None:
        """When chunking_source.report.num_documents_limit_reached, ingestion stops."""
        src = _make_source()

        files = [{"id": f"file{i}", "mimeType": MIME_GOOGLE_DOC} for i in range(5)]
        ingested_ids: List[str] = []

        call_count = 0

        def fake_ingest(
            file_metadata: Dict[str, Any], already_emitted_folders: Any
        ) -> Any:
            nonlocal call_count
            ingested_ids.append(file_metadata["id"])
            call_count += 1
            if call_count >= 2:
                src.chunking_source.report.num_documents_limit_reached = True
            return iter([])

        with (
            patch.object(src, "_list_all_accessible_files", return_value=files),
            patch.object(src, "_emit_platform_metadata", return_value=iter([])),
            patch.object(src, "_ingest_file", side_effect=fake_ingest),
        ):
            list(src.get_workunits_internal())

        # Should stop after 2 files (limit triggered after second file)
        assert len(ingested_ids) <= 3  # at most 3, limit check before next iteration


# ===========================================================================
# GoogleDriveSource.get_report and close
# ===========================================================================


class TestGetReportAndClose:
    def test_get_report_returns_report(self) -> None:
        src = _make_source()
        report = src.get_report()
        assert report is src.report

    def test_close_does_not_raise(self) -> None:
        src = _make_source()
        with patch.object(
            type(src).__mro__[1], "close", return_value=None, create=True
        ):
            # close calls super().close(); just verify it doesn't error
            try:
                src.close()
            except Exception as e:
                pytest.fail(f"close() raised an exception: {e}")


# ===========================================================================
# GoogleDriveSource.test_connection
# ===========================================================================


class TestTestConnection:
    def test_test_connection_success(self) -> None:
        """test_connection reports basic_connectivity=True on success."""
        import datahub.ingestion.source.unstructured.chunking_config as cc
        import datahub.ingestion.source.unstructured.chunking_source as cs

        mock_chunking = MagicMock()
        mock_chunking.report.num_documents_limit_reached = False

        mock_drive = MagicMock()
        mock_drive.files().list().execute.return_value = {"files": [{"id": "f1"}]}

        config_dict: Dict[str, Any] = {}

        with (
            patch.object(
                GoogleDriveSource, "_build_drive_service", return_value=mock_drive
            ),
            patch.object(cc, "DocumentChunkingSourceConfig", return_value=MagicMock()),
            patch.object(cs, "DocumentChunkingSource", return_value=mock_chunking),
        ):
            report = GoogleDriveSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True

    def test_test_connection_failure(self) -> None:
        """test_connection reports basic_connectivity=False on API error."""
        import datahub.ingestion.source.unstructured.chunking_config as cc
        import datahub.ingestion.source.unstructured.chunking_source as cs

        mock_chunking = MagicMock()
        mock_chunking.report.num_documents_limit_reached = False

        mock_drive = MagicMock()
        mock_drive.files().list().execute.side_effect = Exception(
            "authentication failed"
        )

        config_dict: Dict[str, Any] = {}

        with (
            patch.object(
                GoogleDriveSource, "_build_drive_service", return_value=mock_drive
            ),
            patch.object(cc, "DocumentChunkingSourceConfig", return_value=MagicMock()),
            patch.object(cs, "DocumentChunkingSource", return_value=mock_chunking),
        ):
            report = GoogleDriveSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "authentication failed" in report.basic_connectivity.failure_reason


# ===========================================================================
# GoogleDriveSource._add_platform_instance
# ===========================================================================


class TestAddPlatformInstance:
    def test_add_platform_instance_sets_aspect(self) -> None:
        """_add_platform_instance calls _set_aspect on the document."""
        src = _make_source({"platform_instance": "test-instance"})
        mock_doc = MagicMock()
        src._add_platform_instance(mock_doc)
        mock_doc._set_aspect.assert_called_once()


# ===========================================================================
# GoogleDriveSource._ingest_file — chunking/embedding and continue_on_failure
# ===========================================================================


class TestIngestFileChunking:
    def test_chunking_error_does_not_fail_document(self) -> None:
        """If process_elements_inline raises a non-limit Exception, the doc is still processed."""
        src = _make_source({"filtering": {"min_text_length": 1}})
        long_text = "x" * 100
        src.chunking_source.process_elements_inline.side_effect = Exception(  # type: ignore[attr-defined]
            "embedding error"
        )

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
                        "id": "f-chunk",
                        "name": "Chunky",
                        "mimeType": MIME_GOOGLE_DOC,
                        "modifiedTime": "2024-01-01T00:00:00Z",
                    },
                    set(),
                )
            )

        # Document should still be processed (embeddings just skipped)
        assert src.report.docs_processed == 1

    def test_continue_on_failure_false_re_raises(self) -> None:
        """When continue_on_failure=False, exceptions in _ingest_file are re-raised."""
        src = _make_source({"advanced": {"continue_on_failure": False}})

        with patch.object(src, "_extract_text", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                list(
                    src._ingest_file(
                        {"id": "f-err", "name": "Error", "mimeType": MIME_GOOGLE_DOC},
                        set(),
                    )
                )

    def test_continue_on_failure_true_records_failure(self) -> None:
        """When continue_on_failure=True, exceptions are swallowed and failure is recorded."""
        src = _make_source({"advanced": {"continue_on_failure": True}})

        with patch.object(src, "_extract_text", side_effect=ValueError("bad value")):
            list(
                src._ingest_file(
                    {"id": "f-cont", "name": "Continue", "mimeType": MIME_GOOGLE_DOC},
                    set(),
                )
            )

        assert src.report.docs_failed == 1
