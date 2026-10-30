"""Tests for NotionSource behaviour at the Notion API boundary.

Covers test_connection, page/database access validation, auto-discovery
pagination and how pipeline failures surface. The ``notion_client.Client`` and
unstructured-ingest ``Pipeline`` are the only things mocked.
"""

from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.notion.notion_source import NotionSource
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
)
from tests.unit.ingestion.source.notion.conftest import (
    DB_ID,
    PAGE_ID,
    make_notion_config,
    make_notion_source,
    notion_config_dict,
)


def test_connection_unparseable_config_is_internal_failure() -> None:
    """A config that fails validation is an internal failure, not a connectivity one."""
    report = NotionSource.test_connection({"api_key": "", "page_ids": ["nope"]})

    assert report.internal_failure
    assert report.basic_connectivity is None


def test_connection_invalid_api_key_fails_basic_connectivity(
    notion_client: MagicMock,
) -> None:
    """A rejected API key fails basic connectivity without probing capabilities."""
    notion_client.search.side_effect = Exception("401 unauthorized")

    report = NotionSource.test_connection(notion_config_dict())

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert not report.internal_failure
    assert report.capability_report is None


def test_connection_flags_inaccessible_configured_ids(
    notion_client: MagicMock,
) -> None:
    """Only the ids the integration cannot retrieve are named in the access failure."""
    notion_client.pages.retrieve.side_effect = Exception("404 object_not_found")

    report = NotionSource.test_connection(
        notion_config_dict(page_ids=[PAGE_ID], database_ids=[DB_ID])
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    access = report.capability_report["Page/Database Access"]
    assert not access.capable
    assert access.failure_reason is not None
    assert PAGE_ID in access.failure_reason
    assert DB_ID not in access.failure_reason


def test_connection_with_accessible_ids_reports_access_and_semantic_search(
    notion_client: MagicMock,
) -> None:
    """Configured ids are probed individually and auto-discovery is not reported."""
    report = NotionSource.test_connection(
        notion_config_dict(page_ids=[PAGE_ID], database_ids=[DB_ID])
    )

    notion_client.pages.retrieve.assert_called_once_with(PAGE_ID)
    notion_client.databases.retrieve.assert_called_once_with(DB_ID)
    assert report.capability_report is not None
    assert report.capability_report["Page/Database Access"].capable
    assert report.capability_report["Semantic Search"].capable
    assert "Auto-Discovery" not in report.capability_report


def test_connection_without_ids_reports_auto_discovery(
    notion_client: MagicMock,
) -> None:
    """With no ids configured, test_connection reports auto-discovery instead of access."""
    report = NotionSource.test_connection(notion_config_dict())

    notion_client.pages.retrieve.assert_not_called()
    assert report.capability_report is not None
    assert report.capability_report["Auto-Discovery"].capable
    assert "Page/Database Access" not in report.capability_report


def test_semantic_search_capability_reports_config_error_instead_of_raising() -> None:
    """An embedding-config lookup error becomes a failed capability with a mitigation."""
    config = make_notion_config()

    with patch.object(
        DocumentChunkingSource,
        "resolve_embedding_config",
        autospec=True,
        side_effect=ValueError("semantic search is not enabled on the server"),
    ):
        report = NotionSource._test_semantic_search_capability(config)

    assert not report.capable
    assert report.failure_reason
    assert report.mitigation_message


def test_validate_page_accessibility_lists_every_inaccessible_id(
    notion_client: MagicMock,
) -> None:
    """All inaccessible ids are collected into one error rather than failing on the first."""
    source = make_notion_source(page_ids=[PAGE_ID], database_ids=[DB_ID])
    notion_client.pages.retrieve.side_effect = Exception("404")
    notion_client.databases.retrieve.side_effect = Exception("403")

    with pytest.raises(ValueError) as exc_info:
        source._validate_page_accessibility([PAGE_ID], [DB_ID])

    assert PAGE_ID in str(exc_info.value)
    assert DB_ID in str(exc_info.value)


def test_validate_page_accessibility_passes_when_all_ids_resolve(
    notion_client: MagicMock,
) -> None:
    source = make_notion_source(page_ids=[PAGE_ID], database_ids=[DB_ID])

    source._validate_page_accessibility([PAGE_ID], [DB_ID])

    notion_client.pages.retrieve.assert_called_once_with(PAGE_ID)
    notion_client.databases.retrieve.assert_called_once_with(DB_ID)


def test_discover_all_accessible_pages_paginates_and_splits_by_type(
    notion_client: MagicMock,
) -> None:
    """Discovery follows next_cursor across pages and ignores results without an id."""
    source = make_notion_source()
    notion_client.search.side_effect = [
        {
            "results": [
                {"object": "page", "id": "page-1"},
                {"object": "database", "id": "db-1"},
                {"object": "page"},  # no id: ignored
            ],
            "has_more": True,
            "next_cursor": "cursor-1",
        },
        {"results": [{"object": "page", "id": "page-2"}], "has_more": False},
    ]

    page_ids, database_ids = source._discover_all_accessible_pages()

    assert page_ids == ["page-1", "page-2"]
    assert database_ids == ["db-1"]
    first_call, second_call = notion_client.search.call_args_list
    assert "start_cursor" not in first_call.kwargs
    assert second_call.kwargs["start_cursor"] == "cursor-1"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Bug: _discover_all_accessible_pages eagerly indexes title[0] inside a "
        "logger.debug f-string, so an untitled page (properties.title.title == []) "
        "or untitled database (title == []) raises IndexError and aborts discovery."
    ),
)
def test_discover_all_accessible_pages_tolerates_untitled_results(
    notion_client: MagicMock,
) -> None:
    """Untitled pages and databases must still be discovered, not abort the run."""
    source = make_notion_source()
    notion_client.search.return_value = {
        "results": [
            {
                "object": "page",
                "id": "untitled-page",
                "properties": {"title": {"type": "title", "title": []}},
            },
            {"object": "database", "id": "untitled-db", "title": []},
        ],
        "has_more": False,
    }

    page_ids, database_ids = source._discover_all_accessible_pages()

    assert page_ids == ["untitled-page"]
    assert database_ids == ["untitled-db"]


def test_get_workunits_internal_reports_pipeline_failure() -> None:
    """A pipeline crash is a report failure, not an exception, unless raise_on_error."""
    source = make_notion_source(page_ids=[PAGE_ID])

    with patch.object(
        source,
        "_run_unstructured_pipeline",
        autospec=True,
        side_effect=RuntimeError("boom"),
    ):
        assert list(source.get_workunits_internal()) == []

    assert any(
        failure.message == "Failed to run Unstructured pipeline"
        for failure in source.report.failures
    )


def test_get_workunits_internal_reraises_pipeline_failure_when_configured() -> None:
    source = make_notion_source(page_ids=[PAGE_ID], advanced={"raise_on_error": True})

    with (
        patch.object(
            source,
            "_run_unstructured_pipeline",
            autospec=True,
            side_effect=RuntimeError("boom"),
        ),
        pytest.raises(RuntimeError),
    ):
        list(source.get_workunits_internal())


def test_run_pipeline_indexes_discovered_ids_when_none_configured(
    tmp_path: Path, pipeline_patches: MagicMock
) -> None:
    """Without configured ids the indexer is built from the auto-discovered ones."""
    source = make_notion_source(advanced={"work_dir": str(tmp_path / "work")})
    discovered: List[str] = ["discovered-page"]

    with patch.object(
        source,
        "_discover_all_accessible_pages",
        autospec=True,
        return_value=(discovered, [DB_ID]),
    ):
        output_dir = source._run_unstructured_pipeline()

    pipeline_patches.return_value.run.assert_called_once()
    indexer_config = pipeline_patches.call_args.kwargs["indexer_config"]
    assert indexer_config.page_ids == discovered
    assert indexer_config.database_ids == [DB_ID]
    assert indexer_config.recursive is source.config.recursive
    assert output_dir == tmp_path / "work" / "output"
    assert output_dir.is_dir()


def test_run_pipeline_keeps_partial_output_when_pipeline_fails(
    tmp_path: Path, pipeline_patches: MagicMock
) -> None:
    """A failed pipeline run must not wipe the output dir a retry can resume from."""
    source = make_notion_source(
        page_ids=[PAGE_ID], advanced={"work_dir": str(tmp_path / "work")}
    )
    pipeline_patches.return_value.run.side_effect = RuntimeError("rate limited")

    with (
        patch.object(source, "_validate_page_accessibility", autospec=True),
        pytest.raises(RuntimeError),
    ):
        source._run_unstructured_pipeline()

    output_dir = tmp_path / "work" / "output"
    (output_dir / "partial.json").write_text("[]")

    with patch.object(source, "_validate_page_accessibility", autospec=True):
        assert source._run_unstructured_pipeline() == output_dir
