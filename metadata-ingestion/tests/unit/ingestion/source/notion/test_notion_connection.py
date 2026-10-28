"""Tests for NotionSource behaviour at the Notion API boundary.

Covers test_connection, page/database access validation, auto-discovery
pagination and how pipeline failures surface. The ``notion_client.Client`` and
unstructured-ingest ``Pipeline`` are the only things mocked.
"""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.source.notion.notion_config import NotionSourceConfig
from datahub.ingestion.source.notion.notion_source import NotionSource

PAGE_ID = "2bffc6a6-4277-8024-97c9-d0f26faa4480"
DB_ID = "abcdef01-2345-6789-0123-456789012345"
EMBEDDING: Dict[str, Any] = {
    "provider": "bedrock",
    "model": "cohere.embed-english-v3",
    "aws_region": "us-west-2",
    "allow_local_embedding_config": True,
}


def _config_dict(**overrides: Any) -> Dict[str, Any]:
    return {"api_key": "secret_test_key", "embedding": EMBEDDING, **overrides}


def _make_source(**overrides: Any) -> NotionSource:
    config = NotionSourceConfig.parse_obj(_config_dict(**overrides))
    return NotionSource(config=config, ctx=PipelineContext(run_id="test"))


@pytest.fixture
def notion_client() -> Any:
    """Mocked ``notion_client.Client`` whose search succeeds by default.

    Also stubs the semantic-search capability check so test_connection stays
    off the embedding provider / DataHub server.
    """
    with (
        patch("notion_client.Client") as client_cls,
        patch.object(
            NotionSource,
            "_test_semantic_search_capability",
            return_value=CapabilityReport(capable=True),
        ),
    ):
        client = client_cls.return_value
        client.search.return_value = {"results": [{"object": "page", "id": PAGE_ID}]}
        yield client


def test_connection_unparseable_config_is_internal_failure() -> None:
    report = NotionSource.test_connection({"api_key": "", "page_ids": ["nope"]})

    assert report.internal_failure
    assert report.basic_connectivity is None


def test_connection_invalid_api_key_fails_basic_connectivity(
    notion_client: MagicMock,
) -> None:
    notion_client.search.side_effect = Exception("401 unauthorized")

    report = NotionSource.test_connection(_config_dict())

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert not report.internal_failure
    assert report.capability_report is None


def test_connection_flags_inaccessible_configured_ids(
    notion_client: MagicMock,
) -> None:
    notion_client.pages.retrieve.side_effect = Exception("404 object_not_found")

    report = NotionSource.test_connection(
        _config_dict(page_ids=[PAGE_ID], database_ids=[DB_ID])
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
    report = NotionSource.test_connection(
        _config_dict(page_ids=[PAGE_ID], database_ids=[DB_ID])
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
    report = NotionSource.test_connection(_config_dict())

    notion_client.pages.retrieve.assert_not_called()
    assert report.capability_report is not None
    assert report.capability_report["Auto-Discovery"].capable
    assert "Page/Database Access" not in report.capability_report


def test_semantic_search_capability_reports_config_error_instead_of_raising() -> None:
    config = NotionSourceConfig.parse_obj(_config_dict())

    with patch(
        "datahub.ingestion.source.unstructured.chunking_source.DocumentChunkingSource.resolve_embedding_config",
        side_effect=ValueError("semantic search is not enabled on the server"),
    ):
        report = NotionSource._test_semantic_search_capability(config)

    assert not report.capable
    assert report.failure_reason
    assert report.mitigation_message


def test_validate_page_accessibility_lists_every_inaccessible_id(
    notion_client: MagicMock,
) -> None:
    source = _make_source(page_ids=[PAGE_ID], database_ids=[DB_ID])
    notion_client.pages.retrieve.side_effect = Exception("404")
    notion_client.databases.retrieve.side_effect = Exception("403")

    with pytest.raises(ValueError) as exc_info:
        source._validate_page_accessibility([PAGE_ID], [DB_ID])

    assert PAGE_ID in str(exc_info.value)
    assert DB_ID in str(exc_info.value)

    # All accessible: no error.
    notion_client.pages.retrieve.side_effect = None
    notion_client.databases.retrieve.side_effect = None
    source._validate_page_accessibility([PAGE_ID], [DB_ID])


def test_discover_all_accessible_pages_paginates_and_splits_by_type(
    notion_client: MagicMock,
) -> None:
    source = _make_source()
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
    source = _make_source()
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
    source = _make_source(page_ids=[PAGE_ID])

    with patch.object(
        source, "_run_unstructured_pipeline", side_effect=RuntimeError("boom")
    ):
        assert list(source.get_workunits_internal()) == []

    assert len(source.report.failures) == 1

    strict_source = _make_source(page_ids=[PAGE_ID], advanced={"raise_on_error": True})
    with (
        patch.object(
            strict_source,
            "_run_unstructured_pipeline",
            side_effect=RuntimeError("boom"),
        ),
        pytest.raises(RuntimeError),
    ):
        list(strict_source.get_workunits_internal())


@pytest.fixture
def pipeline_patches() -> Any:
    """Silence the global unstructured-ingest monkeypatches and the Pipeline."""
    pytest.importorskip("unstructured_ingest")
    with (
        patch.object(NotionSource, "_monkeypatch_database_title_extraction"),
        patch.object(NotionSource, "_monkeypatch_icon_dispatcher_unknown_types"),
        patch.object(NotionSource, "_monkeypatch_notion_types_filter_unknown_fields"),
        patch.object(NotionSource, "_warn_on_empty_original_synced_blocks"),
        patch(
            "unstructured_ingest.pipeline.pipeline.Pipeline.from_configs"
        ) as from_configs,
    ):
        yield from_configs


def test_run_pipeline_indexes_discovered_ids_when_none_configured(
    tmp_path: Path, pipeline_patches: MagicMock
) -> None:
    source = _make_source(advanced={"work_dir": str(tmp_path / "work")})
    discovered: List[str] = ["discovered-page"]

    with patch.object(
        source, "_discover_all_accessible_pages", return_value=(discovered, [DB_ID])
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
    source = _make_source(
        page_ids=[PAGE_ID], advanced={"work_dir": str(tmp_path / "work")}
    )
    pipeline_patches.return_value.run.side_effect = RuntimeError("rate limited")

    with (
        patch.object(source, "_validate_page_accessibility"),
        pytest.raises(RuntimeError),
    ):
        source._run_unstructured_pipeline()

    output_dir = tmp_path / "work" / "output"
    (output_dir / "partial.json").write_text("[]")

    with patch.object(source, "_validate_page_accessibility"):
        assert source._run_unstructured_pipeline() == output_dir
