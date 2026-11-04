"""Tests for how NotionSource turns unstructured-ingest output into documents.

Exercises the indexer-metadata loader, the two-pass output processing
(filtering, parent exemption, hierarchy emission, malformed files), Notion
timestamp parsing, the content-hash state file and work-dir cleanup. Only the
inline chunking/embedding step is stubbed.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.notion.notion_source import NotionSource
from datahub.metadata.schema_classes import BrowsePathsV2Class
from tests.unit.ingestion.source.notion.conftest import (
    CHILD_ID,
    DB_ID,
    LONG_TEXT,
    ORPHAN_ID,
    PAGE_ID,
    document_infos,
    make_notion_source,
    write_indexer_record,
    write_output_file,
)

pytestmark = pytest.mark.usefixtures("inline_processing_stub")

ROOT_ID = PAGE_ID


def _make_source(
    work_dir: Path, ctx: Optional[PipelineContext] = None, **overrides: Any
) -> NotionSource:
    """Source scoped to ROOT_ID whose work dir is under the test's tmp_path."""
    return make_notion_source(
        ctx=ctx,
        page_ids=[ROOT_ID],
        advanced={"work_dir": str(work_dir), **overrides.pop("advanced", {})},
        **overrides,
    )


def test_load_parent_metadata_merges_timestamps_and_skips_corrupt_files(
    tmp_path: Path,
) -> None:
    """Indexer records are keyed by page or database id, carry the Notion
    timestamps when present, and a corrupt record does not abort the load."""
    source = _make_source(tmp_path)
    assert source._load_notion_parent_metadata(tmp_path) == {}

    write_indexer_record(
        tmp_path,
        CHILD_ID,
        {"type": "page_id", "page_id": ROOT_ID},
        date_created="2025-12-04T18:35:00.000Z",
        date_modified="2026-01-17T08:19:00.000Z",
    )
    write_indexer_record(
        tmp_path,
        DB_ID,
        {"type": "workspace", "workspace": True},
        record_key="database_id",
    )
    (tmp_path / "indexer" / "corrupt.json").write_text("{not json")

    loaded = source._load_notion_parent_metadata(tmp_path)

    assert set(loaded) == {CHILD_ID, DB_ID}
    assert loaded[CHILD_ID]["parent"] == {"type": "page_id", "page_id": ROOT_ID}
    assert loaded[CHILD_ID]["date_created"] == "2025-12-04T18:35:00.000Z"
    assert loaded[CHILD_ID]["date_modified"] == "2026-01-17T08:19:00.000Z"
    assert "date_modified" not in loaded[DB_ID]


def test_process_output_exempts_short_parents_and_links_children(
    tmp_path: Path,
) -> None:
    """A parent page below min_text_length is still ingested so that its child's
    parent reference and browse path resolve; a short leaf page is filtered."""
    source = _make_source(tmp_path, filtering={"min_text_length": 50})
    write_indexer_record(tmp_path, ROOT_ID, {"type": "workspace", "workspace": True})
    write_indexer_record(tmp_path, CHILD_ID, {"type": "page_id", "page_id": ROOT_ID})
    write_indexer_record(tmp_path, ORPHAN_ID, {"type": "page_id", "page_id": ROOT_ID})
    output_dir = tmp_path / "output"
    write_output_file(output_dir, ROOT_ID, ["Engineering"])
    write_output_file(output_dir, CHILD_ID, ["Backend", LONG_TEXT])
    write_output_file(output_dir, ORPHAN_ID, ["Stub"])

    workunits = list(source._process_unstructured_output(output_dir))

    infos = document_infos(workunits)
    root_urn = source._build_notion_document_urn(ROOT_ID)
    child_urn = source._build_notion_document_urn(CHILD_ID)
    assert set(infos) == {root_urn, child_urn}
    assert infos[root_urn].parentDocument is None
    child = infos[child_urn]
    assert child.parentDocument is not None
    assert child.parentDocument.document == root_urn
    assert child.source is not None
    assert child.source.externalUrl == (
        f"https://www.notion.so/{CHILD_ID.replace('-', '')}"
    )

    browse_paths: List[BrowsePathsV2Class] = []
    for wu in workunits:
        browse_path = wu.get_aspect_of_type(BrowsePathsV2Class)
        if browse_path is not None and wu.get_urn() == child_urn:
            browse_paths.append(browse_path)
    assert len(browse_paths) == 1
    assert [(e.id, e.urn) for e in browse_paths[0].path] == [("Engineering", root_urn)]

    assert source.report.num_files_scanned == 3
    assert source.report.num_files_skipped == 1
    assert source.report.num_documents_created == 2


def test_process_output_drops_parent_reference_to_filtered_parent(
    tmp_path: Path,
) -> None:
    """Parent references must only point at documents that are actually emitted."""
    source = _make_source(tmp_path)
    write_indexer_record(tmp_path, ROOT_ID, {"type": "workspace", "workspace": True})
    write_indexer_record(tmp_path, CHILD_ID, {"type": "page_id", "page_id": ROOT_ID})
    output_dir = tmp_path / "output"
    write_output_file(output_dir, ROOT_ID, [])  # image-only page: no text
    write_output_file(output_dir, CHILD_ID, ["Backend", LONG_TEXT])

    workunits = list(source._process_unstructured_output(output_dir))

    infos = document_infos(workunits)
    child_urn = source._build_notion_document_urn(CHILD_ID)
    assert set(infos) == {child_urn}
    assert infos[child_urn].parentDocument is None
    assert not any(
        wu.get_aspect_of_type(BrowsePathsV2Class) is not None for wu in workunits
    )


def test_process_output_malformed_file_honours_continue_on_failure(
    tmp_path: Path,
) -> None:
    """A malformed output file is counted as failed and skipped by default, but
    aborts the run when continue_on_failure is off."""
    output_dir = tmp_path / "output"
    write_output_file(output_dir, ROOT_ID, ["Engineering", LONG_TEXT])
    (output_dir / "bad.json").write_text(json.dumps({"elements": []}))

    lenient = _make_source(tmp_path)
    workunits = list(lenient._process_unstructured_output(output_dir))
    assert len(document_infos(workunits)) == 1
    assert lenient.report.num_files_failed == 1

    strict = _make_source(tmp_path, advanced={"continue_on_failure": False})
    with pytest.raises(ValueError):
        list(strict._process_unstructured_output(output_dir))


def test_notion_timestamps_parsed_from_indexer_metadata(tmp_path: Path) -> None:
    """Timestamps come from the indexer record for the page; an unparseable or
    missing value yields None rather than an error."""
    source = _make_source(tmp_path)
    source.notion_parent_metadata = {
        CHILD_ID: {
            "date_created": "2025-12-04T18:35:00.000Z",
            "date_modified": "not-a-timestamp",
        }
    }
    metadata = {"data_source": {"record_locator": {"page_id": CHILD_ID}}}

    assert source._extract_created_time(metadata) == datetime(
        2025, 12, 4, 18, 35, tzinfo=timezone.utc
    )
    assert source._extract_last_modified_time(metadata) is None
    assert (
        source._extract_created_time(
            {"data_source": {"record_locator": {"page_id": "unknown"}}}
        )
        is None
    )


def test_should_skip_file_enforces_file_size_bounds(tmp_path: Path) -> None:
    source = _make_source(
        tmp_path, filtering={"min_file_size": 100, "max_file_size": 1000}
    )

    def data(file_size: int) -> Dict[str, Any]:
        return {
            "elements": [{"text": LONG_TEXT}],
            "metadata": {"filename": "page.html", "file_size": file_size},
        }

    assert source._should_skip_file(data(50))
    assert source._should_skip_file(data(5000))
    assert not source._should_skip_file(data(500))
    assert source.report.num_files_skipped == 2


def _stateful_source(tmp_path: Path) -> NotionSource:
    return _make_source(
        tmp_path / "work",
        ctx=PipelineContext(run_id="test", pipeline_name="notion_pipeline"),
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "file",
                "config": {"filename": str(tmp_path / "checkpoint.json")},
            },
        },
    )


def _run_once(tmp_path: Path) -> List[MetadataWorkUnit]:
    """Run a fresh stateful source over one page, bypassing the Notion pipeline."""
    source = _stateful_source(tmp_path)
    output_dir = tmp_path / "work" / "output"
    write_output_file(output_dir, ROOT_ID, ["Engineering", LONG_TEXT])
    with patch.object(
        source, "_run_unstructured_pipeline", autospec=True, return_value=output_dir
    ):
        return list(source.get_workunits_internal())


def test_state_file_written_after_run_and_loaded_by_next_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The content-hash state lands in ~/.datahub/notion_state/<pipeline>.json
    keyed by emitted URN, and a new source for the same pipeline loads it."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")

    workunits = _run_once(tmp_path)

    emitted_urns = set(document_infos(workunits))
    assert len(emitted_urns) == 1
    state_file = (
        tmp_path / "home" / ".datahub" / "notion_state" / "notion_pipeline.json"
    )
    saved = json.loads(state_file.read_text())
    assert set(saved) == emitted_urns

    next_run = _stateful_source(tmp_path)
    assert set(next_run.document_state) == emitted_urns


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Bug: _create_document_entity checks state under "
        "'urn:li:document:notion.<page_id>' but records it under the emitted URN "
        "'urn:li:document:notion-<page_id>', so unchanged documents are never "
        "skipped on later runs."
    ),
)
def test_unchanged_document_skipped_on_second_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Change detection must skip a page whose content and config are unchanged."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")

    first_run = _run_once(tmp_path)
    second_run = _run_once(tmp_path)

    assert len(document_infos(first_run)) == 1
    assert document_infos(second_run) == {}


def test_close_removes_work_dir_unless_outputs_preserved(tmp_path: Path) -> None:
    work_dir = tmp_path / "work"
    (work_dir / "output").mkdir(parents=True)

    _make_source(work_dir).close()
    assert not work_dir.exists()

    (work_dir / "output").mkdir(parents=True)
    _make_source(work_dir, advanced={"preserve_outputs": True}).close()
    assert (work_dir / "output").is_dir()


def test_database_title_monkeypatch_prepends_title_heading() -> None:
    """The patched extract_database_html puts the database title as an h1 ahead
    of the table, and leaves untitled databases alone."""
    pytest.importorskip("unstructured_ingest")
    from htmlBuilder.tags import Table
    from unstructured_ingest.processes.connectors.notion import helpers
    from unstructured_ingest.processes.connectors.notion.helpers import (
        HtmlExtractionResponse,
    )

    client = MagicMock()
    original = MagicMock(
        side_effect=lambda *_: HtmlExtractionResponse(html=Table([], []))
    )

    # patch.object restores the unpatched helper after the test, so the global
    # monkeypatch does not leak into other tests.
    with patch.object(helpers, "extract_database_html", original):
        NotionSource._monkeypatch_database_title_extraction()

        client.databases.retrieve.return_value = {
            "title": [{"plain_text": "Employee Directory"}]
        }
        titled = helpers.extract_database_html(client, DB_ID, logging.getLogger())

        client.databases.retrieve.return_value = {"title": []}
        untitled = helpers.extract_database_html(client, DB_ID, logging.getLogger())

    assert titled.html is not None and untitled.html is not None
    rendered = titled.html.render()
    assert "<h1>Employee Directory</h1>" in rendered
    assert rendered.index("<h1>") < rendered.index("<table")
    assert "<h1>" not in untitled.html.render()
