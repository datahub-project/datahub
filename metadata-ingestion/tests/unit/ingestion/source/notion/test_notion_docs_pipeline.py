"""End-to-end unit test for the Notion → DataHub Docs → Re-ingest pipeline.

Tests the three-step round-trip that proves rawChunks are preserved correctly:

1. Notion ingests a page with no embedding provider (or with broken credentials)
   → probe call fails at startup → emits SemanticContent with rawChunks, empty embeddings dict
2. DataHub documents source reads those rawChunks, computes embeddings
   → emits SemanticContent with BOTH embeddings and rawChunks preserved
3. Notion re-ingests the same (unchanged) page
   → content hash matches → document SKIPPED, no overwrite of step-2 state

No live services required. All external I/O is mocked.
"""

import json
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

# Skip entire module if unstructured is not installed
pytest.importorskip("unstructured")

from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
    DataHubDocumentsSourceConfig,
)
from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
    DataHubDocumentsSource,
)
from datahub.ingestion.source.notion.notion_config import NotionSourceConfig
from datahub.ingestion.source.notion.notion_source import NotionSource
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.metadata.schema_classes import SemanticContentClass

# ── Constants ────────────────────────────────────────────────────────────────

# 32-char hex Notion page ID (valid UUID hex, no dashes)
PAGE_ID = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4"

# Text must exceed FilteringConfig.min_text_length (default: 50 chars)
PAGE_TEXT = (
    "This is a long enough Notion page for testing semantic search "
    "and chunking capabilities in the DataHub ingestion pipeline."
)

# A single fake chunk (mocked output of _chunk_elements)
FAKE_CHUNK = {"text": PAGE_TEXT}
FAKE_CHUNKS = [FAKE_CHUNK]

# Fake embedding vector (dimension doesn't matter for unit tests)
FAKE_EMBEDDING: list[float] = [0.1, 0.2, 0.3] * 20  # 60-dim
FAKE_EMBEDDINGS = [FAKE_EMBEDDING]  # one per chunk

# Simulated embedding failure used in steps where credentials are absent
_NO_CREDS = RuntimeError("Simulated: embedding credentials unavailable")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _find_semantic_content(
    workunits: list,
) -> tuple[Optional[str], Optional[SemanticContentClass]]:
    """Return (entity_urn, SemanticContentClass) from the first SemanticContent MCP."""
    for wu in workunits:
        if hasattr(wu, "metadata") and hasattr(wu.metadata, "aspect"):
            if isinstance(wu.metadata.aspect, SemanticContentClass):
                return wu.metadata.entityUrn, wu.metadata.aspect
    return None, None


def _write_fake_unstructured_output(output_dir: Path) -> None:
    """Write a synthetic Unstructured pipeline JSON output for PAGE_ID."""
    elements = [
        {
            "type": "NarrativeText",
            "text": PAGE_TEXT,
            "metadata": {
                "data_source": {
                    "record_locator": {"page_id": PAGE_ID},
                },
                # filename drives the DocumentEntityBuilder URN:
                # id_pattern="{source_type}-{basename}" → "notion-{PAGE_ID}"
                "filename": f"{PAGE_ID}.html",
                "filetype": "text/html",
            },
        }
    ]
    output_file = output_dir / f"{PAGE_ID}.json"
    output_file.write_text(json.dumps(elements))


def _make_notion_config(checkpoint_file: Path) -> NotionSourceConfig:
    """Build a minimal NotionSourceConfig with no embedding provider.

    Uses a file-based state_provider so the test runs without a live DataHub connection.
    The StatefulIngestionConfig validator auto-sets state_provider="datahub" when
    enabled=True and no provider is specified, so we must be explicit here.
    """
    return NotionSourceConfig(
        api_key=SecretStr("secret_fake_key"),
        # page_ids filter: only keep docs whose page_id starts with the same prefix
        page_ids=[PAGE_ID],
        # stateful ingestion enabled so _should_process_document checks content hash;
        # file provider so no live DataHub connection is required in tests.
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "file",
                "config": {"filename": str(checkpoint_file)},
            },
        },
    )


def _make_notion_source(
    config: NotionSourceConfig,
    run_id: str,
    content_state_file: Path,
) -> NotionSource:
    """Construct a NotionSource, then redirect its content-hash state file to tmp_path.

    The NotionSource uses TWO separate state stores:
      1. StatefulIngestionBase checkpointing (stale entity removal) → controlled via
         the state_provider in the config (file-based in tests).
      2. Content-based change detection (document_state / state_file_path) → redirected
         here to an isolated tmp file to avoid touching ~/.datahub/ during tests.

    IMPORTANT: call this inside a patch.object(DocumentChunkingSource,
    "_generate_embeddings", ...) context so the startup probe call is controlled.
    """
    ctx = PipelineContext(run_id=run_id, pipeline_name="test_pipeline")
    source = NotionSource(config=config, ctx=ctx)

    # Redirect the content-hash state file from the default ~/.datahub/notion_state/
    # path to an isolated tmp file.
    source.state_file_path = content_state_file
    source.document_state = {}  # reset any state loaded from the default path
    return source


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestNotionDocsPipeline:
    """Round-trip: Notion (no embedding) → DataHub Docs (embed) → Notion (skip)."""

    def test_step1_rawchunks_emitted_without_embedding_provider(
        self, tmp_path: Path
    ) -> None:
        """Notion with broken/absent credentials emits rawChunks, no embeddings.

        The startup embedding probe fails (simulating missing credentials), which
        sets embedding_model=None. The source then falls back to raw-chunks mode
        and emits SemanticContent with rawChunks populated and no embeddings.
        """
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _write_fake_unstructured_output(output_dir)

        config = _make_notion_config(tmp_path / "checkpoint.json")

        # Wrap source construction inside the mock so the startup probe fails fast
        # (no real network call to AWS/OpenAI).
        with (
            patch.object(
                NotionSource,
                "_run_unstructured_pipeline",
                return_value=output_dir,
            ),
            patch.object(
                DocumentChunkingSource,
                "_chunk_elements",
                return_value=FAKE_CHUNKS,
            ),
            patch.object(
                DocumentChunkingSource,
                "_generate_embeddings",
                side_effect=_NO_CREDS,
            ),
        ):
            source = _make_notion_source(config, "run1", tmp_path / "state.json")
            workunits = list(source.get_workunits_internal())

        doc_urn, sc = _find_semantic_content(workunits)
        assert doc_urn is not None, "No SemanticContent work unit emitted"
        assert sc is not None
        assert sc.rawChunks is not None, (
            "rawChunks must be populated when embedding is unavailable"
        )
        assert not sc.embeddings, (
            "embeddings must be empty when embedding is unavailable"
        )

    def test_step2_embeddings_and_rawchunks_preserved(self, tmp_path: Path) -> None:
        """DataHub docs source adds embeddings while preserving rawChunks."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _write_fake_unstructured_output(output_dir)

        # ── Step 1: produce sc1 (rawChunks, no embeddings) ──────────────────
        config1 = _make_notion_config(tmp_path / "checkpoint1.json")

        with (
            patch.object(
                NotionSource, "_run_unstructured_pipeline", return_value=output_dir
            ),
            patch.object(
                DocumentChunkingSource, "_chunk_elements", return_value=FAKE_CHUNKS
            ),
            patch.object(
                DocumentChunkingSource, "_generate_embeddings", side_effect=_NO_CREDS
            ),
        ):
            source1 = _make_notion_source(config1, "run1", tmp_path / "state1.json")
            workunits1 = list(source1.get_workunits_internal())

        doc_urn, sc1 = _find_semantic_content(workunits1)
        assert sc1 is not None, "Step 1 must emit SemanticContent"
        assert sc1.rawChunks is not None, "Step 1 must set rawChunks"

        # ── Step 2: DataHub docs source reads rawChunks and adds embeddings ──
        from unittest.mock import MagicMock

        mock_graph = MagicMock()
        # get_aspect returns the SemanticContent with rawChunks from step 1
        mock_graph.get_aspect.return_value = sc1

        docs_config = DataHubDocumentsSourceConfig(
            embedding={
                "provider": "openai",
                "model": "text-embedding-3-small",
                # fake key prevents the "api key required" ValueError in __init__
                "api_key": "fake-openai-key",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
            incremental={"enabled": False},
        )

        ctx2 = PipelineContext(run_id="run2", pipeline_name="test_pipeline")
        ctx2.graph = mock_graph

        # Wrap source construction AND execution so the startup probe uses the mock,
        # keeping self.embedding_model set throughout.
        with (
            patch.object(
                DocumentChunkingSource,
                "_generate_embeddings",
                return_value=FAKE_EMBEDDINGS,
            ),
            patch.object(
                DataHubDocumentsSource,
                "_fetch_documents_graphql",
                return_value=[{"urn": doc_urn, "text": PAGE_TEXT}],
            ),
        ):
            source2 = DataHubDocumentsSource(ctx=ctx2, config=docs_config)
            workunits2 = list(source2.get_workunits_internal())

        sc2_urn, sc2 = _find_semantic_content(workunits2)
        assert sc2_urn is not None, "Step 2 must emit SemanticContent"
        assert sc2 is not None
        assert sc2.embeddings, "Step 2 must populate embeddings"
        assert sc2.rawChunks is not None, (
            "rawChunks MUST be preserved in step 2 — "
            "this is the key regression guard for the rawChunks preservation fix"
        )

    def test_step3_unchanged_document_skipped(self, tmp_path: Path) -> None:
        """Notion re-ingestion of an unchanged page skips processing.

        When the content hash in state matches the current content hash,
        _should_process_document returns False and no SemanticContent is emitted.
        This prevents the no-embedding Notion source from overwriting
        the embeddings that step 2 added.
        """
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _write_fake_unstructured_output(output_dir)

        config = _make_notion_config(tmp_path / "checkpoint.json")

        with (
            patch.object(
                NotionSource, "_run_unstructured_pipeline", return_value=output_dir
            ),
            patch.object(
                DocumentChunkingSource, "_chunk_elements", return_value=FAKE_CHUNKS
            ),
            patch.object(
                DocumentChunkingSource, "_generate_embeddings", side_effect=_NO_CREDS
            ),
        ):
            source = _make_notion_source(config, "run3", tmp_path / "state3.json")

            # The early-computed URN used by _should_process_document is
            # f"urn:li:document:notion.{page_id}". Inject the expected hash under that
            # key to simulate a previous successful run.
            state_key_urn = f"urn:li:document:notion.{PAGE_ID}"
            expected_hash = source._calculate_document_hash(PAGE_TEXT, PAGE_ID)
            source.document_state = {
                state_key_urn: {
                    "content_hash": expected_hash,
                    "page_id": PAGE_ID,
                }
            }

            workunits3 = list(source.get_workunits_internal())

        _, sc3 = _find_semantic_content(workunits3)
        assert sc3 is None, (
            "Notion re-ingestion should skip the unchanged document. "
            "If not skipped, it would emit rawChunks-only SemanticContent and overwrite "
            "the embeddings that the DataHub docs source computed in step 2."
        )

    def test_full_three_step_round_trip(self, tmp_path: Path) -> None:
        """Full pipeline: Notion (no embed) → DataHub Docs (embed) → Notion (skip).

        This is the integration-style variant that runs all three steps sequentially
        and validates each stage's output feeds correctly into the next.
        """
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _write_fake_unstructured_output(output_dir)

        # ── Step 1: Notion ingests page, embedding probe fails → rawChunks ────
        config1 = _make_notion_config(tmp_path / "checkpoint1.json")

        with (
            patch.object(
                NotionSource, "_run_unstructured_pipeline", return_value=output_dir
            ),
            patch.object(
                DocumentChunkingSource, "_chunk_elements", return_value=FAKE_CHUNKS
            ),
            patch.object(
                DocumentChunkingSource, "_generate_embeddings", side_effect=_NO_CREDS
            ),
        ):
            source1 = _make_notion_source(config1, "run1", tmp_path / "state1.json")
            workunits1 = list(source1.get_workunits_internal())

        doc_urn, sc1 = _find_semantic_content(workunits1)

        assert doc_urn is not None, "Step 1: no SemanticContent emitted"
        assert sc1 is not None
        assert sc1.rawChunks is not None, "Step 1: rawChunks must be set"
        assert not sc1.embeddings, "Step 1: embeddings must be empty"

        # ── Step 2: DataHub docs source embeds the rawChunks ─────────────────
        from unittest.mock import MagicMock

        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = sc1

        docs_config = DataHubDocumentsSourceConfig(
            embedding={
                "provider": "openai",
                "model": "text-embedding-3-small",
                "api_key": "fake-openai-key",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
            incremental={"enabled": False},
        )

        ctx2 = PipelineContext(run_id="run2", pipeline_name="test_pipeline")
        ctx2.graph = mock_graph

        with (
            patch.object(
                DocumentChunkingSource,
                "_generate_embeddings",
                return_value=FAKE_EMBEDDINGS,
            ),
            patch.object(
                DataHubDocumentsSource,
                "_fetch_documents_graphql",
                return_value=[{"urn": doc_urn, "text": PAGE_TEXT}],
            ),
        ):
            source2 = DataHubDocumentsSource(ctx=ctx2, config=docs_config)
            workunits2 = list(source2.get_workunits_internal())

        _, sc2 = _find_semantic_content(workunits2)

        assert sc2 is not None, "Step 2: no SemanticContent emitted"
        assert sc2.embeddings, "Step 2: embeddings must be populated"
        assert sc2.rawChunks is not None, (
            "Step 2: rawChunks MUST be preserved alongside embeddings"
        )

        # ── Step 3: Notion re-ingests same page → skip ────────────────────────
        config3 = _make_notion_config(tmp_path / "checkpoint3.json")

        with (
            patch.object(
                NotionSource, "_run_unstructured_pipeline", return_value=output_dir
            ),
            patch.object(
                DocumentChunkingSource, "_chunk_elements", return_value=FAKE_CHUNKS
            ),
            patch.object(
                DocumentChunkingSource, "_generate_embeddings", side_effect=_NO_CREDS
            ),
        ):
            source3 = _make_notion_source(config3, "run3", tmp_path / "state3.json")

            state_key_urn = f"urn:li:document:notion.{PAGE_ID}"
            expected_hash = source3._calculate_document_hash(PAGE_TEXT, PAGE_ID)
            source3.document_state = {
                state_key_urn: {
                    "content_hash": expected_hash,
                    "page_id": PAGE_ID,
                }
            }

            workunits3 = list(source3.get_workunits_internal())

        _, sc3 = _find_semantic_content(workunits3)

        assert sc3 is None, (
            "Step 3: Notion re-ingestion must skip the unchanged document. "
            "If not skipped, rawChunks-only SemanticContent would overwrite the "
            "embeddings computed in step 2, breaking semantic search."
        )

    def test_datahub_docs_source_raises_on_embedding_failure(
        self, tmp_path: Path
    ) -> None:
        """DataHub documents source must raise (not silently degrade) when embedding fails.

        Unlike Notion/Confluence (which fall back to raw-chunks mode), the DataHub
        documents source exists solely to embed rawChunks. If its embedding provider
        is broken, it must fail loudly so the operator knows embeddings were not produced.
        """
        from unittest.mock import MagicMock

        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = None

        docs_config = DataHubDocumentsSourceConfig(
            embedding={
                "provider": "openai",
                "model": "text-embedding-3-small",
                "api_key": "fake-openai-key",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
            incremental={"enabled": False},
        )

        ctx = PipelineContext(run_id="run-fail", pipeline_name="test_pipeline")
        ctx.graph = mock_graph

        # Probe fails → DataHub docs source should raise, not silently degrade.
        with (
            patch.object(
                DocumentChunkingSource,
                "_generate_embeddings",
                side_effect=_NO_CREDS,
            ),
            pytest.raises(RuntimeError),
        ):
            DataHubDocumentsSource(ctx=ctx, config=docs_config)
