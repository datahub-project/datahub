"""Shared fixtures and builders for the Notion source unit tests.

Everything that more than one Notion test module needs lives here: the config
and source factories, the ids used across modules, the builders that write
unstructured-ingest indexer records and output files into a work dir, and the
fixtures that stub the Notion client, the unstructured-ingest pipeline and the
inline chunking/embedding step.
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.notion.notion_config import NotionSourceConfig
from datahub.ingestion.source.notion.notion_source import NotionSource
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
)
from datahub.metadata.schema_classes import DocumentInfoClass

PAGE_ID = "2bffc6a6-4277-8024-97c9-d0f26faa4480"
CHILD_ID = "3cffc6a6-4277-8024-97c9-d0f26faa4481"
ORPHAN_ID = "4dffc6a6-4277-8024-97c9-d0f26faa4482"
DB_ID = "abcdef01-2345-6789-0123-456789012345"
LONG_TEXT = "This paragraph is comfortably longer than the fifty character floor."

# Bedrock embedding config with allow_local_embedding_config so the source
# never contacts a DataHub server for the embedding settings.
EMBEDDING: Dict[str, Any] = {
    "provider": "bedrock",
    "model": "cohere.embed-english-v3",
    "aws_region": "us-west-2",
    "allow_local_embedding_config": True,
}


def notion_config_dict(**overrides: Any) -> Dict[str, Any]:
    """Minimal raw config (api key + local embedding), with overrides applied."""
    return {"api_key": "secret_test_key", "embedding": dict(EMBEDDING), **overrides}


def make_notion_config(**overrides: Any) -> NotionSourceConfig:
    return NotionSourceConfig.model_validate(notion_config_dict(**overrides))


def make_notion_source(
    ctx: Optional[PipelineContext] = None, **overrides: Any
) -> NotionSource:
    return NotionSource(
        config=make_notion_config(**overrides),
        ctx=ctx or PipelineContext(run_id="test"),
    )


@pytest.fixture
def config() -> NotionSourceConfig:
    return make_notion_config(page_ids=[PAGE_ID])


@pytest.fixture
def pipeline_context() -> PipelineContext:
    return PipelineContext(run_id="test_run")


@pytest.fixture
def notion_source(
    config: NotionSourceConfig, pipeline_context: PipelineContext
) -> NotionSource:
    return NotionSource(config=config, ctx=pipeline_context)


@pytest.fixture
def notion_client() -> Iterator[MagicMock]:
    """Mocked ``notion_client.Client`` whose search succeeds by default.

    Also stubs the semantic-search capability check so test_connection stays
    off the embedding provider / DataHub server.
    """
    with (
        patch("notion_client.Client") as client_cls,
        patch.object(
            NotionSource,
            "_test_semantic_search_capability",
            autospec=True,
            return_value=CapabilityReport(capable=True),
        ),
    ):
        client = client_cls.return_value
        client.search.return_value = {"results": [{"object": "page", "id": PAGE_ID}]}
        yield client


@pytest.fixture
def pipeline_patches() -> Iterator[MagicMock]:
    """Silence the global unstructured-ingest monkeypatches and the Pipeline.

    Yields the patched ``Pipeline.from_configs`` so tests can inspect the
    configs the source built and drive ``run()`` outcomes.
    """
    pytest.importorskip("unstructured_ingest")
    with (
        patch.object(NotionSource, "_monkeypatch_database_title_extraction"),
        patch.object(NotionSource, "_monkeypatch_icon_dispatcher_unknown_types"),
        patch.object(NotionSource, "_monkeypatch_notion_types_filter_unknown_fields"),
        patch.object(
            NotionSource, "_warn_on_empty_original_synced_blocks", autospec=True
        ),
        patch(
            "unstructured_ingest.pipeline.pipeline.Pipeline.from_configs"
        ) as from_configs,
    ):
        yield from_configs


@pytest.fixture
def inline_processing_stub() -> Iterator[MagicMock]:
    """Turn the chunk+embed step into a no-op for every source built in the test.

    ``DocumentChunkingSource.process_elements_inline`` is patched on the class
    (autospecced) so document emission can be exercised without ``unstructured``
    or an embedding provider.
    """
    with patch.object(
        DocumentChunkingSource,
        "process_elements_inline",
        autospec=True,
        return_value=[],
    ) as stub:
        yield stub


def write_indexer_record(
    work_dir: Path,
    page_id: str,
    parent: Dict[str, Any],
    record_key: str = "page_id",
    **file_metadata: str,
) -> None:
    """Write the ``<work_dir>/indexer/<id>.json`` record the Notion indexer emits."""
    indexer_dir = work_dir / "indexer"
    indexer_dir.mkdir(parents=True, exist_ok=True)
    (indexer_dir / f"{page_id}.json").write_text(
        json.dumps(
            {
                "metadata": {"record_locator": {record_key: page_id}, **file_metadata},
                "additional_metadata": {
                    "parent": parent,
                    "url": f"https://www.notion.so/{page_id.replace('-', '')}",
                },
            }
        )
    )


def write_output_file(output_dir: Path, page_id: str, texts: List[str]) -> None:
    """Write a partitioned-elements output file as unstructured-ingest would.

    The first text becomes the Title element, the rest NarrativeText.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "filename": f"{page_id}.html",
        "filetype": "text/html",
        "data_source": {"record_locator": {"page_id": page_id}},
    }
    elements = [
        {
            "type": "Title" if i == 0 else "NarrativeText",
            "text": t,
            "metadata": metadata,
        }
        for i, t in enumerate(texts)
    ]
    (output_dir / f"{page_id}.json").write_text(json.dumps(elements))


def document_infos(workunits: List[MetadataWorkUnit]) -> Dict[str, DocumentInfoClass]:
    """Map emitted document URN -> DocumentInfo, failing on a duplicate emission."""
    infos: Dict[str, DocumentInfoClass] = {}
    for wu in workunits:
        aspect = wu.get_aspect_of_type(DocumentInfoClass)
        if aspect is None:
            continue
        urn = wu.get_urn()
        assert urn not in infos, f"DocumentInfo emitted twice for {urn}"
        infos[urn] = aspect
    return infos
