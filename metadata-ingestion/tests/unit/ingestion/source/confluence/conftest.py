"""Shared fixtures and builders for the Confluence source unit tests.

The Confluence REST client (``atlassian.Confluence``) is an external SDK, so it
is replaced by a plain ``MagicMock`` (or by the scenario-driven fake in
``confluence_test_fixtures``); everything DataHub-internal stays real except the
inline chunking/embedding step, which is autospecced so no embedding work runs.
"""

from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.confluence.confluence_config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.confluence_source import ConfluenceSource
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
)
from datahub.metadata.schema_classes import DocumentInfoClass
from tests.unit.conftest import aspects_by_urn, aspects_of_type

CONFLUENCE_CLIENT = "datahub.ingestion.source.confluence.confluence_source.Confluence"
CLOUD_URL = "https://test.atlassian.net/wiki"


def cloud_config_dict(**overrides: Any) -> Dict[str, Any]:
    """Minimal raw Confluence Cloud config (username + API token), with overrides."""
    return {
        "url": CLOUD_URL,
        "username": "test@example.com",
        "api_token": "test-token-123",
        **overrides,
    }


def make_config(**overrides: Any) -> ConfluenceSourceConfig:
    return ConfluenceSourceConfig.model_validate(
        cloud_config_dict(cloud=True, **overrides)
    )


def make_source(
    config: Optional[ConfluenceSourceConfig] = None,
    client: Optional[Any] = None,
    ctx: Optional[PipelineContext] = None,
) -> ConfluenceSource:
    """A source whose Confluence client is ``client`` (a bare MagicMock by
    default), with the inline chunking/embedding step stubbed to emit nothing."""
    with patch(CONFLUENCE_CLIENT, return_value=client or MagicMock()):
        source = ConfluenceSource(
            config or make_config(), ctx or PipelineContext(run_id="test-run")
        )
    stub_chunking(source)
    return source


def stub_chunking(source: ConfluenceSource, **mock_kwargs: Any) -> MagicMock:
    """Replace ``process_elements_inline`` with an autospecced mock. Without
    ``mock_kwargs`` it yields no workunits."""
    if not mock_kwargs:
        mock_kwargs = {"side_effect": lambda *_args, **_kwargs: iter([])}
    # Spec off the class so re-stubbing a source never specs a mock off a mock.
    real = DocumentChunkingSource.process_elements_inline.__get__(
        source.chunking_source
    )
    stub = create_autospec(real, **mock_kwargs)
    # setattr rather than assignment: mypy rejects assigning to a method.
    setattr(source.chunking_source, "process_elements_inline", stub)  # noqa: B010
    return stub


def make_page(
    page_id: str,
    space_key: str = "TEAM",
    ancestors: Optional[List[Dict[str, Any]]] = None,
    body: str = "<p>Enough content to pass every filter in the pipeline.</p>",
    **extra: Any,
) -> Dict[str, Any]:
    """A Confluence REST page payload (``expand=ancestors,version,space,body``)."""
    page: Dict[str, Any] = {
        "id": page_id,
        "title": f"Page {page_id}",
        "body": {"storage": {"value": body}},
        "ancestors": ancestors or [],
        "space": {"key": space_key, "name": f"{space_key} Space"},
        "_links": {"webui": f"/spaces/{space_key}/pages/{page_id}/Page"},
    }
    page.update(extra)
    return page


def serve_space(client: MagicMock, pages: List[Dict[str, Any]]) -> None:
    """Make ``client`` discover one space, TEAM, holding ``pages`` on one page."""
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
    client.get_all_pages_from_space.return_value = {"results": pages, "_links": {}}


def document_infos(
    workunits: Iterable[MetadataWorkUnit],
) -> Dict[str, DocumentInfoClass]:
    """The DocumentInfo aspect of every emitted document, keyed by urn."""
    infos: Dict[str, DocumentInfoClass] = {}
    for urn, aspects in aspects_by_urn(workunits).items():
        for info in aspects_of_type(aspects, DocumentInfoClass):
            infos[urn] = info
    return infos


def run_test_connection(client: MagicMock, **overrides: Any) -> TestConnectionReport:
    """``ConfluenceSource.test_connection`` against ``client``, with the semantic
    search probe (which would contact an embedding backend) reported capable."""
    with (
        patch(CONFLUENCE_CLIENT, return_value=client),
        patch.object(
            ConfluenceSource,
            "_test_semantic_search_capability",
            autospec=True,
            return_value=CapabilityReport(capable=True),
        ),
    ):
        return ConfluenceSource.test_connection(cloud_config_dict(**overrides))


@pytest.fixture
def pipeline_context() -> PipelineContext:
    return PipelineContext(run_id="test-run")


@pytest.fixture
def base_config_dict() -> Dict[str, Any]:
    """Raw Cloud config dict, for tests that add filters before validating."""
    return cloud_config_dict(cloud=True)


@pytest.fixture
def cloud_config() -> ConfluenceSourceConfig:
    return make_config()


@pytest.fixture
def datacenter_config() -> ConfluenceSourceConfig:
    return ConfluenceSourceConfig.model_validate(
        {
            "url": "https://confluence.company.com",
            "personal_access_token": "test-pat-123",
            "cloud": False,
        }
    )
