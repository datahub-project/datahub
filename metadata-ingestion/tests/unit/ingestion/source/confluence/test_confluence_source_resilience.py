"""Failure handling and edge cases of the Confluence crawl, document emission and
test_connection: what happens when the Confluence API returns nothing, raises,
paginates, or hands back pages missing the fields ingestion relies on."""

from typing import Any, Dict, Iterable, List, Optional, Set, cast
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.source.confluence.confluence_config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.confluence_source import ConfluenceSource
from datahub.ingestion.source.unstructured.chunking_source import (
    SkipMarkerReadError,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentSourceTypeClass,
)

CONFLUENCE_CLIENT = "datahub.ingestion.source.confluence.confluence_source.Confluence"
EXPAND = "ancestors,version,history,space,body.storage"


def _config(**overrides: Any) -> ConfluenceSourceConfig:
    base: Dict[str, Any] = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "cloud": True,
    }
    base.update(overrides)
    return ConfluenceSourceConfig.model_validate(base)


def _source(config: ConfluenceSourceConfig) -> ConfluenceSource:
    """A source whose Confluence client is a bare MagicMock; the chunking step is
    stubbed out so no embedding work happens."""
    with patch(CONFLUENCE_CLIENT) as mock_confluence:
        mock_confluence.return_value = MagicMock()
        source = ConfluenceSource(config, PipelineContext(run_id="test-run"))
    source.chunking_source.process_elements_inline = MagicMock(  # type: ignore[method-assign]
        return_value=iter([])
    )
    return source


def _client(source: ConfluenceSource) -> MagicMock:
    return cast(MagicMock, source.confluence_client)


def _page(
    page_id: str,
    space_key: str = "TEAM",
    ancestors: Optional[List[Dict[str, Any]]] = None,
    body: str = "<p>Enough content to pass every filter in the pipeline.</p>",
    **extra: Any,
) -> Dict[str, Any]:
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


def _document_infos(
    workunits: Iterable[Any],
) -> Dict[str, DocumentInfoClass]:
    infos: Dict[str, DocumentInfoClass] = {}
    for wu in workunits:
        md = wu.metadata
        if isinstance(md, MetadataChangeProposalWrapper) and isinstance(
            md.aspect, DocumentInfoClass
        ):
            infos[str(md.entityUrn)] = md.aspect
    return infos


# --------------------------------------------------------------------------- #
# Space discovery
# --------------------------------------------------------------------------- #


def test_space_discovery_with_no_results_yields_no_spaces() -> None:
    source = _source(_config())
    _client(source).get_all_spaces.return_value = {}

    assert source._get_spaces() == []
    assert source.report.failures.total_elements == 0


def test_space_discovery_api_error_is_reported_not_raised() -> None:
    source = _source(_config())
    _client(source).get_all_spaces.side_effect = Exception("503 unavailable")

    assert source._get_spaces() == []
    assert source.report.failures.total_elements == 1


def test_no_discoverable_spaces_produces_no_workunits() -> None:
    source = _source(_config())
    _client(source).get_all_spaces.return_value = {"results": []}

    assert list(source.get_workunits_internal()) == []


# --------------------------------------------------------------------------- #
# Recursive page crawl
# --------------------------------------------------------------------------- #


def test_recursive_crawl_skips_page_the_api_cannot_return() -> None:
    source = _source(_config())
    _client(source).get_page_by_id.return_value = None

    assert list(source._get_pages_recursively("404")) == []
    assert source.report.pages_scanned == 0


def test_recursive_crawl_keeps_parent_when_child_listing_fails() -> None:
    source = _source(_config(recursive=True))
    client = _client(source)
    client.get_page_by_id.return_value = _page("1")
    client.get_child_pages.side_effect = Exception("children endpoint 500")

    pages = list(source._get_pages_recursively("1"))

    assert [p["id"] for p in pages] == ["1"]
    assert source.report.failures.total_elements == 0


def test_recursive_crawl_page_fetch_error_is_reported_when_continuing() -> None:
    source = _source(_config())
    _client(source).get_page_by_id.side_effect = Exception("403")

    assert list(source._get_pages_recursively("1")) == []
    assert source.report.failures.total_elements == 1


def test_recursive_crawl_page_fetch_error_raises_when_not_continuing() -> None:
    source = _source(_config(advanced={"continue_on_failure": False}))
    _client(source).get_page_by_id.side_effect = Exception("403")

    with pytest.raises(Exception, match="403"):
        list(source._get_pages_recursively("1"))


# --------------------------------------------------------------------------- #
# Space page listing (pagination)
# --------------------------------------------------------------------------- #


def test_space_listing_follows_next_links_and_stops_at_last_page() -> None:
    source = _source(_config())
    client = _client(source)
    client.get_all_pages_from_space.side_effect = [
        {"results": [_page("1")], "_links": {"next": "/next?start=100"}},
        {"results": [_page("2")], "_links": {}},
    ]

    pages = list(source._get_pages_in_space("TEAM"))

    assert [p["id"] for p in pages] == ["1", "2"]
    starts = [c.kwargs["start"] for c in client.get_all_pages_from_space.call_args_list]
    assert starts == [0, 100]


def test_space_listing_honours_max_pages_per_space() -> None:
    source = _source(_config(max_pages_per_space=2))
    client = _client(source)
    client.get_all_pages_from_space.return_value = {
        "results": [_page("1"), _page("2")],
        "_links": {"next": "/more"},
    }

    pages = list(source._get_pages_in_space("TEAM"))

    assert len(pages) == 2
    # The limit is also pushed down into the request so no more than needed is asked for.
    assert client.get_all_pages_from_space.call_args.kwargs["limit"] == 2
    assert client.get_all_pages_from_space.call_count == 1


def test_space_listing_accepts_a_bare_list_response() -> None:
    """Data Center clients return a list rather than a {results: [...]} envelope."""
    source = _source(_config())
    client = _client(source)
    client.get_all_pages_from_space.side_effect = [[_page("1")], []]

    pages = list(source._get_pages_in_space("TEAM"))

    assert [p["id"] for p in pages] == ["1"]


def test_space_listing_stops_on_empty_response() -> None:
    source = _source(_config())
    _client(source).get_all_pages_from_space.return_value = None

    assert list(source._get_pages_in_space("TEAM")) == []


def test_space_listing_error_marks_space_failed_without_raising() -> None:
    source = _source(_config())
    _client(source).get_all_pages_from_space.side_effect = Exception("timeout")

    assert list(source._get_pages_in_space("TEAM")) == []
    assert source.report.spaces_failed == 1


# --------------------------------------------------------------------------- #
# pages.allow seed handling
# --------------------------------------------------------------------------- #


def test_page_allow_non_recursive_skips_unreachable_seed() -> None:
    source = _source(_config(pages={"allow": ["111", "222"]}, recursive=False))
    client = _client(source)
    client.get_page_by_id.side_effect = lambda page_id, expand=None: (
        _page("222") if page_id == "222" else None
    )

    pages = list(source._get_pages_from_page_allow())

    assert [p["id"] for p in pages] == ["222"]
    assert source.report.pages_scanned == 1


def test_page_allow_seed_error_is_reported_when_continuing() -> None:
    source = _source(_config(pages={"allow": ["111"]}, recursive=False))
    _client(source).get_page_by_id.side_effect = Exception("boom")

    assert list(source._get_pages_from_page_allow()) == []
    assert source.report.failures.total_elements == 1


def test_page_allow_seed_error_aborts_run_when_not_continuing() -> None:
    source = _source(
        _config(
            pages={"allow": ["111"]},
            recursive=False,
            advanced={"continue_on_failure": False},
        )
    )
    _client(source).get_page_by_id.side_effect = Exception("boom")

    with pytest.raises(Exception, match="boom"):
        list(source.get_workunits_internal())


# --------------------------------------------------------------------------- #
# Document emission
# --------------------------------------------------------------------------- #


def test_page_without_id_is_not_emitted() -> None:
    source = _source(_config())
    page = _page("1")
    del page["id"]

    assert list(source._create_document_entity(page, set(), set())) == []
    assert source.report.pages_processed == 0


def test_denied_page_is_skipped_and_counted() -> None:
    source = _source(_config(pages={"deny": ["999"]}))

    assert list(source._create_document_entity(_page("999"), {"999"}, set())) == []
    assert source.report.pages_skipped == 1


def test_native_import_mode_emits_native_document_without_external_link() -> None:
    source = _source(_config(document_import_mode="NATIVE"))

    infos = _document_infos(source._create_document_entity(_page("1"), {"1"}, set()))

    assert len(infos) == 1
    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.sourceType == DocumentSourceTypeClass.NATIVE
    assert info.source.externalUrl is None


def test_external_import_mode_keeps_link_back_to_confluence() -> None:
    source = _source(_config())

    infos = _document_infos(source._create_document_entity(_page("1"), {"1"}, set()))

    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.sourceType == DocumentSourceTypeClass.EXTERNAL
    assert info.source.externalUrl == (
        "https://test.atlassian.net/wiki/spaces/TEAM/pages/1/Page"
    )


def test_page_without_webui_link_gets_url_built_from_base_url() -> None:
    source = _source(_config())
    page = _page("1")
    page["_links"] = {}

    infos = _document_infos(source._create_document_entity(page, {"1"}, set()))

    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.externalUrl == "https://test.atlassian.net/wiki/pages/1"


def test_unparseable_timestamps_do_not_block_emission() -> None:
    source = _source(_config())
    page = _page(
        "1",
        version={"when": "not-a-date"},
        history={"createdDate": "also-not-a-date"},
    )

    infos = _document_infos(source._create_document_entity(page, {"1"}, set()))

    assert len(infos) == 1
    assert source.report.pages_processed == 1


def test_native_import_mode_emits_native_folder_document() -> None:
    from datahub.ingestion.source.confluence.confluence_hierarchy import FolderNode

    source = _source(_config(document_import_mode="NATIVE"))
    folder = FolderNode(
        id="900", title="Eng", parent_id=None, space_name="TEAM Space", space_key="TEAM"
    )

    infos = _document_infos(source._create_folder_entity(folder, {"900"}))

    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.sourceType == DocumentSourceTypeClass.NATIVE
    assert info.source.externalUrl is None


def test_platform_instance_aspect_is_derived_from_url_subdomain() -> None:
    source = _source(_config())

    instances = [
        wu.metadata.aspect
        for wu in source._create_document_entity(_page("1"), {"1"}, set())
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DataPlatformInstanceClass)
    ]

    assert len(instances) == 1
    assert (
        instances[0].instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:confluence,test)"
    )


def test_deferred_skip_marker_leaves_page_unprocessed_for_retry() -> None:
    source = _source(_config())
    source.chunking_source.process_elements_inline = MagicMock(  # type: ignore[method-assign]
        side_effect=SkipMarkerReadError("could not read semanticContent")
    )

    workunits = list(source._create_document_entity(_page("1"), {"1"}, set()))

    # The document itself is still emitted ...
    assert len(_document_infos(workunits)) == 1
    # ... but the page is not recorded as processed, so it is retried next run.
    assert source.report.pages_processed == 0


def test_embedding_runtime_error_without_limit_still_counts_page_processed() -> None:
    source = _source(_config())
    source.chunking_source.process_elements_inline = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("embedding backend down")
    )

    workunits = list(source._create_document_entity(_page("1"), {"1"}, set()))

    assert len(_document_infos(workunits)) == 1
    assert source.report.pages_processed == 1


# --------------------------------------------------------------------------- #
# Main loop resilience
# --------------------------------------------------------------------------- #


def _space_with_pages(source: ConfluenceSource, pages: List[Dict[str, Any]]) -> None:
    client = _client(source)
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
    client.get_all_pages_from_space.return_value = {"results": pages, "_links": {}}


def test_spaces_without_pages_produce_no_workunits() -> None:
    source = _source(_config())
    _space_with_pages(source, [])

    assert list(source.get_workunits_internal()) == []


def test_duplicate_pages_across_listings_are_emitted_once() -> None:
    source = _source(_config())
    _space_with_pages(source, [_page("1"), _page("1")])

    infos = _document_infos(source.get_workunits_internal())

    assert len(infos) == 1


def test_folder_discovery_failure_does_not_block_page_ingestion() -> None:
    source = _source(_config())
    _space_with_pages(source, [_page("1")])

    with patch(
        "datahub.ingestion.source.confluence.confluence_source."
        "ConfluenceHierarchyExtractor.collect_folder_nodes",
        side_effect=Exception("ancestry parse failed"),
    ):
        infos = _document_infos(source.get_workunits_internal())

    assert len(infos) == 1
    assert source.report.folders_failed == 1


def test_folder_emission_failure_does_not_block_page_ingestion() -> None:
    folder = {"id": "f1", "type": "folder", "title": "Folder"}
    source = _source(_config())
    _space_with_pages(source, [_page("1", ancestors=[folder])])

    with patch.object(
        source, "_create_folder_entity", side_effect=Exception("folder broke")
    ):
        infos = _document_infos(source.get_workunits_internal())

    assert len(infos) == 1
    assert source.report.folders_failed == 1


def test_page_emission_failure_is_reported_and_run_continues() -> None:
    source = _source(_config())
    _space_with_pages(source, [_page("1"), _page("2")])
    real_create = source._create_document_entity

    def flaky(
        page: Dict[str, Any], ingested: Set[str], parents: Set[str]
    ) -> Iterable[Any]:
        if page["id"] == "1":
            raise Exception("render failed")
        return real_create(page, ingested, parents)

    with patch.object(source, "_create_document_entity", side_effect=flaky):
        infos = _document_infos(source.get_workunits_internal())

    assert len(infos) == 1
    assert source.report.pages_failed == 1
    assert source.report.failed_pages == [("TEAM", "1")]


def test_page_emission_failure_aborts_run_when_not_continuing() -> None:
    source = _source(_config(advanced={"continue_on_failure": False}))
    _space_with_pages(source, [_page("1")])

    with (
        patch.object(
            source, "_create_document_entity", side_effect=Exception("render failed")
        ),
        pytest.raises(Exception, match="render failed"),
    ):
        list(source.get_workunits_internal())


def test_get_report_merges_chunking_statistics() -> None:
    source = _source(_config())
    source.chunking_source.report.num_documents_with_embeddings = 3
    source.chunking_source.report.num_embedding_failures = 1
    source.chunking_source.report.embedding_failures.append("urn:li:document:x")

    report = source.get_report()

    assert report.num_documents_with_embeddings == 3
    assert report.num_embedding_failures == 1
    assert list(report.embedding_failures) == ["urn:li:document:x"]


# --------------------------------------------------------------------------- #
# test_connection
# --------------------------------------------------------------------------- #

_BASE_CONNECTION_DICT: Dict[str, Any] = {
    "url": "https://test.atlassian.net/wiki",
    "username": "test@example.com",
    "api_token": "test-token-123",
}


def _test_connection(client: MagicMock, **overrides: Any) -> Any:
    config_dict = {**_BASE_CONNECTION_DICT, **overrides}
    with (
        patch(CONFLUENCE_CLIENT, return_value=client),
        patch(
            "datahub.ingestion.source.confluence.confluence_source."
            "ConfluenceSource._test_semantic_search_capability",
            return_value=CapabilityReport(capable=True),
        ),
    ):
        return ConfluenceSource.test_connection(config_dict)


def test_connection_unparseable_config_is_internal_failure() -> None:
    report = ConfluenceSource.test_connection({"url": "https://x.atlassian.net"})

    assert report.internal_failure is True


def test_connection_basic_connectivity_failure_names_the_error() -> None:
    client = MagicMock()
    client.get_all_spaces.side_effect = PermissionError("401 Unauthorized")

    report = _test_connection(client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "PermissionError" in (report.basic_connectivity.failure_reason or "")


def test_connection_validates_configured_pages() -> None:
    client = MagicMock()
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}

    def page_lookup(page_id: str) -> Dict[str, str]:
        if page_id == "111":
            return {"id": page_id}
        raise KeyError(page_id)

    client.get_page_by_id.side_effect = page_lookup

    report = _test_connection(client, pages={"allow": ["111", "222"]})

    access = report.capability_report["Space/Page Access"]
    assert access.capable is False
    assert "page 222" in (access.failure_reason or "")
    assert "page 111" not in (access.failure_reason or "")


def test_connection_space_page_count_failure_is_not_fatal() -> None:
    client = MagicMock()
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
    client.get_space.return_value = {"key": "TEAM"}
    client.get_all_pages_from_space.side_effect = Exception("listing disabled")

    report = _test_connection(client, spaces={"allow": ["TEAM"]})

    assert report.capability_report["Space/Page Access"].capable is True


def test_connection_auto_discovery_page_sampling_failure_is_not_fatal() -> None:
    client = MagicMock()
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
    client.get_all_pages_from_space.side_effect = Exception("listing disabled")

    report = _test_connection(client)

    assert report.capability_report["Auto-Discovery"].capable is True


def test_connection_auto_discovery_listing_failure_is_reported() -> None:
    client = MagicMock()
    # Basic connectivity probe succeeds, the fuller discovery call fails.
    client.get_all_spaces.side_effect = [
        {"results": [{"key": "TEAM"}]},
        Exception("rate limited"),
    ]

    report = _test_connection(client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    discovery = report.capability_report["Auto-Discovery"]
    assert discovery.capable is False
    assert "rate limited" in (discovery.failure_reason or "")


def test_semantic_search_capability_reports_invalid_embedding_config() -> None:
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source."
        "DocumentChunkingSource.resolve_embedding_config",
        side_effect=ValueError("semantic search disabled on server"),
    ):
        report = ConfluenceSource._test_semantic_search_capability(_config())

    assert report.capable is False
    assert "semantic search disabled" in (report.failure_reason or "")


def test_semantic_search_capability_reports_unexpected_resolution_error() -> None:
    with patch(
        "datahub.ingestion.source.unstructured.chunking_source."
        "DocumentChunkingSource.resolve_embedding_config",
        side_effect=ConnectionError("server unreachable"),
    ):
        report = ConfluenceSource._test_semantic_search_capability(_config())

    assert report.capable is False
    assert "server unreachable" in (report.failure_reason or "")


def test_semantic_search_capability_delegates_to_resolved_config() -> None:
    expected = CapabilityReport(capable=True)
    with (
        patch(
            "datahub.ingestion.source.unstructured.chunking_source."
            "DocumentChunkingSource.resolve_embedding_config",
            return_value=MagicMock(),
        ),
        patch(
            "datahub.ingestion.source.unstructured.chunking_source."
            "DocumentChunkingSource.test_embedding_capability",
            return_value=expected,
        ),
    ):
        report = ConfluenceSource._test_semantic_search_capability(_config())

    assert report is expected
