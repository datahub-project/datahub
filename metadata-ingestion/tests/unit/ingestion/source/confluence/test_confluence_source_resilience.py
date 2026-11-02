"""Failure handling and edge cases of the Confluence crawl, document emission and
test_connection: what happens when the Confluence API returns nothing, raises,
paginates, or hands back pages missing the fields ingestion relies on."""

from typing import Any, Dict, Iterable, Set
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.source.confluence.confluence_config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.confluence_hierarchy import (
    ConfluenceHierarchyExtractor,
    FolderNode,
)
from datahub.ingestion.source.confluence.confluence_source import ConfluenceSource
from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
    SkipMarkerReadError,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DocumentSourceTypeClass,
)
from tests.unit.conftest import as_mock, aspects_of_type, log_contexts, log_messages
from tests.unit.ingestion.source.confluence.conftest import (
    cloud_config_dict,
    document_infos,
    make_config,
    make_page,
    make_source,
    run_test_connection,
    serve_space,
    stub_chunking,
)

# --------------------------------------------------------------------------- #
# Space discovery
# --------------------------------------------------------------------------- #


def test_space_discovery_with_no_results_yields_no_spaces() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_all_spaces.return_value = {}

    assert source._get_spaces() == []
    assert not source.report.failures


def test_space_discovery_api_error_is_reported_not_raised() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_all_spaces.side_effect = Exception(
        "503 unavailable"
    )

    assert source._get_spaces() == []
    assert "Failed to discover spaces" in log_messages(source.report.failures)


def test_no_discoverable_spaces_produces_no_workunits() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_all_spaces.return_value = {"results": []}

    assert list(source.get_workunits_internal()) == []


# --------------------------------------------------------------------------- #
# Recursive page crawl
# --------------------------------------------------------------------------- #


def test_recursive_crawl_skips_page_the_api_cannot_return() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_page_by_id.return_value = None

    assert list(source._get_pages_recursively("404")) == []
    assert source.report.pages_scanned == 0


def test_recursive_crawl_keeps_parent_when_child_listing_fails() -> None:
    """A failing children endpoint must not discard the parent page that was
    already fetched, and is not escalated to a run failure."""
    source = make_source(make_config(recursive=True))
    client = as_mock(source.confluence_client)
    client.get_page_by_id.return_value = make_page("1")
    client.get_child_pages.side_effect = Exception("children endpoint 500")

    pages = list(source._get_pages_recursively("1"))

    assert [p["id"] for p in pages] == ["1"]
    assert not source.report.failures


def test_recursive_crawl_page_fetch_error_is_reported_when_continuing() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_page_by_id.side_effect = Exception("403")

    assert list(source._get_pages_recursively("1")) == []
    assert "1" in log_contexts(source.report.failures)


def test_recursive_crawl_page_fetch_error_raises_when_not_continuing() -> None:
    source = make_source(make_config(advanced={"continue_on_failure": False}))
    as_mock(source.confluence_client).get_page_by_id.side_effect = Exception("403")

    with pytest.raises(Exception, match="403"):
        list(source._get_pages_recursively("1"))


# --------------------------------------------------------------------------- #
# Space page listing (pagination)
# --------------------------------------------------------------------------- #


def test_space_listing_follows_next_links_and_stops_at_last_page() -> None:
    """Pagination advances ``start`` by the page size while the response carries
    a ``next`` link, and stops on the first response without one."""
    source = make_source()
    client = as_mock(source.confluence_client)
    client.get_all_pages_from_space.side_effect = [
        {"results": [make_page("1")], "_links": {"next": "/next?start=100"}},
        {"results": [make_page("2")], "_links": {}},
    ]

    pages = list(source._get_pages_in_space("TEAM"))

    assert [p["id"] for p in pages] == ["1", "2"]
    starts = [c.kwargs["start"] for c in client.get_all_pages_from_space.call_args_list]
    assert starts == [0, 100]


def test_space_listing_honours_max_pages_per_space() -> None:
    """max_pages_per_space caps the pages yielded and is pushed down into the
    request, so a space with a ``next`` link is not paged past the cap."""
    source = make_source(make_config(max_pages_per_space=2))
    client = as_mock(source.confluence_client)
    client.get_all_pages_from_space.return_value = {
        "results": [make_page("1"), make_page("2")],
        "_links": {"next": "/more"},
    }

    pages = list(source._get_pages_in_space("TEAM"))

    assert len(pages) == 2
    assert client.get_all_pages_from_space.call_args.kwargs["limit"] == 2
    assert client.get_all_pages_from_space.call_count == 1


def test_space_listing_accepts_a_bare_list_response() -> None:
    """Data Center clients return a list rather than a {results: [...]} envelope."""
    source = make_source()
    as_mock(source.confluence_client).get_all_pages_from_space.side_effect = [
        [make_page("1")],
        [],
    ]

    pages = list(source._get_pages_in_space("TEAM"))

    assert [p["id"] for p in pages] == ["1"]


def test_space_listing_stops_on_empty_response() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_all_pages_from_space.return_value = None

    assert list(source._get_pages_in_space("TEAM")) == []


def test_space_listing_error_marks_space_failed_without_raising() -> None:
    source = make_source()
    as_mock(source.confluence_client).get_all_pages_from_space.side_effect = Exception(
        "timeout"
    )

    assert list(source._get_pages_in_space("TEAM")) == []
    assert source.report.failed_spaces == ["TEAM"]


# --------------------------------------------------------------------------- #
# pages.allow seed handling
# --------------------------------------------------------------------------- #


def test_page_allow_non_recursive_skips_unreachable_seed() -> None:
    """A seed page the API returns nothing for is skipped; the other seeds are
    still fetched and counted."""
    source = make_source(make_config(pages={"allow": ["111", "222"]}, recursive=False))
    as_mock(source.confluence_client).get_page_by_id.side_effect = (
        lambda page_id, expand=None: make_page("222") if page_id == "222" else None
    )

    pages = list(source._get_pages_from_page_allow())

    assert [p["id"] for p in pages] == ["222"]
    assert source.report.pages_scanned == 1


def test_page_allow_seed_error_is_reported_when_continuing() -> None:
    source = make_source(make_config(pages={"allow": ["111"]}, recursive=False))
    as_mock(source.confluence_client).get_page_by_id.side_effect = Exception("boom")

    assert list(source._get_pages_from_page_allow()) == []
    assert "111" in log_contexts(source.report.failures)


def test_page_allow_seed_error_aborts_run_when_not_continuing() -> None:
    source = make_source(
        make_config(
            pages={"allow": ["111"]},
            recursive=False,
            advanced={"continue_on_failure": False},
        )
    )
    as_mock(source.confluence_client).get_page_by_id.side_effect = Exception("boom")

    with pytest.raises(Exception, match="boom"):
        list(source.get_workunits_internal())


# --------------------------------------------------------------------------- #
# Document emission
# --------------------------------------------------------------------------- #


def test_page_without_id_is_not_emitted() -> None:
    source = make_source()
    page = make_page("1")
    del page["id"]

    assert list(source._create_document_entity(page, set(), set())) == []
    assert source.report.pages_processed == 0


def test_denied_page_is_skipped_and_counted() -> None:
    source = make_source(make_config(pages={"deny": ["999"]}))

    assert list(source._create_document_entity(make_page("999"), {"999"}, set())) == []
    assert source.report.pages_skipped == 1


def test_native_import_mode_emits_native_document_without_external_link() -> None:
    """NATIVE mode makes DataHub the system of record, so the document must not
    point back at Confluence."""
    source = make_source(make_config(document_import_mode="NATIVE"))

    infos = document_infos(source._create_document_entity(make_page("1"), {"1"}, set()))

    assert len(infos) == 1
    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.sourceType == DocumentSourceTypeClass.NATIVE
    assert info.source.externalUrl is None


def test_external_import_mode_keeps_link_back_to_confluence() -> None:
    source = make_source()

    infos = document_infos(source._create_document_entity(make_page("1"), {"1"}, set()))

    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.sourceType == DocumentSourceTypeClass.EXTERNAL
    assert info.source.externalUrl == (
        "https://test.atlassian.net/wiki/spaces/TEAM/pages/1/Page"
    )


def test_page_without_webui_link_gets_url_built_from_base_url() -> None:
    """Some API responses omit ``_links.webui``; the external URL then falls back
    to ``<base>/pages/<id>`` instead of being dropped."""
    source = make_source()
    page = make_page("1")
    page["_links"] = {}

    infos = document_infos(source._create_document_entity(page, {"1"}, set()))

    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.externalUrl == "https://test.atlassian.net/wiki/pages/1"


def test_unparseable_timestamps_do_not_block_emission() -> None:
    source = make_source()
    page = make_page(
        "1",
        version={"when": "not-a-date"},
        history={"createdDate": "also-not-a-date"},
    )

    infos = document_infos(source._create_document_entity(page, {"1"}, set()))

    assert len(infos) == 1
    assert source.report.pages_processed == 1


def test_native_import_mode_emits_native_folder_document() -> None:
    source = make_source(make_config(document_import_mode="NATIVE"))
    folder = FolderNode(
        id="900", title="Eng", parent_id=None, space_name="TEAM Space", space_key="TEAM"
    )

    infos = document_infos(source._create_folder_entity(folder, {"900"}))

    info = next(iter(infos.values()))
    assert info.source is not None
    assert info.source.sourceType == DocumentSourceTypeClass.NATIVE
    assert info.source.externalUrl is None


def test_platform_instance_aspect_is_derived_from_url_subdomain() -> None:
    """Without an explicit platform_instance the DataPlatformInstance aspect uses
    the Atlassian subdomain, so two Cloud sites stay distinguishable."""
    source = make_source()

    instances = aspects_of_type(
        source._create_document_entity(make_page("1"), {"1"}, set()),
        DataPlatformInstanceClass,
    )

    assert [i.instance for i in instances] == [
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:confluence,test)"
    ]


def test_deferred_skip_marker_leaves_page_unprocessed_for_retry() -> None:
    """When the skip marker cannot be read, the document is still emitted but
    the page is not recorded as processed, so the next run retries it."""
    source = make_source()
    stub_chunking(source, side_effect=SkipMarkerReadError("could not read marker"))

    workunits = list(source._create_document_entity(make_page("1"), {"1"}, set()))

    assert len(document_infos(workunits)) == 1
    assert source.report.pages_processed == 0


def test_embedding_runtime_error_without_limit_still_counts_page_processed() -> None:
    """An embedding backend failure (other than the max_documents limit) loses
    only the embeddings: the document is emitted and the page counts as done."""
    source = make_source()
    stub_chunking(source, side_effect=RuntimeError("embedding backend down"))

    workunits = list(source._create_document_entity(make_page("1"), {"1"}, set()))

    assert len(document_infos(workunits)) == 1
    assert source.report.pages_processed == 1


# --------------------------------------------------------------------------- #
# Main loop resilience
# --------------------------------------------------------------------------- #


def test_spaces_without_pages_produce_no_workunits() -> None:
    source = make_source()
    serve_space(as_mock(source.confluence_client), [])

    assert list(source.get_workunits_internal()) == []


def test_duplicate_pages_across_listings_are_emitted_once() -> None:
    source = make_source()
    serve_space(as_mock(source.confluence_client), [make_page("1"), make_page("1")])

    infos = document_infos(source.get_workunits_internal())

    assert len(infos) == 1


def test_folder_discovery_failure_does_not_block_page_ingestion() -> None:
    source = make_source()
    serve_space(as_mock(source.confluence_client), [make_page("1")])

    with patch.object(
        ConfluenceHierarchyExtractor,
        "collect_folder_nodes",
        autospec=True,
        side_effect=Exception("ancestry parse failed"),
    ):
        infos = document_infos(source.get_workunits_internal())

    assert len(infos) == 1
    assert source.report.folders_failed == 1


def test_folder_emission_failure_does_not_block_page_ingestion() -> None:
    folder = {"id": "f1", "type": "folder", "title": "Folder"}
    source = make_source()
    serve_space(as_mock(source.confluence_client), [make_page("1", ancestors=[folder])])

    with patch.object(
        source,
        "_create_folder_entity",
        autospec=True,
        side_effect=Exception("folder broke"),
    ):
        infos = document_infos(source.get_workunits_internal())

    assert len(infos) == 1
    assert source.report.folders_failed == 1


def test_page_emission_failure_is_reported_and_run_continues() -> None:
    """One page failing to render is recorded against its space and id, and the
    remaining pages in the space are still emitted."""
    source = make_source()
    serve_space(as_mock(source.confluence_client), [make_page("1"), make_page("2")])
    real_create = source._create_document_entity

    def flaky(
        page: Dict[str, Any], ingested: Set[str], parents: Set[str]
    ) -> Iterable[Any]:
        if page["id"] == "1":
            raise Exception("render failed")
        return real_create(page, ingested, parents)

    with patch.object(
        source, "_create_document_entity", autospec=True, side_effect=flaky
    ):
        infos = document_infos(source.get_workunits_internal())

    assert len(infos) == 1
    assert source.report.pages_failed == 1
    assert source.report.failed_pages == [("TEAM", "1")]


def test_page_emission_failure_aborts_run_when_not_continuing() -> None:
    source = make_source(make_config(advanced={"continue_on_failure": False}))
    serve_space(as_mock(source.confluence_client), [make_page("1")])

    with (
        patch.object(
            source,
            "_create_document_entity",
            autospec=True,
            side_effect=Exception("render failed"),
        ),
        pytest.raises(Exception, match="render failed"),
    ):
        list(source.get_workunits_internal())


def test_get_report_merges_chunking_statistics() -> None:
    """Embedding statistics live on the chunking sub-source's report and must be
    copied onto the Confluence report the pipeline prints."""
    source = make_source()
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


def test_connection_unparseable_config_is_internal_failure() -> None:
    report = ConfluenceSource.test_connection({"url": "https://x.atlassian.net"})

    assert report.internal_failure is True


def test_connection_basic_connectivity_failure_names_the_error() -> None:
    client = MagicMock()
    client.get_all_spaces.side_effect = PermissionError("401 Unauthorized")

    report = run_test_connection(client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "PermissionError" in (report.basic_connectivity.failure_reason or "")


def test_connection_validates_configured_pages() -> None:
    """Each configured pages.allow id is probed; only the unreachable ones are
    named in the failure reason."""
    client = MagicMock()
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}

    def page_lookup(page_id: str) -> Dict[str, str]:
        if page_id == "111":
            return {"id": page_id}
        raise KeyError(page_id)

    client.get_page_by_id.side_effect = page_lookup

    report = run_test_connection(client, pages={"allow": ["111", "222"]})

    assert report.capability_report is not None
    access = report.capability_report["Space/Page Access"]
    assert access.capable is False
    assert "page 222" in (access.failure_reason or "")
    assert "page 111" not in (access.failure_reason or "")


def test_connection_space_page_count_failure_is_not_fatal() -> None:
    """Counting pages in a configured space is informational; a listing error
    there must not mark the space as inaccessible."""
    client = MagicMock()
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
    client.get_space.return_value = {"key": "TEAM"}
    client.get_all_pages_from_space.side_effect = Exception("listing disabled")

    report = run_test_connection(client, spaces={"allow": ["TEAM"]})

    assert report.capability_report is not None
    assert report.capability_report["Space/Page Access"].capable is True


def test_connection_auto_discovery_page_sampling_failure_is_not_fatal() -> None:
    client = MagicMock()
    client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
    client.get_all_pages_from_space.side_effect = Exception("listing disabled")

    report = run_test_connection(client)

    assert report.capability_report is not None
    assert report.capability_report["Auto-Discovery"].capable is True


def test_connection_auto_discovery_listing_failure_is_reported() -> None:
    """The connectivity probe and the discovery listing are separate calls; a
    failure in the second is reported under Auto-Discovery, not connectivity."""
    client = MagicMock()
    client.get_all_spaces.side_effect = [
        {"results": [{"key": "TEAM"}]},
        Exception("rate limited"),
    ]

    report = run_test_connection(client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    discovery = report.capability_report["Auto-Discovery"]
    assert discovery.capable is False
    assert "rate limited" in (discovery.failure_reason or "")


def test_semantic_search_capability_reports_invalid_embedding_config() -> None:
    with patch.object(
        DocumentChunkingSource,
        "resolve_embedding_config",
        autospec=True,
        side_effect=ValueError("semantic search disabled on server"),
    ):
        report = ConfluenceSource._test_semantic_search_capability(make_config())

    assert report.capable is False
    assert "semantic search disabled" in (report.failure_reason or "")


def test_semantic_search_capability_reports_unexpected_resolution_error() -> None:
    with patch.object(
        DocumentChunkingSource,
        "resolve_embedding_config",
        autospec=True,
        side_effect=ConnectionError("server unreachable"),
    ):
        report = ConfluenceSource._test_semantic_search_capability(make_config())

    assert report.capable is False
    assert "server unreachable" in (report.failure_reason or "")


def test_semantic_search_capability_delegates_to_resolved_config() -> None:
    expected = CapabilityReport(capable=True)
    resolved = create_autospec(EmbeddingConfig, instance=True)
    with (
        patch.object(
            DocumentChunkingSource,
            "resolve_embedding_config",
            autospec=True,
            return_value=resolved,
        ),
        patch.object(
            DocumentChunkingSource,
            "test_embedding_capability",
            autospec=True,
            return_value=expected,
        ) as test_capability,
    ):
        report = ConfluenceSource._test_semantic_search_capability(make_config())

    assert report is expected
    test_capability.assert_called_once_with(resolved)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "Bug: _test_semantic_search_capability only builds a DataHubGraph when "
        "`config.datahub.server` is set, but ConfluenceSourceConfig has no "
        "`datahub` field, so the hasattr guard is always False and test_connection "
        "resolves the embedding config without ever consulting the DataHub server."
    ),
)
def test_semantic_search_capability_consults_configured_datahub_server() -> None:
    # parse_obj_allow_extras so today's config (no `datahub` field) drops the
    # block instead of rejecting the recipe; once the field exists it is kept.
    config = ConfluenceSourceConfig.parse_obj_allow_extras(
        cloud_config_dict(datahub={"server": "http://localhost:8080"})
    )
    with (
        patch("datahub.ingestion.graph.client.DataHubGraph", autospec=True),
        patch.object(
            DocumentChunkingSource, "resolve_embedding_config", autospec=True
        ) as resolve,
        patch.object(
            DocumentChunkingSource,
            "test_embedding_capability",
            autospec=True,
            return_value=CapabilityReport(capable=True),
        ),
    ):
        ConfluenceSource._test_semantic_search_capability(config)

    _embedding, graph = resolve.call_args.args
    assert graph is not None
