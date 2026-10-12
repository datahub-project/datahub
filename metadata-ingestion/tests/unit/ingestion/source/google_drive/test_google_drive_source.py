import copy
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Tuple, cast
from unittest import mock

import pytest
from googleapiclient.discovery import Resource

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api import source as api_source
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    make_document_urn,
)
from datahub.ingestion.source.google_drive.google_drive_source import (
    GoogleDriveSource,
    clean_markdown,
)
from datahub.ingestion.source.google_drive.models import ExportResult
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
    SkipMarkerReadError,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DocumentInfoClass,
    OwnershipClass,
    StatusClass,
    SubTypesClass,
)
from tests.test_helpers.google_drive_fakes import (
    DOC_A1,
    DOC_B1,
    DOC_B2,
    DOC_EMPTY,
    DOC_HTML,
    DOC_HUGE,
    DOC_IMAGES,
    DOC_RUNBOOK,
    ENG,
    ROOT_A,
    SHARED_B,
    SPECS,
    FakeDriveService,
    build_fixture_service,
)
from tests.test_helpers.test_connection_helpers import (
    assert_basic_connectivity_failure,
    assert_basic_connectivity_success,
    run_test_connection,
)

Aspects = Dict[str, Dict[str, Any]]
SOURCE_MODULE = "datahub.ingestion.source.google_drive.google_drive_source"


def make_source(
    svc: FakeDriveService,
    graph: Optional[DataHubGraph] = None,
    **overrides: Any,
) -> GoogleDriveSource:
    config_dict: Dict[str, Any] = {
        "root_folder_ids": [ROOT_A, SHARED_B],
        "folder_pattern": {"deny": [".*/Archive$"]},
        "document_pattern": {"deny": ["^Copy of "]},
    }
    config_dict.update(overrides)
    config = GoogleDriveSourceConfig.model_validate(config_dict)
    source = GoogleDriveSource(
        config, PipelineContext(run_id="google-drive-test"), cast(Resource, svc)
    )
    # Attach the graph after construction so the chunking sub-source does not try to
    # load an embedding config from a mock server.
    source.ctx.graph = graph
    return source


def run(
    source: GoogleDriveSource,
    embed: Callable[..., Iterable[MetadataWorkUnit]] = lambda **_: iter([]),
    aspects: Optional[Aspects] = None,
) -> Aspects:
    collected: Aspects = {} if aspects is None else aspects
    with mock.patch.object(
        source.chunking_source, "process_elements_inline", side_effect=embed
    ):
        workunits: Iterable[MetadataWorkUnit] = source.get_workunits_internal()
        for wu in workunits:
            mcp = wu.metadata
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            assert mcp.entityUrn is not None and mcp.aspect is not None
            collected.setdefault(mcp.entityUrn, {})[mcp.aspect.get_aspect_name()] = (
                mcp.aspect
            )
    return collected


def doc_info(aspects: Aspects, file_id: str) -> DocumentInfoClass:
    info = aspects[make_document_urn(file_id)]["documentInfo"]
    assert isinstance(info, DocumentInfoClass)
    return info


def stored_aspects(
    urn: str, infos: Dict[str, DocumentInfoClass], removed: FrozenSet[str]
) -> Dict[str, object]:
    bag: Dict[str, object] = {"status": StatusClass(removed=urn in removed)}
    if urn in infos:
        bag["documentInfo"] = infos[urn]
    return bag


def graph_returning(
    infos: Dict[str, DocumentInfoClass], removed: FrozenSet[str] = frozenset()
) -> mock.MagicMock:
    """A graph whose batched reads return `infos`, like the OpenAPI v3 batchGet:
    URNs DataHub has never stored are absent from the result."""

    def batch_get(
        entity_name: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Dict[str, Tuple[object, None]]]:
        return {
            urn: {
                name: (value, None)
                for name, value in stored_aspects(urn, infos, removed).items()
            }
            for urn in urns
            if urn in infos or urn in removed
        }

    graph = mock.MagicMock(spec=DataHubGraph)
    graph.get_entities.side_effect = batch_get
    graph.get_entity_semityped.side_effect = AssertionError(
        "stored documents must be read in batches"
    )
    # Soft-deleted state must come from the entity, not from (lagging) search.
    graph.get_urns_by_filter.side_effect = AssertionError("search must not be used")
    return graph


def exported_ids(svc: FakeDriveService) -> List[str]:
    return [target.split(":")[0] for target in svc.calls_for("files.export")]


def test_full_run_emits_folders_and_documents() -> None:
    svc = build_fixture_service()
    source = make_source(svc)
    aspects = run(source)

    emitted_docs = {
        fid
        for fid in (DOC_A1, DOC_RUNBOOK, DOC_IMAGES, DOC_HTML, DOC_B1, DOC_B2)
        if make_document_urn(fid) in aspects
    }
    assert len(emitted_docs) == 6
    assert make_document_urn(DOC_EMPTY) not in aspects
    assert make_document_urn(DOC_HUGE) not in aspects

    report = source.report
    assert report.documents_emitted == 6
    assert report.folders_emitted == 5  # 2 roots + Eng, Specs, Team (Archive denied)
    assert report.documents_skipped_empty == 1
    assert report.documents_export_failed == 1
    assert report.documents_html_fallback == 2  # DOC_HTML and the empty doc

    specs_info = doc_info(aspects, SPECS)
    assert specs_info.parentDocument is not None
    assert specs_info.customProperties["drive_folder_path"] == "Root A/Eng/Specs"
    assert doc_info(aspects, SHARED_B).title == "Shared Drive B"

    browse = aspects[make_document_urn(DOC_IMAGES)]["browsePathsV2"]
    assert isinstance(browse, BrowsePathsV2Class)
    assert [e.id for e in browse.path] == ["Root A", "Eng", "Specs"]


def test_folder_documents_use_drive_timestamps_not_ingestion_time() -> None:
    # Golden-file comparison ignores audit timestamps, so this is checked here: a
    # folder stamped with the run time would change on every ingestion.
    aspects = run(make_source(build_fixture_service()))
    for folder_id in (ROOT_A, ENG, SHARED_B):
        info = doc_info(aspects, folder_id)
        assert info.created.time == 1767323045000  # 2026-01-02T03:04:05Z
        assert info.lastModified.time == 1770091506000  # 2026-02-03T04:05:06Z


def test_owners_are_mapped_and_deduplicated() -> None:
    aspects = run(make_source(build_fixture_service()))

    ownership = aspects[make_document_urn(DOC_RUNBOOK)]["ownership"]
    assert isinstance(ownership, OwnershipClass)
    assert sorted(o.owner for o in ownership.owners) == [
        "urn:li:corpuser:alice@example.com",
        "urn:li:corpuser:bob@example.com",
    ]
    # Shared drive files have no owners.
    assert "ownership" not in aspects[make_document_urn(DOC_B1)]


def test_extract_owners_disabled() -> None:
    aspects = run(make_source(build_fixture_service(), extract_owners=False))
    assert "ownership" not in aspects[make_document_urn(DOC_RUNBOOK)]


def test_clean_markdown_strips_data_uri_images() -> None:
    aspects = run(make_source(build_fixture_service()))
    text = doc_info(aspects, DOC_IMAGES).contents.text
    assert "base64" not in text
    assert "*[Image: Blue diagram]*" in text
    assert "*[Image]*" in text
    assert "![" not in text
    assert "\n\n\n" not in text
    assert text.endswith("End.")


def test_clean_markdown_inline_and_reference_forms() -> None:
    raw = (
        "a ![x](data:image/png;base64,QQ==) b ![y][img] ![z][link]\n\n"
        "[img]: <data:image/png;base64,QQ==>\n[link]: https://example.com/z.png\n"
    )
    assert clean_markdown(raw) == (
        "a *[Image: x]* b *[Image: y]* ![z][link]\n\n[link]: https://example.com/z.png"
    )


def test_documents_over_max_text_length_are_skipped() -> None:
    first = run(make_source(build_fixture_service()))
    lengths = {
        fid: len(doc_info(first, fid).contents.text)
        for fid in (DOC_A1, DOC_RUNBOOK, DOC_IMAGES, DOC_HTML, DOC_B1, DOC_B2)
    }
    limit = lengths[DOC_RUNBOOK] - 1
    source = make_source(build_fixture_service(), max_text_length=limit)
    aspects = run(source)

    assert "documentInfo" not in aspects.get(make_document_urn(DOC_RUNBOOK), {})
    assert source.report.documents_skipped_too_long == sum(
        length > limit for length in lengths.values()
    )


def test_tightening_max_text_length_reevaluates_unchanged_documents() -> None:
    first = run(make_source(build_fixture_service()))
    infos = {make_document_urn(DOC_RUNBOOK): doc_info(first, DOC_RUNBOOK)}

    svc = build_fixture_service()
    run(make_source(svc, graph=graph_returning(infos), max_text_length=10))
    assert DOC_RUNBOOK in exported_ids(svc)


def test_unchanged_document_is_skipped_but_kept_in_state() -> None:
    first = run(make_source(build_fixture_service()))
    infos = {
        make_document_urn(DOC_RUNBOOK): doc_info(first, DOC_RUNBOOK),
    }

    svc = build_fixture_service()
    source = make_source(svc, graph=graph_returning(infos))
    with mock.patch.object(
        source.stale_entity_removal_handler, "add_entity_to_state"
    ) as add_to_state:
        aspects = run(source)

    assert DOC_RUNBOOK not in exported_ids(svc)
    assert "documentInfo" not in aspects.get(make_document_urn(DOC_RUNBOOK), {})
    add_to_state.assert_any_call("document", make_document_urn(DOC_RUNBOOK))
    assert source.report.documents_skipped_unchanged == 1


@pytest.mark.parametrize(
    "mutation",
    ["version", "parent", "title", "fingerprint"],
)
def test_changes_force_reexport(mutation: str) -> None:
    first = run(make_source(build_fixture_service()))
    stored_info = copy.deepcopy(doc_info(first, DOC_RUNBOOK))
    if mutation == "version":
        stored_info.customProperties["drive_version"] = "6"
    elif mutation == "parent":
        stored_info.customProperties["drive_parent_id"] = "someOtherFolder"
    elif mutation == "title":
        stored_info.title = "Old title"
    else:
        stored_info.customProperties["drive_ingest_fingerprint"] = "stale"

    svc = build_fixture_service()
    source = make_source(
        svc, graph=graph_returning({make_document_urn(DOC_RUNBOOK): stored_info})
    )
    run(source)
    assert DOC_RUNBOOK in exported_ids(svc)


def test_embedding_failure_blanks_version_so_next_run_retries() -> None:
    source = make_source(build_fixture_service())

    def fail(**_: Any) -> Iterable[MetadataWorkUnit]:
        raise ValueError("embedding provider unavailable")

    info = doc_info(run(source, embed=fail), DOC_RUNBOOK)
    assert info.customProperties["drive_version"] == ""
    assert source.report.documents_embedding_deferred == source.report.documents_emitted


def test_unreadable_embedding_state_defers_embedding() -> None:
    source = make_source(build_fixture_service())

    def unreadable(**_: Any) -> Iterable[MetadataWorkUnit]:
        raise SkipMarkerReadError("skip marker unavailable")

    info = doc_info(run(source, embed=unreadable), DOC_RUNBOOK)
    assert info.customProperties["drive_version"] == ""
    assert source.report.documents_embedding_deferred == source.report.documents_emitted


def test_successful_embedding_is_emitted_and_keeps_version() -> None:
    def embed(document_urn: str, **_: Any) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=document_urn, aspect=SubTypesClass(typeNames=["embedded"])
        ).as_workunit()

    aspects = run(make_source(build_fixture_service()), embed=embed)
    urn = make_document_urn(DOC_RUNBOOK)
    assert "subTypes" in aspects[urn]
    assert doc_info(aspects, DOC_RUNBOOK).customProperties["drive_version"] == "7"


def test_soft_deleted_unchanged_document_is_resurrected() -> None:
    first = run(make_source(build_fixture_service()))
    urn = make_document_urn(DOC_RUNBOOK)
    graph = graph_returning(
        {urn: doc_info(first, DOC_RUNBOOK)}, removed=frozenset({urn})
    )

    svc = build_fixture_service()
    source = make_source(svc, graph=graph)
    aspects = run(source)

    status = aspects[urn]["status"]
    assert isinstance(status, StatusClass) and status.removed is False
    assert source.report.documents_resurrected == 1
    assert DOC_RUNBOOK not in exported_ids(svc)


def test_document_limit_stops_the_run_after_emitting_the_document() -> None:
    source = make_source(build_fixture_service())

    def hit_limit(**_: Any) -> Iterable[MetadataWorkUnit]:
        source.chunking_source.report.num_documents_limit_reached = True
        raise RuntimeError("max_documents reached")

    aspects: Aspects = {}
    with pytest.raises(RuntimeError):
        run(source, embed=hit_limit, aspects=aspects)

    assert source.report.num_documents_limit_reached
    assert source.report.documents_emitted == 1
    assert source.report.failures
    emitted_docs = [
        fid
        for fid in (DOC_A1, DOC_RUNBOOK, DOC_IMAGES, DOC_HTML, DOC_B1, DOC_B2)
        if "documentInfo" in aspects.get(make_document_urn(fid), {})
    ]
    assert len(emitted_docs) == 1


def test_failed_document_is_reported_and_the_run_continues() -> None:
    source = make_source(build_fixture_service())
    real_export = source.client.export_markdown

    def export(file_id: str) -> ExportResult:
        if file_id == DOC_RUNBOOK:
            raise RuntimeError("boom")
        return real_export(file_id)

    with mock.patch.object(source.client, "export_markdown", side_effect=export):
        aspects = run(source)

    assert source.report.documents_failed == 1
    assert any(DOC_RUNBOOK in failed for failed in source.report.failed_documents)
    assert make_document_urn(DOC_B1) in aspects


def test_stored_documents_are_read_in_pages() -> None:
    first = run(make_source(build_fixture_service()))
    infos = {make_document_urn(DOC_RUNBOOK): doc_info(first, DOC_RUNBOOK)}
    graph = graph_returning(infos)

    with mock.patch(f"{SOURCE_MODULE}.CHANGE_DETECTION_BATCH_SIZE", 2):
        source = make_source(build_fixture_service(), graph=graph)
        run(source)

    documents = source.report.documents_discovered - source.report.documents_filtered
    assert graph.get_entities.call_count == -(-documents // 2)
    assert source.report.change_detection_batch_reads == graph.get_entities.call_count
    assert source.report.documents_skipped_unchanged == 1


def test_failed_batch_read_falls_back_to_individual_reads() -> None:
    first = run(make_source(build_fixture_service()))
    urn = make_document_urn(DOC_RUNBOOK)
    infos = {urn: doc_info(first, DOC_RUNBOOK)}
    removed = frozenset({urn})
    graph = graph_returning(infos, removed)
    graph.get_entities.side_effect = ConnectionError("batchGet unavailable")
    graph.get_entity_semityped.side_effect = lambda urn, aspects: stored_aspects(
        urn, infos, removed
    )

    svc = build_fixture_service()
    source = make_source(svc, graph=graph)
    aspects = run(source)

    assert source.report.change_detection_batch_fallbacks >= 1
    assert DOC_RUNBOOK not in exported_ids(svc)
    # The soft-deleted state still comes through on the slower path.
    assert source.report.documents_resurrected == 1
    status = aspects[urn]["status"]
    assert isinstance(status, StatusClass) and status.removed is False


def test_change_detection_read_failure_falls_back_to_export() -> None:
    graph = mock.MagicMock(spec=DataHubGraph)
    graph.get_entities.side_effect = ConnectionError("gms unavailable")
    graph.get_entity_semityped.side_effect = ConnectionError("gms unavailable")
    svc = build_fixture_service()
    source = make_source(svc, graph=graph)
    run(source)

    assert DOC_RUNBOOK in exported_ids(svc)
    assert source.report.change_detection_lookup_failures == len(
        {make_document_urn(fid) for fid in exported_ids(svc)}
    )


def test_without_root_documents_root_children_attach_to_parent_document() -> None:
    parent = "urn:li:document:handbook"
    aspects = run(
        make_source(
            build_fixture_service(),
            create_root_documents=False,
            parent_document_urn=parent,
        )
    )

    assert make_document_urn(ROOT_A) not in aspects
    assert make_document_urn(SHARED_B) not in aspects
    a1_parent = doc_info(aspects, DOC_A1).parentDocument
    assert a1_parent is not None and a1_parent.document == parent
    runbook_parent = doc_info(aspects, DOC_RUNBOOK).parentDocument
    assert runbook_parent is not None
    assert runbook_parent.document == make_document_urn(ENG)


def test_native_and_external_import_modes() -> None:
    external = doc_info(run(make_source(build_fixture_service())), DOC_A1)
    assert external.source is not None
    assert external.source.sourceType == "EXTERNAL"
    assert external.source.externalId == DOC_A1

    native = doc_info(
        run(make_source(build_fixture_service(), document_import_mode="NATIVE")),
        DOC_A1,
    )
    assert native.source is not None
    assert native.source.sourceType == "NATIVE"
    assert native.source.externalId is None


def test_inaccessible_root_is_a_failure_and_other_roots_continue() -> None:
    source = make_source(
        build_fixture_service(), root_folder_ids=["missingRoot0", SHARED_B]
    )
    aspects = run(source)

    assert source.report.roots_failed == 1
    assert source.report.failures
    assert make_document_urn(DOC_B1) in aspects


def test_continue_on_failure_false_reraises() -> None:
    source = make_source(build_fixture_service(), continue_on_failure=False)
    with (
        mock.patch.object(
            source.client, "export_markdown", side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError),
    ):
        run(source)


def connection_report(
    config: Optional[Dict[str, Any]] = None,
    auth_error: Optional[Exception] = None,
) -> "api_source.TestConnectionReport":
    with (
        mock.patch(
            "datahub.ingestion.source.google_drive.google_drive_client.build_drive_service",
            return_value=build_fixture_service(),
            side_effect=auth_error,
        ),
        mock.patch.object(
            GoogleDriveSource,
            "_test_semantic_search_capability",
            return_value=CapabilityReport(capable=True),
        ),
    ):
        return run_test_connection(
            GoogleDriveSource, config or {"root_folder_ids": [ROOT_A, SHARED_B]}
        )


def test_connection_succeeds_when_every_root_is_reachable() -> None:
    assert_basic_connectivity_success(connection_report())


def test_connection_names_inaccessible_roots() -> None:
    report = connection_report(config={"root_folder_ids": [ROOT_A, "missingRoot0"]})
    assert_basic_connectivity_failure(report, "missingRoot0")
    assert report.basic_connectivity is not None
    assert ROOT_A not in (report.basic_connectivity.failure_reason or "")


def test_connection_reports_authentication_failure() -> None:
    report = connection_report(auth_error=ValueError("invalid key"))
    assert_basic_connectivity_failure(report, "invalid key")


def test_semantic_search_capability_reports_unusable_embedding_config() -> None:
    config = GoogleDriveSourceConfig.model_validate(
        {"root_folder_ids": [ROOT_A], "embedding": {"provider": "bedrock"}}
    )
    with mock.patch.object(
        DocumentChunkingSource,
        "test_embedding_capability",
        side_effect=ValueError("model access denied"),
    ):
        capability = GoogleDriveSource._test_semantic_search_capability(config)
    assert not capability.capable
    assert capability.failure_reason


def test_connection_reports_invalid_config() -> None:
    report = connection_report(config={"root_folder_ids": ["not a drive id!"]})
    assert report.internal_failure
