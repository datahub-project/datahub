import copy
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, cast
from unittest import mock

import pytest
from googleapiclient.discovery import Resource

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
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
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DocumentInfoClass,
    OwnershipClass,
    StatusClass,
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
    ROOT_A,
    SHARED_B,
    SPECS,
    FakeDriveService,
    build_fixture_service,
)

Aspects = Dict[str, Dict[str, Any]]


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


def run(source: GoogleDriveSource) -> Aspects:
    aspects: Aspects = {}
    with mock.patch.object(
        source.chunking_source,
        "process_elements_inline",
        side_effect=lambda **_: iter([]),
    ):
        workunits: Iterable[MetadataWorkUnit] = source.get_workunits_internal()
        for wu in workunits:
            mcp = wu.metadata
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            assert mcp.entityUrn is not None and mcp.aspect is not None
            aspects.setdefault(mcp.entityUrn, {})[mcp.aspect.get_aspect_name()] = (
                mcp.aspect
            )
    return aspects


def doc_info(aspects: Aspects, file_id: str) -> DocumentInfoClass:
    info = aspects[make_document_urn(file_id)]["documentInfo"]
    assert isinstance(info, DocumentInfoClass)
    return info


def graph_returning(
    infos: Dict[str, DocumentInfoClass], removed: FrozenSet[str] = frozenset()
) -> DataHubGraph:
    def stored(urn: str, aspects: List[str]) -> Dict[str, object]:
        bag: Dict[str, object] = {"status": StatusClass(removed=urn in removed)}
        if urn in infos:
            bag["documentInfo"] = infos[urn]
        return bag

    graph = mock.MagicMock(spec=DataHubGraph)
    graph.get_entity_semityped.side_effect = stored
    # Soft-deleted state must come from the entity, not from (lagging) search.
    graph.get_urns_by_filter.side_effect = AssertionError("search must not be used")
    return cast(DataHubGraph, graph)


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
    assert any(w.title == "Google Doc too large to export" for w in report.warnings)

    specs_info = doc_info(aspects, SPECS)
    assert specs_info.parentDocument is not None
    assert specs_info.customProperties["drive_folder_path"] == "Root A/Eng/Specs"
    assert doc_info(aspects, SHARED_B).title == "Shared Drive B"

    browse = aspects[make_document_urn(DOC_IMAGES)]["browsePathsV2"]
    assert isinstance(browse, BrowsePathsV2Class)
    assert [e.id for e in browse.path] == ["Root A", "Eng", "Specs"]


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

    aspects: Aspects = {}
    with mock.patch.object(
        source.chunking_source, "process_elements_inline", side_effect=fail
    ):
        for wu in source.get_workunits_internal():
            mcp = wu.metadata
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            assert mcp.entityUrn is not None and mcp.aspect is not None
            aspects.setdefault(mcp.entityUrn, {})[mcp.aspect.get_aspect_name()] = (
                mcp.aspect
            )

    info = doc_info(aspects, DOC_RUNBOOK)
    assert info.customProperties["drive_version"] == ""
    assert any(
        w.title == "Embedding generation deferred" for w in source.report.warnings
    )


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


def test_native_and_external_import_modes() -> None:
    external = doc_info(run(make_source(build_fixture_service())), DOC_A1)
    assert external.source is not None
    assert external.source.sourceType == "EXTERNAL"
    assert external.source.externalId == DOC_A1

    native = doc_info(
        run(make_source(build_fixture_service(), document_import_mode="NATIVE")),
        DOC_A1,
    )
    assert native.source is None or native.source.sourceType == "NATIVE"


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


@pytest.mark.parametrize(
    "failure_reason, kept_in_state",
    [("export_failed", True), ("too_large", False), ("not_found", False)],
)
def test_export_failure_state_handling(
    failure_reason: str, kept_in_state: bool
) -> None:
    source = make_source(build_fixture_service())
    with (
        mock.patch.object(
            source.client,
            "export_markdown",
            return_value=ExportResult(None, False, failure_reason),
        ),
        mock.patch.object(
            source.stale_entity_removal_handler, "add_entity_to_state"
        ) as add_to_state,
    ):
        aspects = run(source)

    urn = make_document_urn(DOC_RUNBOOK)
    assert "documentInfo" not in aspects.get(urn, {})
    registered = mock.call("document", urn) in add_to_state.call_args_list
    assert registered == kept_in_state
