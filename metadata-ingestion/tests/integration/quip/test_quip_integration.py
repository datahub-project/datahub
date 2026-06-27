from typing import Dict, List

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quip.quip_config import QuipSourceConfig
from datahub.ingestion.source.quip.quip_source import QuipSource
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    DocumentInfoClass,
    SubTypesClass,
)
from tests.integration.quip.quip_mock import build_fake_client

pytestmark = pytest.mark.integration


def _run() -> List[MetadataWorkUnit]:
    config = QuipSourceConfig.model_validate(
        {"access_token": "secret", "platform_instance": "acme"}
    )
    source = QuipSource(config, PipelineContext(run_id="quip-integration"))
    # Only the Quip API is mocked; the rest of the pipeline (document building,
    # inline chunking with no embedding provider) runs for real.
    source.client = build_fake_client()  # type: ignore[assignment]
    return list(source.get_workunits())


def _doc_info_by_urn(
    workunits: List[MetadataWorkUnit],
) -> Dict[str, DocumentInfoClass]:
    result: Dict[str, DocumentInfoClass] = {}
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, DocumentInfoClass
        ):
            result[str(wu.metadata.entityUrn)] = wu.metadata.aspect
    return result


def _subtypes_by_urn(workunits: List[MetadataWorkUnit]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, SubTypesClass
        ):
            result[str(wu.metadata.entityUrn)] = wu.metadata.aspect.typeNames
    return result


@pytest.fixture(scope="module")
def workunits() -> List[MetadataWorkUnit]:
    return _run()


def test_platform_info_emitted(workunits: List[MetadataWorkUnit]) -> None:
    platform_infos = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DataPlatformInfoClass)
    ]
    assert len(platform_infos) == 1
    assert platform_infos[0].displayName == "Quip"
    # Must match the bootstrap data-platforms.yaml seed so the UI delimiter is
    # stable before and after the first ingestion run.
    assert platform_infos[0].datasetNameDelimiter == "."


def test_folders_and_threads_ingested(workunits: List[MetadataWorkUnit]) -> None:
    docs = _doc_info_by_urn(workunits)
    expected = {
        "urn:li:document:quip-acme-folder-FROOT",
        "urn:li:document:quip-acme-folder-FSUB",
        "urn:li:document:quip-acme-folder-FSHARED",
        "urn:li:document:quip-acme-T1",
        "urn:li:document:quip-acme-T2",
        "urn:li:document:quip-acme-T3",
    }
    assert expected.issubset(docs.keys())


def _parent_urn(doc_info: DocumentInfoClass) -> str:
    assert doc_info.parentDocument is not None
    return doc_info.parentDocument.document


def test_hierarchy_parent_relationships(workunits: List[MetadataWorkUnit]) -> None:
    docs = _doc_info_by_urn(workunits)

    # Sub-folder is parented to the root folder; root folders have no parent.
    assert (
        _parent_urn(docs["urn:li:document:quip-acme-folder-FSUB"])
        == "urn:li:document:quip-acme-folder-FROOT"
    )
    assert docs["urn:li:document:quip-acme-folder-FROOT"].parentDocument is None

    # Threads are parented to their (shallowest) containing folder. T1 lives in
    # both FROOT and FSUB but resolves to FROOT.
    assert (
        _parent_urn(docs["urn:li:document:quip-acme-T1"])
        == "urn:li:document:quip-acme-folder-FROOT"
    )
    assert (
        _parent_urn(docs["urn:li:document:quip-acme-T2"])
        == "urn:li:document:quip-acme-folder-FSUB"
    )


def test_subtypes_and_content_hash(workunits: List[MetadataWorkUnit]) -> None:
    subtypes = _subtypes_by_urn(workunits)
    assert subtypes["urn:li:document:quip-acme-folder-FROOT"] == ["Folder"]
    assert subtypes["urn:li:document:quip-acme-T1"] == ["Document"]

    docs = _doc_info_by_urn(workunits)
    t1_props = docs["urn:li:document:quip-acme-T1"].customProperties
    assert len(t1_props["content_hash"]) == 64
    assert t1_props["quip_type"] == "document"
