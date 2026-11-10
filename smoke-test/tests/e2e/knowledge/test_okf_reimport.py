import logging
from collections import Counter
from typing import List, Set

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.okf_frontmatter import (
    parse_okf_frontmatter,
)
from datahub.ingestion.source.github_documents.okf_mapping import (
    MAPPING_VERSION,
    map_okf,
)
from datahub.metadata.schema_classes import (
    DocumentStateClass,
    GlobalTagsClass,
    OkfBundleClass,
    OkfPropertiesClass,
    OkfRevisionClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
    TagPropertiesClass,
)
from datahub.metadata.urns import DocumentUrn
from datahub.sdk.document import Document
from datahub.specific.document import DocumentPatchBuilder
from tests.e2e.utils import delete_urn, unique_suffix, with_test_retry
from utilities.domains import Domain

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.domain(Domain.INGESTION, Domain.AI)


def _frontmatter(owners: List[str], metrics_urn: str) -> str:
    return "\n".join(
        [
            "---",
            "id: guide:mau",
            "type: Guide",
            "title: Monthly active users",
            f"owners: [{', '.join(owners)}]",
            f"tags: [{metrics_urn}]",
            "---",
            "",
        ]
    )


def _import_okf(ctx: PipelineContext, document_urn: str, raw: str) -> None:
    assert ctx.graph is not None
    report = GitHubDocumentsSourceReport()
    parsed = parse_okf_frontmatter(
        raw + "# Monthly active users\n\nCount users with an event.\n",
        path="guides/mau.md",
        report=report,
    )
    assert parsed is not None
    mapping = map_okf(
        parsed,
        document_urn=document_urn,
        bundle=OkfBundleClass(
            location="https://github.com/example-org/analytics",
            rootPath=".",
            okfVersion="0.2",
        ),
        file_path="guides/mau.md",
        revision=OkfRevisionClass(ref="main", commit="a" * 40),
        attribution_source=f"urn:li:dataHubIngestionSource:{ctx.run_id}",
        ctx=ctx,
        report=report,
    )
    assert mapping is not None
    assert report.okf_unresolved_values == 0
    assert not report.warnings
    doc = Document.create_document(
        id=DocumentUrn(document_urn).id,
        title=mapping.title or "Monthly active users",
        text=parsed.body,
        status=DocumentStateClass.PUBLISHED,
    )
    for aspect in mapping.aspects:
        doc._set_aspect(aspect)
    for mcp in doc.as_mcps():
        ctx.graph.emit_mcp(mcp)
    for patch in mapping.patches:
        ctx.graph.emit_mcp(patch)


@with_test_retry()
def _assert_associations(
    graph: DataHubGraph,
    document_urn: str,
    expected_owners: Set[str],
    expected_tags: Set[str],
) -> None:
    ownership = graph.get_aspect(document_urn, OwnershipClass)
    tags = graph.get_aspect(document_urn, GlobalTagsClass)
    assert ownership is not None
    assert tags is not None
    assert Counter(owner.owner for owner in ownership.owners) == Counter(
        expected_owners
    )
    assert Counter(tag.tag for tag in tags.tags) == Counter(expected_tags)
    for association in [*ownership.owners, *tags.tags]:
        # Backend audit stamping is allowed; manually added values must never
        # acquire OKF provenance through re-import.
        if (
            isinstance(association, OwnerClass)
            and association.owner == "urn:li:corpuser:carol"
        ) or (
            isinstance(association, TagAssociationClass)
            and association.tag.endswith("-pii")
        ):
            assert (
                association.attribution is None
                or association.attribution.sourceDetail.get("origin") != "okf"
            )
        else:
            assert association.attribution is not None
            assert association.attribution.sourceDetail == {
                "origin": "okf",
                "bundle": "https://github.com/example-org/analytics",
                "rootPath": ".",
                "conceptId": "guides/mau",
                "commit": "a" * 40,
                "key": "owners" if isinstance(association, OwnerClass) else "tags",
                "mappingVersion": MAPPING_VERSION,
            }


@with_test_retry()
def _assert_okf_properties(graph: DataHubGraph, document_urn: str, raw: str) -> None:
    aspect = graph.get_aspect(document_urn, OkfPropertiesClass)
    assert aspect is not None
    assert aspect.declaredId == "guide:mau"
    assert aspect.conceptId == "guides/mau"
    assert aspect.bundle is not None
    assert aspect.bundle.location == "https://github.com/example-org/analytics"
    assert aspect.bundle.rootPath == "."
    assert aspect.bundle.okfVersion == "0.2"
    assert aspect.revision is not None
    assert aspect.revision.ref == "main"
    assert aspect.revision.commit == "a" * 40
    assert "mappingVersion" not in aspect.to_obj()
    assert aspect.type == "Guide"
    assert aspect.raw == raw


def test_okf_reimport_preserves_datahub_edits(graph_client: DataHubGraph) -> None:
    suffix = unique_suffix()
    document_urn = str(DocumentUrn(f"okf-reimport-{suffix}"))
    # Keep the labels from the scenario while isolating tag entities from other
    # smoke tests and from any pre-existing metrics/pii tags.
    metrics_urn = f"urn:li:tag:okf-{suffix}-metrics"
    pii_urn = f"urn:li:tag:okf-{suffix}-pii"
    alice = "urn:li:corpuser:alice@example.com"
    bob = "urn:li:corpuser:bob@example.com"
    carol = "urn:li:corpuser:carol"
    ctx = PipelineContext(run_id=f"okf-smoke-{suffix}", graph=graph_client)
    try:
        for urn, name in [(metrics_urn, "metrics"), (pii_urn, "pii")]:
            graph_client.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=TagPropertiesClass(name=name)
                )
            )

        raw = _frontmatter(["alice@example.com", "bob@example.com"], metrics_urn)
        _import_okf(ctx, document_urn, raw)
        _assert_associations(graph_client, document_urn, {alice, bob}, {metrics_urn})

        manual = DocumentPatchBuilder(document_urn)
        manual.add_owner(
            OwnerClass(owner=carol, type=OwnershipTypeClass.TECHNICAL_OWNER)
        )
        manual.add_tag(TagAssociationClass(tag=pii_urn))
        for patch in manual.build():
            graph_client.emit_mcp(patch)
        _assert_associations(
            graph_client, document_urn, {alice, bob, carol}, {metrics_urn, pii_urn}
        )

        _import_okf(ctx, document_urn, raw)
        _assert_associations(
            graph_client, document_urn, {alice, bob, carol}, {metrics_urn, pii_urn}
        )

        raw = _frontmatter(["alice@example.com"], metrics_urn)
        _import_okf(ctx, document_urn, raw)
        _assert_associations(
            graph_client, document_urn, {alice, carol}, {metrics_urn, pii_urn}
        )
        _assert_okf_properties(graph_client, document_urn, raw)
    finally:
        for urn in [document_urn, metrics_urn, pii_urn]:
            try:
                delete_urn(graph_client, urn)
            except Exception:
                logger.warning("Cleanup failed for %s", urn, exc_info=True)
