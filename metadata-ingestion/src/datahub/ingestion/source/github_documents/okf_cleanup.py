from typing import Iterable

from datahub.emitter.mce_builder import SYSTEM_ACTOR, get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.okf_patches import (
    reconcile_okf_associations,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    OkfPropertiesClass,
    OwnershipClass,
    SubTypesClass,
)
from datahub.specific.document import DocumentPatchBuilder


def clear_okf_metadata(
    document_urn: str, *, ctx: PipelineContext, report: GitHubDocumentsSourceReport
) -> Iterable[MetadataWorkUnit]:
    if ctx.graph is None:
        return
    try:
        stored = ctx.graph.get_aspect(document_urn, OkfPropertiesClass)
    except Exception as exc:
        report.warning(
            title="Could not read previous OKF metadata",
            message="Existing OKF metadata could not be cleared for this document.",
            context=document_urn,
            exc=exc,
        )
        return
    if not isinstance(stored, OkfPropertiesClass) or stored.raw is None:
        return

    try:
        subtypes = ctx.graph.get_aspect(document_urn, SubTypesClass)
        if stored.type is not None and subtypes and stored.type in subtypes.typeNames:
            yield MetadataChangeProposalWrapper(
                entityUrn=document_urn,
                aspect=SubTypesClass(
                    typeNames=[t for t in subtypes.typeNames if t != stored.type]
                ),
            ).as_workunit()
    except Exception as exc:
        report.warning(
            title="Could not read previous OKF subtype",
            message="The document subtype was preserved because it could not be read.",
            context=document_urn,
            exc=exc,
        )

    patcher = DocumentPatchBuilder(document_urn)
    for aspect_name in [
        OwnershipClass.ASPECT_NAME,
        GlobalTagsClass.ASPECT_NAME,
        GlossaryTermsClass.ASPECT_NAME,
    ]:
        reconcile_okf_associations(
            patcher,
            aspect_name=aspect_name,
            urns=[],
            attribution=MetadataAttributionClass(
                time=get_sys_time(), actor=SYSTEM_ACTOR, sourceDetail={"origin": "okf"}
            ),
            allow_removals=True,
            ctx=ctx,
            report=report,
        )
    for index, patch in enumerate(patcher.build()):
        yield MetadataWorkUnit(
            id=f"{document_urn}-okf-cleanup-{patch.aspectName}-{index}", mcp_raw=patch
        )
    # Clear the marker last: if a removal above fails, the next run still sees
    # the stored aspect and retries the cleanup.
    yield MetadataChangeProposalWrapper(
        entityUrn=document_urn,
        aspect=OkfPropertiesClass(),
    ).as_workunit()
