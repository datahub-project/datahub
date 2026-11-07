from typing import List, Optional, Sequence, Tuple, Union

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.specific.document import DocumentPatchBuilder

Association = Union[OwnerClass, TagAssociationClass, GlossaryTermAssociationClass]


def _from_file(
    attribution: Optional[MetadataAttributionClass], repo: str, path: str
) -> bool:
    return bool(
        attribution
        and attribution.sourceDetail.get("origin") == "okf"
        and attribution.sourceDetail.get("repo") == repo
        and attribution.sourceDetail.get("path") == path
    )


def _association_urn(association: Association) -> str:
    if isinstance(association, OwnerClass):
        return association.owner
    if isinstance(association, TagAssociationClass):
        return association.tag
    return association.urn


def _association_key(association: Association) -> Tuple[str, ...]:
    source = association.attribution.source or "" if association.attribution else ""
    if isinstance(association, OwnerClass):
        return (
            association.owner,
            str(association.type or ""),
            association.typeUrn or "",
            source,
        )
    return (_association_urn(association), source)


def _current_associations(
    document_urn: str, aspect_name: str, ctx: PipelineContext
) -> Sequence[Association]:
    assert ctx.graph is not None
    if aspect_name == OwnershipClass.ASPECT_NAME:
        ownership = ctx.graph.get_aspect(document_urn, OwnershipClass)
        return ownership.owners if ownership else []
    if aspect_name == GlobalTagsClass.ASPECT_NAME:
        tags = ctx.graph.get_aspect(document_urn, GlobalTagsClass)
        return tags.tags if tags else []
    terms = ctx.graph.get_aspect(document_urn, GlossaryTermsClass)
    return terms.terms if terms else []


def _remove_association(
    patcher: DocumentPatchBuilder, association: Association
) -> None:
    source = association.attribution.source or "" if association.attribution else ""
    # Empty strings select the unattributed-source entry. None would remove
    # every source's association for this URN, including metadata added in DataHub.
    if isinstance(association, OwnerClass):
        patcher.remove_owner(
            association.owner,
            owner_type=association.type or "",
            type_urn=association.typeUrn or "",
            attribution_source=source,
        )
    elif isinstance(association, TagAssociationClass):
        patcher.remove_tag(association.tag, attribution_source=source)
    else:
        patcher.remove_term(association.urn, attribution_source=source)


def reconcile_okf_associations(
    patcher: DocumentPatchBuilder,
    *,
    aspect_name: str,
    urns: List[str],
    attribution: MetadataAttributionClass,
    allow_removals: bool,
    ctx: PipelineContext,
    report: GitHubDocumentsSourceReport,
) -> None:
    current: Sequence[Association] = []
    path = attribution.sourceDetail["path"]
    repo = attribution.sourceDetail["repo"]
    if ctx.graph:
        try:
            current = _current_associations(patcher.urn, aspect_name, ctx)
        except Exception as exc:
            allow_removals = False
            report.warning(
                title="Could not read existing OKF associations",
                message="Only additions will be emitted for this aspect.",
                context=f"path={path}, aspect={aspect_name}",
                exc=exc,
            )
    else:
        allow_removals = False

    if allow_removals:
        for association in current:
            if (
                _from_file(association.attribution, repo, path)
                and _association_urn(association) not in urns
            ):
                _remove_association(patcher, association)

    for urn in urns:
        added: Association
        if aspect_name == OwnershipClass.ASPECT_NAME:
            added = OwnerClass(
                owner=urn,
                type=OwnershipTypeClass.TECHNICAL_OWNER,
                attribution=attribution,
            )
        elif aspect_name == GlobalTagsClass.ASPECT_NAME:
            added = TagAssociationClass(tag=urn, attribution=attribution)
        else:
            added = GlossaryTermAssociationClass(urn=urn, attribution=attribution)

        # Do not claim an existing association that shares the patch key. This
        # matters especially when no ingestion-source URN is available: both
        # the file's and a manually added value would then use an empty source.
        if any(
            _association_key(existing) == _association_key(added)
            and not _from_file(existing.attribution, repo, path)
            for existing in current
        ):
            continue
        if isinstance(added, OwnerClass):
            patcher.add_owner(added)
        elif isinstance(added, TagAssociationClass):
            patcher.add_tag(added)
        else:
            patcher.add_term(added)
