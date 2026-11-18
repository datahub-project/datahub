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


def _from_okf(attribution: Optional[MetadataAttributionClass]) -> bool:
    return bool(attribution and attribution.sourceDetail.get("origin") == "okf")


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


_SYSTEM_OWNERSHIP_TYPE = "urn:li:ownershipType:__system__"


def _owner_role(owner: OwnerClass) -> str:
    # The UI stores a system role only as a type URN (type NONE), e.g.
    # __system__technical_owner for TECHNICAL_OWNER.
    if owner.typeUrn and owner.typeUrn.startswith(_SYSTEM_OWNERSHIP_TYPE):
        return owner.typeUrn.removeprefix(_SYSTEM_OWNERSHIP_TYPE).upper()
    return owner.typeUrn or str(owner.type or "")


def _is_handover(existing: Association, added: Association) -> bool:
    """An unattributed DataHub edit of the value the file now declares."""
    if existing.attribution is not None:
        return False
    if isinstance(existing, OwnerClass) and isinstance(added, OwnerClass):
        # A different role (e.g. business owner) is a separate DataHub fact.
        return _owner_role(existing) == _owner_role(added)
    return True


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
    if ctx.graph:
        try:
            current = _current_associations(patcher.urn, aspect_name, ctx)
        except Exception as exc:
            allow_removals = False
            report.warning(
                title="Could not read existing OKF associations",
                message="Only additions will be emitted for this aspect.",
                context=f"document={patcher.urn}, aspect={aspect_name}",
                exc=exc,
            )
    else:
        allow_removals = False

    if allow_removals:
        for association in current:
            if (
                _from_okf(association.attribution)
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

        # One document identifies one file. Move its OKF value to the current source
        # key even when another unresolved value makes missing-value removal unsafe.
        # An unattributed value in the same role (a DataHub edit, e.g. written to
        # the file by sync-back) is handed over to the file the same way.
        for existing in current:
            if (
                (_from_okf(existing.attribution) or _is_handover(existing, added))
                and _association_urn(existing) == urn
                and _association_key(existing) != _association_key(added)
            ):
                _remove_association(patcher, existing)

        # Do not claim an association another source attributed that shares the
        # patch key; an unattributed one at the same key is simply overwritten.
        if any(
            _association_key(existing) == _association_key(added)
            and existing.attribution is not None
            and not _from_okf(existing.attribution)
            for existing in current
        ):
            continue
        if isinstance(added, OwnerClass):
            patcher.add_owner(added)
        elif isinstance(added, TagAssociationClass):
            patcher.add_tag(added)
        else:
            patcher.add_term(added)
