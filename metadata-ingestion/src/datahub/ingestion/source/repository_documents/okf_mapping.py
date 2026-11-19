import re
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Set

from datahub.emitter.mce_builder import (
    SYSTEM_ACTOR,
    get_sys_time,
    make_tag_urn,
    make_user_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.repository_documents.okf_bundles import concept_id
from datahub.ingestion.source.repository_documents.okf_frontmatter import (
    OkfFrontmatter,
    is_okf_concept,
)
from datahub.ingestion.source.repository_documents.okf_metadata import (
    parse_stamp,
    parse_status,
    parse_timestamp,
    parse_verified,
)
from datahub.ingestion.source.repository_documents.okf_patches import (
    reconcile_okf_associations,
)
from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport,
)
from datahub.metadata.schema_classes import (
    GlossaryTermInfoClass,
    MetadataAttributionClass,
    MetadataChangeProposalClass,
    OkfBundleClass,
    OkfPropertiesClass,
    OkfRevisionClass,
    SubTypesClass,
    _Aspect,
)
from datahub.metadata.urns import Urn
from datahub.specific.document import DocumentPatchBuilder

MAPPING_VERSION = "6"
# The frontmatter keys whose values are references to other entities.
OkfReferenceKey = Literal["owners", "tags", "glossary_terms"]

# The reference keys and the aspect each one maps to.
REFERENCE_MAPPINGS: Dict[OkfReferenceKey, str] = {
    "owners": "ownership",
    "tags": "globalTags",
    "glossary_terms": "glossaryTerms",
}
NATIVE_MAPPINGS: Dict[str, str] = {
    "title": "documentInfo.title",
    "type": "subTypes",
    **{str(key): target for key, target in REFERENCE_MAPPINGS.items()},
}
EMAIL_PATTERN = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")


@dataclass
class OkfLookupCache:
    owners: Dict[str, bool] = field(default_factory=dict)
    terms: Dict[str, Optional[str]] = field(default_factory=dict)


@dataclass
class OkfMapping:
    title: Optional[str]
    aspects: List[_Aspect]
    patches: List[MetadataChangeProposalClass]


def _optional_string(values: Dict[str, object], key: str) -> Optional[str]:
    value = values.get(key)
    return value if isinstance(value, str) else None


def _resolve_term_name(name: str, ctx: PipelineContext) -> Optional[str]:
    if ctx.graph is None:
        return None
    candidates = ctx.graph.get_urns_by_filter(
        entity_types=["glossaryTerm"],
        extraFilters=[{"field": "name", "condition": "EQUAL", "values": [name]}],
    )
    matches: Set[str] = set()
    for urn in candidates:
        info = ctx.graph.get_aspect(urn, GlossaryTermInfoClass)
        if info is None:
            continue
        if info.name == name:
            matches.add(urn)
            if len(matches) > 1:
                return None
    return next(iter(matches)) if matches else None


def resolve_reference(
    value: str, key: OkfReferenceKey, ctx: PipelineContext, cache: OkfLookupCache
) -> Optional[str]:
    """The URN the OKF import writes for one ``owners``, ``tags`` or
    ``glossary_terms`` value, or None when the value is left unmapped.

    Owners and glossary terms are looked up in DataHub through ``ctx.graph``,
    with the results kept in ``cache``. Raises ``InvalidUrnError`` when a
    value starting with ``urn:`` is not a valid URN, and lets a failed catalog
    lookup raise.
    """
    if value.startswith("urn:"):
        parsed = Urn.from_string(value)
        if key == "owners":
            return value if parsed.entity_type in {"corpuser", "corpGroup"} else None
        expected = {"tags": "tag", "glossary_terms": "glossaryTerm"}
        return value if parsed.entity_type == expected[key] else None
    if key == "owners":
        urn = make_user_urn(value)
        # An unqualified owner could be either a user or a team. Only map it
        # when the catalog confirms that it identifies a user.
        if EMAIL_PATTERN.fullmatch(value):
            return urn
        if value not in cache.owners:
            exists = bool(ctx.graph and ctx.graph.exists(urn))
            cache.owners[value] = exists
        return urn if cache.owners[value] else None
    if key == "tags":
        # Tags are keyed by name and need not exist first, as in other sources.
        return make_tag_urn(value)
    if value not in cache.terms:
        term = _resolve_term_name(value, ctx)
        cache.terms[value] = term
    return cache.terms[value]


def map_okf(
    frontmatter: OkfFrontmatter,
    *,
    document_urn: str,
    bundle: OkfBundleClass,
    file_path: str,
    revision: OkfRevisionClass,
    attribution_source: Optional[str],
    ctx: PipelineContext,
    report: RepositoryDocumentsSourceReport,
    lookup_cache: Optional[OkfLookupCache] = None,
) -> Optional[OkfMapping]:
    cache = lookup_cache if lookup_cache is not None else OkfLookupCache()
    values = frontmatter.values
    concept = concept_id(file_path, bundle)

    def unresolved(key: str, message: Optional[str] = None) -> None:
        report.okf_unresolved_values += 1
        report.warning(
            title="Unresolved OKF value",
            message=message
            or (
                "A frontmatter value could not be mapped; it remains in "
                "okfProperties.raw."
            ),
            context=f"path={file_path}, key={key}",
        )

    if not is_okf_concept(frontmatter):
        unresolved(
            "type",
            "The file has no usable type, so it is imported as a plain "
            "document, not an OKF concept.",
        )
        return None
    concept_type = values["type"]
    assert isinstance(concept_type, str)

    aspects: List[_Aspect] = [
        OkfPropertiesClass(
            bundle=bundle,
            conceptId=concept,
            declaredId=_optional_string(values, "id"),
            type=concept_type,
            description=_optional_string(values, "description"),
            status=parse_status(values, path=file_path, report=report),
            staleAfter=(
                parse_timestamp(
                    values["stale_after"],
                    key="stale_after",
                    path=file_path,
                    report=report,
                )
                if "stale_after" in values
                else None
            ),
            generated=(
                parse_stamp(
                    values["generated"],
                    key="generated",
                    path=file_path,
                    report=report,
                )
                if "generated" in values
                else None
            ),
            verified=parse_verified(values, path=file_path, report=report),
            raw=frontmatter.raw,
            revision=revision,
        ),
        SubTypesClass(typeNames=[concept_type]),
    ]
    title: Optional[str] = None
    if "title" in values:
        title_value = values["title"]
        if isinstance(title_value, str) and title_value.strip():
            title = title_value
        else:
            unresolved("title")
    patcher = DocumentPatchBuilder(document_urn)
    for key, target in REFERENCE_MAPPINGS.items():
        value = values.get(key, [])
        entries: List[object]
        if value is None or (isinstance(value, str) and not value.strip()):
            entries = []
        elif isinstance(value, list):
            entries = value
        else:
            entries = [value]
        resolved: List[str] = []
        allow_removals = True
        for entry in entries:
            urn = None
            if isinstance(entry, str) and entry.strip():
                try:
                    urn = resolve_reference(entry.strip(), key, ctx, cache)
                except Exception:
                    # A failed catalog lookup or invalid URN must not discard
                    # the concept, including its original unresolved value.
                    urn = None
            if urn is None:
                unresolved(key)
                # A failed resolution does not prove that a previously mapped
                # value was removed from the file. Reconcile this key add-only.
                allow_removals = False
            elif urn not in resolved:
                resolved.append(urn)

        attribution = MetadataAttributionClass(
            time=get_sys_time(),
            actor=SYSTEM_ACTOR,
            source=attribution_source,
            sourceDetail={
                "origin": "okf",
                "bundle": bundle.location,
                "rootPath": bundle.rootPath,
                "conceptId": concept,
                **({"commit": revision.commit} if revision.commit else {}),
                "key": key,
                "mappingVersion": MAPPING_VERSION,
            },
        )
        reconcile_okf_associations(
            patcher,
            aspect_name=target,
            urns=resolved,
            attribution=attribution,
            allow_removals=allow_removals,
            ctx=ctx,
            report=report,
        )
    return OkfMapping(title=title, aspects=aspects, patches=patcher.build())
