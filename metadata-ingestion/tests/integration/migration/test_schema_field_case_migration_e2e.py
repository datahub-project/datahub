"""End-to-end reconciliation tests for the schema-field-case migration against an
in-memory fake graph. The store stands in for GMS: writes are reflected so
re-reads are consistent, soft-deletes are honoured by discovery, and schemaField
entities are discovered via their parent, exactly as the real graph behaves."""

from typing import Dict, Iterator, List, Optional, Type

import pytest

from datahub.cli.schema_field_case_migration import (
    discover_dataset_urns,
    discover_schema_field_urns,
    run_migration,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn as sf,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.metadata.schema_classes import (
    AiContextClass,
    AuditStampClass,
    BusinessAttributeAssociationClass,
    BusinessAttributesClass,
    DeprecationClass,
    DocumentationAssociationClass,
    DocumentationClass,
    DomainsClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
    _Aspect,
)

pytestmark = pytest.mark.integration

INSTANCE = "ci"
AUDIT = AuditStampClass(time=0, actor="urn:li:corpuser:datahub")
TAG = "urn:li:tag:pii"
TERM = "urn:li:glossaryTerm:t1"


class FakeGraph:
    def __init__(self) -> None:
        self.store: Dict[str, Dict[str, _Aspect]] = {}
        self.soft_deleted: set = set()

        class _Config:
            server = "http://fake"

        self.config = _Config()

    def add(self, urn: str, aspects: Dict[str, _Aspect]) -> None:
        self.store.setdefault(urn, {}).update(aspects)

    def get_aspect(
        self, entity_urn: str, aspect_type: Type[_Aspect], version: int = 0
    ) -> Optional[_Aspect]:
        return self.store.get(entity_urn, {}).get(aspect_type.ASPECT_NAME)

    def get_entity_semityped(
        self, entity_urn: str, aspects: Optional[List[str]] = None
    ) -> Dict[str, _Aspect]:
        stored = self.store.get(entity_urn, {})
        return {
            name: a for name, a in stored.items() if aspects is None or name in aspects
        }

    def get_urns_by_filter(
        self,
        *,
        entity_types: List[str],
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        extraFilters: Optional[List[Dict[str, object]]] = None,
        status: object = RemovedStatusFilter.NOT_SOFT_DELETED,
        **kw: object,
    ) -> Iterator[str]:
        exclude_deleted = status == RemovedStatusFilter.NOT_SOFT_DELETED
        want_parent = extraFilters[0]["values"][0] if extraFilters else None  # type: ignore[index]
        out: List[str] = []
        for urn in self.store:
            if exclude_deleted and urn in self.soft_deleted:
                continue
            if "schemaField" in entity_types:
                if not urn.startswith("urn:li:schemaField:"):
                    continue
                if want_parent is not None and want_parent not in urn:
                    continue
            elif "dataset" in entity_types:
                if not urn.startswith("urn:li:dataset:"):
                    continue
                if platform is not None and f"dataPlatform:{platform}," not in urn:
                    continue
            else:
                continue
            out.append(urn)
        return iter(out)

    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
        assert mcp.entityUrn is not None
        aspect = mcp.aspect
        assert aspect is not None
        self.store.setdefault(mcp.entityUrn, {})[aspect.ASPECT_NAME] = aspect
        self.soft_deleted.discard(mcp.entityUrn)

    def soft_delete_entity(self, urn: str, **kw: object) -> None:
        self.soft_deleted.add(urn)


def _ds(name: str, platform: str = "snowflake") -> str:
    return make_dataset_urn_with_platform_instance(platform, name, INSTANCE, "PROD")


def _schema(dataset_urn: str, *paths: str) -> Dict[str, _Aspect]:
    return {
        "schemaMetadata": SchemaMetadataClass(
            schemaName="s",
            platform=f"urn:li:dataPlatform:{dataset_urn.split('dataPlatform:')[1].split(',')[0]}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=p,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="VARCHAR",
                )
                for p in paths
            ],
        )
    }


def _doc(text: str) -> DocumentationClass:
    return DocumentationClass(
        documentations=[DocumentationAssociationClass(documentation=text)]
    )


def _tags(*urns: str) -> GlobalTagsClass:
    return GlobalTagsClass(tags=[TagAssociationClass(tag=u) for u in urns])


def _attr_tag(urn: str) -> TagAssociationClass:
    return TagAssociationClass(
        tag=urn,
        attribution=MetadataAttributionClass(
            time=0,
            actor="urn:li:corpuser:__datahub_system",
            source="urn:li:dataHubAction:propagation",
        ),
    )


def _terms(*urns: str) -> GlossaryTermsClass:
    return GlossaryTermsClass(
        terms=[GlossaryTermAssociationClass(urn=u) for u in urns], auditStamp=AUDIT
    )


def _full_governance() -> Dict[str, _Aspect]:
    return {
        "documentation": _doc("column doc"),
        "globalTags": _tags(TAG),
        "glossaryTerms": _terms(TERM),
        "structuredProperties": StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:p", values=["yes"]
                )
            ]
        ),
        "businessAttributes": BusinessAttributesClass(
            businessAttribute=BusinessAttributeAssociationClass(
                businessAttributeUrn="urn:li:businessAttribute:ba"
            )
        ),
        "deprecation": DeprecationClass(
            deprecated=True, note="x", actor="urn:li:corpuser:datahub"
        ),
        "ownership": OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:datahub",
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        ),
        "domains": DomainsClass(domains=["urn:li:domain:d"]),
        "aiContext": AiContextClass(synonyms=["c"], instructions="i", examples=["e"]),
    }


D_BASIC = _ds("orders")
D_NESTED = _ds("nested")
D_V2 = _ds("v2")
D_COLLIDE = _ds("collide")
D_OK = _ds("already_ok")
D_ORPHAN = _ds("orphan")
D_MERGE = _ds("merge")
D_ORACLE = _ds("people", platform="oracle")
D_UNION = _ds("union")
D_CONFLICT = _ds("conflict")
D_RICH = _ds("rich")

V2_NEW = "[version=2.0].[type=struct].[type=string].Product2Id"
V2_OLD = "[version=2.0].[type=struct].[type=string].product2id"


def _seed() -> FakeGraph:
    g = FakeGraph()

    g.add(D_BASIC, _schema(D_BASIC, "Cust_Id", "Order_Amount", "Product2Id"))
    g.add(sf(D_BASIC, "product2id"), _full_governance())
    g.add(
        D_BASIC,
        {
            "editableSchemaMetadata": EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath="product2id",
                        description="product id",
                        globalTags=_tags(TAG),
                    ),
                    EditableSchemaFieldInfoClass(
                        fieldPath="order_amount", description="amount"
                    ),
                ]
            )
        },
    )

    g.add(D_NESTED, _schema(D_NESTED, "address.PostCode", "address.City"))
    g.add(sf(D_NESTED, "address.postcode"), {"documentation": _doc("nested")})

    g.add(D_V2, _schema(D_V2, V2_NEW))
    g.add(
        sf(D_V2, V2_OLD),
        {"documentation": _doc("v2"), "aiContext": AiContextClass(synonyms=["s"])},
    )

    g.add(D_COLLIDE, _schema(D_COLLIDE, "MixedCol", "MIXEDCOL"))
    g.add(sf(D_COLLIDE, "mixedcol"), {"documentation": _doc("ambiguous")})

    g.add(D_OK, _schema(D_OK, "Product2Id"))
    g.add(sf(D_OK, "Product2Id"), {"documentation": _doc("ok")})

    g.add(D_ORPHAN, _schema(D_ORPHAN, "Amount"))
    g.add(sf(D_ORPHAN, "removed_col"), {"documentation": _doc("orphan")})
    g.add(
        D_ORPHAN,
        {
            "editableSchemaMetadata": EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath="removed_col", description="orphan editable"
                    )
                ]
            )
        },
    )

    g.add(D_MERGE, _schema(D_MERGE, "Product2Id"))
    g.add(
        D_MERGE,
        {
            "editableSchemaMetadata": EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath="Product2Id",
                        description="kept",
                        globalTags=_tags("urn:li:tag:keep"),
                    ),
                    EditableSchemaFieldInfoClass(
                        fieldPath="product2id",
                        description="stale",
                        globalTags=_tags(TAG),
                    ),
                ]
            )
        },
    )

    g.add(D_ORACLE, _schema(D_ORACLE, "First_Name", "Last_Name"))
    g.add(
        sf(D_ORACLE, "first_name"),
        {"documentation": _doc("oracle"), "globalTags": _tags(TAG)},
    )

    # destination already carries an attributed (propagated/immutable) tag; the old
    # field has a bare UI dup + a UI-only tag → union, attribution preserved.
    g.add(D_UNION, _schema(D_UNION, "Col"))
    g.add(sf(D_UNION, "col"), {"globalTags": _tags(TAG, "urn:li:tag:ui_only")})
    g.add(sf(D_UNION, "Col"), {"globalTags": GlobalTagsClass(tags=[_attr_tag(TAG)])})

    # destination already has a different documentation → conflict, source kept.
    g.add(D_CONFLICT, _schema(D_CONFLICT, "Col"))
    g.add(sf(D_CONFLICT, "col"), {"documentation": _doc("ui doc")})
    g.add(sf(D_CONFLICT, "Col"), {"documentation": _doc("propagated doc")})

    # multiple glossary terms + multiple (multi-value) structured properties.
    g.add(D_RICH, _schema(D_RICH, "Col"))
    g.add(
        sf(D_RICH, "col"),
        {
            "glossaryTerms": _terms(
                "urn:li:glossaryTerm:t1",
                "urn:li:glossaryTerm:t2",
                "urn:li:glossaryTerm:t3",
            ),
            "structuredProperties": StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:p1", values=["a", "b"]
                    ),
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:p2", values=["x"]
                    ),
                ]
            ),
        },
    )

    return g


ALL = [D_BASIC, D_NESTED, D_V2, D_COLLIDE, D_OK, D_ORPHAN, D_MERGE, D_ORACLE]


def test_full_reconciliation_across_scenarios() -> None:
    g = _seed()
    report = run_migration(
        g,  # type: ignore[arg-type]
        ALL,
        dry_run=False,
        delete_source=True,
        include_soft_deleted=False,
    )

    # basic: every governance aspect moved to the mixed-case field entity
    new_basic = sf(D_BASIC, "Product2Id")
    for name in _full_governance():
        assert g.store[new_basic].get(name) is not None, name
    assert sf(D_BASIC, "product2id") in g.soft_deleted
    esm = g.store[D_BASIC]["editableSchemaMetadata"]
    assert isinstance(esm, EditableSchemaMetadataClass)
    assert {i.fieldPath for i in esm.editableSchemaFieldInfo} == {
        "Product2Id",
        "Order_Amount",
    }

    # nested + v2 both re-anchored onto the full current path
    assert g.store[sf(D_NESTED, "address.PostCode")].get("documentation") is not None
    assert g.store[sf(D_V2, V2_NEW)].get("documentation") is not None
    assert g.store[sf(D_V2, V2_NEW)].get("aiContext") is not None

    # collision + orphan refused, left in place, surfaced for review
    assert g.store[sf(D_COLLIDE, "mixedcol")].get("documentation") is not None
    assert "schemaMetadata" not in g.store.get(sf(D_COLLIDE, "MixedCol"), {})
    assert g.store[sf(D_ORPHAN, "removed_col")].get("documentation") is not None
    review_urns = {r.dataset_urn for r in report.results if r.skipped}
    assert D_COLLIDE in review_urns and D_ORPHAN in review_urns

    # editable merge: single entry, description kept, tags unioned
    merge_esm = g.store[D_MERGE]["editableSchemaMetadata"]
    assert isinstance(merge_esm, EditableSchemaMetadataClass)
    merged = merge_esm.editableSchemaFieldInfo
    assert len(merged) == 1
    assert merged[0].description == "kept"
    assert merged[0].globalTags is not None
    assert {t.tag for t in merged[0].globalTags.tags} == {"urn:li:tag:keep", TAG}

    # connector-agnostic: oracle moved too
    assert g.store[sf(D_ORACLE, "First_Name")].get("documentation") is not None

    assert {r.dataset_urn for r in report.results} == set(ALL)  # every input scanned
    total = sum(len(r.remaps) for r in report.results)
    assert (
        total == 6
    )  # basic, nested, v2, merge(editable), oracle, + basic order_amount


def test_idempotent_second_run_is_noop() -> None:
    g = _seed()
    run_migration(
        g,  # type: ignore[arg-type]
        ALL,
        dry_run=False,
        delete_source=True,
        include_soft_deleted=False,
    )
    second = run_migration(
        g,  # type: ignore[arg-type]
        ALL,
        dry_run=False,
        delete_source=True,
        include_soft_deleted=False,
    )
    assert sum(len(r.remaps) for r in second.results) == 0
    assert all(not r.editable_updated for r in second.results)


def test_dry_run_writes_nothing() -> None:
    g = _seed()
    before = {u: dict(a) for u, a in g.store.items()}
    report = run_migration(
        g,  # type: ignore[arg-type]
        ALL,
        dry_run=True,
        delete_source=True,
        include_soft_deleted=False,
    )
    assert g.store == before
    assert not g.soft_deleted
    assert sum(len(r.remaps) for r in report.results) > 0  # still reports intended work


def test_discovery_helpers() -> None:
    g = _seed()
    datasets = discover_dataset_urns(
        g,  # type: ignore[arg-type]
        platform="snowflake",
        platform_instance=None,
        env=None,
    )
    assert D_BASIC in datasets
    assert D_ORACLE not in datasets  # platform-scoped

    fields = discover_schema_field_urns(g, D_BASIC, include_soft_deleted=False)  # type: ignore[arg-type]
    assert sf(D_BASIC, "product2id") in fields

    # after a live run the stale field is soft-deleted and excluded from discovery
    run_migration(
        g,  # type: ignore[arg-type]
        [D_BASIC],
        dry_run=False,
        delete_source=True,
        include_soft_deleted=False,
    )
    fields_after = discover_schema_field_urns(g, D_BASIC, include_soft_deleted=False)  # type: ignore[arg-type]
    assert sf(D_BASIC, "product2id") not in fields_after


def test_entity_merge_guard_scenarios() -> None:
    g = _seed()
    report = run_migration(
        g,  # type: ignore[arg-type]
        [D_UNION, D_CONFLICT, D_RICH],
        dry_run=False,
        delete_source=True,
        include_soft_deleted=False,
    )

    # union: both tags present, the attributed (immutable) one preserved, source gone
    union_tags = g.store[sf(D_UNION, "Col")]["globalTags"]
    assert isinstance(union_tags, GlobalTagsClass)
    by_urn = {t.tag: t for t in union_tags.tags}
    assert set(by_urn) == {TAG, "urn:li:tag:ui_only"}
    assert by_urn[TAG].attribution is not None
    assert sf(D_UNION, "col") in g.soft_deleted

    # conflict: destination doc untouched, source kept, surfaced for review
    conflict_doc = g.store[sf(D_CONFLICT, "Col")]["documentation"]
    assert conflict_doc == _doc("propagated doc")
    assert sf(D_CONFLICT, "col") not in g.soft_deleted
    assert any(r.dataset_urn == D_CONFLICT and r.skipped for r in report.results)

    # rich: all three terms + both structured properties carried wholesale
    terms = g.store[sf(D_RICH, "Col")]["glossaryTerms"]
    assert isinstance(terms, GlossaryTermsClass)
    assert len(terms.terms) == 3
    props = g.store[sf(D_RICH, "Col")]["structuredProperties"]
    assert isinstance(props, StructuredPropertiesClass)
    assert {p.propertyUrn for p in props.properties} == {
        "urn:li:structuredProperty:p1",
        "urn:li:structuredProperty:p2",
    }
    assert sf(D_RICH, "col") in g.soft_deleted


def test_include_soft_deleted_rediscovers_and_remigrates() -> None:
    # A stale field left soft-deleted by an earlier run (or a prior connector
    # delete) is invisible to default discovery, so its still-live aspects are
    # stranded. --include-soft-deleted must find and re-anchor it.
    ds = _ds("db.sch.reanchor")
    old_sf = sf(ds, "product2id")
    g = FakeGraph()
    g.add(ds, _schema(ds, "Product2Id"))
    g.add(old_sf, {"documentation": _doc("stranded on a soft-deleted field")})
    g.soft_deleted.add(old_sf)

    assert old_sf not in discover_schema_field_urns(g, ds, include_soft_deleted=False)  # type: ignore[arg-type]
    assert old_sf in discover_schema_field_urns(g, ds, include_soft_deleted=True)  # type: ignore[arg-type]

    run_migration(
        g,  # type: ignore[arg-type]
        [ds],
        dry_run=False,
        delete_source=True,
        include_soft_deleted=True,
    )
    assert g.store[sf(ds, "Product2Id")]["documentation"] == _doc(
        "stranded on a soft-deleted field"
    )
