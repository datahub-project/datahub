"""Tests for datahub.cli.schema_field_case_migration (in-memory fake graph)."""

from pathlib import Path
from typing import Dict, Iterator, List, Optional, Type
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from datahub.cli.migrate import schema_field_case
from datahub.cli.schema_field_case_migration import (
    ClashResolver,
    InteractiveClashResolver,
    PathReconciler,
    reconcile_dataset,
    run_migration,
)
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
    _Aspect,
)

_DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_AUDIT = AuditStampClass(time=0, actor="urn:li:corpuser:datahub")


def _schema(*field_paths: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="s",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=fp,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
            )
            for fp in field_paths
        ],
    )


def _tags(*urns: str) -> GlobalTagsClass:
    return GlobalTagsClass(tags=[TagAssociationClass(tag=u) for u in urns])


def _terms(*urns: str) -> GlossaryTermsClass:
    return GlossaryTermsClass(
        terms=[GlossaryTermAssociationClass(urn=u) for u in urns], auditStamp=_AUDIT
    )


def _doc(text: str) -> DocumentationClass:
    return DocumentationClass(
        documentations=[DocumentationAssociationClass(documentation=text)]
    )


def _structured_prop(prop_urn: str, value: str) -> StructuredPropertiesClass:
    return StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(propertyUrn=prop_urn, values=[value])
        ]
    )


class FakeGraph:
    """In-memory graph: aspects keyed by (urn, aspect_name); records writes/deletes."""

    def __init__(self, store: Dict[str, Dict[str, _Aspect]]) -> None:
        self._store = store
        self.emitted: List[tuple] = []  # (urn, aspect)
        self.soft_deleted: List[str] = []

    def get_aspect(
        self, entity_urn: str, aspect_type: Type[_Aspect], version: int = 0
    ) -> Optional[_Aspect]:
        return self._store.get(entity_urn, {}).get(aspect_type.ASPECT_NAME)

    def get_entity_semityped(
        self, entity_urn: str, aspects: Optional[List[str]] = None
    ) -> Dict[str, _Aspect]:
        stored = self._store.get(entity_urn, {})
        return {
            name: a for name, a in stored.items() if aspects is None or name in aspects
        }

    def get_urns_by_filter(
        self,
        *,
        entity_types: List[str],
        extraFilters: Optional[List[Dict[str, object]]] = None,
        status: object = None,
        **kw: object,
    ) -> Iterator[str]:
        # Return every stored schemaField urn whose parent matches the filter.
        want_parent = extraFilters[0]["values"][0] if extraFilters else None  # type: ignore[index]
        out: List[str] = []
        for urn in self._store:
            if not urn.startswith("urn:li:schemaField:"):
                continue
            if want_parent is not None and want_parent not in urn:
                continue
            out.append(urn)
        return iter(out)

    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
        assert mcp.entityUrn is not None
        aspect = mcp.aspect
        assert aspect is not None
        self.emitted.append((mcp.entityUrn, aspect))
        # reflect the write so re-reads within a run are consistent
        self._store.setdefault(mcp.entityUrn, {})[aspect.ASPECT_NAME] = aspect

    def soft_delete_entity(self, urn: str, **kw: object) -> None:
        self.soft_deleted.append(urn)


def _sf(field_path: str) -> str:
    return make_schema_field_urn(_DATASET, field_path)


class TestPathReconciler:
    def test_exact_match_is_noop(self):
        r = PathReconciler.build(["Product2Id", "amount"])
        assert r.resolve("Product2Id") == ("Product2Id", None)

    def test_casefold_match(self):
        r = PathReconciler.build(["Product2Id", "Amount"])
        assert r.resolve("product2id") == ("Product2Id", None)
        assert r.resolve("AMOUNT") == ("Amount", None)

    def test_unmatched_reports_reason(self):
        r = PathReconciler.build(["amount"])
        new_path, reason = r.resolve("gone_column")
        assert new_path is None
        assert reason and "no current schema field" in reason

    def test_case_only_collision_is_ambiguous(self):
        r = PathReconciler.build(["col", "COL"])
        new_path, reason = r.resolve("col")  # exact match wins even amid collision
        assert new_path == "col"
        # a stale lowercased-from-elsewhere spelling that only casefold-matches
        new_path, reason = r.resolve("Col")
        assert new_path is None
        assert reason and "collision" in reason

    def test_nested_field_path_casefold(self):
        r = PathReconciler.build(["address.PostCode"])
        assert r.resolve("address.postcode") == ("address.PostCode", None)

    def test_v2_field_path_matches_and_returns_full_path(self):
        new = "[version=2.0].[type=struct].[type=string].Product2Id"
        r = PathReconciler.build([new])
        # old v2 path with lowercased leaf resolves to the full new v2 path
        old = "[version=2.0].[type=struct].[type=string].product2id"
        assert r.resolve(old) == (new, None)

    def test_v1_to_v2_encoding_change_matches(self):
        new = "[version=2.0].[type=struct].[type=string].Product2Id"
        r = PathReconciler.build([new])
        assert r.resolve("product2id") == (new, None)

    def test_v2_case_only_collision_is_ambiguous(self):
        r = PathReconciler.build(
            [
                "[version=2.0].[type=struct].[type=string].MixedCol",
                "[version=2.0].[type=struct].[type=string].MIXEDCOL",
            ]
        )
        new_path, reason = r.resolve("mixedcol")
        assert new_path is None
        assert reason and "collision" in reason

    def test_v2_union_members_sharing_leaf_do_not_collapse(self):
        # An Avro-style union emits one schemaField per member with the same leaf.
        # get_simple_field_path_from_v2_field_path strips the [type=...] tokens, so
        # simplifying would bucket both members together and misreport a casing
        # variant of one as a case-only collision. The nested-token guard keeps
        # them distinct so each resolves uniquely.
        int_member = "[version=2.0].[type=union].[type=int].a"
        str_member = "[version=2.0].[type=union].[type=string].a"
        r = PathReconciler.build([int_member, str_member])
        assert r.resolve("[version=2.0].[type=union].[type=int].A") == (
            int_member,
            None,
        )
        assert r.resolve("[version=2.0].[type=union].[type=string].A") == (
            str_member,
            None,
        )


class TestReconcileDataset:
    def test_no_schema_metadata_errors(self):
        graph = FakeGraph({_DATASET: {}})
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert result.error and "no schemaMetadata" in result.error
        assert not graph.emitted

    def test_schema_field_entity_moved_and_source_deleted(self):
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id", "amount")},
                old_sf: {
                    "documentation": _doc("hello"),
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:pii", "yes"
                    ),
                    "globalTags": _tags("urn:li:tag:sensitive"),
                },
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        emitted_to_new = [a for (u, a) in graph.emitted if u == new_sf]
        emitted_aspect_names = {a.ASPECT_NAME for a in emitted_to_new}
        assert emitted_aspect_names == {
            "documentation",
            "structuredProperties",
            "globalTags",
        }
        assert old_sf in graph.soft_deleted
        assert len(result.remaps) == 1
        assert result.remaps[0].old_path == "product2id"
        assert result.remaps[0].new_path == "Product2Id"

    def test_reverse_direction_mixed_to_lowercase(self):
        # The mirror image of the Snowflake case: a connector that used to
        # preserve case is switched to lowercasing (e.g. convert_urns_to_lowercase
        # flipped on for a source whose field paths were mixed-case). Matching is
        # casefold-based, so re-anchoring is fully bidirectional.
        old_sf = _sf("Product2Id")
        new_sf = _sf("product2id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("product2id", "amount")},
                old_sf: {"globalTags": _tags("urn:li:tag:sensitive")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert {a.ASPECT_NAME for (u, a) in graph.emitted if u == new_sf} == {
            "globalTags"
        }
        assert old_sf in graph.soft_deleted
        assert result.remaps[0].old_path == "Product2Id"
        assert result.remaps[0].new_path == "product2id"

    def test_v2_schema_field_entity_moved_to_new_v2_urn(self):
        old_path = "[version=2.0].[type=struct].[type=string].product2id"
        new_path = "[version=2.0].[type=struct].[type=string].Product2Id"
        old_sf = _sf(old_path)
        new_sf = _sf(new_path)
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema(new_path)},
                old_sf: {"documentation": _doc("v2 doc")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert [u for (u, _) in graph.emitted] == [new_sf]
        assert old_sf in graph.soft_deleted
        assert result.remaps[0].new_path == new_path

    def test_dry_run_writes_nothing(self):
        old_sf = _sf("product2id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("hello")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=True,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not graph.emitted
        assert not graph.soft_deleted
        assert len(result.remaps) == 1  # still reports what it *would* do

    def test_keep_source_does_not_delete(self):
        old_sf = _sf("product2id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("hello")},
            }
        )
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=False,
            include_soft_deleted=False,
        )
        assert not graph.soft_deleted

    def test_already_correct_is_noop(self):
        good_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                good_sf: {"documentation": _doc("hello")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not graph.emitted
        assert not graph.soft_deleted
        assert not result.remaps

    def test_editable_schema_metadata_rewritten(self):
        graph = FakeGraph(
            {
                _DATASET: {
                    "schemaMetadata": _schema("Product2Id", "Amount"),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="product2id",
                                description="the product id",
                                globalTags=_tags("urn:li:tag:pii"),
                            ),
                            EditableSchemaFieldInfoClass(
                                fieldPath="Amount",  # already correct, untouched
                                description="money",
                            ),
                        ]
                    ),
                },
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert result.editable_updated
        emitted = [a for (u, a) in graph.emitted if u == _DATASET]
        assert len(emitted) == 1
        by_path = {i.fieldPath: i for i in emitted[0].editableSchemaFieldInfo}
        assert set(by_path) == {"Product2Id", "Amount"}
        assert by_path["Product2Id"].description == "the product id"
        assert [t.tag for t in by_path["Product2Id"].globalTags.tags] == [
            "urn:li:tag:pii"
        ]

    def test_editable_unresolved_entry_survives_rewrite(self):
        # A genuinely dropped/renamed column's editable entry can't resolve, but
        # the aspect is still rewritten wholesale because "product2id" resolves.
        # The unresolved entry must be carried over unchanged, not dropped.
        graph = FakeGraph(
            {
                _DATASET: {
                    "schemaMetadata": _schema("Product2Id"),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="product2id",
                                description="the product id",
                            ),
                            EditableSchemaFieldInfoClass(
                                fieldPath="legacy_discontinued_field",
                                description="a note nobody should lose",
                                globalTags=_tags("urn:li:tag:pii"),
                            ),
                        ]
                    ),
                },
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert any(
            "legacy_discontinued_field" in s for s in result.skipped
        )  # reported...
        emitted = [a for (u, a) in graph.emitted if u == _DATASET]
        assert len(emitted) == 1
        by_path = {i.fieldPath: i for i in emitted[0].editableSchemaFieldInfo}
        assert "legacy_discontinued_field" in by_path  # ...but NOT deleted
        assert by_path["legacy_discontinued_field"].description == (
            "a note nobody should lose"
        )
        assert [
            t.tag for t in by_path["legacy_discontinued_field"].globalTags.tags
        ] == ["urn:li:tag:pii"]

    def test_editable_merges_onto_existing_target(self):
        graph = FakeGraph(
            {
                _DATASET: {
                    "schemaMetadata": _schema("Product2Id"),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="Product2Id",
                                description="new-cased edit",
                                globalTags=_tags("urn:li:tag:a"),
                            ),
                            EditableSchemaFieldInfoClass(
                                fieldPath="product2id",
                                description="stale edit",
                                globalTags=_tags("urn:li:tag:b"),
                            ),
                        ]
                    ),
                },
            }
        )
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        emitted = [a for (u, a) in graph.emitted if u == _DATASET][0]
        infos = emitted.editableSchemaFieldInfo
        assert len(infos) == 1
        info = infos[0]
        assert info.fieldPath == "Product2Id"
        assert info.description == "new-cased edit"  # existing target wins
        assert {t.tag for t in info.globalTags.tags} == {
            "urn:li:tag:a",
            "urn:li:tag:b",
        }

    def test_editable_stranded_description_drop_is_reported(self):
        # Two stranded editable entries ("product2id"/"PRODUCT2ID") fold onto one
        # current "Product2Id" with *different* descriptions. The wholesale rewrite
        # keeps one, so the dropped description must be surfaced for review rather
        # than silently lost; tags from both peers still union.
        graph = FakeGraph(
            {
                _DATASET: {
                    "schemaMetadata": _schema("Product2Id"),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="product2id",
                                description="from lower",
                                globalTags=_tags("urn:li:tag:a"),
                            ),
                            EditableSchemaFieldInfoClass(
                                fieldPath="PRODUCT2ID",
                                description="from upper",
                                globalTags=_tags("urn:li:tag:b"),
                            ),
                        ]
                    ),
                },
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        infos = [a for (u, a) in graph.emitted if u == _DATASET][
            0
        ].editableSchemaFieldInfo
        assert len(infos) == 1
        assert infos[0].fieldPath == "Product2Id"
        assert infos[0].description in {"from lower", "from upper"}
        assert {t.tag for t in infos[0].globalTags.tags} == {
            "urn:li:tag:a",
            "urn:li:tag:b",
        }
        assert any(
            "description differs from an entry already mapped there" in s
            for s in result.skipped
        )

    def test_case_collision_reported_not_migrated(self):
        # Two current fields differ only by case; a stale lowercased schemaField
        # cannot be attributed to either.
        stale_sf = _sf("mixedcol")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("MixedCol", "MIXEDCOL")},
                stale_sf: {"documentation": _doc("ambiguous")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not graph.emitted
        assert not graph.soft_deleted
        assert not result.remaps
        assert any("collision" in s for s in result.skipped)

    def test_empty_source_field_not_reported(self):
        # A key-only schemaField (no user aspects) on an orphaned path is ignored.
        empty_sf = _sf("gone")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Amount")},
                empty_sf: {},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not result.skipped
        assert not result.remaps

    def test_orphaned_field_with_aspects_but_no_match_is_reported(self):
        gone_sf = _sf("removed_col")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Amount")},
                gone_sf: {"documentation": _doc("orphan")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not graph.emitted
        assert any("removed_col" in s for s in result.skipped)

    def test_pre_lowercased_field_now_split_into_two_is_ambiguous_both_sides(self):
        # Historically two distinct source columns ("Col"/"COL") were both
        # lowercased to a single "col" on ingest, so all UI metadata piled onto
        # one schemaField entity and one editableSchemaMetadata entry. After
        # enabling case preservation the schema now has two distinct fields, and
        # the single stale "col" cannot be attributed to either. Both the
        # schemaField entity and the editable entry must be reported, not guessed.
        stale_sf = _sf("col")
        graph = FakeGraph(
            {
                _DATASET: {
                    "schemaMetadata": _schema("Col", "COL"),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="col",
                                description="ambiguous edit",
                                globalTags=_tags("urn:li:tag:pii"),
                            ),
                        ]
                    ),
                },
                stale_sf: {"documentation": _doc("ambiguous")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not graph.emitted
        assert not graph.soft_deleted
        assert not result.remaps
        assert any("schemaField 'col'" in s for s in result.skipped)
        assert any("editableSchemaMetadata 'col'" in s for s in result.skipped)

    def test_two_stale_case_variants_consolidate_onto_one_current(self):
        # The inverse: two stale schemaField entities ("col" and "COL") that both
        # casefold to a single current "Col". Each maps unambiguously, so both
        # re-anchor onto the one new urn and their tags are unioned — no metadata
        # is dropped even though the source had a redundant pair.
        lower_sf = _sf("col")
        upper_sf = _sf("COL")
        new_sf = _sf("Col")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Col")},
                lower_sf: {"globalTags": _tags("urn:li:tag:a")},
                upper_sf: {"globalTags": _tags("urn:li:tag:b")},
            }
        )
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        final_tags = graph._store[new_sf]["globalTags"]
        assert isinstance(final_tags, GlobalTagsClass)
        assert {t.tag for t in final_tags.tags} == {"urn:li:tag:a", "urn:li:tag:b"}
        assert lower_sf in graph.soft_deleted
        assert upper_sf in graph.soft_deleted

    def test_two_stale_variants_disagree_on_nonunion_aspect_are_reported(self):
        # Two stale entities ("col", "COL") casefold to one current "Col" but carry
        # *different* documentation. A non-union aspect can't be consolidated by
        # ordering the peers (whichever wrote first would win, and the loser would
        # be soft-deleted, run-dependent), so neither is written, both sources are
        # kept for review, and the outcome is independent of discovery order.
        lower_sf = _sf("col")
        upper_sf = _sf("COL")
        new_sf = _sf("Col")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Col")},
                lower_sf: {"documentation": _doc("from lower")},
                upper_sf: {"documentation": _doc("from upper")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not [a for (u, a) in graph.emitted if u == new_sf]
        assert lower_sf not in graph.soft_deleted
        assert upper_sf not in graph.soft_deleted
        assert not result.remaps
        assert any("carry 'documentation'" in s for s in result.skipped)

    def test_two_stale_variants_with_same_nonunion_text_are_still_reported(self):
        # Even when two peers carry byte-identical documentation *text*, real
        # aspects vary in their per-write attribution/audit stamps, so we don't
        # try to detect agreement — a non-union aspect supplied by more than one
        # stranded peer is always left in place for review, never auto-merged.
        lower_sf = _sf("col")
        upper_sf = _sf("COL")
        new_sf = _sf("Col")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Col")},
                lower_sf: {"documentation": _doc("same doc")},
                upper_sf: {"documentation": _doc("same doc")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not [a for (u, a) in graph.emitted if u == new_sf]
        assert lower_sf not in graph.soft_deleted
        assert upper_sf not in graph.soft_deleted
        assert any("carry 'documentation'" in s for s in result.skipped)


def _attr_tag(urn: str, source: str) -> TagAssociationClass:
    return TagAssociationClass(
        tag=urn,
        attribution=MetadataAttributionClass(
            time=0, actor="urn:li:corpuser:__datahub_system", source=source
        ),
    )


class TestSchemaFieldEntityMergeGuard:
    def test_destination_tags_unioned_and_attribution_preserved(self):
        # Propagation already put an attributed tag on the correctly-cased field;
        # the old field has a UI tag plus the same tag without attribution.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        propagated = _attr_tag("urn:li:tag:pii", "urn:li:dataHubAction:propagation")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "globalTags": GlobalTagsClass(
                        tags=[
                            TagAssociationClass(tag="urn:li:tag:pii"),  # UI dup
                            TagAssociationClass(tag="urn:li:tag:ui_only"),
                        ]
                    )
                },
                new_sf: {"globalTags": GlobalTagsClass(tags=[propagated])},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        emitted = [a for (u, a) in graph.emitted if u == new_sf][-1]
        by_urn = {t.tag: t for t in emitted.tags}
        assert set(by_urn) == {"urn:li:tag:pii", "urn:li:tag:ui_only"}
        # the attributed (immutable/propagated) association wins over the bare dup
        assert by_urn["urn:li:tag:pii"].attribution is not None
        assert old_sf in graph.soft_deleted
        assert not result.skipped

    def test_conflicting_nonunion_aspect_is_reported_and_source_kept(self):
        # Destination already has a different documentation (e.g. propagated); we
        # must not clobber it, and must not soft-delete the source that still holds
        # the un-migrated value.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("original UI doc")},
                new_sf: {"documentation": _doc("propagated doc")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not [a for (u, a) in graph.emitted if u == new_sf]  # not overwritten
        assert old_sf not in graph.soft_deleted  # source preserved
        assert any("documentation" in s for s in result.skipped)
        # a conflict-only field carried nothing, so it is not counted as a remap
        assert not result.remaps

    def test_identical_nonunion_aspect_is_noop_and_source_deleted(self):
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("same doc")},
                new_sf: {"documentation": _doc("same doc")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not result.skipped
        assert old_sf in graph.soft_deleted  # identical → safe to retire source


class TestStructuredPropertyMerge:
    def test_disjoint_structured_properties_union(self):
        # A property propagated onto the correctly-cased field and a different one
        # on the stranded field both survive.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:pii", "yes"
                    )
                },
                new_sf: {
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:tier", "gold"
                    )
                },
            }
        )
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        emitted = [a for (u, a) in graph.emitted if u == new_sf][-1]
        by_urn = {p.propertyUrn: p for p in emitted.properties}
        assert set(by_urn) == {
            "urn:li:structuredProperty:pii",
            "urn:li:structuredProperty:tier",
        }
        assert old_sf in graph.soft_deleted

    def test_same_property_different_values_is_conflict(self):
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:tier", "silver"
                    )
                },
                new_sf: {
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:tier", "gold"
                    )
                },
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert not [a for (u, a) in graph.emitted if u == new_sf]
        assert old_sf not in graph.soft_deleted
        assert any("structuredProperties" in s for s in result.skipped)

    def test_same_values_keeps_destination_attribution(self):
        # The destination carries an attributed (e.g. propagated) assignment; the
        # stranded field has the same property/value but no attribution. Equal
        # values are not a conflict, so the destination assignment must survive
        # rather than be downgraded to the un-attributed stranded copy.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        attributed = StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:tier",
                    values=["gold"],
                    attribution=MetadataAttributionClass(
                        time=123,
                        actor="urn:li:corpuser:propagation",
                    ),
                )
            ]
        )
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:tier", "gold"
                    )
                },
                new_sf: {"structuredProperties": attributed},
            }
        )
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        # Equal values are a true no-op on the destination: no downgrade emitted,
        # attribution intact, and the stranded source still soft-deleted.
        assert not [a for (u, a) in graph.emitted if u == new_sf]
        stored = graph.get_aspect(new_sf, StructuredPropertiesClass)
        assert isinstance(stored, StructuredPropertiesClass)
        attribution = stored.properties[0].attribution
        assert attribution is not None
        assert attribution.actor == "urn:li:corpuser:propagation"
        assert old_sf in graph.soft_deleted

    def test_peer_fold_promotes_attributed_property(self):
        # Two stranded peers ("col"/"COL") carry the same structured-property value
        # onto an empty live field; only one copy is attributed. Folding the peers
        # must land the attributed assignment, not silently keep the first peer's
        # un-attributed copy and then soft-delete the attributed source.
        lower_sf = _sf("col")
        upper_sf = _sf("COL")
        new_sf = _sf("Col")
        attributed = StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:tier",
                    values=["gold"],
                    attribution=MetadataAttributionClass(
                        time=123, actor="urn:li:corpuser:propagation"
                    ),
                )
            ]
        )
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Col")},
                lower_sf: {
                    "structuredProperties": _structured_prop(
                        "urn:li:structuredProperty:tier", "gold"
                    )
                },
                upper_sf: {"structuredProperties": attributed},
            }
        )
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        stored = graph.get_aspect(new_sf, StructuredPropertiesClass)
        assert isinstance(stored, StructuredPropertiesClass)
        attribution = stored.properties[0].attribution
        assert attribution is not None
        assert attribution.actor == "urn:li:corpuser:propagation"
        assert lower_sf in graph.soft_deleted
        assert upper_sf in graph.soft_deleted


class _FixedResolver(ClashResolver):
    """Test double: returns a fixed target / overwrite decision and records calls."""

    def __init__(self, target: Optional[str] = None, overwrite: bool = False) -> None:
        self._target = target
        self._overwrite = overwrite
        self.choose_calls: List[str] = []
        self.conflict_calls: List[str] = []

    def choose_target(
        self, old_path: str, candidates: List[str], what: str
    ) -> Optional[str]:
        self.choose_calls.append(old_path)
        return self._target

    def resolve_conflict(self, old_path: str, new_path: str, aspect_name: str) -> bool:
        self.conflict_calls.append(aspect_name)
        return self._overwrite


class TestInteractiveResolver:
    def test_ambiguous_collision_resolved_to_chosen_target(self):
        stale_sf = _sf("col")
        chosen_sf = _sf("COL")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Col", "COL")},
                stale_sf: {"documentation": _doc("was ambiguous")},
            }
        )
        resolver = _FixedResolver(target="COL")
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
            resolver=resolver,
        )
        assert [u for (u, _) in graph.emitted] == [chosen_sf]
        assert resolver.choose_calls == ["col"]
        assert stale_sf in graph.soft_deleted
        assert not result.skipped

    def test_conflict_overwrite_replaces_destination(self):
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("stranded doc")},
                new_sf: {"documentation": _doc("destination doc")},
            }
        )
        resolver = _FixedResolver(overwrite=True)
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
            resolver=resolver,
        )
        emitted = [a for (u, a) in graph.emitted if u == new_sf][-1]
        assert emitted.documentations[0].documentation == "stranded doc"
        assert resolver.conflict_calls == ["documentation"]
        assert old_sf in graph.soft_deleted
        assert not result.skipped

    def test_structured_property_overwrite_keeps_destination_only(self):
        # Overwriting a structuredProperties conflict lets the stranded value win
        # the *conflicting* property, but it is still a union: a destination-only
        # property that was never in conflict must survive, not be clobbered.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "structuredProperties": StructuredPropertiesClass(
                        properties=[
                            StructuredPropertyValueAssignmentClass(
                                propertyUrn="urn:li:structuredProperty:tier",
                                values=["silver"],
                            ),
                            StructuredPropertyValueAssignmentClass(
                                propertyUrn="urn:li:structuredProperty:pii",
                                values=["yes"],
                            ),
                        ]
                    )
                },
                new_sf: {
                    "structuredProperties": StructuredPropertiesClass(
                        properties=[
                            StructuredPropertyValueAssignmentClass(
                                propertyUrn="urn:li:structuredProperty:tier",
                                values=["gold"],
                            ),
                            StructuredPropertyValueAssignmentClass(
                                propertyUrn="urn:li:structuredProperty:region",
                                values=["us"],
                            ),
                        ]
                    )
                },
            }
        )
        resolver = _FixedResolver(overwrite=True)
        reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
            resolver=resolver,
        )
        emitted = [a for (u, a) in graph.emitted if u == new_sf][-1]
        by_urn = {p.propertyUrn: p.values for p in emitted.properties}
        assert by_urn == {
            "urn:li:structuredProperty:tier": ["silver"],  # src won the conflict
            "urn:li:structuredProperty:pii": ["yes"],  # src-only, carried
            "urn:li:structuredProperty:region": ["us"],  # dest-only, preserved
        }
        assert old_sf in graph.soft_deleted

    def test_abort_at_prompt_stops_whole_run(self):
        # Ctrl-C at a prompt raises click.Abort; it must propagate out and halt the
        # run, not be caught per-dataset and let later datasets be rewritten
        # (and their source fields soft-deleted) without any prompt.
        d2 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.d2,PROD)"
        d1_old, d2_old = _sf("product2id"), make_schema_field_urn(d2, "amount")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                _sf("product2id"): {"documentation": _doc("stranded")},
                _sf("Product2Id"): {"documentation": _doc("destination")},  # conflict
                d2: {"schemaMetadata": _schema("Amount")},
                d2_old: {"documentation": _doc("simple remap")},
            }
        )

        class _AbortingResolver(ClashResolver):
            def resolve_conflict(
                self, old_path: str, new_path: str, aspect_name: str
            ) -> bool:
                raise click.Abort()

        with pytest.raises(click.Abort):
            run_migration(
                graph,  # type: ignore[arg-type]
                [_DATASET, d2],
                dry_run=False,
                delete_source=True,
                include_soft_deleted=False,
                resolver=_AbortingResolver(),
            )
        # d1's conflicting source is kept, and d2 was never reached.
        assert d1_old not in graph.soft_deleted
        assert d2_old not in graph.soft_deleted


class TestMixedPerFieldOutcome:
    def test_clean_union_carried_conflict_reported_source_kept(self):
        # One aspect unions cleanly onto the destination while another genuinely
        # conflicts: the clean one is carried, the conflicting one is reported and
        # left, and the source is kept because not everything moved.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "globalTags": _tags("urn:li:tag:pii"),
                    "documentation": _doc("stranded doc"),
                },
                new_sf: {"documentation": _doc("destination doc")},  # conflicts
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        tag_writes = [
            a
            for (u, a) in graph.emitted
            if u == new_sf and a.ASPECT_NAME == "globalTags"
        ]
        assert tag_writes and {t.tag for t in tag_writes[-1].tags} == {"urn:li:tag:pii"}
        # destination documentation untouched, conflict surfaced
        assert graph._store[new_sf]["documentation"] == _doc("destination doc")
        assert any("documentation" in s for s in result.skipped)
        # only the clean aspect is recorded as carried; source not deleted
        assert result.remaps[0].schema_field_aspects == ["globalTags"]
        assert old_sf not in graph.soft_deleted


class _FailingGraph(FakeGraph):
    """FakeGraph that raises on emit for a targeted (urn-substring, aspect)."""

    def __init__(
        self,
        store: Dict[str, Dict[str, _Aspect]],
        *,
        fail_urn_contains: str,
        fail_aspect: str,
    ) -> None:
        super().__init__(store)
        self._fail_urn_contains = fail_urn_contains
        self._fail_aspect = fail_aspect

    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
        assert mcp.entityUrn is not None and mcp.aspect is not None
        if (
            self._fail_urn_contains in mcp.entityUrn
            and self._fail_aspect == mcp.aspect.ASPECT_NAME
        ):
            raise RuntimeError("simulated GMS write failure")
        super().emit_mcp(mcp)


class _ReadFailingGraph(FakeGraph):
    """FakeGraph that raises when reading schemaMetadata for a targeted dataset."""

    def __init__(
        self, store: Dict[str, Dict[str, _Aspect]], *, fail_dataset: str
    ) -> None:
        super().__init__(store)
        self._fail_dataset = fail_dataset

    def get_aspect(
        self, entity_urn: str, aspect_type: Type[_Aspect], version: int = 0
    ) -> Optional[_Aspect]:
        if entity_urn == self._fail_dataset and aspect_type is SchemaMetadataClass:
            raise RuntimeError("simulated GMS read failure")
        return super().get_aspect(entity_urn, aspect_type, version)


class TestFailureHandling:
    def test_schema_field_write_failure_keeps_source_and_reports(self):
        old_sf = _sf("product2id")
        graph = _FailingGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("stranded")},
            },
            fail_urn_contains="Product2Id",
            fail_aspect="documentation",
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert old_sf not in graph.soft_deleted  # source preserved on write failure
        assert result.error is None  # per-field handled, not a dataset-level abort
        assert any("failed to write" in s for s in result.skipped)
        assert not result.remaps  # nothing carried

    def test_partial_field_failure_carries_survivor(self):
        # documentation write fails but globalTags succeeds: the survivor is
        # carried, the failure is attributed, and the source is kept.
        old_sf = _sf("product2id")
        new_sf = _sf("Product2Id")
        graph = _FailingGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {
                    "documentation": _doc("stranded"),
                    "globalTags": _tags("urn:li:tag:pii"),
                },
            },
            fail_urn_contains="Product2Id",
            fail_aspect="documentation",
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        emitted = {a.ASPECT_NAME for (u, a) in graph.emitted if u == new_sf}
        assert emitted == {"globalTags"}
        assert any(
            "failed to write" in s and "documentation" in s for s in result.skipped
        )
        assert old_sf not in graph.soft_deleted
        assert result.remaps[0].schema_field_aspects == ["globalTags"]

    def test_editable_write_failure_not_reported_as_updated(self):
        graph = _FailingGraph(
            {
                _DATASET: {
                    "schemaMetadata": _schema("Product2Id"),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="product2id", description="d"
                            ),
                        ]
                    ),
                },
            },
            fail_urn_contains=_DATASET,
            fail_aspect="editableSchemaMetadata",
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert result.editable_updated is False  # write failed → not claimed
        assert any("editableSchemaMetadata rewrite failed" in s for s in result.skipped)
        assert all(not r.editable for r in result.remaps)

    def test_unparseable_schema_field_urn_is_reported(self):
        graph = FakeGraph({_DATASET: {"schemaMetadata": _schema("Product2Id")}})

        def _one_bad_urn(**kw: object) -> Iterator[str]:
            return iter(["not-a-urn-at-all"])

        graph.get_urns_by_filter = _one_bad_urn  # type: ignore[assignment]
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert result.error is None
        assert any("could not be parsed" in s for s in result.skipped)

    def test_read_failure_on_one_dataset_does_not_abort_batch(self):
        # A read failure on one dataset in a multi-dataset run is isolated to that
        # dataset: it is reported as an error while the rest of the batch still
        # migrates normally.
        bad_ds = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.bad,PROD)"
        )
        good_old = _sf("product2id")
        good_new = _sf("Product2Id")
        graph = _ReadFailingGraph(
            {
                bad_ds: {"schemaMetadata": _schema("Whatever")},
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                good_old: {"documentation": _doc("stranded")},
            },
            fail_dataset=bad_ds,
        )
        report = run_migration(
            graph,  # type: ignore[arg-type]
            [bad_ds, _DATASET],
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        by_urn = {r.dataset_urn: r for r in report.results}
        assert by_urn[bad_ds].error is not None  # bad dataset surfaced as an error
        # the healthy dataset in the same batch still fully migrated
        assert by_urn[_DATASET].error is None
        assert graph._store[good_new]["documentation"] == _doc("stranded")
        assert good_old in graph.soft_deleted

    def test_sibling_field_still_processed_after_write_failure(self):
        # Two stranded fields on one dataset; the write for one fails. The other,
        # unaffected field must still be re-anchored and its source soft-deleted.
        fail_old, fail_new = _sf("alpha"), _sf("Alpha")
        ok_old, ok_new = _sf("beta"), _sf("Beta")
        graph = _FailingGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Alpha", "Beta")},
                fail_old: {"documentation": _doc("alpha doc")},
                ok_old: {"documentation": _doc("beta doc")},
            },
            fail_urn_contains="Alpha",
            fail_aspect="documentation",
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        # sibling migrated despite the other field's failure
        assert graph._store[ok_new]["documentation"] == _doc("beta doc")
        assert ok_old in graph.soft_deleted
        # failed field kept and surfaced
        assert fail_old not in graph.soft_deleted
        assert fail_new not in graph._store
        assert any("failed to write" in s and "alpha" in s for s in result.skipped)

    def test_destination_read_failure_isolated_to_its_group(self):
        # Reading the destination aspects of one group fails transiently. That
        # group is skipped with its source kept, while the other group on the same
        # dataset still migrates — the read error must not abort the dataset after
        # earlier groups were already written.
        fail_old, fail_new = _sf("alpha"), _sf("Alpha")
        ok_old, ok_new = _sf("beta"), _sf("Beta")

        class _DestReadFailingGraph(FakeGraph):
            def get_entity_semityped(
                self, entity_urn: str, aspects: Optional[List[str]] = None
            ) -> Dict[str, _Aspect]:
                if entity_urn == fail_new:  # destination read for the "Alpha" group
                    raise RuntimeError("simulated GMS read failure")
                return super().get_entity_semityped(entity_urn, aspects)

        graph = _DestReadFailingGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Alpha", "Beta")},
                fail_old: {"documentation": _doc("alpha doc")},
                ok_old: {"documentation": _doc("beta doc")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert result.error is None  # dataset not aborted
        assert graph._store[ok_new]["documentation"] == _doc("beta doc")
        assert ok_old in graph.soft_deleted
        assert fail_old not in graph.soft_deleted  # source kept for a re-run
        assert any(
            "could not read destination" in s and "Alpha" in s for s in result.skipped
        )

    def test_source_read_failure_isolated_to_its_field(self):
        # Reading one stranded source's aspects fails; that field is skipped and
        # kept, while a sibling field is still discovered and migrated.
        fail_old = _sf("alpha")
        ok_old, ok_new = _sf("beta"), _sf("Beta")

        class _SourceReadFailingGraph(FakeGraph):
            def get_entity_semityped(
                self, entity_urn: str, aspects: Optional[List[str]] = None
            ) -> Dict[str, _Aspect]:
                if entity_urn == fail_old:
                    raise RuntimeError("simulated GMS read failure")
                return super().get_entity_semityped(entity_urn, aspects)

        graph = _SourceReadFailingGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Alpha", "Beta")},
                fail_old: {"documentation": _doc("alpha doc")},
                ok_old: {"documentation": _doc("beta doc")},
            }
        )
        result = reconcile_dataset(
            graph,  # type: ignore[arg-type]
            _DATASET,
            dry_run=False,
            delete_source=True,
            include_soft_deleted=False,
        )
        assert result.error is None
        assert graph._store[ok_new]["documentation"] == _doc("beta doc")
        assert ok_old in graph.soft_deleted
        assert fail_old not in graph.soft_deleted
        assert any(
            "could not read aspects" in s and "alpha" in s for s in result.skipped
        )


class TestRunMigrationReport:
    def test_report_counts(self):
        old_sf = _sf("product2id")
        graph = FakeGraph(
            {
                _DATASET: {"schemaMetadata": _schema("Product2Id")},
                old_sf: {"documentation": _doc("hello")},
            }
        )
        report = run_migration(
            graph,  # type: ignore[arg-type]
            [_DATASET],
            dry_run=True,
            delete_source=True,
            include_soft_deleted=False,
        )
        text = report.render()
        assert "Fields re-anchored = 1" in text
        assert "[Dry Run]" in text


class TestSchemaFieldCaseCli:
    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_force_skips_confirmation(
        self, mock_graph: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(schema_field_case, ["--urn", _DATASET, "--force"])
        assert result.exit_code == 0, result.output
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["delete_source"] is True

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_keep_source_fields_flag(
        self, mock_graph: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(
            schema_field_case, ["--urn", _DATASET, "--force", "--keep-source-fields"]
        )
        assert result.exit_code == 0, result.output
        assert mock_run.call_args.kwargs["delete_source"] is False

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_dry_run_skips_confirmation(
        self, mock_graph: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(schema_field_case, ["--urn", _DATASET, "--dry-run"])
        assert result.exit_code == 0, result.output
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["dry_run"] is True

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_confirmation_aborts_on_no(
        self, mock_graph: MagicMock, mock_run: MagicMock
    ) -> None:
        result = CliRunner().invoke(schema_field_case, ["--urn", _DATASET], input="n\n")
        assert result.exit_code != 0
        mock_run.assert_not_called()

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_urn_file_combined_with_explicit_urns(
        self, mock_graph: MagicMock, mock_run: MagicMock, tmp_path: Path
    ) -> None:
        other = "urn:li:dataset:(urn:li:dataPlatform:oracle,db.s.t,PROD)"
        urn_file = tmp_path / "urns.txt"
        urn_file.write_text(f"# comment\n\n{other}\n")
        mock_run.return_value = MagicMock()

        result = CliRunner().invoke(
            schema_field_case,
            ["--urn", _DATASET, "--urn-file", str(urn_file), "--force"],
        )
        assert result.exit_code == 0, result.output
        assert set(mock_run.call_args.kwargs["dataset_urns"]) == {_DATASET, other}

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.discover_schema_field_dataset_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_discovery_used_when_no_urns_given(
        self, mock_graph: MagicMock, mock_discover: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_discover.return_value = [_DATASET]
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(
            schema_field_case, ["--platform", "snowflake", "--force"]
        )
        assert result.exit_code == 0, result.output
        mock_discover.assert_called_once()
        assert mock_run.call_args.kwargs["dataset_urns"] == [_DATASET]

    @patch("datahub.cli.migrate.discover_schema_field_dataset_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_no_datasets_found_message(
        self, mock_graph: MagicMock, mock_discover: MagicMock
    ) -> None:
        mock_discover.return_value = []
        result = CliRunner().invoke(schema_field_case, ["--platform", "snowflake"])
        assert result.exit_code == 0, result.output
        assert "No datasets found" in result.output

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_interactive_flag_uses_interactive_resolver(
        self, mock_graph: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(
            schema_field_case, ["--urn", _DATASET, "--force", "--interactive"]
        )
        assert result.exit_code == 0, result.output
        assert isinstance(
            mock_run.call_args.kwargs["resolver"], InteractiveClashResolver
        )

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_interactive_ignored_under_dry_run(
        self, mock_graph: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(
            schema_field_case, ["--urn", _DATASET, "--interactive", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert not isinstance(
            mock_run.call_args.kwargs["resolver"], InteractiveClashResolver
        )

    @patch("datahub.cli.migrate.run_schema_field_case_migration")
    @patch("datahub.cli.migrate.discover_schema_field_dataset_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_platform_instance_and_env_forwarded_to_discovery(
        self, mock_graph: MagicMock, mock_discover: MagicMock, mock_run: MagicMock
    ) -> None:
        mock_discover.return_value = [_DATASET]
        mock_run.return_value = MagicMock()
        result = CliRunner().invoke(
            schema_field_case,
            [
                "--platform",
                "snowflake",
                "--platform-instance",
                "prod_wh",
                "--env",
                "PROD",
                "--force",
            ],
        )
        assert result.exit_code == 0, result.output
        assert mock_discover.call_args.kwargs["platform_instance"] == "prod_wh"
        assert mock_discover.call_args.kwargs["env"] == "PROD"
