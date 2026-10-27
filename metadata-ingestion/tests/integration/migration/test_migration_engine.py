"""End-to-end migration-engine tests against an in-memory fake graph.

These exercise the full fetch → transform → migrate flow across several entity
types and aspects without a live GMS: an in-memory store stands in for the
backend, and the relationship "index" is computed by scanning stored aspects.
"""

import copy
from typing import Callable, Dict, Iterator, List, Optional
from unittest.mock import patch

import pytest
from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.openapi import Relationship, RelationshipScrollResult
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    DataPlatformInstanceClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.migration import engine
from datahub.migration.models import (
    ConflictStrategy,
    MigrationOptions,
    MigrationPair,
    MigrationReport,
)
from datahub.migration.transform import make_urn_builder
from datahub.utilities.urns.urn_iter import list_urns

pytestmark = pytest.mark.integration

PLATFORM = "snowflake"
ENV = "PROD"


def _ds(name: str, instance: Optional[str] = None) -> str:
    return make_dataset_urn_with_platform_instance(PLATFORM, name, instance, ENV)


def _instance_aspect(instance: str) -> DataPlatformInstanceClass:
    return DataPlatformInstanceClass(
        platform=make_data_platform_urn(PLATFORM),
        instance=make_dataplatform_instance_urn(PLATFORM, instance),
    )


class FakeGraph:
    """An in-memory metadata store that mimics the handful of graph operations the
    migration engine relies on.

    The relationship "index" is derived by scanning every stored aspect for URN
    references — a superset of the real @Relationship index, which is exactly what
    we want when asserting that references get rewritten.
    """

    def __init__(self) -> None:
        self.store: Dict[str, Dict[str, _Aspect]] = {}
        self._session = object()

        class _Config:
            server = "http://fake"

        self.config = _Config()

    def add(self, urn: str, *aspects: _Aspect) -> None:
        for aspect in aspects:
            self.store.setdefault(urn, {})[aspect.ASPECT_NAME] = aspect

    def exists(self, urn: str) -> bool:
        return urn in self.store

    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
        assert mcp.entityUrn and mcp.aspectName and mcp.aspect is not None
        self.store.setdefault(mcp.entityUrn, {})[mcp.aspectName] = mcp.aspect

    def emit(self, mcp: object) -> None:  # patch-builder path (not exercised here)
        raise NotImplementedError("patch emit not supported by FakeGraph")

    def scroll_relationships(
        self,
        *,
        destination_urns: List[str],
        relationship_types: Optional[List[str]] = None,
        include_soft_delete: Optional[bool] = None,
        scroll_id: Optional[str] = None,
        **_: object,
    ) -> RelationshipScrollResult:
        target = destination_urns[0]
        prefix = f"urn:li:schemaField:({target},"
        rels: List[Relationship] = []
        for urn, aspects in self.store.items():
            if urn == target:
                continue
            for aspect in aspects.values():
                if not isinstance(aspect, DictWrapper):
                    continue
                urns = list_urns(aspect)
                if target in urns or any(u.startswith(prefix) for u in urns):
                    rels.append(
                        Relationship(
                            relationship_type="Ref",
                            source_urn=urn,
                            source_entity_type="",
                            destination_urn=target,
                            destination_entity_type="",
                        )
                    )
                    break
        return RelationshipScrollResult(scroll_id=None, relationships=rels)


@pytest.fixture
def fake() -> Iterator[FakeGraph]:
    """A FakeGraph with the engine's aspect reads and source deletes routed at its
    in-memory store (via closures over this instance — no module-level state)."""
    graph = FakeGraph()

    def get_aspects(
        session: object,
        gms_host: str,
        entity_urn: str,
        aspects: List[str],
        typed: bool = False,
        **_: object,
    ) -> Dict[str, _Aspect]:
        stored = graph.store.get(entity_urn, {})
        selected = (
            {k: v for k, v in stored.items() if k in aspects}
            if aspects
            else dict(stored)
        )
        return {k: copy.deepcopy(v) for k, v in selected.items()}

    def delete(
        g: object, urn: str, soft: bool = True, run_id: Optional[str] = None
    ) -> None:
        graph.store.pop(urn, None)

    with (
        patch(
            "datahub.cli.migration_utils.cli_utils.get_aspects_for_entity",
            side_effect=get_aspects,
        ),
        patch("datahub.cli.delete_cli._delete_one_urn", side_effect=delete),
    ):
        yield graph


def _migrate_one(fake: FakeGraph, pair: MigrationPair, **opts: object) -> None:
    options = MigrationOptions(run_id="test", **opts)  # type: ignore[arg-type]
    engine.migrate_pair(
        fake,  # type: ignore[arg-type]
        pair,
        options,
        MigrationReport("test", False, bool(opts.get("keep", False))),
    )


def _lineage(upstream: str, downstream: str) -> UpstreamLineageClass:
    return UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=upstream, type=DatasetLineageTypeClass.TRANSFORMED)
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[make_schema_field_urn(upstream, "id")],
                downstreams=[make_schema_field_urn(downstream, "amt")],
            )
        ],
    )


def test_dataset_full_migration_rewrites_self_incoming_and_assertion(
    fake: FakeGraph,
) -> None:
    """A dataset migration carries every user aspect to the target, rewrites the
    entity's own lineage references, repoints an incoming lineage reference and an
    assertion, and deletes the source."""
    upstream = _ds("db.upstream", "keep_inst")
    src = _ds("db.events", "old_inst")
    tgt = _ds("db.events", "new_inst")
    downstream = _ds("db.rollup", "keep_inst")
    assertion = "urn:li:assertion:a1"

    fake.add(upstream, DatasetPropertiesClass(description="up"))
    fake.add(
        src,
        DatasetPropertiesClass(description="events"),
        OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:alice", type=OwnershipTypeClass.DATAOWNER
                )
            ]
        ),
        GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:pii")]),
        GlossaryTermsClass(
            terms=[GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:Revenue")],
            auditStamp=None,  # type: ignore[arg-type]
        ),
        StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:tier",
                    values=["gold"],
                )
            ]
        ),
        _lineage(upstream, src),
    )
    fake.add(downstream, _lineage(src, downstream))
    fake.add(
        assertion,
        AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=src,
                scope=DatasetAssertionScopeClass.DATASET_ROWS,
                operator="_NATIVE_",  # type: ignore[arg-type]
            ),
        ),
    )

    _migrate_one(
        fake,
        MigrationPair(src, tgt, data_platform_instance=_instance_aspect("new_inst")),
        keep=False,
        on_conflict=None,
    )

    # Source removed, target created with the migration-carried aspects.
    assert not fake.exists(src)
    assert fake.exists(tgt)
    for aspect_name in (
        "datasetProperties",
        "ownership",
        "globalTags",
        "glossaryTerms",
        "structuredProperties",
        "upstreamLineage",
        "dataPlatformInstance",
    ):
        assert aspect_name in fake.store[tgt], f"{aspect_name} not migrated"

    # Self-reference: column-level downstream rewritten to target; the untouched
    # cross-instance upstream (table + column) is preserved.
    tgt_lineage = fake.store[tgt]["upstreamLineage"]
    assert isinstance(tgt_lineage, UpstreamLineageClass)
    assert [u.dataset for u in tgt_lineage.upstreams] == [upstream]
    fgl = tgt_lineage.fineGrainedLineages
    assert fgl is not None
    assert fgl[0].downstreams == [make_schema_field_urn(tgt, "amt")]
    assert fgl[0].upstreams == [make_schema_field_urn(upstream, "id")]

    # Incoming lineage reference rewritten (table + column). The downstream's
    # fine-grained upstream referenced the migrated dataset's "id" field.
    down_lineage = fake.store[downstream]["upstreamLineage"]
    assert isinstance(down_lineage, UpstreamLineageClass)
    assert [u.dataset for u in down_lineage.upstreams] == [tgt]
    assert down_lineage.fineGrainedLineages is not None
    assert down_lineage.fineGrainedLineages[0].upstreams == [
        make_schema_field_urn(tgt, "id")
    ]

    # The assertion's reference is rewritten — the case the old hardcoded
    # relationship-type list ("Asserts" missing) would have left dangling.
    assertion_info = fake.store[assertion]["assertionInfo"]
    assert isinstance(assertion_info, AssertionInfoClass)
    assert assertion_info.datasetAssertion is not None
    assert assertion_info.datasetAssertion.dataset == tgt


@pytest.mark.parametrize(
    "src_urn,transform",
    [
        (
            "urn:li:chart:(looker,old_inst.c1)",
            make_urn_builder("chart", new_instance="new_inst", old_instance="old_inst"),
        ),
        (
            "urn:li:dashboard:(looker,old_inst.d1)",
            make_urn_builder(
                "dashboard", new_instance="new_inst", old_instance="old_inst"
            ),
        ),
        (
            "urn:li:dataFlow:(airflow,old_inst.f1,PROD)",
            make_urn_builder(
                "dataFlow", new_instance="new_inst", old_instance="old_inst"
            ),
        ),
        (
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,old_inst.f1,PROD),j1)",
            make_urn_builder(
                "dataJob", new_instance="new_inst", old_instance="old_inst"
            ),
        ),
    ],
)
def test_non_dataset_entity_types_carry_aspects(
    fake: FakeGraph, src_urn: str, transform: Callable[[str], str]
) -> None:
    """Non-dataset entity types (chart/dashboard/dataFlow/dataJob) migrate their
    aspects to the transform-computed target and delete the source."""
    tgt = transform(src_urn)
    fake.add(
        src_urn,
        GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:pii")]),
        OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:bob", type=OwnershipTypeClass.DATAOWNER
                )
            ]
        ),
    )

    _migrate_one(fake, MigrationPair(src_urn, tgt), keep=False, on_conflict=None)

    assert not fake.exists(src_urn)
    assert fake.exists(tgt)
    assert "globalTags" in fake.store[tgt]
    assert "ownership" in fake.store[tgt]


def test_preserve_leaves_target_untouched_but_repoints_and_deletes(
    fake: FakeGraph,
) -> None:
    """With on_conflict=preserve, an existing target keeps its own aspects, yet the
    referring entity is still repointed to it and the source is still deleted."""
    src = _ds("db.t", "old_inst")
    tgt = _ds("db.t", "new_inst")
    downstream = _ds("db.down", "keep_inst")

    fake.add(src, DatasetPropertiesClass(description="from source"))
    # Target already exists with its own value that must NOT be overwritten.
    fake.add(tgt, DatasetPropertiesClass(description="pre-existing target"))
    fake.add(downstream, _lineage(src, downstream))

    _migrate_one(
        fake,
        MigrationPair(src, tgt, data_platform_instance=_instance_aspect("new_inst")),
        keep=False,
        on_conflict=ConflictStrategy.PRESERVE,
    )

    # Target's own aspect is untouched...
    tgt_props = fake.store[tgt]["datasetProperties"]
    assert isinstance(tgt_props, DatasetPropertiesClass)
    assert tgt_props.description == "pre-existing target"
    # ...but the referrer is repointed and the source is deleted.
    down_lineage = fake.store[downstream]["upstreamLineage"]
    assert isinstance(down_lineage, UpstreamLineageClass)
    assert [u.dataset for u in down_lineage.upstreams] == [tgt]
    assert not fake.exists(src)


def test_dry_run_makes_no_changes(fake: FakeGraph) -> None:
    """A dry run neither creates the target nor deletes the source."""
    src = _ds("db.t", "old_inst")
    tgt = _ds("db.t", "new_inst")
    fake.add(src, DatasetPropertiesClass(description="x"))

    _migrate_one(
        fake,
        MigrationPair(src, tgt, data_platform_instance=_instance_aspect("new_inst")),
        dry_run=True,
        keep=False,
        on_conflict=None,
    )

    assert fake.exists(src)
    assert not fake.exists(tgt)
