"""Tests for datahub.cli.dbt_semantic_model_migration -- urn mapping and
governance migration between legacy dbt "Semantic Model" datasets and the
"Semantic Model Dataset" entities. All tests mock the graph; no live GMS."""

from typing import Callable, Dict, List, Type
from unittest.mock import MagicMock

import pytest

from datahub.cli.dbt_semantic_model_migration import (
    LEGACY_SUBTYPE,
    SEMANTIC_MODEL_DATASET_SUBTYPE,
    DbtSemanticModelIdentity,
    build_mapping,
    discover_legacy_dataset_urns,
    env_mismatches,
    filter_by_expected_subtype,
    gen_semantic_model_dataset_urn,
    migrate_one_dataset,
    parse_legacy_identity,
    parse_semantic_model_dataset_identity,
    run_migration,
)
from datahub.cli.semantic_model_migration_common import (
    MigrationDirection,
    collect_dataset_field_governance,
)
from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.source.dbt.dbt_common import DBTCommonConfig, DBTSourceReport
from datahub.metadata.schema_classes import (
    EditableDatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    NullTypeClass,
    OwnerClass,
    OwnershipClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    _Aspect,
)
from datahub.metadata.urns import DatasetUrn

_LEGACY = "urn:li:dataset:(urn:li:dataPlatform:dbt,pagila.public.orders,PROD)"
_NEW = "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.orders,PROD)"


def _schema_field(name: str, **kwargs: object) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=name,
        type=SchemaFieldDataTypeClass(type=NullTypeClass()),
        nativeDataType="entity:primary",
        **kwargs,  # type: ignore[arg-type]
    )


def _graph(aspects_by_urn: Dict[str, Dict[str, _Aspect]]) -> MagicMock:
    """A graph whose get_aspects_for_entity is backed by a nested dict."""
    graph = MagicMock()
    graph.exists.side_effect = lambda urn: urn in aspects_by_urn

    def get_aspects(
        entity_urn: str, aspects: List[str], aspect_types: List[Type[_Aspect]]
    ) -> Dict[str, _Aspect]:
        available = aspects_by_urn.get(entity_urn, {})
        return {name: available[name] for name in aspects if name in available}

    graph.get_aspects_for_entity.side_effect = get_aspects
    graph.get_related_entities.return_value = []
    return graph


def _raise_on_schema_read(
    graph: MagicMock,
) -> Callable[[str, List[str], List[Type[_Aspect]]], Dict[str, _Aspect]]:
    """Make the column-governance read fail while the entity copy succeeds."""
    original = graph.get_aspects_for_entity.side_effect

    def side_effect(
        entity_urn: str, aspects: List[str], aspect_types: List[Type[_Aspect]]
    ) -> Dict[str, _Aspect]:
        if "schemaMetadata" in aspects:
            raise KeyError("globalTags")
        return original(entity_urn, aspects, aspect_types)

    return side_effect


def _emitted(graph: MagicMock) -> Dict[str, List[_Aspect]]:
    by_urn: Dict[str, List[_Aspect]] = {}
    for call in graph.emit_mcp.call_args_list:
        mcp = call[0][0]
        by_urn.setdefault(mcp.entityUrn, []).append(mcp.aspect)
    return by_urn


# --- URN mapping ------------------------------------------------------------


class TestUrnParsing:
    def test_three_part_legacy_name(self):
        assert parse_legacy_identity(_LEGACY, None) == DbtSemanticModelIdentity(
            "orders"
        )

    def test_two_part_legacy_name(self):
        """DBTNode.get_db_fqn drops a falsy database, so 2 parts is valid."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,public.orders,PROD)"
        assert parse_legacy_identity(urn, None) == DbtSemanticModelIdentity("orders")

    def test_one_part_legacy_name_is_rejected(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,orders,PROD)"
        with pytest.raises(ValueError, match="does not resolve"):
            parse_legacy_identity(urn, None)

    def test_platform_instance_prefix_is_stripped(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,inst.pagila.public.orders,PROD)"
        assert parse_legacy_identity(urn, "inst") == DbtSemanticModelIdentity("orders")

    def test_missing_platform_instance_prefix_is_an_error(self):
        with pytest.raises(ValueError, match="platform instance prefix"):
            parse_legacy_identity(_LEGACY, "inst")

    def test_new_side_requires_the_semantic_layer_segment(self):
        """A legacy three-part name must not parse as a new-side name."""
        assert parse_semantic_model_dataset_identity(
            _NEW, None
        ) == DbtSemanticModelIdentity("orders")
        with pytest.raises(ValueError, match="Semantic Model Dataset name"):
            parse_semantic_model_dataset_identity(_LEGACY, None)


class TestGenerateDestinationUrn:
    def test_lowercased_by_default(self):
        assert (
            gen_semantic_model_dataset_urn(
                DbtSemanticModelIdentity("Orders"), "Jaffle_Shop", None, "PROD", True
            )
            == _NEW
        )

    def test_case_preserved_when_disabled(self):
        assert gen_semantic_model_dataset_urn(
            DbtSemanticModelIdentity("Orders"), "Jaffle_Shop", None, "PROD", False
        ) == (
            "urn:li:dataset:"
            "(urn:li:dataPlatform:dbt,Jaffle_Shop.semantic_layer.Orders,PROD)"
        )

    def test_platform_instance_appears_exactly_once(self):
        """The urn builder prefixes it; it must not also be in the name."""
        urn = gen_semantic_model_dataset_urn(
            DbtSemanticModelIdentity("orders"), "jaffle_shop", "inst", "PROD", True
        )
        assert urn == (
            "urn:li:dataset:"
            "(urn:li:dataPlatform:dbt,inst.jaffle_shop.semantic_layer.orders,PROD)"
        )
        assert urn.count("inst.") == 1


class TestBuildMapping:
    def test_project_name_synthesizes_destinations(self):
        mapping = build_mapping(
            _graph({}),
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY],
            project_name="jaffle_shop",
            pair_by_name=False,
        )
        assert mapping.pairs == {_LEGACY: _NEW}
        assert mapping.unresolved == {}

    def test_explicit_pairs_take_precedence(self):
        mapping = build_mapping(
            _graph({}),
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY],
            project_name="ignored",
            explicit_pairs={
                _LEGACY: "urn:li:dataset:(urn:li:dataPlatform:dbt,x.y,PROD)"
            },
        )
        assert mapping.pairs[_LEGACY].endswith("x.y,PROD)")

    def test_unmapped_urn_is_reported_not_guessed(self):
        mapping = build_mapping(
            _graph({}),
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY],
            explicit_pairs={
                "urn:li:dataset:(urn:li:dataPlatform:dbt,a.b.c,PROD)": _NEW
            },
        )
        assert mapping.pairs == {}
        assert "not present in the mapping file" in mapping.unresolved[_LEGACY]

    def test_pair_by_name_joins_on_the_shared_component(self):
        graph = _graph({})
        graph.get_urns_by_filter.return_value = [_NEW]
        mapping = build_mapping(
            graph, MigrationDirection.DATASET_TO_SM, [_LEGACY], pair_by_name=True
        )
        assert mapping.pairs == {_LEGACY: _NEW}

    def test_pair_by_name_reports_an_ambiguous_name(self):
        other = (
            "urn:li:dataset:"
            "(urn:li:dataPlatform:dbt,other_project.semantic_layer.orders,PROD)"
        )
        graph = _graph({})
        graph.get_urns_by_filter.return_value = [_NEW, other]
        mapping = build_mapping(
            graph, MigrationDirection.DATASET_TO_SM, [_LEGACY], pair_by_name=True
        )
        assert mapping.pairs == {}
        assert "ambiguous" in mapping.unresolved[_LEGACY]

    def test_pair_by_name_reports_a_missing_counterpart(self):
        graph = _graph({})
        graph.get_urns_by_filter.return_value = []
        mapping = build_mapping(
            graph, MigrationDirection.DATASET_TO_SM, [_LEGACY], pair_by_name=True
        )
        assert mapping.pairs == {}
        assert "no counterpart" in mapping.unresolved[_LEGACY]

    def test_rollback_pairs_by_name_in_the_other_direction(self):
        graph = _graph({})
        graph.get_urns_by_filter.return_value = [_LEGACY]
        mapping = build_mapping(
            graph, MigrationDirection.SM_TO_DATASET, [_NEW], pair_by_name=True
        )
        assert mapping.pairs == {_NEW: _LEGACY}

    def test_project_name_is_ignored_for_rollback(self):
        """The legacy database.schema prefix cannot be synthesized."""
        graph = _graph({})
        graph.get_urns_by_filter.return_value = []
        mapping = build_mapping(
            graph,
            MigrationDirection.SM_TO_DATASET,
            [_NEW],
            project_name="jaffle_shop",
            pair_by_name=True,
        )
        assert mapping.pairs == {}

    def test_no_mapping_option_reports_rather_than_failing(self):
        mapping = build_mapping(
            _graph({}), MigrationDirection.DATASET_TO_SM, [_LEGACY], pair_by_name=False
        )
        assert mapping.pairs == {}
        assert "cannot resolve" in mapping.unresolved[_LEGACY]


# --- Discovery --------------------------------------------------------------


class TestDiscovery:
    def test_legacy_discovery_filters_on_the_exact_subtype(self):
        graph = _graph({})
        graph.get_urns_by_filter.return_value = [_LEGACY]

        assert discover_legacy_dataset_urns(graph, env="PROD") == [_LEGACY]

        kwargs = graph.get_urns_by_filter.call_args[1]
        assert kwargs["entity_types"] == ["dataset"]
        assert kwargs["platform"] == "dbt"
        extra_filter = kwargs["extraFilters"][0]
        assert extra_filter["field"] == "typeNames"
        assert extra_filter["condition"] == "EQUAL"
        # An exact match, so it can never pick up its own destinations.
        assert extra_filter["values"] == [LEGACY_SUBTYPE]


class TestSubtypeFilter:
    def test_forward_requires_the_legacy_subtype(self):
        graph = _graph(
            {
                _LEGACY: {"subTypes": SubTypesClass(typeNames=[LEGACY_SUBTYPE])},
                _NEW: {
                    "subTypes": SubTypesClass(
                        typeNames=[SEMANTIC_MODEL_DATASET_SUBTYPE]
                    )
                },
            }
        )
        valid, skipped = filter_by_expected_subtype(
            graph, [_LEGACY, _NEW], False, MigrationDirection.DATASET_TO_SM
        )
        assert valid == [_LEGACY]
        assert skipped == [_NEW]

    def test_rollback_requires_the_new_subtype(self):
        graph = _graph(
            {
                _LEGACY: {"subTypes": SubTypesClass(typeNames=[LEGACY_SUBTYPE])},
                _NEW: {
                    "subTypes": SubTypesClass(
                        typeNames=[SEMANTIC_MODEL_DATASET_SUBTYPE]
                    )
                },
            }
        )
        valid, skipped = filter_by_expected_subtype(
            graph, [_LEGACY, _NEW], False, MigrationDirection.SM_TO_DATASET
        )
        assert valid == [_NEW]
        assert skipped == [_LEGACY]

    def test_force_bypasses_the_check(self):
        graph = _graph({})
        valid, skipped = filter_by_expected_subtype(
            graph, [_LEGACY, _NEW], True, MigrationDirection.DATASET_TO_SM
        )
        assert valid == [_LEGACY, _NEW]
        assert skipped == []


# --- Governance copy --------------------------------------------------------


class TestMigrateOneDataset:
    def test_entity_governance_and_editable_description_are_copied(self):
        ownership = OwnershipClass(
            owners=[OwnerClass(owner="urn:li:corpuser:a", type="TECHNICAL_OWNER")]
        )
        tags = GlobalTagsClass(tags=[TagAssociationClass(tag=make_tag_urn("gold"))])
        # editableDatasetProperties is not in GOVERNANCE_ASPECTS; it reaches the
        # destination only via this migration's extra_aspects, and it is the
        # only path by which a hand-authored description survives.
        editable = EditableDatasetPropertiesClass(description="hand-written")
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": ownership,
                    "globalTags": tags,
                    "editableDatasetProperties": editable,
                    "status": StatusClass(removed=False),
                }
            }
        )

        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        assert result.error is None
        assert set(result.aspects_copied) == {
            "ownership",
            "globalTags",
            "editableDatasetProperties",
        }
        emitted = _emitted(graph)
        assert ownership in emitted[_NEW]
        assert tags in emitted[_NEW]
        assert editable in emitted[_NEW]

    def test_field_governance_failure_is_not_reported_as_migrated(self):
        """Losing every column tag must not read as a clean migration."""
        graph = _graph({_LEGACY: {"ownership": OwnershipClass(owners=[])}})
        graph.get_aspects_for_entity.side_effect = _raise_on_schema_read(graph)

        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        assert result.field_errors
        # The entity-level copy did succeed, so `error` stays None -- but the
        # report has its own headline for this.
        report = run_migration(
            graph,
            MigrationDirection.DATASET_TO_SM,
            [],
            build_mapping(graph, MigrationDirection.DATASET_TO_SM, []),
            False,
            False,
            False,
        )
        report.results.append(result)
        assert "Entities with field-governance failures = 1" in repr(report)
        assert "field governance FAILED" in repr(report)

    def test_missing_source_is_an_error_not_a_crash(self):
        result = migrate_one_dataset(_graph({}), _LEGACY, _NEW, False, False, False)
        assert result.error is not None
        assert "does not exist" in result.error

    def test_dry_run_emits_nothing(self):
        graph = _graph({_LEGACY: {"ownership": OwnershipClass(owners=[])}})
        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, True, False)
        assert result.aspects_copied == ["ownership"]
        graph.emit_mcp.assert_not_called()

    def test_soft_deleted_source_is_noted(self):
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "status": StatusClass(removed=True),
                }
            }
        )
        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)
        assert "source is soft-deleted" in result.notes


class TestFieldGovernance:
    def test_dbt_column_tags_are_never_stripped_as_synthetic(self):
        """Snowflake strips DIMENSION/FACT/METRIC tags; dbt has no such tags."""
        graph = _graph(
            {
                _LEGACY: {
                    "schemaMetadata": SchemaMetadataClass(
                        schemaName="x",
                        platform="urn:li:dataPlatform:dbt",
                        version=0,
                        hash="",
                        platformSchema=None,  # type: ignore[arg-type]
                        fields=[
                            _schema_field(
                                "order_id",
                                globalTags=GlobalTagsClass(
                                    tags=[
                                        TagAssociationClass(
                                            tag=make_tag_urn("dimension")
                                        )
                                    ]
                                ),
                            )
                        ],
                    )
                }
            }
        )

        fields = collect_dataset_field_governance(graph, _LEGACY)

        assert len(fields) == 1
        assert fields[0].global_tags is not None
        assert [t.tag for t in fields[0].global_tags.tags] == [
            make_tag_urn("dimension")
        ]

    def test_column_tags_are_merged_into_the_destination_editable_schema(self):
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="order_id",
                                globalTags=GlobalTagsClass(
                                    tags=[TagAssociationClass(tag=make_tag_urn("pii"))]
                                ),
                                glossaryTerms=GlossaryTermsClass(
                                    terms=[
                                        GlossaryTermAssociationClass(
                                            urn="urn:li:glossaryTerm:Key"
                                        )
                                    ],
                                    auditStamp=None,  # type: ignore[arg-type]
                                ),
                            )
                        ]
                    ),
                },
                _NEW: {
                    "schemaMetadata": SchemaMetadataClass(
                        schemaName="x",
                        platform="urn:li:dataPlatform:dbt",
                        version=0,
                        hash="",
                        platformSchema=None,  # type: ignore[arg-type]
                        fields=[_schema_field("order_id")],
                    ),
                    # A description on the destination must survive the merge.
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="order_id", description="kept"
                            )
                        ]
                    ),
                },
            }
        )

        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        assert result.error is None
        merged = [
            a
            for a in _emitted(graph)[_NEW]
            if isinstance(a, EditableSchemaMetadataClass)
        ]
        assert len(merged) == 1
        info = merged[0].editableSchemaFieldInfo[0]
        assert info.fieldPath == "order_id"
        assert info.description == "kept"
        assert info.globalTags is not None
        assert [t.tag for t in info.globalTags.tags] == [make_tag_urn("pii")]
        assert info.glossaryTerms is not None
        assert [t.urn for t in info.glossaryTerms.terms] == ["urn:li:glossaryTerm:Key"]

    def test_field_path_is_synthesized_with_a_note_before_ingest(self):
        """Migrate-before-ingest: the destination has no schema to join to."""
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="Order_Id",
                                globalTags=GlobalTagsClass(
                                    tags=[TagAssociationClass(tag=make_tag_urn("pii"))]
                                ),
                            )
                        ]
                    ),
                }
            }
        )

        result = migrate_one_dataset(graph, _LEGACY, _NEW, True, False, False)

        assert any("no schemaMetadata on destination" in n for n in result.notes)
        merged = [
            a
            for a in _emitted(graph)[_NEW]
            if isinstance(a, EditableSchemaMetadataClass)
        ]
        assert merged[0].editableSchemaFieldInfo[0].fieldPath == "order_id"


# --- Batch behaviour --------------------------------------------------------


class TestRunMigration:
    def test_one_unmapped_urn_does_not_abort_the_batch(self):
        other_legacy = (
            "urn:li:dataset:(urn:li:dataPlatform:dbt,pagila.public.customers,PROD)"
        )
        graph = _graph(
            {
                _LEGACY: {"ownership": OwnershipClass(owners=[])},
                other_legacy: {"ownership": OwnershipClass(owners=[])},
            }
        )
        mapping = build_mapping(
            graph,
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY, other_legacy],
            explicit_pairs={_LEGACY: _NEW},
        )

        report = run_migration(
            graph,
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY, other_legacy],
            mapping,
            False,
            False,
            False,
        )

        assert len(report.results) == 2
        succeeded = [r for r in report.results if r.error is None]
        failed = [r for r in report.results if r.error is not None]
        assert [r.src_urn for r in succeeded] == [_LEGACY]
        assert [r.src_urn for r in failed] == [other_legacy]
        assert "not present in the mapping file" in (failed[0].error or "")

    def test_report_names_dbt_and_the_legacy_subtype(self):
        report = run_migration(
            _graph({}),
            MigrationDirection.DATASET_TO_SM,
            [],
            build_mapping(_graph({}), MigrationDirection.DATASET_TO_SM, []),
            False,
            True,
            False,
            subtype_skipped=[_NEW],
        )
        text = repr(report)
        assert "dbt Semantic Model Migration Report" in text
        assert "[Dry Run]" in text
        assert LEGACY_SUBTYPE in text


class TestRoundTrip:
    def test_forward_then_rollback_preserves_governance(self):
        ownership = OwnershipClass(
            owners=[OwnerClass(owner="urn:li:corpuser:a", type="TECHNICAL_OWNER")]
        )
        editable = EditableDatasetPropertiesClass(description="hand-written")
        aspects: Dict[str, Dict[str, _Aspect]] = {
            _LEGACY: {"ownership": ownership, "editableDatasetProperties": editable}
        }
        graph = _graph(aspects)

        forward = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)
        assert forward.error is None

        # Feed the forward writes back in as the rollback source.
        for aspect in _emitted(graph)[_NEW]:
            aspects.setdefault(_NEW, {})[aspect.get_aspect_name()] = aspect
        graph.emit_mcp.reset_mock()

        back = migrate_one_dataset(graph, _NEW, _LEGACY, False, False, False)

        assert back.error is None
        emitted = _emitted(graph)[_LEGACY]
        assert ownership in emitted
        assert editable in emitted


class TestFieldPathResolution:
    def test_renamed_column_falls_back_with_a_note(self):
        """The realistic case: the destination has a schema, minus this column."""
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="renamed_measure",
                                globalTags=GlobalTagsClass(
                                    tags=[TagAssociationClass(tag=make_tag_urn("pii"))]
                                ),
                            )
                        ]
                    ),
                },
                _NEW: {
                    "schemaMetadata": SchemaMetadataClass(
                        schemaName="x",
                        platform="urn:li:dataPlatform:dbt",
                        version=0,
                        hash="",
                        platformSchema=None,  # type: ignore[arg-type]
                        fields=[_schema_field("a_different_column")],
                    )
                },
            }
        )

        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        assert any("not in destination schemaMetadata" in note for note in result.notes)


class TestBatchIsolation:
    def test_an_unexpected_exception_does_not_abort_the_batch(self):
        """Distinct from a mapping miss, which returns an error result."""
        other = "urn:li:dataset:(urn:li:dataPlatform:dbt,pagila.public.customers,PROD)"
        graph = _graph(
            {
                _LEGACY: {"ownership": OwnershipClass(owners=[])},
                other: {"ownership": OwnershipClass(owners=[])},
            }
        )
        graph.exists.side_effect = lambda urn: _raise_boom() if urn == _LEGACY else True
        mapping = build_mapping(
            graph,
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY, other],
            explicit_pairs={_LEGACY: _NEW, other: _NEW},
        )

        report = run_migration(
            graph,
            MigrationDirection.DATASET_TO_SM,
            [_LEGACY, other],
            mapping,
            False,
            False,
            False,
        )

        assert len(report.results) == 2
        failed = [r for r in report.results if r.error is not None]
        assert [r.src_urn for r in failed] == [_LEGACY]
        # The type is carried, not just str(e).
        assert failed[0].error is not None
        assert failed[0].error.startswith("RuntimeError:")


def _raise_boom() -> bool:
    raise RuntimeError("boom")


def test_migration_destination_urn_matches_what_ingest_emits() -> None:
    """The two must not drift: governance would land on an unused URN.

    Built from the mapper's own naming so a change to either side fails here
    rather than silently writing to a URN nothing else uses.
    """
    from datahub.ingestion.source.dbt.dbt_semantic_model import DbtSemanticModelMapper

    config = DBTCommonConfig.model_validate({"target_platform": "postgres"})
    mapper = DbtSemanticModelMapper(
        config=config, report=DBTSourceReport(), project_name="jaffle_shop"
    )
    ingest_name = mapper._logical_dataset_name("orders")

    migration_urn = gen_semantic_model_dataset_urn(
        DbtSemanticModelIdentity("orders"), "jaffle_shop", None, "PROD", True
    )

    assert DatasetUrn.from_string(migration_urn).name == ingest_name
    # And the parse round-trips what the generator produced.
    assert parse_semantic_model_dataset_identity(
        migration_urn, None
    ) == DbtSemanticModelIdentity("orders")


class TestColumnDescriptions:
    def test_human_authored_column_description_is_carried(self):
        """editableDatasetProperties is copied; the column analogue must be too."""
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="customer_email",
                                description="hand-written column note",
                                globalTags=GlobalTagsClass(
                                    tags=[TagAssociationClass(tag=make_tag_urn("pii"))]
                                ),
                            )
                        ]
                    ),
                }
            }
        )

        result = migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        merged = next(
            a
            for a in _emitted(graph)[_NEW]
            if isinstance(a, EditableSchemaMetadataClass)
        )
        info = merged.editableSchemaFieldInfo[0]
        assert info.description == "hand-written column note"
        assert any("description:customer_email" in m for m in result.fields_migrated)

    def test_ingested_column_description_is_not_promoted_to_the_editable_layer(self):
        """A schemaMetadata description would shadow the destination's own ingest."""
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "schemaMetadata": SchemaMetadataClass(
                        schemaName="x",
                        platform="urn:li:dataPlatform:dbt",
                        version=0,
                        hash="",
                        platformSchema=None,  # type: ignore[arg-type]
                        fields=[
                            _schema_field(
                                "customer_id",
                                description="ingested, regenerated every run",
                                globalTags=GlobalTagsClass(
                                    tags=[TagAssociationClass(tag=make_tag_urn("pii"))]
                                ),
                            )
                        ],
                    ),
                }
            }
        )

        migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        merged = next(
            a
            for a in _emitted(graph)[_NEW]
            if isinstance(a, EditableSchemaMetadataClass)
        )
        info = merged.editableSchemaFieldInfo[0]
        # The tag is carried; the ingested description deliberately is not.
        assert info.globalTags is not None
        assert info.description is None

    def test_destination_description_survives_when_the_source_has_none(self):
        graph = _graph(
            {
                _LEGACY: {
                    "ownership": OwnershipClass(owners=[]),
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="customer_email",
                                globalTags=GlobalTagsClass(
                                    tags=[TagAssociationClass(tag=make_tag_urn("pii"))]
                                ),
                            )
                        ]
                    ),
                },
                _NEW: {
                    "editableSchemaMetadata": EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=[
                            EditableSchemaFieldInfoClass(
                                fieldPath="customer_email",
                                description="authored on the destination",
                            )
                        ]
                    )
                },
            }
        )

        migrate_one_dataset(graph, _LEGACY, _NEW, False, False, False)

        merged = next(
            a
            for a in _emitted(graph)[_NEW]
            if isinstance(a, EditableSchemaMetadataClass)
        )
        assert (
            merged.editableSchemaFieldInfo[0].description
            == "authored on the destination"
        )


class TestEnvMismatch:
    def test_matching_env_is_silent(self):
        assert env_mismatches(_LEGACY, "PROD") is None

    def test_mismatched_env_is_reported(self):
        reason = env_mismatches(_LEGACY, "DEV")
        assert reason is not None
        assert "source env PROD does not match --env DEV" in reason

    def test_an_unparseable_urn_is_left_to_the_mapping_step(self):
        assert env_mismatches("not-a-urn", "PROD") is None
