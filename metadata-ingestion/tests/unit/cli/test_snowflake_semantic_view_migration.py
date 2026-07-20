"""Tests for datahub.cli.snowflake_semantic_view_migration — urn mapping and
governance-aspect migration between legacy Snowflake datasets and the new
semanticModel/metric entities. All tests mock the graph; no live GMS."""

from typing import Optional
from unittest.mock import MagicMock

import pytest

from datahub.cli.snowflake_semantic_view_migration import (
    GOVERNANCE_ASPECTS,
    SKIPPED_ASPECTS,
    EntityMigrationResult,
    MigrationDirection,
    SnowflakeViewIdentity,
    collect_dataset_field_governance,
    dataset_urn_to_semantic_model_urn,
    discover_semantic_model_urns,
    discover_semantic_view_dataset_urns,
    filter_by_semantic_view_subtype,
    gen_dataset_urn,
    gen_metric_urn,
    gen_semantic_model_urn,
    migrate_dataset_to_semantic_model,
    migrate_semantic_model_to_dataset,
    parse_dataset_identity,
    parse_semantic_model_identity,
    run_migration,
    semantic_model_urn_to_dataset_urn,
    snowflake_identifier,
)
from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.graph.openapi import RelatedEntity
from datahub.metadata.schema_classes import (
    DocumentationAssociationClass,
    DocumentationClass,
    EditableDatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
)

_DB = "TEST_DB"
_SCHEMA = "PUBLIC"
_VIEW = "Sales_Analytics"


def _identity(
    db: str = _DB, schema: str = _SCHEMA, view: str = _VIEW
) -> SnowflakeViewIdentity:
    return SnowflakeViewIdentity(db=db, schema=schema, view=view)


# --- URN mapping: golden examples matching the Snowflake connector exactly ---


class TestUrnMappingLowercaseNoInstance:
    def test_semantic_model_urn(self):
        urn = gen_semantic_model_urn(
            _identity(), platform_instance=None, convert_urns_to_lowercase=True
        )
        assert urn == (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"
        )

    def test_metric_urn(self):
        urn = gen_metric_urn(
            _identity(),
            "Total_Revenue",
            platform_instance=None,
            convert_urns_to_lowercase=True,
        )
        assert urn == (
            "urn:li:metric:(urn:li:dataPlatform:snowflake,"
            "test_db.public.sales_analytics,total_revenue)"
        )

    def test_dataset_urn(self):
        urn = gen_dataset_urn(
            _identity(),
            platform_instance=None,
            env="PROD",
            convert_urns_to_lowercase=True,
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"
        )


class TestUrnMappingInstanceNoLowercase:
    def test_semantic_model_urn(self):
        urn = gen_semantic_model_urn(
            _identity(),
            platform_instance="my_instance",
            convert_urns_to_lowercase=False,
        )
        assert urn == (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,"
            f"my_instance.{_DB}.{_SCHEMA},{_VIEW})"
        )

    def test_metric_urn(self):
        urn = gen_metric_urn(
            _identity(),
            "Total_Revenue",
            platform_instance="my_instance",
            convert_urns_to_lowercase=False,
        )
        assert urn == (
            "urn:li:metric:(urn:li:dataPlatform:snowflake,"
            f"my_instance.{_DB}.{_SCHEMA}.{_VIEW},Total_Revenue)"
        )

    def test_dataset_urn(self):
        urn = gen_dataset_urn(
            _identity(),
            platform_instance="my_instance",
            env="PROD",
            convert_urns_to_lowercase=False,
        )
        assert urn == (
            f"urn:li:dataset:(urn:li:dataPlatform:snowflake,my_instance.{_DB}.{_SCHEMA}.{_VIEW},PROD)"
        )


class TestSnowflakeIdentifier:
    def test_lowercases_when_configured(self):
        assert snowflake_identifier("Sales_Analytics", True) == "sales_analytics"

    def test_preserves_case_when_disabled(self):
        assert snowflake_identifier("Sales_Analytics", False) == "Sales_Analytics"


# --- URN parsing (urn -> identity) and round trips ---


class TestParseDatasetIdentity:
    def test_parses_without_instance(self):
        identity = parse_dataset_identity(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)",
            platform_instance=None,
        )
        assert identity == SnowflakeViewIdentity("test_db", "public", "sales_analytics")

    def test_parses_with_instance(self):
        identity = parse_dataset_identity(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            f"my_instance.{_DB}.{_SCHEMA}.{_VIEW},PROD)",
            platform_instance="my_instance",
        )
        assert identity == SnowflakeViewIdentity(_DB, _SCHEMA, _VIEW)

    def test_raises_on_missing_instance_prefix(self):
        with pytest.raises(ValueError, match="does not start with expected"):
            parse_dataset_identity(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)",
                platform_instance="my_instance",
            )

    def test_raises_on_wrong_number_of_parts(self):
        with pytest.raises(ValueError, match="exactly"):
            parse_dataset_identity(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,just_two.parts,PROD)",
                platform_instance=None,
            )


class TestParseSemanticModelIdentity:
    def test_parses_without_instance(self):
        identity = parse_semantic_model_identity(
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)",
            platform_instance=None,
        )
        assert identity == SnowflakeViewIdentity("test_db", "public", "sales_analytics")

    def test_parses_with_instance(self):
        identity = parse_semantic_model_identity(
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,"
            f"my_instance.{_DB}.{_SCHEMA},{_VIEW})",
            platform_instance="my_instance",
        )
        assert identity == SnowflakeViewIdentity(_DB, _SCHEMA, _VIEW)

    def test_raises_on_wrong_number_of_parts(self):
        with pytest.raises(ValueError, match="exactly"):
            parse_semantic_model_identity(
                "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,only_one,view)",
                platform_instance=None,
            )


class TestRoundTrip:
    def test_dataset_to_sm_and_back_lowercase(self):
        ds = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"
        sm = dataset_urn_to_semantic_model_urn(
            ds, platform_instance=None, convert_urns_to_lowercase=True
        )
        assert (
            sm
            == "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"
        )
        back = semantic_model_urn_to_dataset_urn(
            sm, platform_instance=None, env="PROD", convert_urns_to_lowercase=True
        )
        assert back == ds

    def test_dataset_to_sm_and_back_with_instance_no_lowercase(self):
        ds = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            f"my_instance.{_DB}.{_SCHEMA}.{_VIEW},PROD)"
        )
        sm = dataset_urn_to_semantic_model_urn(
            ds, platform_instance="my_instance", convert_urns_to_lowercase=False
        )
        assert sm == (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,"
            f"my_instance.{_DB}.{_SCHEMA},{_VIEW})"
        )
        back = semantic_model_urn_to_dataset_urn(
            sm,
            platform_instance="my_instance",
            env="PROD",
            convert_urns_to_lowercase=False,
        )
        assert back == ds


# --- Aspect allowlist ---


class TestAspectAllowlist:
    def test_governance_aspects_are_human_governance_only(self):
        assert set(GOVERNANCE_ASPECTS) == {
            "ownership",
            "domains",
            "globalTags",
            "glossaryTerms",
            "institutionalMemory",
            "structuredProperties",
            "documentation",
            "deprecation",
            "applications",
        }

    def test_skipped_aspects_exclude_governance_aspects(self):
        assert not set(SKIPPED_ASPECTS) & set(GOVERNANCE_ASPECTS)

    def test_skipped_aspects_include_lineage_and_schema(self):
        for aspect in ("upstreamLineage", "schemaMetadata", "status", "subTypes"):
            assert aspect in SKIPPED_ASPECTS


# --- migrate_entity / migrate_dataset_to_semantic_model / migrate_semantic_model_to_dataset ---


def _graph_with_aspects(**stored) -> MagicMock:
    graph = MagicMock()

    def get_aspects(entity_urn, aspects, aspect_types):
        return {name: stored.get(name) for name in aspects}

    graph.get_aspects_for_entity.side_effect = get_aspects
    graph.get_related_entities.return_value = []
    return graph


def _schema_field(
    field_path: str,
    tags: Optional[list] = None,
    json_props: Optional[str] = None,
) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=field_path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR",
        globalTags=(
            GlobalTagsClass(tags=[TagAssociationClass(tag=t) for t in tags])
            if tags
            else None
        ),
        jsonProps=json_props,
    )


class TestMigrateDatasetToSemanticModel:
    SRC = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"

    def test_copies_present_governance_aspects(self):
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:alice", type=OwnershipTypeClass.DATAOWNER
                )
            ]
        )
        tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:pii")])
        graph = _graph_with_aspects(ownership=ownership, globalTags=tags)

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert result.dst_urn == (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"
        )
        assert set(result.aspects_copied) == {"ownership", "globalTags"}
        assert graph.emit_mcp.call_count == 2
        for call in graph.emit_mcp.call_args_list:
            mcp = call.args[0]
            assert mcp.entityUrn == result.dst_urn

    def test_dry_run_emits_nothing(self):
        graph = _graph_with_aspects(ownership=OwnershipClass(owners=[]))

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert result.aspects_copied == ["ownership"]
        graph.emit_mcp.assert_not_called()

    def test_no_governance_aspects_present(self):
        graph = _graph_with_aspects()

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert result.aspects_copied == []
        graph.emit_mcp.assert_not_called()

    def test_bad_instance_prefix_produces_error_result_not_exception(self):
        graph = MagicMock()

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance="some_other_instance",
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=False,
        )

        assert result.error is not None
        assert "does not start with expected" in result.error
        graph.emit_mcp.assert_not_called()

    def test_inbound_refs_collected_when_requested(self):
        graph = _graph_with_aspects()
        related = RelatedEntity(
            urn="urn:li:dashboard:(looker,my_dash)", relationship_type="Consumes"
        )
        graph.get_related_entities.return_value = [related]

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=True,
        )

        assert result.inbound_refs == [related]
        graph.get_related_entities.assert_called_once()

    def test_inbound_refs_not_collected_by_default(self):
        graph = _graph_with_aspects()

        migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=False,
        )

        graph.get_related_entities.assert_not_called()


class TestEditableDescriptionFallback:
    SRC = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"

    def test_folds_editable_description_when_no_documentation_present(self):
        def get_aspects(entity_urn: str, aspects, aspect_types):
            if aspects == ["editableDatasetProperties"]:
                return {
                    "editableDatasetProperties": EditableDatasetPropertiesClass(
                        description="legacy hand-written description"
                    )
                }
            if aspects == ["documentation"]:
                return {"documentation": None}
            return {}

        graph = MagicMock()
        graph.get_aspects_for_entity.side_effect = get_aspects

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert "documentation" in result.aspects_copied
        assert any("editableDatasetProperties.description" in n for n in result.notes)
        emitted_aspects = [
            call.args[0].aspect for call in graph.emit_mcp.call_args_list
        ]
        assert any(
            isinstance(a, DocumentationClass)
            and a.documentations[0].documentation == "legacy hand-written description"
            for a in emitted_aspects
        )

    def test_does_not_overwrite_existing_documentation_on_destination(self):
        def get_aspects(entity_urn: str, aspects, aspect_types):
            if aspects == ["editableDatasetProperties"]:
                return {
                    "editableDatasetProperties": EditableDatasetPropertiesClass(
                        description="legacy hand-written description"
                    )
                }
            if aspects == ["documentation"]:
                return {
                    "documentation": DocumentationClass(
                        documentations=[
                            DocumentationAssociationClass(
                                documentation="already documented"
                            )
                        ]
                    )
                }
            return {}

        graph = MagicMock()
        graph.get_aspects_for_entity.side_effect = get_aspects

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert "documentation" not in result.aspects_copied
        assert any("skipped" in n for n in result.notes)

    def test_explicit_documentation_aspect_wins_over_editable_description(self):
        explicit_doc = DocumentationClass(
            documentations=[DocumentationAssociationClass(documentation="explicit doc")]
        )

        def get_aspects(entity_urn: str, aspects, aspect_types):
            if aspects == ["editableDatasetProperties"]:
                return {
                    "editableDatasetProperties": EditableDatasetPropertiesClass(
                        description="legacy description"
                    )
                }
            if "documentation" in aspects:
                return {"documentation": explicit_doc}
            return {}

        graph = MagicMock()
        graph.get_aspects_for_entity.side_effect = get_aspects

        migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        emitted_aspects = [
            call.args[0].aspect for call in graph.emit_mcp.call_args_list
        ]
        docs = [a for a in emitted_aspects if isinstance(a, DocumentationClass)]
        assert len(docs) == 1
        assert docs[0].documentations[0].documentation == "explicit doc"


class TestMigrateSemanticModelToDataset:
    SRC = "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"

    def test_maps_to_dataset_urn_with_env(self):
        graph = _graph_with_aspects(domains=None)

        result = migrate_semantic_model_to_dataset(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert result.dst_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"
        )

    def test_dry_run_emits_nothing(self):
        graph = _graph_with_aspects(
            ownership=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:bob", type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
        )

        result = migrate_semantic_model_to_dataset(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
        )

        assert result.aspects_copied == ["ownership"]
        graph.emit_mcp.assert_not_called()

    def test_does_not_fold_editable_description(self):
        """sm-to-dataset has no editableDatasetProperties source, so the fallback must not fire."""
        graph = _graph_with_aspects()

        migrate_semantic_model_to_dataset(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
        )

        for call in graph.get_aspects_for_entity.call_args_list:
            assert call.kwargs.get("aspects") != ["editableDatasetProperties"]

    def test_uses_env_for_dataset_reconstruction(self):
        graph = _graph_with_aspects()

        result = migrate_semantic_model_to_dataset(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="DEV",
            dry_run=True,
            report_inbound_refs=False,
        )

        assert result.dst_urn.endswith(",DEV)")


# --- Discovery ---


class TestDiscoverSemanticViewDatasetUrns:
    def test_filters_by_subtype_and_platform(self):
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)"]
        )

        result = discover_semantic_view_dataset_urns(graph, env="PROD")

        assert result == ["urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)"]
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["entity_types"] == ["dataset"]
        assert kwargs["platform"] == "snowflake"
        assert kwargs["env"] == "PROD"
        assert kwargs["extraFilters"][0]["field"] == "typeNames"
        assert kwargs["extraFilters"][0]["values"] == ["Semantic View"]


class TestDiscoverSemanticModelUrns:
    def test_filters_by_platform(self):
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            ["urn:li:semanticModel:(urn:li:dataPlatform:snowflake,a.b,c)"]
        )

        result = discover_semantic_model_urns(graph)

        assert result == ["urn:li:semanticModel:(urn:li:dataPlatform:snowflake,a.b,c)"]
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["entity_types"] == ["semanticModel"]
        assert kwargs["platform"] == "snowflake"


class TestFilterBySemanticViewSubtype:
    URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)"

    def test_force_bypasses_check(self):
        graph = MagicMock()
        valid, skipped = filter_by_semantic_view_subtype(graph, [self.URN], force=True)
        assert valid == [self.URN]
        assert skipped == []
        graph.get_aspects_for_entity.assert_not_called()

    def test_keeps_urns_with_semantic_view_subtype(self):
        graph = MagicMock()
        graph.get_aspects_for_entity.return_value = {
            "subTypes": SubTypesClass(typeNames=["Semantic View"])
        }
        valid, skipped = filter_by_semantic_view_subtype(graph, [self.URN], force=False)
        assert valid == [self.URN]
        assert skipped == []

    def test_skips_urns_without_semantic_view_subtype(self):
        graph = MagicMock()
        graph.get_aspects_for_entity.return_value = {
            "subTypes": SubTypesClass(typeNames=["View"])
        }
        valid, skipped = filter_by_semantic_view_subtype(graph, [self.URN], force=False)
        assert valid == []
        assert skipped == [self.URN]

    def test_skips_urns_with_no_subtype_aspect(self):
        graph = MagicMock()
        graph.get_aspects_for_entity.return_value = {"subTypes": None}
        valid, skipped = filter_by_semantic_view_subtype(graph, [self.URN], force=False)
        assert valid == []
        assert skipped == [self.URN]


# --- run_migration (batch driver) ---


class TestRunMigration:
    def test_dataset_to_sm_direction_dry_run(self):
        graph = _graph_with_aspects(ownership=OwnershipClass(owners=[]))
        src = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"

        report = run_migration(
            graph=graph,
            direction=MigrationDirection.DATASET_TO_SM,
            urns=[src],
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
        )

        assert len(report.results) == 1
        assert report.results[0].dst_urn.startswith("urn:li:semanticModel:")
        graph.emit_mcp.assert_not_called()

    def test_sm_to_dataset_direction_dry_run(self):
        graph = _graph_with_aspects(ownership=OwnershipClass(owners=[]))
        src = "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"

        report = run_migration(
            graph=graph,
            direction=MigrationDirection.SM_TO_DATASET,
            urns=[src],
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
        )

        assert len(report.results) == 1
        assert report.results[0].dst_urn.startswith("urn:li:dataset:")
        graph.emit_mcp.assert_not_called()

    def test_multiple_urns_all_processed(self):
        graph = _graph_with_aspects()
        srcs = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.view_a,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.view_b,PROD)",
        ]

        report = run_migration(
            graph=graph,
            direction=MigrationDirection.DATASET_TO_SM,
            urns=srcs,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
        )

        assert len(report.results) == 2
        assert {r.src_urn for r in report.results} == set(srcs)

    def test_report_repr_includes_direction_and_counts(self):
        graph = _graph_with_aspects()
        report = run_migration(
            graph=graph,
            direction=MigrationDirection.DATASET_TO_SM,
            urns=[],
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=True,
            report_inbound_refs=False,
            subtype_skipped=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,skipped.a.b,PROD)"
            ],
        )

        text = repr(report)
        assert "dataset-to-sm" in text
        assert "Entities skipped" in text
        assert "1" in text


class TestEntityMigrationResultDefaults:
    def test_defaults_are_independent_per_instance(self):
        """Mutable dataclass defaults must not be shared across instances."""
        a = EntityMigrationResult(src_urn="a", dst_urn="b")
        b = EntityMigrationResult(src_urn="c", dst_urn="d")
        a.aspects_copied.append("ownership")
        a.fields_migrated.append("x")
        assert b.aspects_copied == []
        assert b.fields_migrated == []


class TestFieldGovernanceFanOut:
    SRC = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"
    SM = "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"
    METRIC = (
        "urn:li:metric:(urn:li:dataPlatform:snowflake,"
        "test_db.public.sales_analytics,total_revenue)"
    )
    DIM_FIELD = (
        "urn:li:schemaField:("
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics),"
        "customer_id)"
    )

    def test_collect_strips_synthetic_subtype_tags_keeps_customer_tags(self):
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field(
                    "CUSTOMER_ID",
                    tags=[make_tag_urn("DIMENSION"), make_tag_urn("pii")],
                ),
                _schema_field(
                    "TOTAL_REVENUE",
                    tags=[make_tag_urn("METRIC"), make_tag_urn("finance")],
                    json_props='{"columnSubType": "METRIC"}',
                ),
            ],
        )
        graph = _graph_with_aspects(schemaMetadata=schema)

        fields = collect_dataset_field_governance(graph, self.SRC)
        by_name = {f.column_name: f for f in fields}

        assert set(by_name) == {"CUSTOMER_ID", "TOTAL_REVENUE"}
        assert by_name["CUSTOMER_ID"].is_metric is False
        assert by_name["TOTAL_REVENUE"].is_metric is True
        assert [t.tag for t in by_name["CUSTOMER_ID"].global_tags.tags] == [
            make_tag_urn("pii")
        ]
        assert [t.tag for t in by_name["TOTAL_REVENUE"].global_tags.tags] == [
            make_tag_urn("finance")
        ]

    def test_editable_schema_tags_merge_onto_columns(self):
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field("TOTAL_REVENUE", tags=[make_tag_urn("METRIC")]),
            ],
        )
        editable = EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="TOTAL_REVENUE",
                    globalTags=GlobalTagsClass(
                        tags=[TagAssociationClass(tag=make_tag_urn("customer_tag"))]
                    ),
                )
            ]
        )
        graph = _graph_with_aspects(
            schemaMetadata=schema, editableSchemaMetadata=editable
        )

        fields = collect_dataset_field_governance(graph, self.SRC)
        assert len(fields) == 1
        assert fields[0].is_metric is True
        assert [t.tag for t in fields[0].global_tags.tags] == [
            make_tag_urn("customer_tag")
        ]

    def test_dataset_to_sm_emits_metric_and_schema_field_tags(self):
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field(
                    "CUSTOMER_ID",
                    tags=[make_tag_urn("DIMENSION"), make_tag_urn("pii")],
                ),
                _schema_field(
                    "TOTAL_REVENUE",
                    tags=[make_tag_urn("METRIC"), make_tag_urn("finance")],
                ),
            ],
        )
        graph = _graph_with_aspects(schemaMetadata=schema)

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert result.dst_urn == self.SM
        emitted = {
            (call.args[0].entityUrn, type(call.args[0].aspect).__name__)
            for call in graph.emit_mcp.call_args_list
        }
        assert (self.METRIC, "GlobalTagsClass") in emitted
        assert (self.DIM_FIELD, "GlobalTagsClass") in emitted
        # Synthetic DIMENSION/METRIC tags must not be copied
        for call in graph.emit_mcp.call_args_list:
            aspect = call.args[0].aspect
            if isinstance(aspect, GlobalTagsClass):
                tag_urns = {t.tag for t in aspect.tags}
                assert make_tag_urn("DIMENSION") not in tag_urns
                assert make_tag_urn("METRIC") not in tag_urns

    def test_dataset_to_sm_field_fan_out_respects_dry_run(self):
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field(
                    "TOTAL_REVENUE",
                    tags=[make_tag_urn("METRIC"), make_tag_urn("finance")],
                ),
            ],
        )
        graph = _graph_with_aspects(schemaMetadata=schema)

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=False,
        )

        assert any("total_revenue" in entry for entry in result.fields_migrated)
        graph.emit_mcp.assert_not_called()
