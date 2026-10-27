"""Tests for datahub.cli.snowflake_semantic_view_migration — urn mapping and
governance-aspect migration between legacy Snowflake datasets and the new
semanticModel/metric entities. All tests mock the graph; no live GMS."""

from typing import Dict, List, Optional, Type
from unittest.mock import MagicMock

import pytest

from datahub.cli.snowflake_semantic_view_migration import (
    GOVERNANCE_ASPECTS,
    SKIPPED_ASPECTS,
    EntityMigrationResult,
    MigrationDirection,
    SemanticViewMigrationReport,
    SnowflakeViewIdentity,
    _logical_table_names_from_source,
    _logical_table_names_from_view_logic,
    _schema_field_is_metric,
    _semantic_model_field_path,
    collect_dataset_field_governance,
    collect_semantic_model_field_governance,
    dataset_urn_to_semantic_model_urn,
    discover_semantic_model_urns,
    discover_semantic_view_dataset_urns,
    filter_by_semantic_view_subtype,
    gen_dataset_urn,
    gen_metric_urn,
    gen_semantic_model_urn,
    migrate_dataset_field_governance,
    migrate_dataset_to_semantic_model,
    migrate_semantic_model_field_governance,
    migrate_semantic_model_to_dataset,
    parse_dataset_identity,
    parse_semantic_model_identity,
    run_migration,
    semantic_model_urn_to_dataset_urn,
    snowflake_identifier,
)
from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.ingestion.graph.openapi import RelatedEntity
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableDatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
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
    ViewPropertiesClass,
    _Aspect,
)

_DB = "TEST_DB"
_SCHEMA = "PUBLIC"
_VIEW = "Sales_Analytics"
_AUDIT_STAMP = AuditStampClass(time=0, actor="urn:li:corpuser:datahub")


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


class TestSemanticModelFieldPath:
    """Pins the schemaField path shape mirrored from SnowflakeSemanticModelMapper
    (unmerged #18395) so a future casing/quoting change there has a CI signal here.
    """

    def test_uppercases_and_lowercases_when_configured(self):
        assert _semantic_model_field_path("customer_id", True) == "customer_id"

    def test_uppercases_and_preserves_case_when_disabled(self):
        assert _semantic_model_field_path("customer_id", False) == "CUSTOMER_ID"


class TestLogicalTableNamesFromViewLogic:
    def test_skips_sql_query_logical_table(self):
        """`alias AS (SELECT ...)` must not yield a logical name (no SMD to write to)."""
        ddl = (
            "CREATE SEMANTIC VIEW v AS TABLES (\n"
            "  derived AS (SELECT id, region FROM DB1.SCH1.RAW),\n"
            "  customers AS DB2.SCH2.CUSTOMERS PRIMARY KEY (id)\n"
            ")"
        )
        assert _logical_table_names_from_view_logic(ddl) == ["customers"]

    def test_alias_and_unaliased_physical_tables(self):
        ddl = (
            "CREATE SEMANTIC VIEW v AS TABLES (\n"
            "  orders AS DB1.SCH1.ORDERS PRIMARY KEY (id),\n"
            "  DB2.SCH2.CUSTOMERS UNIQUE (email)\n"
            ")"
        )
        assert _logical_table_names_from_view_logic(ddl) == ["orders", "CUSTOMERS"]

    def test_unions_synonym_keys_with_tables_clause(self):
        """TABLE_SYNONYM_* is incomplete (only tables with synonyms); union TABLES(...)."""
        src = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.public.sales_analytics,PROD)"
        )
        graph = _graph_with_aspects(
            datasetProperties=DatasetPropertiesClass(
                customProperties={"TABLE_SYNONYM_ORDERS": "ord"}
            ),
            viewProperties=ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=(
                    "CREATE SEMANTIC VIEW v AS TABLES (\n"
                    "  orders AS DB.SCH.ORDERS PRIMARY KEY (id),\n"
                    "  customers AS DB.SCH.CUSTOMERS PRIMARY KEY (id)\n"
                    ")"
                ),
            ),
        )
        names = _logical_table_names_from_source(graph, src)
        assert [n.upper() for n in names] == ["ORDERS", "CUSTOMERS"]
        assert names[0] == "ORDERS"


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


def _graph_with_aspects(**stored: Optional[_Aspect]) -> MagicMock:
    graph = MagicMock()

    def get_aspects(entity_urn, aspects, aspect_types):
        return {name: stored.get(name) for name in aspects}

    graph.get_aspects_for_entity.side_effect = get_aspects
    graph.get_related_entities.return_value = []
    # Entity migrate requires the source to exist; destinations may be absent.
    graph.exists.return_value = True
    return graph


def _schema_field(
    field_path: str,
    tags: Optional[list] = None,
    json_props: Optional[str] = None,
    terms: Optional[list] = None,
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
        glossaryTerms=(
            GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=t) for t in terms],
                auditStamp=_AUDIT_STAMP,
            )
            if terms
            else None
        ),
        jsonProps=json_props,
    )


def _schema_metadata(*fields: SchemaFieldClass) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="sales_analytics",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=list(fields),
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

    def test_allows_when_destination_does_not_exist(self):
        """Migrate-first: write governance onto mapped URNs before ingest."""
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:alice", type=OwnershipTypeClass.DATAOWNER
                )
            ]
        )
        graph = _graph_with_aspects(ownership=ownership)
        graph.exists.side_effect = lambda urn: urn == self.SRC

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert result.aspects_copied == ["ownership"]
        assert graph.emit_mcp.call_count == 1
        assert graph.emit_mcp.call_args.args[0].entityUrn == result.dst_urn

    def test_skips_when_source_does_not_exist(self):
        graph = _graph_with_aspects(ownership=OwnershipClass(owners=[]))
        graph.exists.return_value = False

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is not None
        assert "source entity does not exist" in result.error
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

        def get_related(entity_urn, relationship_types, direction):
            if "Consumes" in relationship_types:
                return [related]
            return []

        graph.get_related_entities.side_effect = get_related

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=True,
        )

        assert result.inbound_refs == [related]

    def test_inbound_refs_not_collected_by_default(self):
        graph = _graph_with_aspects()

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=True,
            report_inbound_refs=False,
        )

        assert result.inbound_refs == []
        # Field-governance discovery may probe IsPartOf; inbound-ref types must not.
        for call in graph.get_related_entities.call_args_list:
            types = call.kwargs.get("relationship_types") or (
                call.args[1] if len(call.args) > 1 else []
            )
            assert "Consumes" not in types
            assert "DownstreamOf" not in types


class TestEditableDescriptionFallback:
    SRC = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"

    def test_folds_editable_description_when_no_documentation_present(self):
        def get_aspects(
            entity_urn: str, aspects: List[str], aspect_types: List[Type[_Aspect]]
        ) -> Dict[str, Optional[_Aspect]]:
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
        def get_aspects(
            entity_urn: str, aspects: List[str], aspect_types: List[Type[_Aspect]]
        ) -> Dict[str, Optional[_Aspect]]:
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

        def get_aspects(
            entity_urn: str, aspects: List[str], aspect_types: List[Type[_Aspect]]
        ) -> Dict[str, Optional[_Aspect]]:
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

        result = discover_semantic_view_dataset_urns(
            graph, env="PROD", platform_instance="acct"
        )

        assert result == ["urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)"]
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["entity_types"] == ["dataset"]
        assert kwargs["platform"] == "snowflake"
        assert kwargs["platform_instance"] == "acct"
        assert kwargs["env"] == "PROD"
        assert kwargs["status"] == RemovedStatusFilter.NOT_SOFT_DELETED
        assert kwargs["extraFilters"][0]["field"] == "typeNames"
        assert kwargs["extraFilters"][0]["values"] == ["Semantic View"]

    def test_can_include_soft_deleted(self):
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter([])
        discover_semantic_view_dataset_urns(graph, include_soft_deleted=True)
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["status"] == RemovedStatusFilter.ALL

    def test_only_soft_deleted_probe(self):
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter([])
        discover_semantic_view_dataset_urns(graph, only_soft_deleted=True)
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["status"] == RemovedStatusFilter.ONLY_SOFT_DELETED


class TestDiscoverSemanticModelUrns:
    def test_filters_by_platform(self):
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            ["urn:li:semanticModel:(urn:li:dataPlatform:snowflake,a.b,c)"]
        )

        result = discover_semantic_model_urns(graph, platform_instance="acct")

        assert result == ["urn:li:semanticModel:(urn:li:dataPlatform:snowflake,a.b,c)"]
        _, kwargs = graph.get_urns_by_filter.call_args
        assert kwargs["entity_types"] == ["semanticModel"]
        assert kwargs["platform"] == "snowflake"
        assert kwargs["platform_instance"] == "acct"
        assert kwargs["status"] == RemovedStatusFilter.NOT_SOFT_DELETED


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

    def test_unexpected_error_for_one_urn_does_not_abort_batch(self):
        graph = _graph_with_aspects(ownership=OwnershipClass(owners=[]))
        ok_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.view_a,PROD)"
        )
        bad_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.view_b,PROD)"
        )

        def exists_side_effect(urn):
            if urn == bad_urn:
                raise RuntimeError("transient GMS error")
            return True

        graph.exists.side_effect = exists_side_effect

        report = run_migration(
            graph=graph,
            direction=MigrationDirection.DATASET_TO_SM,
            urns=[ok_urn, bad_urn],
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=False,
            report_inbound_refs=False,
        )

        assert len(report.results) == 2
        ok_result = next(r for r in report.results if r.src_urn == ok_urn)
        bad_result = next(r for r in report.results if r.src_urn == bad_urn)
        assert ok_result.error is None
        assert bad_result.error is not None
        assert "transient GMS error" in bad_result.error


class TestSemanticViewMigrationReportRepr:
    def test_shows_aspects_copied_before_failure(self):
        result = EntityMigrationResult(src_urn="src", dst_urn="dst", error="boom")
        result.aspects_copied.append("ownership")
        result.fields_migrated.append("globalTags:col->dst")
        report = SemanticViewMigrationReport(
            direction=MigrationDirection.DATASET_TO_SM, dry_run=False, results=[result]
        )

        text = repr(report)
        assert "ERROR: boom" in text
        assert "aspects copied before failure: ['ownership']" in text
        assert "field tags/terms copied before failure: globalTags:col->dst" in text


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
    SMD = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "test_db.public.sales_analytics.customers,PROD)"
    )
    DIM_FIELD = f"urn:li:schemaField:({SMD},customer_id)"
    # migrate-before-ingest: logical tables come from TABLE_SYNONYM_* keys so
    # SMD schemaField URNs are derivable before IsPartOf exists.
    _LOGICAL_TABLE_PROPS = DatasetPropertiesClass(
        customProperties={"TABLE_SYNONYM_CUSTOMERS": "cust"}
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
        customer_tags = by_name["CUSTOMER_ID"].global_tags
        revenue_tags = by_name["TOTAL_REVENUE"].global_tags
        assert customer_tags is not None
        assert revenue_tags is not None
        assert [t.tag for t in customer_tags.tags] == [make_tag_urn("pii")]
        assert [t.tag for t in revenue_tags.tags] == [make_tag_urn("finance")]

    def test_editable_schema_tags_union_with_schema_tags(self):
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field(
                    "TOTAL_REVENUE",
                    tags=[make_tag_urn("METRIC"), make_tag_urn("schema_tag")],
                ),
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
        merged_tags = fields[0].global_tags
        assert merged_tags is not None
        assert {t.tag for t in merged_tags.tags} == {
            make_tag_urn("schema_tag"),
            make_tag_urn("customer_tag"),
        }

    def test_editable_only_metric_tag_sets_is_metric(self):
        editable = EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="TOTAL_REVENUE",
                    globalTags=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(tag=make_tag_urn("METRIC")),
                            TagAssociationClass(tag=make_tag_urn("finance")),
                        ]
                    ),
                )
            ]
        )
        graph = _graph_with_aspects(editableSchemaMetadata=editable)
        fields = collect_dataset_field_governance(graph, self.SRC)
        assert len(fields) == 1
        assert fields[0].is_metric is True
        revenue_tags = fields[0].global_tags
        assert revenue_tags is not None
        assert [t.tag for t in revenue_tags.tags] == [make_tag_urn("finance")]

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
        graph = _graph_with_aspects(
            schemaMetadata=schema, datasetProperties=self._LOGICAL_TABLE_PROPS
        )

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

    def test_emits_metric_tags_when_metric_missing(self):
        """Migrate-first may write tags onto metric URNs before ingest creates them."""
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
        graph.exists.side_effect = lambda urn: urn == self.SRC

        result = migrate_dataset_to_semantic_model(
            graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert any("total_revenue" in entry for entry in result.fields_migrated)
        assert any(
            call.args[0].entityUrn == self.METRIC
            for call in graph.emit_mcp.call_args_list
        )

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

    def test_one_field_failure_does_not_discard_others(self):
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field("CUSTOMER_ID", tags=[make_tag_urn("pii")]),
                _schema_field("REGION", tags=[make_tag_urn("geo")]),
            ],
        )
        graph = _graph_with_aspects(
            schemaMetadata=schema, datasetProperties=self._LOGICAL_TABLE_PROPS
        )

        def emit_side_effect(mcp):
            if mcp.entityUrn == self.DIM_FIELD:
                raise RuntimeError("transient write failure")

        graph.emit_mcp.side_effect = emit_side_effect

        migrated, skipped = migrate_dataset_field_governance(
            graph,
            self.SRC,
            self.SM,
            SnowflakeViewIdentity(
                db="test_db", schema="public", view="sales_analytics"
            ),
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
        )

        assert any("REGION" in entry for entry in migrated)
        assert not any("CUSTOMER_ID" in entry for entry in migrated)
        assert any("CUSTOMER_ID" in note for note in skipped)

    def test_one_destination_failure_does_not_skip_siblings(self):
        """One SMD write failure must still migrate the field onto remaining SMDs."""
        smd_orders = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.public.sales_analytics.orders,PROD)"
        )
        orders_field = f"urn:li:schemaField:({smd_orders},customer_id)"
        schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field("CUSTOMER_ID", tags=[make_tag_urn("pii")]),
            ],
        )
        graph = _graph_with_aspects(
            schemaMetadata=schema,
            datasetProperties=DatasetPropertiesClass(
                customProperties={
                    "TABLE_SYNONYM_CUSTOMERS": "cust",
                    "TABLE_SYNONYM_ORDERS": "ord",
                }
            ),
        )

        def emit_side_effect(mcp):
            if mcp.entityUrn == self.DIM_FIELD:
                raise RuntimeError("transient write failure")

        graph.emit_mcp.side_effect = emit_side_effect

        migrated, skipped = migrate_dataset_field_governance(
            graph,
            self.SRC,
            self.SM,
            SnowflakeViewIdentity(
                db="test_db", schema="public", view="sales_analytics"
            ),
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
        )

        assert any(orders_field in entry for entry in migrated)
        assert not any(self.DIM_FIELD in entry for entry in migrated)
        assert any(self.DIM_FIELD in note for note in skipped)

    def test_forward_then_rollback_preserves_dimension_tags(self):
        """migrate-then-rollback must keep non-metric column tags/terms.

        Forward writes onto SMD-anchored schemaField URNs; rollback reads those
        same URNs via IsPartOf + schemaMetadata.
        """
        src_schema = SchemaMetadataClass(
            schemaName="sales_analytics",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                _schema_field(
                    "CUSTOMER_ID",
                    tags=[make_tag_urn("DIMENSION"), make_tag_urn("pii")],
                    terms=["urn:li:glossaryTerm:CustomerId"],
                ),
            ],
        )
        # Phase 1: forward migrate (derive SMD URN from TABLE_SYNONYM_*).
        forward_graph = _graph_with_aspects(
            schemaMetadata=src_schema, datasetProperties=self._LOGICAL_TABLE_PROPS
        )
        forward = migrate_dataset_to_semantic_model(
            forward_graph,
            self.SRC,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            dry_run=False,
            report_inbound_refs=False,
        )
        assert forward.error is None
        assert any(self.DIM_FIELD in entry for entry in forward.fields_migrated)

        field_tags: Optional[GlobalTagsClass] = None
        field_terms: Optional[GlossaryTermsClass] = None
        for call in forward_graph.emit_mcp.call_args_list:
            mcp = call.args[0]
            if mcp.entityUrn != self.DIM_FIELD:
                continue
            if isinstance(mcp.aspect, GlobalTagsClass):
                field_tags = mcp.aspect
            elif isinstance(mcp.aspect, GlossaryTermsClass):
                field_terms = mcp.aspect
        assert field_tags is not None
        assert {t.tag for t in field_tags.tags} == {make_tag_urn("pii")}
        assert field_terms is not None

        # Phase 2: rollback reads SMD-anchored schemaField aspects via IsPartOf.
        dst_schema = _schema_metadata(_schema_field("customer_id"))

        def get_aspects(entity_urn, aspects, aspect_types):
            out: Dict[str, Optional[_Aspect]] = {}
            for name in aspects:
                if name == "schemaMetadata" and entity_urn == self.SMD:
                    out[name] = _schema_metadata(_schema_field("customer_id"))
                elif name == "schemaMetadata" and entity_urn == self.SRC:
                    out[name] = dst_schema
                elif name == "globalTags" and entity_urn == self.DIM_FIELD:
                    out[name] = field_tags
                elif name == "glossaryTerms" and entity_urn == self.DIM_FIELD:
                    out[name] = field_terms
                elif name == "editableSchemaMetadata" and entity_urn == self.SRC:
                    out[name] = None
                else:
                    out[name] = None
            return out

        def get_related(entity_urn, relationship_types, direction):
            if "IsPartOf" in relationship_types:
                return [RelatedEntity(urn=self.SMD, relationship_type="IsPartOf")]
            return []

        rollback_graph = MagicMock()
        rollback_graph.get_aspects_for_entity.side_effect = get_aspects
        rollback_graph.get_related_entities.side_effect = get_related
        rollback_graph.exists.return_value = True

        migrated, notes = migrate_semantic_model_field_governance(
            rollback_graph,
            self.SM,
            self.SRC,
            convert_urns_to_lowercase=True,
            dry_run=False,
        )
        assert notes == []
        assert any(
            "CUSTOMER_ID" in entry or "customer_id" in entry for entry in migrated
        )

        editable_emits = [
            call.args[0].aspect
            for call in rollback_graph.emit_mcp.call_args_list
            if isinstance(call.args[0].aspect, EditableSchemaMetadataClass)
        ]
        assert len(editable_emits) == 1
        by_path = {fi.fieldPath: fi for fi in editable_emits[0].editableSchemaFieldInfo}
        assert "customer_id" in by_path
        rolled_tags = by_path["customer_id"].globalTags
        assert rolled_tags is not None
        assert {t.tag for t in rolled_tags.tags} == {make_tag_urn("pii")}
        rolled_terms = by_path["customer_id"].glossaryTerms
        assert rolled_terms is not None
        assert [t.urn for t in rolled_terms.terms] == ["urn:li:glossaryTerm:CustomerId"]


class TestSchemaFieldIsMetric:
    def test_comma_separated_column_subtype(self):
        field = _schema_field(
            "TOTAL_REVENUE", json_props='{"columnSubType": "DIMENSION,METRIC"}'
        )
        assert _schema_field_is_metric(field) is True

    def test_malformed_json_props(self):
        field = _schema_field("TOTAL_REVENUE", json_props="{not-json")
        assert _schema_field_is_metric(field) is False


class TestMigrateSemanticModelFieldGovernance:
    SM = "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_analytics)"
    DST = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_analytics,PROD)"
    # A logical dataset linked via IsPartOf, distinct from the legacy dataset
    # the governance is migrated onto.
    MODEL_DATASET = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "test_db.public.sales_analytics_model,PROD)"
    )
    METRIC = (
        "urn:li:metric:(urn:li:dataPlatform:snowflake,"
        "test_db.public.sales_analytics,total_revenue)"
    )

    def test_merges_tags_without_clobbering_field_descriptions(self):
        existing_editable = EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="OrderId",
                    description="hand-written field description",
                    globalTags=GlobalTagsClass(
                        tags=[TagAssociationClass(tag=make_tag_urn("keep_me"))]
                    ),
                ),
                EditableSchemaFieldInfoClass(
                    fieldPath="OtherCol",
                    description="untouched column",
                ),
            ]
        )
        schema = _schema_metadata(_schema_field("OrderId"), _schema_field("OtherCol"))

        def get_aspects(entity_urn, aspects, aspect_types):
            out: Dict[str, Optional[_Aspect]] = {}
            for name in aspects:
                if name == "editableSchemaMetadata" and entity_urn == self.DST:
                    out[name] = existing_editable
                elif name == "schemaMetadata" and entity_urn == self.DST:
                    out[name] = schema
                elif name == "schemaMetadata" and entity_urn == self.MODEL_DATASET:
                    out[name] = _schema_metadata(_schema_field("OrderId"))
                elif name == "globalTags" and entity_urn.startswith(
                    "urn:li:schemaField:"
                ):
                    out[name] = GlobalTagsClass(
                        tags=[TagAssociationClass(tag=make_tag_urn("from_sm"))]
                    )
                else:
                    out[name] = None
            return out

        def get_related(entity_urn, relationship_types, direction):
            if "IsPartOf" in relationship_types:
                return [
                    RelatedEntity(urn=self.MODEL_DATASET, relationship_type="IsPartOf")
                ]
            return []

        graph = MagicMock()
        graph.get_aspects_for_entity.side_effect = get_aspects
        graph.get_related_entities.side_effect = get_related
        graph.exists.return_value = True

        migrated, notes = migrate_semantic_model_field_governance(
            graph,
            self.SM,
            self.DST,
            convert_urns_to_lowercase=False,
            dry_run=False,
        )

        assert migrated
        assert notes == []
        editable_emits = [
            call.args[0].aspect
            for call in graph.emit_mcp.call_args_list
            if isinstance(call.args[0].aspect, EditableSchemaMetadataClass)
        ]
        assert len(editable_emits) == 1
        by_path = {fi.fieldPath: fi for fi in editable_emits[0].editableSchemaFieldInfo}
        assert by_path["OrderId"].description == "hand-written field description"
        order_id_tags = by_path["OrderId"].globalTags
        assert order_id_tags is not None
        assert {t.tag for t in order_id_tags.tags} == {
            make_tag_urn("keep_me"),
            make_tag_urn("from_sm"),
        }
        assert by_path["OtherCol"].description == "untouched column"
        # Mixed-case schema path preserved (not forced to ORDERID).
        assert "ORDERID" not in by_path

    def test_collect_from_modeled_by_metrics(self):
        def get_aspects(entity_urn, aspects, aspect_types):
            if entity_urn == self.METRIC:
                return {
                    "globalTags": GlobalTagsClass(
                        tags=[TagAssociationClass(tag=make_tag_urn("finance"))]
                    ),
                    "glossaryTerms": None,
                }
            if "semanticModelInfo" in aspects:
                return {"semanticModelInfo": None}
            return {name: None for name in aspects}

        def get_related(entity_urn, relationship_types, direction):
            if "ModeledBy" in relationship_types:
                return [RelatedEntity(urn=self.METRIC, relationship_type="ModeledBy")]
            return []

        graph = MagicMock()
        graph.get_aspects_for_entity.side_effect = get_aspects
        graph.get_related_entities.side_effect = get_related

        fields = collect_semantic_model_field_governance(
            graph, self.SM, convert_urns_to_lowercase=True
        )
        assert len(fields) == 1
        assert fields[0].is_metric is True
        assert fields[0].column_name == "total_revenue"
        metric_tags = fields[0].global_tags
        assert metric_tags is not None
        assert [t.tag for t in metric_tags.tags] == [make_tag_urn("finance")]

    def test_collect_falls_back_to_model_dataset_schema_tags(self):
        """No schemaField aspects: read the model dataset's schemaMetadata instead."""
        model_schema = _schema_metadata(
            _schema_field(
                "TOTAL_AMOUNT",
                tags=[make_tag_urn("pii"), make_tag_urn("dimension")],
                terms=["urn:li:glossaryTerm:Revenue"],
            )
        )

        def get_aspects(entity_urn, aspects, aspect_types):
            out: Dict[str, Optional[_Aspect]] = {}
            for name in aspects:
                if name == "schemaMetadata" and entity_urn == self.MODEL_DATASET:
                    out[name] = model_schema
                else:
                    out[name] = None
            return out

        def get_related(entity_urn, relationship_types, direction):
            if "IsPartOf" in relationship_types:
                return [
                    RelatedEntity(urn=self.MODEL_DATASET, relationship_type="IsPartOf")
                ]
            return []

        graph = MagicMock()
        graph.get_aspects_for_entity.side_effect = get_aspects
        graph.get_related_entities.side_effect = get_related

        fields = collect_semantic_model_field_governance(
            graph, self.SM, convert_urns_to_lowercase=False
        )

        assert len(fields) == 1
        assert fields[0].column_name == "TOTAL_AMOUNT"
        assert fields[0].is_metric is False
        tags = fields[0].global_tags
        assert tags is not None
        # Synthetic subtype tag dropped, customer tag kept.
        assert [t.tag for t in tags.tags] == [make_tag_urn("pii")]
        terms = fields[0].glossary_terms
        assert terms is not None
        assert [t.urn for t in terms.terms] == ["urn:li:glossaryTerm:Revenue"]

    def test_sm_to_dataset_folds_documentation_into_editable_description(self):
        doc = DocumentationClass(
            documentations=[
                DocumentationAssociationClass(documentation="from semantic model")
            ]
        )
        graph = _graph_with_aspects(documentation=doc)

        result = migrate_semantic_model_to_dataset(
            graph,
            self.SM,
            platform_instance=None,
            convert_urns_to_lowercase=True,
            env="PROD",
            dry_run=False,
            report_inbound_refs=False,
        )

        assert result.error is None
        assert any("editableDatasetProperties.description" in n for n in result.notes)
        editable_emits = [
            call.args[0].aspect
            for call in graph.emit_mcp.call_args_list
            if isinstance(call.args[0].aspect, EditableDatasetPropertiesClass)
        ]
        assert any(a.description == "from semantic model" for a in editable_emits)
