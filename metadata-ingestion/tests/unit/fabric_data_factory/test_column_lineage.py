"""Unit tests for Fabric Data Factory Copy activity column-level lineage."""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fabric.data_factory.lineage import (
    CopyActivityColumnLineageExtractor,
    DataHubDatasetColumnsResolver,
)
from datahub.ingestion.source.fabric.data_factory.models import (
    DatasetColumns,
    PipelineActivity,
)
from datahub.ingestion.source.fabric.data_factory.report import (
    FabricDataFactorySourceReport,
)
from datahub.ingestion.source.fabric.data_factory.source import (
    FabricDataFactorySource,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

SOURCE_URN = "urn:li:dataset:(urn:li:dataPlatform:mssql,dbo.customers,PROD)"
SINK_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ws-1.lh-1.sales.customers,PROD)"
)
ACTIVITY_KEY = "Sales Analytics.CopyCustomers"


def _copy_activity(
    translator: Optional[Any] = None,
    source_schema: Optional[List[Dict[str, Any]]] = None,
    sink_schema: Optional[List[Dict[str, Any]]] = None,
    table_option: Optional[str] = None,
) -> PipelineActivity:
    sink: Dict[str, Any] = {"datasetSettings": {"schema": sink_schema or []}}
    if table_option is not None:
        sink["tableOption"] = table_option
    type_properties: Dict[str, Any] = {
        "source": {"datasetSettings": {"schema": source_schema or []}},
        "sink": sink,
    }
    if translator is not None:
        type_properties["translator"] = translator
    return PipelineActivity(
        name="CopyCustomers", type="Copy", type_properties=type_properties
    )


def _resolver(
    schemas: Dict[str, List[str]],
) -> Tuple[MagicMock, Any]:
    lookups = MagicMock()

    def resolve(urn: str) -> Optional[DatasetColumns]:
        lookups(urn)
        fields = schemas.get(urn)
        return DatasetColumns(field_paths=fields) if fields else None

    return lookups, resolve


def _pairs(lineages: List[FineGrainedLineageClass]) -> List[Tuple[str, str]]:
    pairs = []
    for fgl in lineages:
        assert fgl.upstreams and fgl.downstreams
        assert fgl.transformOperation == "COPY"
        upstream_col = fgl.upstreams[0].rsplit(",", 1)[1].rstrip(")")
        downstream_col = fgl.downstreams[0].rsplit(",", 1)[1].rstrip(")")
        assert fgl.upstreams[0].startswith(f"urn:li:schemaField:({SOURCE_URN},")
        assert fgl.downstreams[0].startswith(f"urn:li:schemaField:({SINK_URN},")
        pairs.append((upstream_col, downstream_col))
    return pairs


def _extract(
    extractor: CopyActivityColumnLineageExtractor, activity: PipelineActivity
) -> List[FineGrainedLineageClass]:
    return extractor.extract_column_lineage(
        activity=activity,
        input_urn=SOURCE_URN,
        output_urn=SINK_URN,
        activity_key=ACTIVITY_KEY,
    )


class TestExplicitMappings:
    def test_mappings_list(self) -> None:
        report = FabricDataFactorySourceReport()
        extractor = CopyActivityColumnLineageExtractor(report=report)
        activity = _copy_activity(
            translator={
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"name": "id"}, "sink": {"name": "customer_id"}},
                    {"source": {"name": "email"}, "sink": {"name": "email"}},
                ],
            }
        )
        assert _pairs(_extract(extractor, activity)) == [
            ("id", "customer_id"),
            ("email", "email"),
        ]
        assert report.column_lineage_activities_explicit == 1
        assert report.column_lineage_extracted == 2

    @pytest.mark.parametrize(
        "column_mappings",
        [
            {"id": "customer_id", "email": "email"},
            "id: customer_id, email: email",
        ],
    )
    def test_legacy_column_mappings(self, column_mappings: Any) -> None:
        extractor = CopyActivityColumnLineageExtractor(
            report=FabricDataFactorySourceReport()
        )
        activity = _copy_activity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": column_mappings,
            }
        )
        assert _pairs(_extract(extractor, activity)) == [
            ("id", "customer_id"),
            ("email", "email"),
        ]

    def test_names_normalized_to_known_schema_casing(self) -> None:
        _, resolve = _resolver(
            {SOURCE_URN: ["customer_id", "email"], SINK_URN: ["CustomerId"]}
        )
        extractor = CopyActivityColumnLineageExtractor(
            report=FabricDataFactorySourceReport(), columns_resolver=resolve
        )
        activity = _copy_activity(
            translator={
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"name": "CUSTOMER_ID"}, "sink": {"name": "customerid"}},
                    # Not in the sink schema: emitted as written.
                    {"source": {"name": "EMAIL"}, "sink": {"name": "Email"}},
                ],
            }
        )
        assert _pairs(_extract(extractor, activity)) == [
            ("customer_id", "CustomerId"),
            ("email", "Email"),
        ]

    def test_ordinal_only_mappings_do_not_fall_back_to_by_name(self) -> None:
        """Ordinal mappings are applied by Fabric, not by-name matching."""
        report = FabricDataFactorySourceReport()
        lookups, resolve = _resolver(
            {SOURCE_URN: ["Prop_0", "Prop_1"], SINK_URN: ["Prop_0", "email"]}
        )
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        activity = _copy_activity(
            translator={
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"ordinal": 1}, "sink": {"name": "id"}},
                    {"source": {"ordinal": 2}, "sink": {"name": "email"}},
                ],
            }
        )
        assert _extract(extractor, activity) == []
        assert report.column_lineage_skipped_unresolvable_mappings == 1
        assert list(report.column_lineage_skipped_unresolvable_mappings_details) == [
            ACTIVITY_KEY
        ]
        assert report.column_lineage_activities_auto_mapped == 0
        lookups.assert_not_called()

    def test_partially_ordinal_mappings_count_dropped_entries(self) -> None:
        report = FabricDataFactorySourceReport()
        extractor = CopyActivityColumnLineageExtractor(report=report)
        activity = _copy_activity(
            translator={
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"ordinal": 1}, "sink": {"name": "id"}},
                    {"source": {"name": "email"}, "sink": {"name": "email"}},
                ],
            }
        )
        assert _pairs(_extract(extractor, activity)) == [("email", "email")]
        assert report.column_lineage_activities_explicit == 1
        assert report.column_lineage_mappings_skipped == 1


class TestAutoMapping:
    @pytest.mark.parametrize(
        "translator",
        [None, {"type": "TabularTranslator", "typeConversion": True}],
        ids=["no_translator", "tabular_without_mappings"],
    )
    def test_maps_by_name_when_both_schemas_known(
        self, translator: Optional[Dict[str, Any]]
    ) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver(
            {
                SOURCE_URN: ["id", "Name", "email", "created_at"],
                SINK_URN: ["ID", "name", "email", "loaded_at"],
            }
        )
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        lineages = _extract(extractor, _copy_activity(translator=translator))
        # Case-insensitive match; unmatched columns on either side are dropped.
        assert _pairs(lineages) == [("id", "ID"), ("Name", "name"), ("email", "email")]
        assert report.column_lineage_activities_auto_mapped == 1
        assert report.column_lineage_extracted == 3
        # created_at has no same-named sink column.
        assert report.column_lineage_unmatched_columns == 1

    def test_uses_inline_dataset_schema(self) -> None:
        lookups, resolve = _resolver({SINK_URN: ["id", "email"]})
        extractor = CopyActivityColumnLineageExtractor(
            report=FabricDataFactorySourceReport(), columns_resolver=resolve
        )
        activity = _copy_activity(
            source_schema=[
                {"name": "id", "type": "int"},
                {"name": "email", "type": "nvarchar"},
                {"type": "nvarchar"},
            ]
        )
        assert _pairs(_extract(extractor, activity)) == [
            ("id", "id"),
            ("email", "email"),
        ]
        # Only the sink needed a DataHub lookup.
        lookups.assert_called_once_with(SINK_URN)

    @pytest.mark.parametrize(
        "schemas",
        [
            {},
            {SOURCE_URN: ["id", "email"]},
            {SINK_URN: ["id", "email"]},
        ],
        ids=["no_schemas", "source_only", "sink_only"],
    )
    def test_no_lineage_without_both_schemas(
        self, schemas: Dict[str, List[str]]
    ) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver(schemas)
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        assert _extract(extractor, _copy_activity()) == []
        assert report.column_lineage_skipped_no_schema == 1
        assert list(report.column_lineage_skipped_no_schema_details) == [ACTIVITY_KEY]
        assert report.column_lineage_extracted == 0

    def test_no_lineage_without_resolver(self) -> None:
        report = FabricDataFactorySourceReport()
        extractor = CopyActivityColumnLineageExtractor(report=report)
        assert _extract(extractor, _copy_activity()) == []
        assert report.column_lineage_skipped_no_schema == 1


class TestAutoCreateSink:
    @pytest.mark.parametrize(
        "translator",
        [None, {"type": "TabularTranslator", "typeConversion": True}],
        ids=["no_translator", "tabular_without_mappings"],
    )
    def test_sink_columns_taken_from_source(
        self, translator: Optional[Dict[str, Any]]
    ) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver(
            {SOURCE_URN: ["id", "[version=2.0].[type=string].email"]}
        )
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        activity = _copy_activity(translator=translator, table_option="autoCreate")
        lineages = _extract(extractor, activity)
        assert [fgl.downstreams for fgl in lineages] == [
            [f"urn:li:schemaField:({SINK_URN},id)"],
            [f"urn:li:schemaField:({SINK_URN},email)"],
        ]
        assert report.column_lineage_activities_auto_created_sink == 1
        assert report.column_lineage_activities_auto_mapped == 0
        assert report.column_lineage_skipped_no_schema == 0
        assert report.column_lineage_extracted == 2

    def test_known_sink_schema_still_matched_by_name(self) -> None:
        """autoCreate only creates a missing table; an existing one wins."""
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver(
            {SOURCE_URN: ["id", "email"], SINK_URN: ["ID", "loaded_at"]}
        )
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        lineages = _extract(extractor, _copy_activity(table_option="autoCreate"))
        assert _pairs(lineages) == [("id", "ID")]
        assert report.column_lineage_activities_auto_mapped == 1
        assert report.column_lineage_activities_auto_created_sink == 0

    def test_requires_source_schema(self) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver({})
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        assert _extract(extractor, _copy_activity(table_option="autoCreate")) == []
        assert report.column_lineage_skipped_no_schema == 1
        assert report.column_lineage_activities_auto_created_sink == 0

    def test_explicit_mappings_take_precedence(self) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver({SOURCE_URN: ["id", "email"]})
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        activity = _copy_activity(
            translator={
                "type": "TabularTranslator",
                "mappings": [{"source": {"name": "id"}, "sink": {"name": "cid"}}],
            },
            table_option="autoCreate",
        )
        assert _pairs(_extract(extractor, activity)) == [("id", "cid")]
        assert report.column_lineage_activities_auto_created_sink == 0

    @pytest.mark.parametrize("table_option", [None, "none"])
    def test_other_table_options_need_sink_schema(
        self, table_option: Optional[str]
    ) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver({SOURCE_URN: ["id", "email"]})
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        assert _extract(extractor, _copy_activity(table_option=table_option)) == []
        assert report.column_lineage_skipped_no_schema == 1


class TestUnsupportedTranslators:
    def test_dynamic_expression_translator(self) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver({SOURCE_URN: ["id"], SINK_URN: ["id"]})
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        activity = _copy_activity(
            translator={
                "value": "@json(pipeline().parameters.mapping)",
                "type": "Expression",
            }
        )
        # Runtime mappings are unknown, so no by-name fallback either.
        assert _extract(extractor, activity) == []
        assert report.column_lineage_skipped_dynamic_translator == 1
        assert list(report.column_lineage_skipped_translator_details) == [
            f"{ACTIVITY_KEY} (dynamic)"
        ]

    @pytest.mark.parametrize(
        "translator", [{"type": "SomeOtherTranslator"}, "not-a-dict"]
    )
    def test_unsupported_translator(self, translator: Any) -> None:
        report = FabricDataFactorySourceReport()
        _, resolve = _resolver({SOURCE_URN: ["id"], SINK_URN: ["id"]})
        extractor = CopyActivityColumnLineageExtractor(
            report=report, columns_resolver=resolve
        )
        assert _extract(extractor, _copy_activity(translator=translator)) == []
        assert report.column_lineage_skipped_unsupported_translator == 1


def _schema(*field_paths: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="customers",
        platform="urn:li:dataPlatform:mssql",
        version=0,
        hash="",
        platformSchema=MagicMock(),
        fields=[
            SchemaFieldClass(
                fieldPath=path,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="nvarchar",
            )
            for path in field_paths
        ],
    )


class TestDataHubDatasetColumnsResolver:
    def test_resolves_and_caches(self) -> None:
        report = FabricDataFactorySourceReport()
        graph = MagicMock()
        graph.get_schema_metadata.return_value = _schema(
            "id", "[version=2.0].[type=string].Email"
        )
        resolver = DataHubDatasetColumnsResolver(graph, report)

        columns = resolver.get_columns(SOURCE_URN)
        assert columns is not None
        assert columns.lookup("ID") == "id"
        # v2 field paths match by their simple name but keep the stored path.
        assert columns.lookup("email") == "[version=2.0].[type=string].Email"
        assert resolver.get_columns(SOURCE_URN) is columns
        graph.get_schema_metadata.assert_called_once_with(SOURCE_URN)

    def test_missing_schema_returns_none_without_warning(self) -> None:
        report = FabricDataFactorySourceReport()
        graph = MagicMock()
        graph.get_schema_metadata.return_value = None
        resolver = DataHubDatasetColumnsResolver(graph, report)
        assert resolver.get_columns(SOURCE_URN) is None
        assert report.column_lineage_schema_lookup_failed == 0
        assert len(report.warnings) == 0

    def test_lookup_error_is_reported_and_cached(self) -> None:
        report = FabricDataFactorySourceReport()
        graph = MagicMock()
        graph.get_schema_metadata.side_effect = RuntimeError("401 Unauthorized")
        resolver = DataHubDatasetColumnsResolver(graph, report)
        assert resolver.get_columns(SINK_URN) is None
        assert resolver.get_columns(SINK_URN) is None
        graph.get_schema_metadata.assert_called_once_with(SINK_URN)
        assert report.column_lineage_schema_lookup_failed == 1
        warnings = list(report.warnings)
        assert len(warnings) == 1
        assert warnings[0].title == "Column Lineage Schema Lookup Failed"
        assert warnings[0].context[0].startswith(SINK_URN)


class TestSourceWiring:
    CONFIG: Dict[str, Any] = {
        "credential": {
            "authentication_method": "service_principal",
            "client_id": "test-client",
            "client_secret": "test-secret",
            "tenant_id": "test-tenant",
        },
    }

    def _source(
        self, graph: Optional[MagicMock], **config: Any
    ) -> FabricDataFactorySource:
        ctx = PipelineContext(run_id="fabric-df-column-lineage", graph=graph)
        return FabricDataFactorySource.create({**self.CONFIG, **config}, ctx)

    def _run(self, source: FabricDataFactorySource) -> List[FineGrainedLineageClass]:
        source._copy_column_lineage_extractor = source._build_column_lineage_extractor()
        pipeline_item = MagicMock()
        pipeline_item.name = "Sales Analytics"
        return source._extract_copy_column_lineage(
            _copy_activity(), pipeline_item, [SOURCE_URN], [SINK_URN]
        )

    def test_auto_mapping_uses_graph_schemas(self) -> None:
        graph = MagicMock()
        graph.get_schema_metadata.side_effect = lambda urn: (
            _schema("id", "email") if urn == SOURCE_URN else _schema("ID", "Email")
        )
        source = self._source(graph)
        assert _pairs(self._run(source)) == [("id", "ID"), ("email", "Email")]

    def test_without_graph_skips_auto_mapping(self) -> None:
        source = self._source(graph=None)
        assert self._run(source) == []
        assert source.report.column_lineage_skipped_no_schema == 1

    def test_disabled_by_config(self) -> None:
        graph = MagicMock()
        source = self._source(graph, include_column_lineage=False)
        assert self._run(source) == []
        graph.get_schema_metadata.assert_not_called()

    def test_requires_both_datasets(self) -> None:
        graph = MagicMock()
        source = self._source(graph)
        source._copy_column_lineage_extractor = source._build_column_lineage_extractor()
        assert (
            source._extract_copy_column_lineage(
                _copy_activity(), MagicMock(), [SOURCE_URN], []
            )
            == []
        )
        graph.get_schema_metadata.assert_not_called()
