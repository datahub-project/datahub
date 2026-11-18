from types import SimpleNamespace
from unittest import mock

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.lineage import AasLineageExtractor
from datahub.ingestion.source.azure_analysis_services.models import (
    AasCalcDependency,
    AasColumn,
    AasMeasure,
    AasPartition,
    AasTable,
    AasTabularModel,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.metadata.schema_classes import NullTypeClass

_SERVER = "asazure://westeurope.asazure.windows.net/myserver"


def _extractor() -> AasLineageExtractor:
    config = AzureAnalysisServicesConfig.model_validate(
        {"server": _SERVER, "tenant_id": "t", "client_id": "c", "client_secret": "s"}
    )
    return AasLineageExtractor(
        config, AzureAnalysisServicesReport(), PipelineContext(run_id="t")
    )


def _calc_dep(
    object_type: str,
    table: str,
    obj: str,
    ref_type: str,
    ref_table: str,
    ref_obj: str,
) -> AasCalcDependency:
    return AasCalcDependency(
        object_type=object_type,
        table=table,
        object_name=obj,
        referenced_object_type=ref_type,
        referenced_table=ref_table,
        referenced_object=ref_obj,
    )


def test_intra_model_lineage_edges() -> None:
    extractor = _extractor()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Sales Model",
        tables=[
            AasTable(
                name="Sales",
                columns=[AasColumn(name="Amount", datahub_data_type=NullTypeClass())],
                measures=[AasMeasure(name="Total Sales")],
            ),
            AasTable(
                name="SalesSummary",
                columns=[
                    AasColumn(name="TotalAmount", datahub_data_type=NullTypeClass())
                ],
            ),
        ],
        calc_dependencies=[
            _calc_dep("MEASURE", "Sales", "Total Sales", "COLUMN", "Sales", "Amount"),
            _calc_dep(
                "CALC_COLUMN",
                "SalesSummary",
                "TotalAmount",
                "COLUMN",
                "Sales",
                "Amount",
            ),
            # Self-reference carries no edge.
            _calc_dep("COLUMN", "Sales", "Amount", "COLUMN", "Sales", "Amount"),
            # Unknown object type is ignored.
            _calc_dep("TABLE", "Sales", "Sales", "COLUMN", "Sales", "Amount"),
            # System RowNumber column is not an emitted field: no edge.
            _calc_dep(
                "CALC_COLUMN",
                "SalesSummary",
                "TotalAmount",
                "COLUMN",
                "Sales",
                "RowNumber-11111111-2222-3333-4444-555555555555",
            ),
            # Calc-table self-reference (the "column" is the table name): no edge.
            _calc_dep(
                "CALC_COLUMN",
                "SalesSummary",
                "TotalAmount",
                "CALC_COLUMN",
                "SalesSummary",
                "SalesSummary",
            ),
        ],
    )
    sales_urn = builder.make_dataset_urn("azure-analysis-services", "SalesModel.Sales")
    summary_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.SalesSummary"
    )
    dataset_urn_by_table = {"sales": sales_urn, "salessummary": summary_urn}

    edges = extractor.extract_intra_model_lineage(model, dataset_urn_by_table)

    downstreams = {d for edge in edges for d in (edge.downstreams or [])}
    assert builder.make_schema_field_urn(sales_urn, "Total Sales") in downstreams
    assert builder.make_schema_field_urn(summary_urn, "TotalAmount") in downstreams
    # The only upstream for TotalAmount is Sales.Amount — the RowNumber and
    # calc-table self-reference dependencies are dropped as non-emitted fields.
    upstreams = {u for edge in edges for u in (edge.upstreams or [])}
    assert builder.make_schema_field_urn(sales_urn, "Amount") in upstreams
    assert not any("RowNumber-" in u for u in upstreams)
    assert builder.make_schema_field_urn(summary_urn, "SalesSummary") not in upstreams
    # Self-reference and unknown-type rows produce no edges.
    assert len(edges) == 2


def test_upstream_lineage_via_engine() -> None:
    extractor = _extractor()
    table = AasTable(
        name="Sales",
        columns=[
            AasColumn(name="Amount", datahub_data_type=NullTypeClass()),
        ],
        partitions=[
            AasPartition(name="p", query_definition="let Source = 1 in Source")
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )

    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,salesdb.dbo.sales,PROD)"
    fake_lineage = SimpleNamespace(
        upstreams=[SimpleNamespace(urn=upstream_urn)],
        column_lineage=[
            SimpleNamespace(
                downstream=SimpleNamespace(column="Amount"),
                upstreams=[SimpleNamespace(table=upstream_urn, column="amount")],
            )
        ],
    )

    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        return_value=[fake_lineage],
    ):
        result = extractor.extract_upstream_for_table(table, dataset_urn)

    assert len(result.upstreams) == 1
    # convert_lineage_urns_to_lowercase defaults True, but only the dataset name
    # is lowercased (already lowercase here), so the URN is preserved.
    assert result.upstreams[0].dataset == upstream_urn
    assert len(result.fine_grained) == 1


def test_upstream_lineage_merges_all_partitions() -> None:
    # A table with several query partitions must contribute lineage from every
    # partition, not just the first, with duplicate upstreams collapsed.
    extractor = _extractor()
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="p2024", query_definition="let Source = 1 in Source"),
            AasPartition(name="p2025", query_definition="let Source = 2 in Source"),
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )
    urn_2024 = "urn:li:dataset:(urn:li:dataPlatform:mssql,salesdb.dbo.sales_2024,PROD)"
    urn_2025 = "urn:li:dataset:(urn:li:dataPlatform:mssql,salesdb.dbo.sales_2025,PROD)"

    def fake_parse(table, **_kwargs):
        # The engine sees a one-partition copy; return a different upstream per
        # partition expression.
        urn = urn_2025 if "= 2 " in (table.expression or "") else urn_2024
        return [
            SimpleNamespace(upstreams=[SimpleNamespace(urn=urn)], column_lineage=[])
        ]

    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        side_effect=fake_parse,
    ):
        result = extractor.extract_upstream_for_table(table, dataset_urn)

    datasets = {u.dataset for u in result.upstreams}
    # Names are already lowercase, so the lineage URNs are preserved as-is.
    assert datasets == {urn_2024, urn_2025}


def test_upstream_lineage_dedupes_fine_grained_across_partitions() -> None:
    # Every partition of an incremental-refresh table resolves the SAME source
    # column, so all partitions produce an identical column edge. These must be
    # deduplicated, not emitted (and counted) once per partition.
    extractor = _extractor()
    table = AasTable(
        name="Sales",
        columns=[AasColumn(name="Amount", datahub_data_type=NullTypeClass())],
        partitions=[
            AasPartition(name="p2024", query_definition="let Source = 1 in Source"),
            AasPartition(name="p2025", query_definition="let Source = 2 in Source"),
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )
    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,salesdb.dbo.sales,PROD)"
    fake_lineage = SimpleNamespace(
        upstreams=[SimpleNamespace(urn=upstream_urn)],
        column_lineage=[
            SimpleNamespace(
                downstream=SimpleNamespace(column="Amount"),
                upstreams=[SimpleNamespace(table=upstream_urn, column="amount")],
            )
        ],
    )

    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        return_value=[fake_lineage],
    ):
        result = extractor.extract_upstream_for_table(table, dataset_urn)

    # Both partitions yield the same column edge; only one survives.
    assert len(result.fine_grained) == 1
    assert extractor.report.column_lineage_edges == 1


def test_upstream_lineage_engine_failure_degrades() -> None:
    # A table's lineage extraction must never abort the model: an engine
    # exception is downgraded to a warning and an empty result.
    extractor = _extractor()
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="p", query_definition="let Source = 1 in Source")
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )

    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        side_effect=RuntimeError("engine boom"),
    ):
        result = extractor.extract_upstream_for_table(table, dataset_urn)

    assert result.upstreams == []
    assert result.fine_grained == []
    assert len(extractor.report.warnings) == 1


def test_upstream_skipped_when_disabled() -> None:
    extractor = _extractor()
    extractor.config.extract_lineage = False
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="p", query_definition="let Source = 1 in Source")
        ],
    )
    result = extractor.extract_upstream_for_table(
        table, builder.make_dataset_urn("azure-analysis-services", "SalesModel.Sales")
    )
    assert result.upstreams == []
    assert result.fine_grained == []


def test_upstream_skips_calculated_table_dax_partition() -> None:
    # A calculated table's partition holds DAX, not M/Power Query. It must not be
    # sent to the M-Query engine (which would warn or time out), nor counted as an
    # import table missing upstream lineage.
    extractor = _extractor()
    table = AasTable(
        name="SalesSummary",
        is_calculated=True,
        partitions=[
            AasPartition(
                name="c",
                query_definition="SUMMARIZE(Sales, Sales[Region])",
                partition_type=constants.PartitionType.CALCULATED,
            )
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.SalesSummary"
    )

    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
    ) as mocked_parser:
        result = extractor.extract_upstream_for_table(table, dataset_urn)

    mocked_parser.assert_not_called()
    assert result.upstreams == []
    assert result.fine_grained == []
    assert extractor.report.tables_without_upstream_lineage == 0


def test_upstream_parses_only_m_partitions_of_mixed_table() -> None:
    # An import table can carry a DAX partition next to its M partitions; only
    # the M expression may reach the M-Query engine.
    extractor = _extractor()
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="m", query_definition="let Source = 1 in Source"),
            AasPartition(
                name="dax",
                query_definition="SUMMARIZE(Sales, Sales[Region])",
                partition_type=constants.PartitionType.CALCULATED,
            ),
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )

    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        return_value=[],
    ) as mocked_parser:
        extractor.extract_upstream_for_table(table, dataset_urn)

    assert mocked_parser.call_count == 1
    assert (
        mocked_parser.call_args.kwargs["table"].expression == "let Source = 1 in Source"
    )


def test_partitions_sharing_a_query_are_parsed_once() -> None:
    extractor = _extractor()
    query = "let Source = 1 in Source"
    table = AasTable(
        name="Sales",
        partitions=[
            AasPartition(name="2024", query_definition=query),
            AasPartition(name="2025", query_definition=query),
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )
    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        return_value=[],
    ) as mocked_parser:
        extractor.extract_upstream_for_table(table, dataset_urn)
    assert mocked_parser.call_count == 1


def test_intra_model_edges_use_emitted_field_casing() -> None:
    # DISCOVER_CALC_DEPENDENCY can spell a name differently from TMSCHEMA; the
    # edge must point at the field the schema emits.
    extractor = _extractor()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        tables=[
            AasTable(
                name="Sales",
                columns=[AasColumn(name="Amount", datahub_data_type=NullTypeClass())],
                measures=[AasMeasure(name="Total Sales")],
            )
        ],
        calc_dependencies=[
            _calc_dep("MEASURE", "sales", "TOTAL SALES", "COLUMN", "SALES", "amount")
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )
    [edge] = extractor.extract_intra_model_lineage(model, {"sales": dataset_urn})
    assert edge.downstreams == [
        builder.make_schema_field_urn(dataset_urn, "Total Sales")
    ]
    assert edge.upstreams == [builder.make_schema_field_urn(dataset_urn, "Amount")]


def test_intra_model_edges_use_the_first_of_colliding_tables() -> None:
    # The mapper only emits the first of two tables whose names collide
    # case-insensitively, so field names must come from that table.
    extractor = _extractor()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        tables=[
            AasTable(
                name="Sales",
                columns=[AasColumn(name="Amount", datahub_data_type=NullTypeClass())],
                measures=[AasMeasure(name="Total")],
            ),
            AasTable(
                name="SALES",
                columns=[AasColumn(name="AMOUNT", datahub_data_type=NullTypeClass())],
                measures=[AasMeasure(name="TOTAL")],
            ),
        ],
        calc_dependencies=[
            _calc_dep("MEASURE", "Sales", "Total", "COLUMN", "Sales", "Amount")
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )
    [edge] = extractor.extract_intra_model_lineage(model, {"sales": dataset_urn})
    assert edge.downstreams == [builder.make_schema_field_urn(dataset_urn, "Total")]
    assert edge.upstreams == [builder.make_schema_field_urn(dataset_urn, "Amount")]


def test_intra_model_edges_prefer_exact_field_name() -> None:
    # A column and a measure can differ only by case; both are emitted, so an
    # exact name must not be folded into the other.
    extractor = _extractor()
    model = AasTabularModel(
        catalog="SalesModel",
        name="Model",
        tables=[
            AasTable(
                name="Sales",
                columns=[AasColumn(name="amount", datahub_data_type=NullTypeClass())],
                measures=[AasMeasure(name="Amount"), AasMeasure(name="Total")],
            )
        ],
        calc_dependencies=[
            _calc_dep("MEASURE", "Sales", "Total", "MEASURE", "Sales", "Amount")
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.Sales"
    )
    [edge] = extractor.extract_intra_model_lineage(model, {"sales": dataset_urn})
    assert edge.upstreams == [builder.make_schema_field_urn(dataset_urn, "Amount")]


def test_sql_partition_uses_connection_catalog_and_schema() -> None:
    extractor = _extractor()
    table = AasTable(
        name="FactOpex",
        partitions=[
            AasPartition(
                name="p",
                query_definition="SELECT Amount\nFROM FactOpex\nGROUP BY ALL",
                databricks_catalog="prod",
                databricks_schema="finance",
            )
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.FactOpex"
    )
    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables"
    ) as parse:
        result = extractor.extract_upstream_for_table(table, dataset_urn)
    parse.assert_not_called()
    assert [u.dataset for u in result.upstreams] == [
        builder.make_dataset_urn("databricks", "prod.finance.factopex")
    ]


def test_sql_partition_with_leading_comment_uses_connection() -> None:
    extractor = _extractor()
    table = AasTable(
        name="FactOpex",
        partitions=[
            AasPartition(
                name="p",
                query_definition="-- keep the last four years\nSELECT Amount FROM FactOpex",
                databricks_catalog="prod",
                databricks_schema="finance",
            )
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.FactOpex"
    )
    result = extractor.extract_upstream_for_table(table, dataset_urn)
    assert [u.dataset for u in result.upstreams] == [
        builder.make_dataset_urn("databricks", "prod.finance.factopex")
    ]


def test_sql_partition_without_connection_catalog_stays_on_m_parser() -> None:
    extractor = _extractor()
    table = AasTable(
        name="FactOpex",
        partitions=[
            AasPartition(name="p", query_definition="SELECT Amount FROM FactOpex")
        ],
    )
    dataset_urn = builder.make_dataset_urn(
        "azure-analysis-services", "SalesModel.FactOpex"
    )
    with mock.patch(
        "datahub.ingestion.source.azure_analysis_services.lineage.parser.get_upstream_tables",
        return_value=[],
    ) as parse:
        result = extractor.extract_upstream_for_table(table, dataset_urn)
    parse.assert_called_once()
    assert result.upstreams == []
