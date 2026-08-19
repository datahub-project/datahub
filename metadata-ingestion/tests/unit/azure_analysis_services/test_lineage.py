from types import SimpleNamespace
from unittest import mock

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.lineage import AasLineageExtractor
from datahub.ingestion.source.azure_analysis_services.models import (
    AasCalcDependency,
    AasColumn,
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
    # convert_lineage_urns_to_lowercase defaults True.
    assert result.upstreams[0].dataset == upstream_urn.lower()
    assert len(result.fine_grained) == 1


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
