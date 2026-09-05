import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    FabricArtifact,
    Measure,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import (
    MetricInfoClass,
    MetricUpstreamsClass,
    NumberTypeClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamLineageClass,
)

_ARTIFACT_ID = "2afa2dbd-555b-48c8-b082-35d94f4b7836"
_WORKSPACE_ID = "ff23fbe3-7418-42f8-a675-9f10eb2b78cb"
_DS_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:powerbi,"
    "TestWorkspace.TestDataset.green_tripdata_2017,PROD)"
)


@pytest.fixture
def mapper() -> Mapper:
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        emit_semantic_model_entities=True,
    )
    return Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )


def _workspace() -> Workspace:
    artifact = FabricArtifact(
        id=_ARTIFACT_ID,
        name="TestLakehouse",
        artifact_type="Lakehouse",
        workspace_id=_WORKSPACE_ID,
        physical_item_ids=None,
    )
    return Workspace(
        id=_WORKSPACE_ID,
        name="TestWorkspace",
        type="Workspace",
        webUrl=f"https://app.powerbi.com/groups/{_WORKSPACE_ID}",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
        fabric_artifacts={_ARTIFACT_ID: artifact},
    )


def _dataset() -> PowerBIDataset:
    dataset = PowerBIDataset(
        id="ds-123",
        name="TestDataset",
        description="Test dataset",
        webUrl="https://app.powerbi.com/datasets/ds-123",
        workspace_id=_WORKSPACE_ID,
        workspace_name="TestWorkspace",
        parameters={},
        tables=[],
        tags=[],
        configuredBy=None,
        dependent_on_artifact_id=_ARTIFACT_ID,
    )
    # DirectLake table gives a deterministic warehouse upstream without the
    # M-Query engine, so the full lineage chain can be asserted in a unit test.
    table = Table(
        name="green_tripdata_2017",
        full_name="TestWorkspace.TestDataset.green_tripdata_2017",
        storage_mode="DirectLake",
        source_schema="dbo",
        source_expression="green_tripdata_2017",
        columns=[
            Column(
                name="pickup_date",
                dataType="DateTime",
                isHidden=False,
                datahubDataType=StringTypeClass(),
            )
        ],
        measures=[
            Measure(
                name="Total Trips",
                expression="COUNTROWS(green_tripdata_2017)",
                isHidden=False,
                datahubDataType=NumberTypeClass(),
                description="Row count",
            )
        ],
    )
    table.dataset = dataset
    dataset.tables = [table]
    return dataset


def _aspects_of(mcps, cls):
    return [mcp.aspect for mcp in mcps if isinstance(mcp.aspect, cls)]


def test_emits_semantic_model_metric_and_logical_dataset(mapper: Mapper) -> None:
    mcps = mapper.to_datahub_dataset(_dataset(), _workspace())

    # A semanticModel entity replaces the "Semantic Model" container.
    sm_infos = _aspects_of(mcps, SemanticModelInfoClass)
    assert len(sm_infos) == 1
    assert sm_infos[0].name == "TestDataset"
    sm_urn = next(
        mcp.entityUrn for mcp in mcps if isinstance(mcp.aspect, SemanticModelInfoClass)
    )
    assert sm_urn.startswith("urn:li:semanticModel:")

    # The table keeps its dataset URN but becomes a Semantic Model Dataset.
    subtypes = [
        mcp
        for mcp in mcps
        if mcp.entityUrn == _DS_URN and isinstance(mcp.aspect, SubTypesClass)
    ]
    assert subtypes[0].aspect.typeNames == [DatasetSubTypes.SEMANTIC_MODEL_DATASET]

    props = [
        mcp.aspect
        for mcp in mcps
        if mcp.entityUrn == _DS_URN
        and isinstance(mcp.aspect, SemanticModelPropertiesClass)
    ]
    assert props[0].alias == "green_tripdata_2017"
    assert props[0].semanticModel == sm_urn


def test_field_annotations_distinguish_measure_from_dimension(mapper: Mapper) -> None:
    mcps = mapper.to_datahub_dataset(_dataset(), _workspace())

    annotations = {
        mcp.entityUrn: mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, SemanticFieldAnnotationClass)
    }
    measure_ann = next(a for urn, a in annotations.items() if "Total Trips" in urn)
    dim_ann = next(a for urn, a in annotations.items() if "pickup_date" in urn)

    assert measure_ann.type == SemanticFieldTypeClass.MEASURE
    assert dim_ann.type == SemanticFieldTypeClass.DIMENSION
    # A DateTime column is flagged as a time dimension.
    assert dim_ann.dimension is not None and dim_ann.dimension.isTime is True


def test_lineage_chain_metric_to_logical_to_physical(mapper: Mapper) -> None:
    mcps = mapper.to_datahub_dataset(_dataset(), _workspace())

    # Metric -> Semantic Model Dataset: the metric reads from the logical dataset.
    metric_upstreams = _aspects_of(mcps, MetricUpstreamsClass)
    assert len(metric_upstreams) == 1
    assert [e.destinationUrn for e in metric_upstreams[0].datasetUpstreams] == [_DS_URN]

    metric_info = _aspects_of(mcps, MetricInfoClass)[0]
    assert "COUNTROWS" in metric_info.expression.dialects[0].expression

    # Semantic Model Dataset -> physical table: the same logical dataset carries
    # the warehouse upstream lineage (unchanged from the classic path).
    upstreams = [
        mcp.aspect
        for mcp in mcps
        if mcp.entityUrn == _DS_URN and isinstance(mcp.aspect, UpstreamLineageClass)
    ]
    assert len(upstreams) == 1
    assert upstreams[0].upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,"
        f"{_WORKSPACE_ID}.{_ARTIFACT_ID}.dbo.green_tripdata_2017,PROD)"
    )


def test_report_counts_semantic_entities(mapper: Mapper) -> None:
    mapper.to_datahub_dataset(_dataset(), _workspace())
    report = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert report.semantic_models_emitted == 1
    assert report.semantic_model_datasets_emitted == 1
    assert report.metrics_emitted == 1
