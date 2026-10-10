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
    ContainerClass,
    DatasetProfileClass,
    MetricInfoClass,
    MetricUpstreamsClass,
    NumberTypeClass,
    SchemaMetadataClass,
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


def _build_mapper(**config_overrides: object) -> Mapper:
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        emit_semantic_model_entities=True,
        **config_overrides,
    )
    return Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )


@pytest.fixture
def mapper() -> Mapper:
    return _build_mapper()


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
    assert sm_urn is not None
    assert sm_urn.startswith("urn:li:semanticModel:")

    # The table keeps its dataset URN but becomes a Semantic Model Dataset.
    subtypes = [
        mcp.aspect
        for mcp in mcps
        if mcp.entityUrn == _DS_URN and isinstance(mcp.aspect, SubTypesClass)
    ]
    assert subtypes[0].typeNames == [DatasetSubTypes.SEMANTIC_MODEL_DATASET]

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
        and mcp.entityUrn is not None
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


def test_logical_dataset_joins_workspace_container(mapper: Mapper) -> None:
    workspace = _workspace()
    mcps = mapper.to_datahub_dataset(_dataset(), workspace)

    # The logical dataset stays a member of its workspace container (parity with
    # the classic path), so it does not drop off the workspace container page.
    containers = [
        mcp.aspect
        for mcp in mcps
        if mcp.entityUrn == _DS_URN and isinstance(mcp.aspect, ContainerClass)
    ]
    assert len(containers) == 1
    assert containers[0].container == mapper.make_container_urn_for_workspace(workspace)


def test_container_skipped_when_workspaces_to_containers_disabled() -> None:
    mapper = _build_mapper(extract_workspaces_to_containers=False)
    mcps = mapper.to_datahub_dataset(_dataset(), _workspace())
    assert not _aspects_of(mcps, ContainerClass)


def test_schema_skipped_when_extract_dataset_schema_disabled() -> None:
    # Column-level lineage requires schema, so it must be off to disable schema.
    mapper = _build_mapper(
        extract_dataset_schema=False, extract_column_level_lineage=False
    )
    mcps = mapper.to_datahub_dataset(_dataset(), _workspace())
    assert not _aspects_of(mcps, SchemaMetadataClass)
    assert not _aspects_of(mcps, SemanticFieldAnnotationClass)


def test_platform_instance_scopes_model_and_metric_urns() -> None:
    mapper = _build_mapper(platform_instance="my_instance")
    mcps = mapper.to_datahub_dataset(_dataset(), _workspace())

    sm_urn = next(
        mcp.entityUrn for mcp in mcps if isinstance(mcp.aspect, SemanticModelInfoClass)
    )
    metric_urn = next(
        mcp.entityUrn for mcp in mcps if isinstance(mcp.aspect, MetricInfoClass)
    )
    assert sm_urn is not None and metric_urn is not None
    # The instance is embedded in the shared identity path exactly once (the key
    # aspects have no instance field of their own), so instances that reuse the
    # same workspace/dataset IDs no longer collide.
    assert sm_urn.count("my_instance") == 1
    assert metric_urn.count("my_instance") == 1


def test_profiling_emitted_for_logical_dataset() -> None:
    mapper = _build_mapper(profiling={"enabled": True})
    dataset = _dataset()
    dataset.tables[0].row_count = 42
    mcps = mapper.to_datahub_dataset(dataset, _workspace())

    profiles = [
        mcp.aspect
        for mcp in mcps
        if mcp.entityUrn == _DS_URN and isinstance(mcp.aspect, DatasetProfileClass)
    ]
    assert len(profiles) == 1
    assert profiles[0].rowCount == 42
