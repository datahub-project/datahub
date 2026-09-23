"""Unit tests for PowerBI DirectLake lineage extraction."""

from typing import Any, List, Literal, Optional, Tuple

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FIELD_TYPE_MAPPING,
    Column,
    FabricArtifact,
    Measure,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    UpstreamLineageClass,
)


@pytest.fixture
def config():
    """Create a PowerBI config for testing."""
    return PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


@pytest.fixture
def mapper(config):
    """Create a Mapper instance for testing."""
    ctx = PipelineContext(run_id="test-run-id")
    reporter = PowerBiDashboardSourceReport()
    platform_instance_resolver = ResolvePlatformInstanceFromDatasetTypeMapping(config)
    return Mapper(
        ctx=ctx,
        config=config,
        reporter=reporter,
        dataplatform_instance_resolver=platform_instance_resolver,
    )


class TestDirectLakeLineageExtraction:
    """Tests for DirectLake lineage extraction in the Mapper class."""

    def create_workspace_with_artifact(
        self,
        artifact_id: str = "2afa2dbd-555b-48c8-b082-35d94f4b7836",
        artifact_type: Literal[
            "Lakehouse", "Warehouse", "SQLAnalyticsEndpoint"
        ] = "Lakehouse",
        physical_item_ids: Optional[list] = None,
    ) -> Workspace:
        """Create a test workspace with a Fabric artifact."""
        artifact = FabricArtifact(
            id=artifact_id,
            name="TestLakehouse",
            artifact_type=artifact_type,
            workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            physical_item_ids=physical_item_ids,
        )
        return Workspace(
            id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            name="TestWorkspace",
            type="Workspace",
            webUrl="https://app.powerbi.com/groups/ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            datasets={},
            dashboards={},
            reports={},
            report_endorsements={},
            dashboard_endorsements={},
            scan_result={},
            independent_datasets={},
            app=None,
            fabric_artifacts={artifact_id: artifact},
        )

    def create_workspace_with_artifacts(self, artifacts: dict) -> Workspace:
        """Create a test workspace with multiple Fabric artifacts (id -> FabricArtifact)."""
        return Workspace(
            id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            name="TestWorkspace",
            type="Workspace",
            webUrl="https://app.powerbi.com/groups/ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            datasets={},
            dashboards={},
            reports={},
            report_endorsements={},
            dashboard_endorsements={},
            scan_result={},
            independent_datasets={},
            app=None,
            fabric_artifacts=artifacts,
        )

    def create_directlake_table(
        self,
        dependent_artifact_id: Optional[str] = "2afa2dbd-555b-48c8-b082-35d94f4b7836",
        source_schema: Optional[str] = "dbo",
        source_expression: Optional[str] = "green_tripdata_2017",
        columns: Optional[List[Column]] = None,
        measures: Optional[List[Measure]] = None,
    ) -> Table:
        """Create a test DirectLake table."""
        dataset = PowerBIDataset(
            id="ds-123",
            name="TestDataset",
            description="Test dataset",
            webUrl=None,
            workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            workspace_name="TestWorkspace",
            parameters={},
            tables=[],
            tags=[],
            configuredBy=None,
            dependent_on_artifact_id=dependent_artifact_id,
        )
        table = Table(
            name="green_tripdata_2017",
            full_name="TestWorkspace.TestDataset.green_tripdata_2017",
            storage_mode="DirectLake",
            source_schema=source_schema,
            source_expression=source_expression,
            columns=columns,
            measures=measures,
        )
        table.dataset = dataset
        return table

    def test_extract_directlake_lineage_with_lakehouse(self, mapper):
        """Test DirectLake lineage extraction for a Lakehouse table."""
        workspace = self.create_workspace_with_artifact(artifact_type="Lakehouse")
        table = self.create_directlake_table()

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 1
        assert mcps[0].entityUrn == ds_urn
        assert mcps[0].aspect is not None

        upstream_lineage = mcps[0].aspect
        assert len(upstream_lineage.upstreams) == 1

        upstream = upstream_lineage.upstreams[0]
        expected_urn = "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
        assert upstream.dataset == expected_urn

    def test_extract_directlake_lineage_with_warehouse(self, mapper):
        """Test DirectLake lineage extraction for a Warehouse table."""
        workspace = self.create_workspace_with_artifact(artifact_type="Warehouse")
        table = self.create_directlake_table()

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 1
        upstream = mcps[0].aspect.upstreams[0]
        expected_urn = "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
        assert upstream.dataset == expected_urn

    def test_extract_directlake_lineage_no_artifact(self, mapper):
        """Test DirectLake lineage extraction when artifact is not found."""
        workspace = self.create_workspace_with_artifact()
        table = self.create_directlake_table(
            dependent_artifact_id="non-existent-artifact-id"
        )

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 0

    def test_extract_directlake_lineage_no_source_expression(self, mapper):
        """Test DirectLake lineage extraction when source expression is missing."""
        workspace = self.create_workspace_with_artifact()
        table = self.create_directlake_table(source_expression=None)

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 0

    def test_extract_directlake_lineage_no_dependent_artifact_id(self, mapper):
        """Test DirectLake lineage extraction when dependent artifact ID is missing."""
        workspace = self.create_workspace_with_artifact()
        table = self.create_directlake_table(dependent_artifact_id=None)

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 0

    def test_extract_lineage_routes_to_directlake(self, mapper):
        """Test that extract_lineage correctly routes DirectLake tables."""
        workspace = self.create_workspace_with_artifact()
        table = self.create_directlake_table()

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_lineage(table, ds_urn, workspace)

        # Should route to DirectLake extraction
        assert len(mcps) == 1
        upstream = mcps[0].aspect.upstreams[0]
        assert "fabric-onelake" in upstream.dataset

    def test_extract_directlake_lineage_with_platform_instance(self):
        """Test DirectLake lineage extraction with platform instance mapping."""
        # Create config with server_to_platform_instance mapping
        config = PowerBiDashboardSourceConfig(
            tenant_id="test-tenant-id",
            client_id="test-client-id",
            client_secret="test-client-secret",
            server_to_platform_instance={
                "ff23fbe3-7418-42f8-a675-9f10eb2b78cb": {  # Workspace ID
                    "platform_instance": "contoso-tenant",  # Fabric tenant/platform instance
                    "env": "PROD",
                }
            },
        )

        ctx = PipelineContext(run_id="test-run-id")
        reporter = PowerBiDashboardSourceReport()
        from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
            ResolvePlatformInstanceFromServerToPlatformInstance,
        )

        platform_instance_resolver = (
            ResolvePlatformInstanceFromServerToPlatformInstance(config)
        )
        mapper = Mapper(
            ctx=ctx,
            config=config,
            reporter=reporter,
            dataplatform_instance_resolver=platform_instance_resolver,
        )

        workspace = self.create_workspace_with_artifact()
        table = self.create_directlake_table()

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"

        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 1
        assert mcps[0].entityUrn == ds_urn
        assert mcps[0].aspect is not None
        assert isinstance(mcps[0].aspect, UpstreamLineageClass)

        upstream_lineage = mcps[0].aspect
        assert len(upstream_lineage.upstreams) == 1

        upstream = upstream_lineage.upstreams[0]
        # URN should include platform instance (contoso-tenant)
        expected_urn = "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,contoso-tenant.ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
        assert upstream.dataset == expected_urn

    def test_extract_directlake_lineage_sqlanalyticsendpoint_uses_physical_id(
        self, mapper
    ):
        """SQLAnalyticsEndpoint artifact: lineage uses dependentOnArtifactId (physical id), not artifact id."""
        lakehouse_id = "2afa2dbd-555b-48c8-b082-35d94f4b7836"
        endpoint_id = "e199683a-5e30-43e9-a054-c6319ab16398"
        workspace = self.create_workspace_with_artifacts(
            {
                lakehouse_id: FabricArtifact(
                    id=lakehouse_id,
                    name="TestLakehouse",
                    artifact_type="Lakehouse",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                ),
                endpoint_id: FabricArtifact(
                    id=endpoint_id,
                    name="TestEndpoint",
                    artifact_type="SQLAnalyticsEndpoint",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                    physical_item_ids=[lakehouse_id],
                ),
            }
        )
        table = self.create_directlake_table(dependent_artifact_id=endpoint_id)

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"
        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 1
        assert len(mcps[0].aspect.upstreams) == 1
        expected_urn = "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
        assert mcps[0].aspect.upstreams[0].dataset == expected_urn

    def test_extract_directlake_lineage_sqlanalyticsendpoint_multiple_relations(
        self, mapper
    ):
        """SQLAnalyticsEndpoint with multiple relations: two upstreams in the same aspect."""
        lakehouse_id_1 = "2afa2dbd-555b-48c8-b082-35d94f4b7836"
        lakehouse_id_2 = "3b0b3ece-6269-49d9-c183-46e95f5c5847"
        endpoint_id = "e199683a-5e30-43e9-a054-c6319ab16398"
        workspace = self.create_workspace_with_artifacts(
            {
                lakehouse_id_1: FabricArtifact(
                    id=lakehouse_id_1,
                    name="Lakehouse1",
                    artifact_type="Lakehouse",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                ),
                lakehouse_id_2: FabricArtifact(
                    id=lakehouse_id_2,
                    name="Lakehouse2",
                    artifact_type="Lakehouse",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                ),
                endpoint_id: FabricArtifact(
                    id=endpoint_id,
                    name="TestEndpoint",
                    artifact_type="SQLAnalyticsEndpoint",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                    physical_item_ids=[lakehouse_id_1, lakehouse_id_2],
                ),
            }
        )
        table = self.create_directlake_table(dependent_artifact_id=endpoint_id)

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"
        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 1
        assert len(mcps[0].aspect.upstreams) == 2
        upstream_urns = [u.dataset for u in mcps[0].aspect.upstreams]
        assert any("2afa2dbd-555b-48c8-b082-35d94f4b7836" in u for u in upstream_urns)
        assert any("3b0b3ece-6269-49d9-c183-46e95f5c5847" in u for u in upstream_urns)

    def test_extract_directlake_lineage_sqlanalyticsendpoint_only_resolvable_used(
        self, mapper
    ):
        """SQLAnalyticsEndpoint: only resolvable Lakehouse/Warehouse ids from relations are used."""
        lakehouse_id = "2afa2dbd-555b-48c8-b082-35d94f4b7836"
        endpoint_id = "e199683a-5e30-43e9-a054-c6319ab16398"
        workspace = self.create_workspace_with_artifacts(
            {
                lakehouse_id: FabricArtifact(
                    id=lakehouse_id,
                    name="TestLakehouse",
                    artifact_type="Lakehouse",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                ),
                endpoint_id: FabricArtifact(
                    id=endpoint_id,
                    name="TestEndpoint",
                    artifact_type="SQLAnalyticsEndpoint",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                    physical_item_ids=[lakehouse_id],
                ),
            }
        )
        table = self.create_directlake_table(dependent_artifact_id=endpoint_id)

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"
        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 1
        assert len(mcps[0].aspect.upstreams) == 1
        assert (
            "2afa2dbd-555b-48c8-b082-35d94f4b7836"
            in mcps[0].aspect.upstreams[0].dataset
        )

    def test_extract_directlake_lineage_sqlanalyticsendpoint_zero_resolvable_no_lineage(
        self, mapper
    ):
        """SQLAnalyticsEndpoint with zero resolvable relations: no lineage emitted, warning logged."""
        endpoint_id = "e199683a-5e30-43e9-a054-c6319ab16398"
        workspace = self.create_workspace_with_artifacts(
            {
                endpoint_id: FabricArtifact(
                    id=endpoint_id,
                    name="TestEndpoint",
                    artifact_type="SQLAnalyticsEndpoint",
                    workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
                    physical_item_ids=[],
                ),
            }
        )
        table = self.create_directlake_table(dependent_artifact_id=endpoint_id)

        ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.green_tripdata_2017,PROD)"
        mcps = mapper.extract_directlake_lineage(table, ds_urn, workspace)

        assert len(mcps) == 0


class TestFabricArtifactDataClass:
    """Tests for the FabricArtifact dataclass."""

    def test_fabric_artifact_creation(self):
        """Test creating a FabricArtifact instance."""
        artifact = FabricArtifact(
            id="test-artifact-id",
            name="TestLakehouse",
            artifact_type="Lakehouse",
            workspace_id="test-workspace-id",
        )
        assert artifact.id == "test-artifact-id"
        assert artifact.name == "TestLakehouse"
        assert artifact.artifact_type == "Lakehouse"
        assert artifact.workspace_id == "test-workspace-id"

    def test_fabric_artifact_types(self):
        """Test different artifact types."""
        artifact_types: list[
            Literal["Lakehouse", "Warehouse", "SQLAnalyticsEndpoint"]
        ] = [
            "Lakehouse",
            "Warehouse",
            "SQLAnalyticsEndpoint",
        ]
        for artifact_type in artifact_types:
            artifact = FabricArtifact(
                id="test-id",
                name="TestArtifact",
                artifact_type=artifact_type,
                workspace_id="workspace-id",
            )
            assert artifact.artifact_type == artifact_type


class TestTableDirectLakeFields:
    """Tests for DirectLake fields on the Table dataclass."""

    def test_table_with_directlake_fields(self):
        """Test creating a Table with DirectLake fields."""
        table = Table(
            name="test_table",
            full_name="workspace.dataset.test_table",
            storage_mode="DirectLake",
            source_schema="dbo",
            source_expression="upstream_table",
        )
        assert table.storage_mode == "DirectLake"
        assert table.source_schema == "dbo"
        assert table.source_expression == "upstream_table"

    def test_table_without_directlake_fields(self):
        """Test creating a Table without DirectLake fields (backward compatibility)."""
        table = Table(
            name="test_table",
            full_name="workspace.dataset.test_table",
        )
        assert table.storage_mode is None
        assert table.source_schema is None
        assert table.source_expression is None

    def test_table_with_import_storage_mode(self):
        """Test table with Import storage mode."""
        table = Table(
            name="test_table",
            full_name="workspace.dataset.test_table",
            storage_mode="Import",
        )
        assert table.storage_mode == "Import"


WORKSPACE_ID = "ff23fbe3-7418-42f8-a675-9f10eb2b78cb"
LAKEHOUSE_ID = "2afa2dbd-555b-48c8-b082-35d94f4b7836"
DS_URN = "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.sales_orders,PROD)"
UPSTREAM_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,"
    f"{WORKSPACE_ID}.{LAKEHOUSE_ID}.dbo.sales_orders,PROD)"
)


def _column(
    name: str,
    source_column: Optional[str] = None,
    column_type: Optional[str] = "Data",
    expression: Optional[str] = None,
    data_type: str = "String",
) -> Column:
    return Column(
        name=name,
        dataType=data_type,
        isHidden=False,
        datahubDataType=FIELD_TYPE_MAPPING[data_type],
        columnType=column_type,
        expression=expression,
        sourceColumn=source_column,
    )


def _field_urn(dataset_urn: str, column: str) -> str:
    return f"urn:li:schemaField:({dataset_urn},{column})"


def _make_mapper(
    **config_overrides: Any,
) -> Tuple[Mapper, PowerBiDashboardSourceReport]:
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        **config_overrides,
    )
    reporter = PowerBiDashboardSourceReport()
    mapper = Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=reporter,
        dataplatform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )
    return mapper, reporter


def _fgls(mapper: Mapper, table: Table, workspace: Workspace) -> UpstreamLineageClass:
    mcps = mapper.extract_directlake_lineage(table, DS_URN, workspace)
    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)
    return aspect


class TestDirectLakeColumnLineage:
    """Column-level lineage from DirectLake columns to OneLake columns."""

    helper = TestDirectLakeLineageExtraction()

    def _table(
        self,
        columns: Optional[List[Column]],
        measures: Optional[List[Measure]] = None,
        source_expression: str = "sales_orders",
    ) -> Table:
        return self.helper.create_directlake_table(
            source_expression=source_expression,
            columns=columns,
            measures=measures,
        )

    def test_column_lineage_uses_column_name(self) -> None:
        mapper, report = _make_mapper()
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table([_column("order_id"), _column("amount")])

        aspect = _fgls(mapper, table, workspace)

        assert aspect.fineGrainedLineages == [
            FineGrainedLineageClass(
                downstreamType="FIELD",
                downstreams=[_field_urn(DS_URN, "order_id")],
                upstreamType="FIELD_SET",
                upstreams=[_field_urn(UPSTREAM_URN, "order_id")],
            ),
            FineGrainedLineageClass(
                downstreamType="FIELD",
                downstreams=[_field_urn(DS_URN, "amount")],
                upstreamType="FIELD_SET",
                upstreams=[_field_urn(UPSTREAM_URN, "amount")],
            ),
        ]
        assert report.directlake_column_lineage_edges == 2

    def test_renamed_column_uses_source_column(self) -> None:
        mapper, report = _make_mapper()
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table(
            [
                _column("Order Amount", source_column="order_amount"),
                # sourceColumn equal to the name is not counted as a rename
                _column("customer_id", source_column="customer_id"),
            ]
        )

        aspect = _fgls(mapper, table, workspace)

        assert aspect.fineGrainedLineages is not None
        edges = {
            tuple(fgl.downstreams or []): fgl.upstreams
            for fgl in aspect.fineGrainedLineages
        }
        assert edges == {
            (_field_urn(DS_URN, "Order Amount"),): [
                _field_urn(UPSTREAM_URN, "order_amount")
            ],
            (_field_urn(DS_URN, "customer_id"),): [
                _field_urn(UPSTREAM_URN, "customer_id")
            ],
        }
        assert report.directlake_columns_mapped_via_source_column == 1

    def test_measures_and_calculated_columns_skipped(self) -> None:
        mapper, report = _make_mapper()
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table(
            columns=[
                _column("order_id"),
                _column(
                    "amount_with_tax",
                    column_type="Calculated",
                    expression="[amount] * 1.1",
                ),
                # Expression without columnType is also treated as calculated
                _column("margin", column_type=None, expression="[amount] - [cost]"),
                _column("RowNumber-2662979B", column_type="RowNumber"),
            ],
            measures=[
                Measure(name="Total Sales", expression="SUM([amount])", isHidden=False)
            ],
        )

        aspect = _fgls(mapper, table, workspace)

        assert aspect.fineGrainedLineages is not None
        assert [fgl.downstreams for fgl in aspect.fineGrainedLineages] == [
            [_field_urn(DS_URN, "order_id")]
        ]
        assert report.directlake_calculated_columns_skipped == 3
        assert report.directlake_measures_skipped == 1
        assert report.directlake_column_lineage_edges == 1

    def test_column_without_column_type_is_physical(self) -> None:
        mapper, report = _make_mapper()
        """Older scan payloads omit columnType; a plain column is still physical."""
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table([_column("order_id", column_type=None)])

        aspect = _fgls(mapper, table, workspace)

        assert aspect.fineGrainedLineages is not None
        assert len(aspect.fineGrainedLineages) == 1

    def test_column_lineage_disabled_by_flag(self) -> None:
        mapper, report = _make_mapper(extract_column_level_lineage=False)
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table([_column("order_id")])

        aspect = _fgls(mapper, table, workspace)

        # Table-level lineage is still emitted
        assert [u.dataset for u in aspect.upstreams] == [UPSTREAM_URN]
        assert aspect.fineGrainedLineages is None
        assert report.directlake_column_lineage_edges == 0

    def test_no_columns_emits_table_lineage_only(self) -> None:
        mapper, report = _make_mapper()
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table(columns=None)

        aspect = _fgls(mapper, table, workspace)

        assert len(aspect.upstreams) == 1
        assert aspect.fineGrainedLineages is None

    def test_lowercase_applies_to_dataset_not_column(self) -> None:
        mapper, report = _make_mapper()
        """convert_lineage_urns_to_lowercase (default True) lowercases only the
        dataset part of the upstream schemaField URN, like the M-Query path."""
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table(
            [_column("Customer Name", source_column="CustomerName")],
            source_expression="Sales_Orders",
        )

        aspect = _fgls(mapper, table, workspace)

        assert aspect.upstreams[0].dataset == UPSTREAM_URN
        assert aspect.fineGrainedLineages is not None
        assert aspect.fineGrainedLineages[0].upstreams == [
            _field_urn(UPSTREAM_URN, "CustomerName")
        ]

    def test_lowercase_disabled_preserves_dataset_case(self) -> None:
        mapper, report = _make_mapper(convert_lineage_urns_to_lowercase=False)
        workspace = self.helper.create_workspace_with_artifact()
        table = self._table([_column("OrderId")], source_expression="Sales_Orders")

        aspect = _fgls(mapper, table, workspace)

        mixed_case_urn = UPSTREAM_URN.replace("sales_orders", "Sales_Orders")
        assert aspect.upstreams[0].dataset == mixed_case_urn
        assert aspect.fineGrainedLineages is not None
        assert aspect.fineGrainedLineages[0].upstreams == [
            _field_urn(mixed_case_urn, "OrderId")
        ]

    def test_sqlanalyticsendpoint_multiple_items_fan_out_per_column(self) -> None:
        mapper, report = _make_mapper()
        """Each column maps to the same column in every resolved physical item."""
        lakehouse_2 = "3b0b3ece-6269-49d9-c183-46e95f5c5847"
        endpoint_id = "e199683a-5e30-43e9-a054-c6319ab16398"
        workspace = self.helper.create_workspace_with_artifacts(
            {
                LAKEHOUSE_ID: FabricArtifact(
                    id=LAKEHOUSE_ID,
                    name="Lakehouse1",
                    artifact_type="Lakehouse",
                    workspace_id=WORKSPACE_ID,
                ),
                lakehouse_2: FabricArtifact(
                    id=lakehouse_2,
                    name="Lakehouse2",
                    artifact_type="Lakehouse",
                    workspace_id=WORKSPACE_ID,
                ),
                endpoint_id: FabricArtifact(
                    id=endpoint_id,
                    name="Endpoint",
                    artifact_type="SQLAnalyticsEndpoint",
                    workspace_id=WORKSPACE_ID,
                    physical_item_ids=[LAKEHOUSE_ID, lakehouse_2],
                ),
            }
        )
        table = self.helper.create_directlake_table(
            dependent_artifact_id=endpoint_id,
            source_expression="sales_orders",
            columns=[_column("order_id")],
        )

        aspect = _fgls(mapper, table, workspace)

        assert aspect.fineGrainedLineages is not None
        assert len(aspect.fineGrainedLineages) == 1
        assert aspect.fineGrainedLineages[0].upstreams == [
            _field_urn(UPSTREAM_URN, "order_id"),
            _field_urn(UPSTREAM_URN.replace(LAKEHOUSE_ID, lakehouse_2), "order_id"),
        ]
