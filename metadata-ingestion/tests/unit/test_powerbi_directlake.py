"""Unit tests for PowerBI DirectLake lineage extraction."""

from typing import Literal, Optional

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.fabric_urn_builder import make_onelake_urn
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FabricArtifact,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import UpstreamLineageClass


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


class TestMakeOnelakeUrn:
    """Tests for the make_onelake_urn helper function."""

    def test_make_onelake_urn_with_schema(self):
        """Test full URN generation with schema."""
        result = make_onelake_urn(
            workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            item_id="2afa2dbd-555b-48c8-b082-35d94f4b7836",
            table_name="green_tripdata_2017",
            schema_name="dbo",
            env="PROD",
        )
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
        )

    def test_make_onelake_urn_without_schema(self):
        """Test URN generation without schema (defaults to 'dbo' for schemas-disabled lakehouses)."""
        result = make_onelake_urn(
            workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            item_id="2afa2dbd-555b-48c8-b082-35d94f4b7836",
            table_name="green_tripdata_2017",
            schema_name=None,
            env="PROD",
        )
        # When schema_name is None, it defaults to "dbo"
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
        )

    def test_make_onelake_urn_with_platform_instance(self):
        """Test URN generation with platform instance."""
        result = make_onelake_urn(
            workspace_id="ff23fbe3-7418-42f8-a675-9f10eb2b78cb",
            item_id="2afa2dbd-555b-48c8-b082-35d94f4b7836",
            table_name="green_tripdata_2017",
            schema_name="dbo",
            env="PROD",
            platform_instance="my-instance",
        )
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,my-instance.ff23fbe3-7418-42f8-a675-9f10eb2b78cb.2afa2dbd-555b-48c8-b082-35d94f4b7836.dbo.green_tripdata_2017,PROD)"
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
