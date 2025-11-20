from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dataform.dataform import DataformSource
from datahub.ingestion.source.dataform.dataform_config import (
    DataformCloudConfig,
    DataformCoreConfig,
    DataformSourceConfig,
)
from datahub.ingestion.source.dataform.dataform_models import (
    DataformColumn,
    DataformEntities,
    DataformTable,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset


class TestDataformSource:
    def test_source_creation_cloud_mode(self):
        """Test Dataform source creation in cloud mode."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        assert source.config == config
        assert source.platform == "dataform"
        assert source.dataform_api is not None

    def test_source_creation_core_mode(self):
        """Test Dataform source creation in core mode."""
        config = DataformSourceConfig(
            core_config=DataformCoreConfig(
                project_path="/path/to/project",
            ),
            target_platform="postgres",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        assert source.config == config
        assert source.platform == "dataform"
        assert source.dataform_api is not None

    def test_create_class_method(self):
        """Test source creation via create class method."""
        config_dict = {
            "cloud_config": {
                "project_id": "test-project",
                "repository_id": "test-repo",
            },
            "target_platform": "bigquery",
        }

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource.create(config_dict, ctx)

        assert isinstance(source, DataformSource)
        assert source.config.target_platform == "bigquery"

    def test_test_connection_success(self):
        """Test successful connection testing."""
        config_dict = {
            "cloud_config": {
                "project_id": "test-project",
                "repository_id": "test-repo",
            },
            "target_platform": "bigquery",
        }

        with patch(
            "datahub.ingestion.source.dataform.dataform_api.DataformAPI"
        ) as mock_api_class:
            mock_api = MagicMock()
            mock_api.get_compilation_result.return_value = {"compiledGraph": {}}
            mock_api_class.return_value = mock_api

            report = DataformSource.test_connection(config_dict)

            assert (
                report.basic_connectivity is not None
                and report.basic_connectivity.capable is True
            )

    def test_test_connection_failure(self):
        """Test connection testing failure."""
        config_dict = {
            "cloud_config": {
                "project_id": "test-project",
                "repository_id": "test-repo",
            },
            "target_platform": "bigquery",
        }

        with patch(
            "datahub.ingestion.source.dataform.dataform_api.DataformAPI"
        ) as mock_api_class:
            mock_api = MagicMock()
            mock_api.get_compilation_result.return_value = None
            mock_api_class.return_value = mock_api

            report = DataformSource.test_connection(config_dict)

            assert (
                report.basic_connectivity is not None
                and report.basic_connectivity.capable is False
            )
            assert report.basic_connectivity.failure_reason is not None and (
                "Failed to get compilation result"
                in report.basic_connectivity.failure_reason
            )

    def test_should_create_sibling_relationships_default(self):
        """Test sibling relationship creation with default settings."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            dataform_is_primary_sibling=True,  # Default
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        table = DataformTable(name="test_table", schema_name="analytics", type="table")

        # Should NOT create siblings when dataform_is_primary_sibling=True (server hook handles it)
        assert source._should_create_sibling_relationships(table) is False

    def test_should_create_sibling_relationships_explicit_control(self):
        """Test sibling relationship creation with explicit control."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            dataform_is_primary_sibling=False,  # Explicit control
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        table = DataformTable(name="test_table", schema_name="analytics", type="table")

        # Should create siblings when dataform_is_primary_sibling=False
        assert source._should_create_sibling_relationships(table) is True

    def test_should_materialize_in_target_platform(self):
        """Test target platform materialization logic."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        # Table should materialize
        table = DataformTable(name="test", schema_name="analytics", type="table")
        assert source._should_materialize_in_target_platform(table) is True

        # View should materialize
        view = DataformTable(name="test", schema_name="analytics", type="view")
        assert source._should_materialize_in_target_platform(view) is True

        # Incremental should materialize
        incremental = DataformTable(
            name="test", schema_name="analytics", type="incremental"
        )
        assert source._should_materialize_in_target_platform(incremental) is True

    def test_generate_dataform_platform_dataset(self):
        """Test Dataform platform dataset generation."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            tag_prefix="dataform:",
            custom_properties={"team": "data-engineering"},
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        table = DataformTable(
            name="customer_metrics",
            schema_name="analytics",
            database="my-project",
            type="table",
            description="Customer analytics table",
            columns=[
                DataformColumn(
                    name="customer_id", type="STRING", description="Customer identifier"
                ),
                DataformColumn(
                    name="total_orders", type="INT64", description="Total orders"
                ),
            ],
            tags=["production", "analytics"],
            materialization_type="table",
            file_path="definitions/analytics/customer_metrics.sqlx",
        )

        dataset = source._generate_dataform_platform_dataset(table)

        assert dataset is not None
        assert isinstance(dataset, Dataset)
        assert dataset.platform == "dataform"
        assert dataset.qualified_name == "my-project.analytics.customer_metrics"
        assert dataset.display_name == "customer_metrics"
        assert dataset.description == "Customer analytics table"
        assert len(dataset.schema) == 2
        assert dataset.schema[0].field_path == "customer_id"
        assert dataset.schema[1].field_path == "total_orders"
        tag_strings = [tag.tag for tag in dataset.tags] if dataset.tags else []
        assert "dataform:production" in tag_strings
        assert "dataform:analytics" in tag_strings
        assert dataset.custom_properties["materialization_type"] == "table"
        assert dataset.custom_properties["team"] == "data-engineering"

    def test_generate_dataform_platform_dataset_with_git_integration(self):
        """Test Dataform platform dataset generation with Git integration."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            git_repository_url="https://github.com/org/dataform-project",
            git_branch="main",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        table = DataformTable(
            name="test_table",
            schema_name="analytics",
            type="table",
            file_path="definitions/analytics/test_table.sqlx",
        )

        dataset = source._generate_dataform_platform_dataset(table)

        assert dataset is not None
        expected_git_url = "https://github.com/org/dataform-project/blob/main/definitions/analytics/test_table.sqlx"
        assert dataset.custom_properties["git_url"] == expected_git_url

    def test_get_dataform_platform_urn(self):
        """Test Dataform platform URN generation."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            env="PROD",
            platform_instance="dataform-prod",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        table = DataformTable(
            name="customer_metrics",
            schema_name="analytics",
            database="my-project",
            type="table",
        )

        urn = source._get_dataform_platform_urn(table)

        expected_urn = "urn:li:dataset:(urn:li:dataPlatform:dataform,my-project.analytics.customer_metrics,PROD)"
        assert urn == expected_urn

    def test_get_target_platform_urn(self):
        """Test target platform URN generation."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            target_platform_instance="prod",
            env="PROD",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        table = DataformTable(
            name="customer_metrics",
            schema_name="analytics",
            database="my-project",
            type="table",
        )

        urn = source._get_target_platform_urn(table)

        expected_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.analytics.customer_metrics,PROD)"
        assert urn == expected_urn

    @patch("datahub.ingestion.source.dataform.dataform_api.DataformAPI")
    def test_get_workunits_internal_success(self, mock_api_class):
        """Test successful workunit generation."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        # Mock API
        mock_api = MagicMock()
        mock_api.get_compilation_result.return_value = {"compiledGraph": {}}
        mock_api.extract_entities.return_value = DataformEntities(
            tables=[
                DataformTable(
                    name="test_table",
                    schema_name="analytics",
                    type="table",
                    columns=[DataformColumn(name="id", type="STRING")],
                )
            ]
        )
        mock_api_class.return_value = mock_api
        source.dataform_api = mock_api

        workunits = list(source.get_workunits_internal())

        # Should have at least schema container and dataset
        assert len(workunits) >= 2

        # Check that we have a container and dataset
        container_count = sum(1 for wu in workunits if isinstance(wu, Container))
        dataset_count = sum(1 for wu in workunits if isinstance(wu, Dataset))

        assert container_count >= 1  # At least one schema container
        assert dataset_count >= 1  # At least one dataset

    @patch("datahub.ingestion.source.dataform.dataform_api.DataformAPI")
    def test_get_workunits_internal_compilation_failure(self, mock_api_class):
        """Test workunit generation with compilation failure."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        # Mock API to return None (compilation failure)
        mock_api = MagicMock()
        mock_api.get_compilation_result.return_value = None
        mock_api_class.return_value = mock_api
        source.dataform_api = mock_api

        workunits = list(source.get_workunits_internal())

        # Should return empty list on compilation failure
        assert len(workunits) == 0

    def test_generate_schema_container_key(self):
        """Test schema container key generation."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            target_platform_instance="prod",
            env="PROD",
        )

        ctx = PipelineContext(run_id="test-run", pipeline_name="dataform-test")
        source = DataformSource(ctx, config)

        container_key = source._generate_schema_container_key("analytics")

        assert container_key.schema == "analytics"
        assert container_key.platform == "bigquery"
        assert container_key.instance == "prod"
        assert container_key.env == "PROD"
