from unittest.mock import ANY, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.source import UnityCatalogSource


class TestUnityCatalogSource:
    @pytest.mark.parametrize(
        "azure_auth_partial",
        [
            {"tenant_id": "tid", "client_secret": "sec"},  # missing client_id
            {"client_id": "cid", "client_secret": "sec"},  # missing tenant_id
            {"client_id": "cid", "tenant_id": "tid"},  # missing client_secret
        ],
    )
    def test_azure_auth_config_missing_fields(self, azure_auth_partial):
        """Test that missing any of client_id, tenant_id, or client_secret in azure_auth raises a validation error."""
        config_dict = {
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "azure_auth": azure_auth_partial,
        }
        with pytest.raises(Exception) as exc_info:
            UnityCatalogSourceConfig.model_validate(config_dict)
        # Should mention the missing field in the error message
        assert (
            "client_id" in str(exc_info.value)
            or "tenant_id" in str(exc_info.value)
            or "client_secret" in str(exc_info.value)
        )

    @pytest.fixture
    def minimal_config(self):
        """Create a minimal config for testing."""
        return UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

    @pytest.fixture
    def config_with_page_size(self):
        """Create a config with custom page size."""
        return UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 75,
            }
        )

    @pytest.fixture
    def config_with_ml_model_settings(self):
        """Create a config with ML model settings."""
        return UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_ml_model_aliases": True,
                "ml_model_max_results": 500,
                "databricks_api_page_size": 100,
            }
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_constructor_passes_default_page_size_to_proxy(
        self, mock_hive_proxy, mock_unity_proxy, minimal_config
    ):
        """Test that UnityCatalogSource passes default databricks_api_page_size to proxy."""
        # Create a mock context
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(minimal_config, ctx)

        # Verify proxy was created with correct parameters including page size
        mock_unity_proxy.assert_called_once_with(
            ANY,  # WorkspaceClient instance
            report=source.report,
            hive_metastore_proxy=source.hive_metastore_proxy,
            lineage_data_source=minimal_config.lineage_data_source,
            usage_data_source=minimal_config.usage_data_source,
            databricks_api_page_size=0,  # Default value
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_constructor_passes_custom_page_size_to_proxy(
        self, mock_hive_proxy, mock_unity_proxy, config_with_page_size
    ):
        """Test that UnityCatalogSource passes custom databricks_api_page_size to proxy."""
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config_with_page_size, ctx)

        # Verify proxy was created with correct parameters including custom page size
        mock_unity_proxy.assert_called_once_with(
            ANY,  # WorkspaceClient instance
            report=source.report,
            hive_metastore_proxy=source.hive_metastore_proxy,
            lineage_data_source=config_with_page_size.lineage_data_source,
            usage_data_source=config_with_page_size.usage_data_source,
            databricks_api_page_size=75,  # Custom value
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_config_page_size_available_to_source(
        self, mock_hive_proxy, mock_unity_proxy, config_with_page_size
    ):
        """Test that UnityCatalogSource has access to databricks_api_page_size config."""
        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config_with_page_size, ctx)

        # Verify the source has access to the configuration value
        assert source.config.databricks_api_page_size == 75

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_with_hive_metastore_disabled(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that UnityCatalogSource works with hive metastore disabled."""
        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 200,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Verify proxy was created with correct page size even when hive metastore is disabled
        mock_unity_proxy.assert_called_once_with(
            ANY,  # WorkspaceClient instance
            report=source.report,
            hive_metastore_proxy=None,  # Should be None when disabled
            lineage_data_source=config.lineage_data_source,
            usage_data_source=config.usage_data_source,
            databricks_api_page_size=200,
        )

    def test_test_connection_with_page_size_config(self):
        """Test that test_connection properly handles databricks_api_page_size."""
        config_dict = {
            "token": "test_token",
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "databricks_api_page_size": 300,
        }

        with patch(
            "datahub.ingestion.source.unity.source.UnityCatalogConnectionTest"
        ) as mock_connection_test:
            mock_connection_test.return_value.get_connection_test.return_value = (
                "test_report"
            )

            result = UnityCatalogSource.test_connection(config_dict)

            # Verify connection test was created with correct config
            assert result == "test_report"
            mock_connection_test.assert_called_once()

            # Get the config that was passed to UnityCatalogConnectionTest
            connection_test_config = mock_connection_test.call_args[0][0]
            assert connection_test_config.databricks_api_page_size == 300

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_source_report_includes_ml_model_stats(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that source report properly tracks ML model statistics."""
        from datahub.ingestion.api.common import PipelineContext

        # Setup mocks
        mock_unity_instance = mock_unity_proxy.return_value
        mock_unity_instance.catalogs.return_value = []
        mock_unity_instance.check_basic_connectivity.return_value = True

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 200,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Verify report has proper ML model tracking attributes
        assert hasattr(source.report, "ml_models")
        assert hasattr(source.report, "ml_model_versions")

        # Verify they are EntityFilterReport objects
        assert isinstance(source.report.ml_models, EntityFilterReport)
        assert isinstance(source.report.ml_model_versions, EntityFilterReport)

    def test_test_connection_with_ml_model_configs(self):
        """Test that test_connection properly handles ML model configs."""
        config_dict = {
            "token": "test_token",
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "include_ml_model_aliases": True,
            "ml_model_max_results": 750,
            "databricks_api_page_size": 200,
        }

        with patch(
            "datahub.ingestion.source.unity.source.UnityCatalogConnectionTest"
        ) as mock_connection_test:
            mock_connection_test.return_value.get_connection_test.return_value = (
                "test_report"
            )

            result = UnityCatalogSource.test_connection(config_dict)

            # Verify connection test was created with correct config
            assert result == "test_report"
            mock_connection_test.assert_called_once()

            # Get the config that was passed to UnityCatalogConnectionTest
            connection_test_config = mock_connection_test.call_args[0][0]
            assert connection_test_config.include_ml_model_aliases is True
            assert connection_test_config.ml_model_max_results == 750
            assert connection_test_config.databricks_api_page_size == 200

    def test_azure_auth_config_validation(self):
        """Test that Azure auth config validates required fields."""
        # Test valid Azure auth config
        valid_config_dict = {
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "azure_auth": {
                "client_id": "test-client-id",
                "tenant_id": "test-tenant-id",
                "client_secret": "test-secret",
            },
        }

        config = UnityCatalogSourceConfig.model_validate(valid_config_dict)
        assert config.azure_auth is not None
        assert config.azure_auth.client_id == "test-client-id"
        assert config.azure_auth.tenant_id == "test-tenant-id"
        assert config.azure_auth.client_secret.get_secret_value() == "test-secret"

        # Test that personal access token is not required when Azure auth is provided
        assert config.token is None

    def test_test_connection_with_azure_auth(self):
        """Test that test_connection properly handles Azure authentication."""
        config_dict = {
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test_warehouse",
            "databricks_api_page_size": 100,
            "azure_auth": {
                "client_id": "test-client-id",
                "tenant_id": "test-tenant-id",
                "client_secret": "test-secret",
            },
        }

        with patch(
            "datahub.ingestion.source.unity.source.UnityCatalogConnectionTest"
        ) as mock_connection_test:
            mock_connection_test.return_value.get_connection_test.return_value = (
                "azure_test_report"
            )

            result = UnityCatalogSource.test_connection(config_dict)

            # Verify connection test was created with Azure auth config
            assert result == "azure_test_report"
            mock_connection_test.assert_called_once()

            # Get the config that was passed to UnityCatalogConnectionTest
            connection_test_config = mock_connection_test.call_args[0][0]
            assert connection_test_config.azure_auth is not None
            assert connection_test_config.azure_auth.client_id == "test-client-id"
            assert connection_test_config.azure_auth.tenant_id == "test-tenant-id"
            assert (
                connection_test_config.azure_auth.client_secret.get_secret_value()
                == "test-secret"
            )
            assert connection_test_config.databricks_api_page_size == 100
            assert (
                connection_test_config.token is None
            )  # Should be None with Azure auth

    def test_source_creation_fails_without_authentication(self):
        """Test that UnityCatalogSource creation fails when neither token nor azure_auth are provided."""
        # Note: Config validation doesn't require authentication, but WorkspaceClient creation will fail
        # This test verifies that the config can be created without auth (validation passes)
        # but actual usage will fail when trying to create the WorkspaceClient
        config = UnityCatalogSourceConfig.model_validate(
            {
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "databricks_api_page_size": 100,
                # Neither token nor azure_auth provided
            }
        )

        # Config validation passes (no auth required at this stage)
        assert config.token is None
        assert config.azure_auth is None

    def test_test_connection_fails_without_authentication(self):
        """Test that test_connection fails when neither token nor azure_auth are provided."""
        # Note: Config validation doesn't require authentication, but WorkspaceClient creation will fail
        # This test verifies that the config can be created without auth (validation passes)
        config = UnityCatalogSourceConfig.model_validate(
            {
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "databricks_api_page_size": 100,
                # Neither token nor azure_auth provided
            }
        )

        # Config validation passes (no auth required at this stage)
        assert config.token is None
        assert config.azure_auth is None

    def test_source_creation_fails_with_both_authentication_methods(self):
        """Test that UnityCatalogSource creation fails when both token and azure_auth are provided."""
        # Test with both token and azure_auth provided - this should fail at config parsing
        with pytest.raises(ValueError) as exc_info:
            UnityCatalogSourceConfig.model_validate(
                {
                    "workspace_url": "https://test.databricks.com",
                    "warehouse_id": "test_warehouse",
                    "include_hive_metastore": False,
                    "databricks_api_page_size": 100,
                    "token": "test_token",  # Both provided
                    "azure_auth": {
                        "client_id": "test-client-id",
                        "tenant_id": "test-tenant-id",
                        "client_secret": "test-secret",
                    },
                }
            )

        # Should mention that only one authentication method is allowed
        assert (
            "More than one of 'token', 'azure_auth', and 'client_id'/'client_secret' provided"
            in str(exc_info.value)
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_process_ml_model_generates_workunits(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that process_ml_model generates proper workunits."""
        from datetime import datetime

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Model,
            ModelVersion,
            Schema,
        )

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Create test schema
        metastore = Metastore(
            id="metastore",
            name="metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="test_catalog",
            name="test_catalog",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="test_catalog.test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        # Create test model
        test_model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description="Test description",
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
        )

        # Create test model version
        test_model_version = ModelVersion(
            id="test_catalog.test_schema.test_model_1",
            name="test_model_1",
            model=test_model,
            version="1",
            aliases=["prod"],
            description="Version 1",
            created_at=datetime(2023, 1, 3),
            updated_at=datetime(2023, 1, 4),
            created_by="test_user",
            run_details=None,
            signature=None,
        )

        # Process the model
        ml_model_workunits = list(source.process_ml_model(test_model, schema))

        # Should generate workunits (MLModelGroup creation and container assignment)
        assert len(ml_model_workunits) > 0

        assert len(source.report.ml_models.processed_entities) == 1
        assert (
            source.report.ml_models.processed_entities[0][1]
            == "test_catalog.test_schema.test_model"
        )

        # Process the model version
        model_urn = source.gen_ml_model_urn(test_model.id)
        ml_model_version_workunits = list(
            source.process_ml_model_version(model_urn, test_model_version, schema)
        )

        # Should generate workunits (MLModel creation and container assignment)
        assert len(ml_model_version_workunits) > 0

        # Verify the report was updated
        assert len(source.report.ml_model_versions.processed_entities) == 1
        assert (
            source.report.ml_model_versions.processed_entities[0][1]
            == "test_catalog.test_schema.test_model_1"
        )

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_process_ml_model_version_with_run_details(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that process_ml_model_version properly handles run_details."""
        from datetime import datetime

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Model,
            ModelRunDetails,
            ModelSignature,
            ModelVersion,
            Schema,
        )

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Create test schema
        metastore = Metastore(
            id="metastore",
            name="metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="test_catalog",
            name="test_catalog",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="test_catalog.test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        # Create test model
        test_model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description="Test description",
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
        )

        # Create model signature
        signature = ModelSignature(
            inputs=[{"name": "feature1", "type": "double"}],
            outputs=[{"name": "prediction", "type": "double"}],
            parameters=None,
        )

        # Create run details
        run_details = ModelRunDetails(
            run_id="test_run_123",
            experiment_id="exp_456",
            status="FINISHED",
            start_time=datetime(2023, 1, 3, 10, 0, 0),
            end_time=datetime(2023, 1, 3, 10, 30, 0),
            user_id="test_user@example.com",
            metrics={"accuracy": "0.95", "loss": "0.05"},
            parameters={"learning_rate": "0.001", "batch_size": "32"},
            tags={"mlflow.user": "test_user", "mlflow.source.type": "NOTEBOOK"},
        )

        # Create test model version with run details and signature
        test_model_version = ModelVersion(
            id="test_catalog.test_schema.test_model_1",
            name="test_model_1",
            model=test_model,
            version="1",
            aliases=["prod"],
            description="Version 1",
            created_at=datetime(2023, 1, 3),
            updated_at=datetime(2023, 1, 4),
            created_by="test_user",
            run_details=run_details,
            signature=signature,
        )

        # Process the model version
        model_urn = source.gen_ml_model_urn(test_model.id)
        ml_model_version_workunits = list(
            source.process_ml_model_version(model_urn, test_model_version, schema)
        )

        # Should generate workunits
        assert len(ml_model_version_workunits) > 0

        # Verify the report was updated
        assert len(source.report.ml_model_versions.processed_entities) == 1

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_process_ml_model_version_with_none_run_details(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that process_ml_model_version handles None run_details gracefully."""
        from datetime import datetime

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Model,
            ModelVersion,
            Schema,
        )

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Create test schema
        metastore = Metastore(
            id="metastore",
            name="metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="test_catalog",
            name="test_catalog",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="test_catalog.test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        # Create test model
        test_model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description="Test description",
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
        )

        # Create test model version WITHOUT run details
        test_model_version = ModelVersion(
            id="test_catalog.test_schema.test_model_1",
            name="test_model_1",
            model=test_model,
            version="1",
            aliases=["prod"],
            description="Version 1",
            created_at=datetime(2023, 1, 3),
            updated_at=datetime(2023, 1, 4),
            created_by="test_user",
            run_details=None,
            signature=None,
        )

        # Process the model version - should not fail
        model_urn = source.gen_ml_model_urn(test_model.id)
        ml_model_version_workunits = list(
            source.process_ml_model_version(model_urn, test_model_version, schema)
        )

        # Should still generate workunits
        assert len(ml_model_version_workunits) > 0

        # Verify the report was updated
        assert len(source.report.ml_model_versions.processed_entities) == 1

    @patch("datahub.ingestion.source.unity.source.UnityCatalogApiProxy")
    @patch("datahub.ingestion.source.unity.source.HiveMetastoreProxy")
    def test_process_ml_model_version_with_partial_run_details(
        self, mock_hive_proxy, mock_unity_proxy
    ):
        """Test that process_ml_model_version handles partial run_details (no metrics/params)."""
        from datetime import datetime

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Model,
            ModelRunDetails,
            ModelVersion,
            Schema,
        )

        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
            }
        )

        ctx = PipelineContext(run_id="test_run")
        source = UnityCatalogSource.create(config, ctx)

        # Create test schema
        metastore = Metastore(
            id="metastore",
            name="metastore",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="test_catalog",
            name="test_catalog",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="test_catalog.test_schema",
            name="test_schema",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        # Create test model
        test_model = Model(
            id="test_catalog.test_schema.test_model",
            name="test_model",
            description="Test description",
            schema_name="test_schema",
            catalog_name="test_catalog",
            created_at=datetime(2023, 1, 1),
            updated_at=datetime(2023, 1, 2),
        )

        # Create run details WITHOUT metrics and parameters
        run_details = ModelRunDetails(
            run_id="test_run_123",
            experiment_id="exp_456",
            status="FINISHED",
            start_time=datetime(2023, 1, 3, 10, 0, 0),
            end_time=datetime(2023, 1, 3, 10, 30, 0),
            user_id="test_user@example.com",
            metrics=None,
            parameters=None,
            tags={"mlflow.user": "test_user"},
        )

        # Create test model version with partial run details
        test_model_version = ModelVersion(
            id="test_catalog.test_schema.test_model_1",
            name="test_model_1",
            model=test_model,
            version="1",
            aliases=["prod"],
            description="Version 1",
            created_at=datetime(2023, 1, 3),
            updated_at=datetime(2023, 1, 4),
            created_by="test_user",
            run_details=run_details,
            signature=None,
        )

        # Process the model version - should not fail
        model_urn = source.gen_ml_model_urn(test_model.id)
        ml_model_version_workunits = list(
            source.process_ml_model_version(model_urn, test_model_version, schema)
        )

        # Should still generate workunits
        assert len(ml_model_version_workunits) > 0

        # Verify the report was updated
        assert len(source.report.ml_model_versions.processed_entities) == 1
