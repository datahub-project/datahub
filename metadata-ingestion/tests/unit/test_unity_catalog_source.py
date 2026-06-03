from unittest.mock import ANY, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy_types import Column
from datahub.ingestion.source.unity.source import UnityCatalogSource


class TestUnityCatalogSource:
    @pytest.fixture(autouse=True)
    def _mock_workspace_client(self):
        """Prevent real WorkspaceClient connections in all tests.

        On CI, the Databricks SDK attempts network calls to the fake
        workspace URL which time out after ~5 minutes per test.
        """
        with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
            yield

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
            source.report.ml_models.processed_entities[0]
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
            source.report.ml_model_versions.processed_entities[0]
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


class TestUnityCatalogMetricViews:
    @pytest.fixture(autouse=True)
    def _mock_workspace_client(self):
        with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
            yield

    @staticmethod
    def _build_source(
        include_metric_views: bool, **extra: object
    ) -> UnityCatalogSource:
        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "test_token",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test_warehouse",
                "include_hive_metastore": False,
                "include_metric_views": include_metric_views,
                **extra,
            }
        )
        ctx = PipelineContext(run_id="test_run")
        return UnityCatalogSource.create(config, ctx)

    @staticmethod
    def _build_metric_view_table(
        view_definition: str = "version: 1.1\nsource: c.s.orders\n",
    ) -> tuple:
        from databricks.sdk.service.catalog import TableType

        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
            Table,
        )

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
            id="c",
            name="c",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="c.s",
            name="s",
            catalog=catalog,
            comment=None,
            owner=None,
        )
        if not hasattr(TableType, "METRIC_VIEW"):
            pytest.skip("Installed databricks-sdk lacks TableType.METRIC_VIEW")
        table = Table(
            id="c.s.revenue",
            name="revenue",
            comment=None,
            schema=schema,
            columns=[],
            storage_location=None,
            data_source_format=None,
            table_type=TableType.METRIC_VIEW,
            owner=None,
            generation=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
            table_id=None,
            view_definition=view_definition,
            properties={},
        )
        return table, schema

    def test_metric_view_flag_default_is_false(self):
        config = UnityCatalogSourceConfig.model_validate(
            {
                "token": "t",
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "w",
                "include_hive_metastore": False,
            }
        )
        assert config.include_metric_views is False

    def test_subtype_flag_off_emits_table(self):
        from datahub.metadata.schema_classes import SubTypesClass

        source = self._build_source(include_metric_views=False)
        table, _ = self._build_metric_view_table()
        aspect: SubTypesClass = source._create_table_sub_type_aspect(table)
        assert aspect.typeNames == ["Table"]

    def test_subtype_flag_on_emits_metric_view(self):
        from datahub.metadata.schema_classes import SubTypesClass

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table()
        aspect: SubTypesClass = source._create_table_sub_type_aspect(table)
        assert aspect.typeNames == ["Metric View"]

    def test_view_property_language_flag_off_is_sql(self):
        source = self._build_source(include_metric_views=False)
        table, _ = self._build_metric_view_table()
        view_props = source._create_view_property_aspect(table)
        assert view_props.viewLanguage == "SQL"

    def test_view_property_language_flag_on_is_yaml(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table()
        view_props = source._create_view_property_aspect(table)
        assert view_props.viewLanguage == "YAML"
        assert view_props.materialized is False

    def test_yaml_lineage_extracts_source_table(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: month\n"
                "    expr: o_orderdate\n"
            )
        )
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is not None
        assert len(lineage.upstreams) == 1
        assert "cat.sch.orders" in lineage.upstreams[0].dataset

    def test_yaml_lineage_extracts_joins(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "joins:\n"
                "  - name: customer\n"
                "    source: cat.sch.customers\n"
            )
        )
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is not None
        assert len(lineage.upstreams) == 2
        upstream_names = [u.dataset for u in lineage.upstreams]
        assert any("cat.sch.orders" in u for u in upstream_names)
        assert any("cat.sch.customers" in u for u in upstream_names)

    def test_yaml_lineage_skips_sql_subquery_source(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: |\n"
                "  SELECT * FROM cat.sch.orders WHERE o_status = 'F'\n"
            )
        )
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is None

    def test_yaml_lineage_malformed_increments_counter(self):
        """Bad YAML must not raise, only bump the counter."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition='version: 1.1\nsource: "unterminated\n'
        )
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is None
        assert source.report.num_metric_views_yaml_parse_failures == 1

    def test_is_metric_view_false_when_sdk_lacks_enum(self):
        """Older databricks-sdk without METRIC_VIEW must short-circuit safely."""
        from datahub.ingestion.source.unity import proxy_types
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
            Table,
        )

        metastore = Metastore(
            id="m",
            name="m",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="c",
            name="c",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="c.s",
            name="s",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        with patch.object(proxy_types, "_TABLE_TYPE_METRIC_VIEW", None):
            table = Table(
                id="c.s.x",
                name="x",
                comment=None,
                schema=schema,
                columns=[],
                storage_location=None,
                data_source_format=None,
                table_type=None,
                owner=None,
                generation=None,
                created_at=None,
                created_by=None,
                updated_at=None,
                updated_by=None,
                table_id=None,
                view_definition=None,
                properties={},
            )
        assert table.is_metric_view is False

    def test_is_metric_view_false_on_old_sdk_with_real_table_type(self):
        """Realistic old-SDK case: shim is None, but the table still has a real type."""
        from databricks.sdk.service.catalog import TableType

        from datahub.ingestion.source.unity import proxy_types
        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
            Table,
        )

        metastore = Metastore(
            id="m",
            name="m",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="c",
            name="c",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="c.s",
            name="s",
            catalog=catalog,
            comment=None,
            owner=None,
        )

        with patch.object(proxy_types, "_TABLE_TYPE_METRIC_VIEW", None):
            view_table = Table(
                id="c.s.v",
                name="v",
                comment=None,
                schema=schema,
                columns=[],
                storage_location=None,
                data_source_format=None,
                table_type=TableType.VIEW,
                owner=None,
                generation=None,
                created_at=None,
                created_by=None,
                updated_at=None,
                updated_by=None,
                table_id=None,
                view_definition="SELECT 1",
                properties={},
            )
        assert view_table.is_metric_view is False
        assert view_table.is_view is True

    def test_old_sdk_with_flag_on_emits_warning(self):
        from datahub.ingestion.source.unity import proxy_types

        with patch.object(proxy_types, "_TABLE_TYPE_METRIC_VIEW", None):
            source = self._build_source(include_metric_views=True)
        assert any("databricks-sdk" in w.message for w in source.report.warnings)

    @pytest.mark.parametrize(
        "raw,expected_none",
        [
            ("tab_only", True),  # 1-part — still rejected
            ("a.b.c.d", True),  # 4-part
            ("cat.sch.tab(", True),  # contains paren
            ("cat sch tab", True),  # bare whitespace (no backticks)
            ("cat\nsch\ntab", True),  # contains newline
            ("", True),  # empty
            ("`cat`.`sch`.`tab`", False),  # backtick-quoted — now resolved
            ('"cat"."sch"."tab"', False),  # double-quoted parts ok
            ("cat.sch.tab", False),  # plain qualified name ok
        ],
    )
    def test_parse_metric_view_source_branches(
        self, raw: str, expected_none: bool
    ) -> None:
        from datahub.ingestion.source.unity.source import _parse_metric_view_source

        table, _ = self._build_metric_view_table()
        ref = _parse_metric_view_source(
            raw,
            default_catalog=table.ref.catalog,
            default_metastore=table.ref.metastore,
        )
        if expected_none:
            assert ref is None
        else:
            assert ref is not None
            assert ref.catalog == "cat"
            assert ref.schema == "sch"
            assert ref.table == "tab"

    def test_parse_metric_view_source_two_part_uses_table_catalog(self) -> None:
        """2-part name resolves with the metric view's own catalog as default."""
        from datahub.ingestion.source.unity.source import _parse_metric_view_source

        table, _ = self._build_metric_view_table()  # catalog "c"
        ref = _parse_metric_view_source(
            "sch.tab",
            default_catalog=table.ref.catalog,
            default_metastore=table.ref.metastore,
        )
        assert ref is not None
        assert ref.catalog == "c"
        assert ref.schema == "sch"
        assert ref.table == "tab"

    def test_parse_metric_view_source_backtick_quotes_dots(self) -> None:
        """`db.with.dots`.s.t round-trips with the inner dotted name preserved."""
        from datahub.ingestion.source.unity.source import _parse_metric_view_source

        table, _ = self._build_metric_view_table()
        ref = _parse_metric_view_source(
            "`db.with.dots`.schema.table",
            default_catalog=table.ref.catalog,
            default_metastore=table.ref.metastore,
        )
        assert ref is not None
        assert ref.catalog == "db.with.dots"
        assert ref.schema == "schema"
        assert ref.table == "table"

    def test_yaml_lineage_falls_back_when_yaml_yields_no_upstreams(self):
        """A SQL-subquery `source:` produces no YAML upstreams, so REST is consulted."""
        source = self._build_source(include_metric_views=True)
        table, schema = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: |\n  SELECT * FROM cat.sch.orders\n"
            )
        )
        with patch.object(source, "ingest_lineage", return_value=None) as rest_path:
            list(source.process_table(table, schema))
        rest_path.assert_called_once_with(table)

    def test_flag_off_uses_ingest_lineage_not_yaml(self):
        source = self._build_source(include_metric_views=False)
        table, schema = self._build_metric_view_table()
        with (
            patch.object(source, "_extract_metric_view_lineage") as yaml_path,
            patch.object(source, "ingest_lineage", return_value=None) as rest_path,
        ):
            list(source.process_table(table, schema))
        yaml_path.assert_not_called()
        rest_path.assert_called_once_with(table)

    def test_yaml_lineage_no_parseable_sources_increments_counter(self):
        """Source entries that cannot resolve to a TableReference are surfaced via a counter."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                # 1-part name with no parseable form — neither 2-part nor 3-part.
                "version: 1.1\nsource: orders_only\n"
            )
        )
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is None
        assert source.report.num_metric_views_no_parseable_sources == 1

    def test_yaml_lineage_empty_body_reports_shape_invalid(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(view_definition="")
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is None
        assert source.report.num_metric_views_yaml_parse_failures == 0
        assert source.report.num_metric_views_yaml_shape_invalid == 1
        assert any("no YAML body" in w.message for w in source.report.warnings)

    @pytest.mark.parametrize(
        "yaml_body",
        [
            "just a string",  # scalar root
            "version: 1.1\nsource: 42\n",  # non-string source
            "version: 1.1\nsource: [a, b]\n",  # list source
            "version: 1.1\nsource: cat.sch.x\njoins: null\n",  # null joins
            "version: 1.1\nsource: cat.sch.x\njoins: bogus\n",  # non-list joins
            "version: 1.1\nsource: cat.sch.x\njoins:\n  - just_a_string\n",  # join scalar
        ],
    )
    def test_yaml_lineage_handles_unexpected_shapes_without_error(
        self, yaml_body: str
    ) -> None:
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(view_definition=yaml_body)
        # Must not raise; empty `source: cat.sch.x` cases still emit one upstream
        source._extract_metric_view_lineage(table)
        # And no parse failure counter bump for these (they parsed cleanly)
        assert source.report.num_metric_views_yaml_parse_failures == 0

    def test_metric_view_skips_sql_parsing_aggregator(self):
        from unittest.mock import MagicMock

        source = self._build_source(include_metric_views=True)
        # Force the Hive-metastore-only branch to fire so we can observe whether
        # the SQL aggregator gets called for a metric view.
        from datahub.ingestion.source.unity.proxy_types import CustomCatalogType

        source.sql_parser_schema_resolver = MagicMock()
        source.sql_parsing_aggregator = MagicMock()
        table, schema = self._build_metric_view_table()
        # Force the schema's catalog to look like the Hive-metastore catalog
        # so the SQL aggregator branch is reachable.
        schema.catalog.type = CustomCatalogType.HIVE_METASTORE_CATALOG
        list(source.process_table(table, schema))
        source.sql_parsing_aggregator.add_view_definition.assert_not_called()

    def test_ordinary_view_still_emits_sql_view_language(self):
        from databricks.sdk.service.catalog import TableType

        from datahub.ingestion.source.unity.proxy_types import (
            Catalog,
            Metastore,
            Schema,
            Table,
        )

        source = self._build_source(include_metric_views=True)
        metastore = Metastore(
            id="m",
            name="m",
            comment=None,
            global_metastore_id=None,
            metastore_id=None,
            owner=None,
            region=None,
            cloud=None,
        )
        catalog = Catalog(
            id="c",
            name="c",
            metastore=metastore,
            comment=None,
            owner=None,
            type=None,
        )
        schema = Schema(
            id="c.s",
            name="s",
            catalog=catalog,
            comment=None,
            owner=None,
        )
        ordinary_view = Table(
            id="c.s.v",
            name="v",
            comment=None,
            schema=schema,
            columns=[],
            storage_location=None,
            data_source_format=None,
            table_type=TableType.VIEW,
            owner=None,
            generation=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
            table_id=None,
            view_definition="SELECT 1",
            properties={},
        )
        view_props = source._create_view_property_aspect(ordinary_view)
        assert view_props.viewLanguage == "SQL"

    def test_metric_view_pattern_deny_drops_view(self):
        from databricks.sdk.service.catalog import TableType

        if not hasattr(TableType, "METRIC_VIEW"):
            pytest.skip("Installed databricks-sdk lacks TableType.METRIC_VIEW")

        source = self._build_source(
            include_metric_views=True,
            metric_view_pattern={"deny": [".*revenue.*"]},
        )
        table, schema = self._build_metric_view_table()

        def _stub_tables(schema, _table=table):
            return iter([_table])

        source.unity_catalog_api_proxy.tables = _stub_tables  # type: ignore[assignment]
        workunits = list(source.process_tables(schema))
        assert workunits == []
        assert table.id in list(source.report.metric_views.dropped_entities)
        assert table.id in list(source.report.tables.dropped_entities)

    def test_metric_view_in_table_refs_regardless_of_flag(self):
        """Stateful soft-delete relies on metric views appearing in table_refs."""
        from databricks.sdk.service.catalog import TableType

        if not hasattr(TableType, "METRIC_VIEW"):
            pytest.skip("Installed databricks-sdk lacks TableType.METRIC_VIEW")

        for flag in (False, True):
            source = self._build_source(include_metric_views=flag)
            table, schema = self._build_metric_view_table()

            def _stub_tables(schema, _table=table):
                return iter([_table])

            source.unity_catalog_api_proxy.tables = _stub_tables  # type: ignore[assignment]
            list(source.process_tables(schema))
            assert table.ref in source.table_refs, (
                f"Metric view dropped from table_refs with include_metric_views={flag}"
            )

    @staticmethod
    def _column(name: str, type_text: str = "bigint") -> Column:
        from datahub.ingestion.source.unity.proxy_types import ColumnTypeName

        return Column(
            id=name,
            name=name,
            comment=None,
            type_text=type_text,
            type_name=ColumnTypeName.LONG,
            type_precision=0,
            type_scale=0,
            position=0,
            nullable=True,
        )

    def _build_metric_view_with_columns(
        self, view_definition: str, column_names: list
    ) -> tuple:
        table, schema = self._build_metric_view_table(view_definition=view_definition)
        table.columns = [self._column(c) for c in column_names]
        return table, schema

    def test_materialization_materialized_emits_true(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 0.1\nsource: cat.sch.orders\nmaterialization: materialized\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        view_props = source._create_view_property_aspect(table, spec)
        assert view_props.materialized is True

    def test_materialization_absent_keeps_false(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition="version: 0.1\nsource: cat.sch.orders\n"
        )
        spec = source._load_metric_view_spec(table)
        view_props = source._create_view_property_aspect(table, spec)
        assert view_props.materialized is False

    def test_filter_yaml_emitted_as_custom_property(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "filter: \"o_orderstatus = 'F'\"\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert props.customProperties.get("metric_view.filter") == "o_orderstatus = 'F'"

    def test_filter_omitted_when_spec_unavailable(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table()
        props = source._create_table_property_aspect(table, metric_view_spec=None)
        assert "metric_view.filter" not in props.customProperties

    def test_cll_bare_column_maps_to_source(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 1
        fgl = lineage.fineGrainedLineages[0]
        assert fgl.upstreams is not None and fgl.downstreams is not None
        assert any("o_orderdate" in u for u in fgl.upstreams)
        assert any("order_date" in d for d in fgl.downstreams)

    def test_cll_aggregate_measure_maps_to_source(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "measures:\n"
                "  - name: total_revenue\n"
                "    expr: SUM(o_totalprice)\n"
            ),
            column_names=["total_revenue"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None
        assert lineage.fineGrainedLineages
        fgl = lineage.fineGrainedLineages[0]
        assert fgl.upstreams is not None
        assert any("o_totalprice" in u for u in fgl.upstreams)

    def test_cll_join_alias_routes_to_joined_table(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "joins:\n"
                "  - name: customers\n"
                "    source: cat.sch.customers\n"
                "    on: orders.customer_id = customers.id\n"
                "dimensions:\n"
                "  - name: country\n"
                "    expr: customers.country\n"
            ),
            column_names=["country"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None and lineage.fineGrainedLineages
        upstreams = lineage.fineGrainedLineages[0].upstreams or []
        assert any("customers,PROD" in u and "country" in u for u in upstreams)

    def test_cll_multi_column_arithmetic(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "measures:\n"
                "  - name: discounted_total\n"
                "    expr: SUM(o_totalprice * o_discount)\n"
            ),
            column_names=["discounted_total"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None and lineage.fineGrainedLineages
        upstreams = lineage.fineGrainedLineages[0].upstreams or []
        assert any("o_totalprice" in u for u in upstreams)
        assert any("o_discount" in u for u in upstreams)

    def test_cll_literal_expr_produces_no_cll(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "measures:\n"
                "  - name: ones\n"
                "    expr: 1\n"
            ),
            column_names=["ones"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        # Coarse lineage still emitted, but no fineGrainedLineages.
        assert lineage is not None
        assert lineage.fineGrainedLineages is None
        assert source.report.num_metric_views_expr_parse_failures == 0

    def test_cll_sqlglot_parse_failure_bumps_counter(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "measures:\n"
                "  - name: bad\n"
                '    expr: "(((((SELECT"\n'
            ),
            column_names=["bad"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        # Coarse lineage still produced; CLL list empty because the only expr failed.
        assert lineage is not None
        assert lineage.fineGrainedLineages is None
        assert source.report.num_metric_views_expr_parse_failures >= 1

    def test_cll_disabled_when_include_column_lineage_false(self):
        source = self._build_source(
            include_metric_views=True, include_column_lineage=False
        )
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None
        assert lineage.fineGrainedLineages is None

    def test_dimension_and_measure_tags_attached_to_columns(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
                "measures:\n"
                "  - name: total_revenue\n"
                "    expr: SUM(o_totalprice)\n"
            ),
            column_names=["order_date", "total_revenue", "extra_col"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        by_name = {f.fieldPath: f for f in schema_metadata.fields}
        dim_field_tags = by_name["order_date"].globalTags
        measure_field_tags = by_name["total_revenue"].globalTags
        assert dim_field_tags is not None and measure_field_tags is not None
        dim_tags = [t.tag for t in dim_field_tags.tags]
        measure_tags = [t.tag for t in measure_field_tags.tags]
        assert "urn:li:tag:Dimension" in dim_tags
        assert "urn:li:tag:Measure" in measure_tags
        assert by_name["extra_col"].globalTags is None

    def test_dimension_tags_skipped_when_flag_off(self):
        source = self._build_source(include_metric_views=False)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
            ),
            column_names=["order_date"],
        )
        schema_metadata, _ = source._create_schema_metadata_aspect(
            table, metric_view_spec=None
        )
        assert schema_metadata.fields[0].globalTags is None

    def test_struct_column_dimension_tag_on_root_only(self):
        """Complex (struct/array) dim columns: tag the root field, nothing on children."""
        source = self._build_source(include_metric_views=True)
        view_yaml = (
            "version: 0.1\n"
            "source: cat.sch.orders\n"
            "dimensions:\n"
            "  - name: address\n"
            "    expr: o_address\n"
        )
        table, _ = self._build_metric_view_table(view_definition=view_yaml)
        table.columns = [
            self._column("address", type_text="struct<street:string,zip:string>"),
        ]
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        assert schema_metadata.fields, (
            "struct column should expand to at least one field"
        )
        root = schema_metadata.fields[0]
        assert root.globalTags is not None
        assert any(t.tag == "urn:li:tag:Dimension" for t in root.globalTags.tags)
        for child in schema_metadata.fields[1:]:
            assert child.globalTags is None

    def test_process_table_malformed_yaml_bumps_counter_once(self):
        source = self._build_source(include_metric_views=True)
        table, schema = self._build_metric_view_table(
            view_definition='version: 1.1\nsource: "unterminated\n'
        )
        list(source.process_table(table, schema))
        assert source.report.num_metric_views_yaml_parse_failures == 1

    def test_process_table_yaml_fail_skips_metric_view_props(self):
        """Parse failure must not emit a metric_view.filter prop or set materialized=True."""
        source = self._build_source(include_metric_views=True)
        table, schema = self._build_metric_view_table(
            view_definition='version: 1.1\nsource: "unterminated\n'
        )
        workunits = list(source.process_table(table, schema))
        for wu in workunits:
            mcp = getattr(wu.metadata, "aspect", None)
            if mcp is None:
                continue
            if hasattr(mcp, "materialized"):
                assert mcp.materialized is False
            if hasattr(mcp, "customProperties"):
                assert "metric_view.filter" not in (mcp.customProperties or {})

    @pytest.mark.parametrize(
        "yaml_value,expected_materialized",
        [
            ("materialized", True),
            ("MATERIALIZED", True),
            ("Materialized", True),
            ("not_materialized", False),
            ("", False),
            (None, False),
        ],
    )
    def test_materialization_case_and_non_string(
        self, yaml_value, expected_materialized
    ):
        source = self._build_source(include_metric_views=True)
        body = "version: 0.1\nsource: cat.sch.orders\n"
        if yaml_value is not None:
            body += f'materialization: "{yaml_value}"\n'
        table, _ = self._build_metric_view_table(view_definition=body)
        spec = source._load_metric_view_spec(table)
        view_props = source._create_view_property_aspect(table, spec)
        assert view_props.materialized is expected_materialized

    def test_filter_non_string_yaml_silently_skipped(self):
        """A list-typed `filter` must not crash and must not become a custom prop."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "filter:\n"
                "  - condition_a\n"
                "  - condition_b\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert "metric_view.filter" not in props.customProperties

    def test_struct_column_with_legacy_tags_unchanged_when_flag_off(self):
        from datahub.metadata.urns import TagUrn

        column = self._column("address", type_text="struct<street:string,zip:string>")
        fields = UnityCatalogSource._create_schema_field(column, tags=[TagUrn("pii")])
        assert fields, "struct column should expand to at least one field"
        for f in fields:
            assert f.globalTags is None

    def test_struct_column_with_extra_tag_only_attaches_extra_tag(self):
        """Metric-view path: dim/measure tag lands on the root; legacy column tags stay dropped."""
        from datahub.metadata.urns import TagUrn

        column = self._column("address", type_text="struct<street:string,zip:string>")
        fields = UnityCatalogSource._create_schema_field(
            column, tags=[TagUrn("pii")], extra_tags=[TagUrn("Dimension")]
        )
        root = fields[0]
        assert root.globalTags is not None
        assert {t.tag for t in root.globalTags.tags} == {"urn:li:tag:Dimension"}
        for child in fields[1:]:
            assert child.globalTags is None

    def test_load_metric_view_spec_non_dict_warns_and_counts(self):
        """YAML that parses to a non-mapping must warn and bump the shape-invalid counter."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(view_definition="just a string")
        spec = source._load_metric_view_spec(table)
        assert spec is None
        assert source.report.num_metric_views_yaml_shape_invalid == 1
        messages = {w.message for w in source.report.warnings}
        assert "Metric view YAML is not a mapping" in messages

    @pytest.mark.parametrize(
        "yaml_body,expected_bump",
        [
            ("version: 1.1\nsource: 42\n", 1),
            ("version: 1.1\nsource: [a, b]\n", 1),
            ("version: 1.1\nsource: cat.sch.x\njoins: bogus\n", 1),
            ("version: 1.1\nsource: cat.sch.x\njoins:\n  - just_a_string\n", 1),
            # Null joins is treated as absent — not a shape error.
            ("version: 1.1\nsource: cat.sch.x\njoins: null\n", 0),
        ],
    )
    def test_yaml_lineage_shape_invalid_counter_bumps(
        self, yaml_body: str, expected_bump: int
    ) -> None:
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(view_definition=yaml_body)
        source._extract_metric_view_lineage(table)
        assert source.report.num_metric_views_yaml_shape_invalid == expected_bump

    def test_metric_view_pattern_allow_path(self):
        """Default (or explicit) allow keeps the view in the workunit stream."""
        from databricks.sdk.service.catalog import TableType

        if not hasattr(TableType, "METRIC_VIEW"):
            pytest.skip("Installed databricks-sdk lacks TableType.METRIC_VIEW")

        source = self._build_source(
            include_metric_views=True,
            metric_view_pattern={"allow": [".*revenue.*"]},
        )
        table, schema = self._build_metric_view_table()  # name is "revenue"

        def _stub_tables(schema, _table=table):
            return iter([_table])

        source.unity_catalog_api_proxy.tables = _stub_tables  # type: ignore[assignment]
        workunits = list(source.process_tables(schema))
        assert workunits, "allow-listed metric view must produce workunits"
        assert table.id not in list(source.report.metric_views.dropped_entities)

    def test_metric_view_pattern_ignored_when_flag_off(self):
        """metric_view_pattern only filters when include_metric_views is on."""
        from databricks.sdk.service.catalog import TableType

        if not hasattr(TableType, "METRIC_VIEW"):
            pytest.skip("Installed databricks-sdk lacks TableType.METRIC_VIEW")

        source = self._build_source(
            include_metric_views=False,
            metric_view_pattern={"deny": [".*revenue.*"]},
        )
        table, schema = self._build_metric_view_table()

        def _stub_tables(schema, _table=table):
            return iter([_table])

        source.unity_catalog_api_proxy.tables = _stub_tables  # type: ignore[assignment]
        workunits = list(source.process_tables(schema))
        assert workunits, "with flag off the view is processed as a plain Table"
        assert table.id not in list(source.report.metric_views.dropped_entities)

    def test_cll_alias_match_is_case_insensitive(self):
        """`name: Customers` must resolve `customers.x` AND `Customers.x` in exprs."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "joins:\n"
                "  - name: Customers\n"
                "    source: cat.sch.customers\n"
                "dimensions:\n"
                "  - name: country\n"
                "    expr: Customers.country\n"
            ),
            column_names=["country"],
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None and lineage.fineGrainedLineages
        upstreams = lineage.fineGrainedLineages[0].upstreams or []
        assert any("customers,PROD" in u and "country" in u for u in upstreams)
        assert source.report.num_metric_view_unresolved_qualifiers == 0

    def test_unparseable_source_surfaces_unqualified_columns_as_unresolved(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: orders_only\n"
                "joins:\n"
                "  - name: customers\n"
                "    source: cat.sch.customers\n"
                "    on: customers.id = customer_id\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        source._extract_metric_view_column_lineage(table, spec)
        assert source.report.num_metric_view_unresolved_qualifiers == 1

    def test_joins_skipped_warns_with_qualified_name(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "joins:\n"
                "  - just_a_string\n"
                "  - name: customers\n"
                "    source: badname\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: orders.o_orderdate\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        source._extract_metric_view_column_lineage(table, spec)
        assert source.report.num_metric_view_joins_skipped == 2
        warnings_for_view = [
            w
            for w in source.report.warnings
            if w.message == "Metric view joins skipped"
            and any(table.ref.qualified_table_name in ctx for ctx in (w.context or []))
        ]
        assert len(warnings_for_view) == 1

    def test_unresolved_qualifier_warns_once_per_view(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: country\n"
                "    expr: typo.country\n"
                "  - name: region\n"
                "    expr: typo.region\n"
            ),
            column_names=["country", "region"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        source._extract_metric_view_column_lineage(table, spec)
        assert source.report.num_metric_view_unresolved_qualifiers == 1
        messages = [w.message for w in source.report.warnings]
        assert (
            messages.count("Metric view expression references unknown qualifier") == 1
        )

    def test_skipped_dim_measure_entries_increment_counter_and_warn(self):
        """Malformed dimension/measure entries must not silently disappear."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - just_a_string\n"  # non-dict entry
                "  - name: missing_expr\n"  # missing `expr`
                "  - name: bad_expr_type\n"
                "    expr: 42\n"  # non-string expr
                "  - name: order_date\n"
                "    expr: o_orderdate\n"  # one valid row to confirm processing continues
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        source._extract_metric_view_column_lineage(table, spec)
        assert source.report.num_metric_view_skipped_dim_measure_entries == 3
        warnings_for_view = [
            w
            for w in source.report.warnings
            if w.message == "Metric view dimension/measure entries skipped"
        ]
        assert len(warnings_for_view) == 1

    def test_coarse_lineage_partial_unparseable_sources_warns(self):
        """When some sources resolve and others don't, surface the rejected ones via a counter."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "joins:\n"
                "  - name: customers\n"
                "    source: badname\n"  # 1-part name, unparseable
                "  - name: regions\n"
                "    source: cat.sch.regions\n"  # parseable
            )
        )
        lineage = source._extract_metric_view_lineage(table)
        assert lineage is not None
        assert source.report.num_metric_view_unparseable_sources == 1
        messages = [w.message for w in source.report.warnings]
        assert "Metric view source(s) skipped" in messages

    def test_expr_empty_tree_increments_counter(self):
        """sqlglot returning None (empty/comment-only expr) must not silently produce empty CLL."""
        source = self._build_source(include_metric_views=True)
        with patch(
            "datahub.ingestion.source.unity.source.sqlglot.parse_one",
            return_value=None,
        ):
            result, unresolved = source._parse_metric_view_expr(
                "-- empty", alias_map={}
            )
        assert result == []
        assert unresolved == set()
        assert source.report.num_metric_view_expr_empty_tree == 1
        # No parse-failure counter should fire — None is a "successful empty parse".
        assert source.report.num_metric_views_expr_parse_failures == 0

    def test_rest_fallback_also_empty_emits_warning(self):
        """When neither YAML nor REST yields upstreams, surface a warning so the gap is visible."""
        source = self._build_source(include_metric_views=True)
        table, schema = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: |\n  SELECT * FROM cat.sch.orders\n"
            )
        )
        with patch.object(source, "ingest_lineage", return_value=None):
            list(source.process_table(table, schema))
        messages = {w.message for w in source.report.warnings}
        assert "Metric view has no upstream lineage" in messages

    def test_dim_measure_description_override_from_yaml(self):
        """YAML `description` on a dim/measure surfaces as the schema field description."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
                "    description: Calendar date of the order.\n"
                "measures:\n"
                "  - name: total_revenue\n"
                "    expr: SUM(o_totalprice)\n"
                "    description: Sum of order totals in USD.\n"
            ),
            column_names=["order_date", "total_revenue", "extra"],
        )
        # Give one column a UC comment that should be preserved (no YAML override).
        table.columns[2].comment = "Untouched column comment from UC."
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        by_name = {f.fieldPath: f for f in schema_metadata.fields}
        assert by_name["order_date"].description == "Calendar date of the order."
        assert by_name["total_revenue"].description == "Sum of order totals in USD."
        assert by_name["extra"].description == "Untouched column comment from UC."

    def test_dim_measure_description_falls_back_to_column_comment(self):
        """When YAML has no description, the existing UC column comment is kept."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
            ),
            column_names=["order_date"],
        )
        table.columns[0].comment = "UC-side comment about order_date."
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        assert (
            schema_metadata.fields[0].description == "UC-side comment about order_date."
        )

    def test_dim_measure_description_non_string_silently_ignored(self):
        """A non-string `description:` must not crash and must not override."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
                "    description:\n"
                "      - not\n"
                "      - a\n"
                "      - string\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        assert schema_metadata.fields[0].description is None

    def test_view_sqlconfig_keys_filtered_on_metric_view(self):
        """`view.sqlConfig.spark.*` keys are dropped; other `view.sqlConfig.*` namespaces survive."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table()
        table.properties = {
            "metric_view.from.name": "cat.sch.orders",
            "view.sqlConfig.spark.sql.session.timeZone": "Etc/UTC",
            "view.sqlConfig.spark.sql.ansi.enabled": "true",
            "view.sqlConfig.hive.some.key": "preserved",
            "view.referredTempViewNames": "[]",
        }
        props = source._create_table_property_aspect(table, metric_view_spec=None)
        cp = props.customProperties
        assert "metric_view.from.name" in cp, (
            "non-sqlConfig view properties must still flow through"
        )
        assert "view.referredTempViewNames" in cp, (
            "non-sqlConfig view.* properties must pass through"
        )
        assert cp.get("view.sqlConfig.hive.some.key") == "preserved", (
            "only view.sqlConfig.spark.* is filtered; other sqlConfig sub-namespaces "
            "must survive so we don't silently drop future Databricks additions"
        )
        assert not any(k.startswith("view.sqlConfig.spark.") for k in cp), (
            "spark sqlConfig keys leaked: "
            f"{sorted(k for k in cp if k.startswith('view.sqlConfig.spark.'))}"
        )

    def test_view_sqlconfig_keys_preserved_when_flag_off(self):
        source = self._build_source(include_metric_views=False)
        table, _ = self._build_metric_view_table()
        table.properties = {
            "view.sqlConfig.spark.sql.session.timeZone": "Etc/UTC",
            "view.sqlConfig.spark.sql.ansi.enabled": "true",
        }
        props = source._create_table_property_aspect(table, metric_view_spec=None)
        cp = props.customProperties
        assert "view.sqlConfig.spark.sql.session.timeZone" in cp
        assert "view.sqlConfig.spark.sql.ansi.enabled" in cp

    # ── Bug fix B1: comment field ──────────────────────────────────────────────

    def test_comment_field_used_as_description(self):
        """v1.1 uses `comment` — must surface as the schema field description."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
                "    comment: Calendar date of the order.\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        assert schema_metadata.fields[0].description == "Calendar date of the order."

    def test_description_field_still_read_as_fallback(self):
        """Older YAML using `description` key is still picked up via the fallback."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 0.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
                "    description: Legacy description field.\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        assert schema_metadata.fields[0].description == "Legacy description field."

    def test_comment_takes_priority_over_description(self):
        """When both `comment` and `description` are present, `comment` wins."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: order_date\n"
                "    expr: o_orderdate\n"
                "    comment: v1.1 comment.\n"
                "    description: old description.\n"
            ),
            column_names=["order_date"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        assert schema_metadata.fields[0].description == "v1.1 comment."

    # ── Bug fix B2: materialization is dict in v1.1 ────────────────────────────

    def test_materialization_dict_marks_view_materialized(self):
        """v1.1 materialization is an object — must set materialized=True."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: d\n"
                "    expr: x\n"
                "materialization:\n"
                "  schedule: every 6 hours\n"
                "  mode: relaxed\n"
                "  materialized_views:\n"
                "    - name: baseline\n"
                "      type: unaggregated\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        view_props = source._create_view_property_aspect(table, spec)
        assert view_props.materialized is True

    def test_materialization_details_stored_as_custom_properties(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: d\n"
                "    expr: x\n"
                "materialization:\n"
                "  schedule: every 6 hours\n"
                "  mode: relaxed\n"
                "  materialized_views:\n"
                "    - name: daily\n"
                "      type: aggregated\n"
                "      dimensions: [d]\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        cp = props.customProperties
        assert cp.get("metric_view.materialization.schedule") == "every 6 hours"
        assert cp.get("metric_view.materialization.mode") == "relaxed"
        assert "metric_view.materialization.materialized_views" in cp

    # ── Agent metadata: display_name ──────────────────────────────────────────

    def test_display_name_stored_as_custom_property(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: region\n"
                "    expr: r\n"
                "    display_name: Sales Region\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        assert cp.get("metric_view.field.region.display_name") == "Sales Region"

    def test_display_name_truncated_at_255_chars(self):
        source = self._build_source(include_metric_views=True)
        long_name = "A" * 300
        table, _ = self._build_metric_view_table(
            view_definition=(
                f"version: 1.1\nsource: cat.sch.orders\n"
                f"dimensions:\n  - name: d\n    expr: x\n    display_name: {long_name}\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        stored = cp.get("metric_view.field.d.display_name")
        assert stored is not None and len(stored) == 255
        assert source.report.num_metric_view_display_name_truncated == 1

    # ── Agent metadata: synonyms ──────────────────────────────────────────────

    def test_synonyms_stored_as_custom_property(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "measures:\n"
                "  - name: revenue\n"
                "    expr: SUM(price)\n"
                "    synonyms: [sales, income]\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert (
            props.customProperties.get("metric_view.field.revenue.synonyms")
            == "sales,income"
        )

    def test_synonyms_capped_at_10(self):
        source = self._build_source(include_metric_views=True)
        syns = [f"syn{i}" for i in range(12)]
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                f"dimensions:\n  - name: d\n    expr: x\n    synonyms: {syns}\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_overflow == 1
        stored = props.customProperties.get("metric_view.field.d.synonyms", "")
        assert len(stored.split(",")) == 10

    def test_synonyms_entry_over_255_chars_truncated_and_counted(self):
        source = self._build_source(include_metric_views=True)
        long_a = "a" * 300
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: d\n    expr: x\n"
                f"    synonyms: ['short', '{long_a}']\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_truncated == 1
        stored = props.customProperties.get("metric_view.field.d.synonyms", "")
        parts = stored.split(",")
        assert parts == ["short", "a" * 255]

    def test_synonyms_invalid_entries_dropped_and_counted(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: d\n    expr: x\n"
                "    synonyms: ['valid', 123, '', '   ', null, ['nested']]\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_dropped_invalid == 5
        assert props.customProperties.get("metric_view.field.d.synonyms") == "valid"

    def test_synonyms_all_invalid_emits_no_property(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: d\n    expr: x\n"
                "    synonyms: [123, '', '   ']\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_dropped_invalid == 3
        assert "metric_view.field.d.synonyms" not in props.customProperties

    def test_synonyms_truncation_and_overflow_both_fire(self):
        source = self._build_source(include_metric_views=True)
        long_a = "a" * 300
        entries = ["s" + str(i) for i in range(9)] + [long_a, long_a]
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: d\n    expr: x\n"
                f"    synonyms: {entries}\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_truncated == 2
        assert source.report.num_metric_view_synonyms_overflow == 1
        stored = props.customProperties.get("metric_view.field.d.synonyms", "")
        assert len(stored.split(",")) == 10

    def test_synonyms_counters_sum_across_entries(self):
        source = self._build_source(include_metric_views=True)
        long_a = "a" * 300
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n"
                "  - name: d1\n    expr: x\n"
                f"    synonyms: [valid1, 1, '{long_a}']\n"
                "  - name: d2\n    expr: y\n"
                f"    synonyms: ['', 2, '{long_a}', '{long_a}']\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_dropped_invalid == 3
        assert source.report.num_metric_view_synonyms_truncated == 3

    def test_synonyms_exact_255_chars_not_counted_as_truncated(self):
        source = self._build_source(include_metric_views=True)
        exact = "a" * 255
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: d\n    expr: x\n"
                f"    synonyms: ['{exact}']\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_synonyms_truncated == 0

    # ── Agent metadata: format ────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "fmt_yaml,expected_keys",
        [
            (
                "type: currency\ncurrency_code: USD\n",
                {
                    "metric_view.field.rev.format.type": "currency",
                    "metric_view.field.rev.format.currency_code": "USD",
                },
            ),
            (
                "type: percentage\n",
                {"metric_view.field.rev.format.type": "percentage"},
            ),
            (
                "type: date\ndate_format: year_month_day\n",
                {
                    "metric_view.field.rev.format.type": "date",
                    "metric_view.field.rev.format.date_format": "year_month_day",
                },
            ),
        ],
    )
    def test_format_flattened_to_custom_properties(self, fmt_yaml, expected_keys):
        source = self._build_source(include_metric_views=True)
        indented = "\n".join(
            f"        {line}" for line in fmt_yaml.strip().splitlines()
        )
        view_def = (
            "version: 1.1\nsource: cat.sch.orders\n"
            "measures:\n  - name: rev\n    expr: SUM(p)\n    format:\n"
            + indented
            + "\n"
        )
        table, _ = self._build_metric_view_table(view_definition=view_def)
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        for k, v in expected_keys.items():
            assert cp.get(k) == v, f"missing {k!r}"

    def test_format_unknown_subkey_dropped_with_counter(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n  - name: m\n    expr: COUNT(1)\n"
                "    format:\n      type: number\n      unknown_key: foo\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        source._create_table_property_aspect(table, spec)
        assert source.report.num_metric_view_format_unknown_subkeys == 1

    def test_format_decimal_places_nested_dict_flattened(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n  - name: rev\n    expr: SUM(p)\n"
                "    format:\n      type: number\n"
                "      decimal_places:\n        type: max\n        places: 4\n"
                "        unsupported_subkey: x\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        assert cp.get("metric_view.field.rev.format.decimal_places.type") == "max"
        assert cp.get("metric_view.field.rev.format.decimal_places.places") == "4"
        assert source.report.num_metric_view_format_unknown_subkeys == 1

    # ── Window measures ───────────────────────────────────────────────────────

    def test_window_measure_tagged_as_window_measure(self):
        from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "dimensions:\n  - name: dt\n    expr: order_date\n"
                "measures:\n"
                "  - name: t7d\n"
                "    expr: COUNT(DISTINCT id)\n"
                "    window:\n"
                "      - order: dt\n"
                "        range: trailing 7 day\n"
                "        semiadditive: last\n"
            ),
            column_names=["dt", "t7d"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        by_name = {f.fieldPath: f for f in schema_metadata.fields}
        t7d_tags = {
            t.tag for t in (by_name["t7d"].globalTags or GlobalTags(tags=[])).tags
        }
        assert "urn:li:tag:Measure" in t7d_tags
        assert "urn:li:tag:Window Measure" in t7d_tags
        dt_tags = {
            t.tag for t in (by_name["dt"].globalTags or GlobalTags(tags=[])).tags
        }
        assert "urn:li:tag:Window Measure" not in dt_tags

    def test_window_measure_config_stored_as_custom_properties(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: dt\n    expr: d\n"
                "measures:\n"
                "  - name: t7d\n    expr: COUNT(1)\n"
                "    window:\n"
                "      - order: dt\n        range: trailing 7 day\n        semiadditive: last\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        assert cp.get("metric_view.field.t7d.window.order") == "dt"
        assert cp.get("metric_view.field.t7d.window.range") == "trailing 7 day"
        assert cp.get("metric_view.field.t7d.window.semiadditive") == "last"

    def test_empty_window_list_does_not_tag_as_window_measure(self):
        """An empty `window: []` is not a real window definition — must not attach the tag."""
        from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n  - name: m\n    expr: COUNT(1)\n    window: []\n"
            ),
            column_names=["m"],
        )
        spec = source._load_metric_view_spec(table)
        schema_metadata, _ = source._create_schema_metadata_aspect(table, spec)
        tags = {
            t.tag
            for t in (schema_metadata.fields[0].globalTags or GlobalTags(tags=[])).tags
        }
        assert "urn:li:tag:Window Measure" not in tags

    def test_multi_entry_window_stored_as_json(self):
        """Multiple window entries fall back to a single JSON blob property."""
        import json as _json

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: dt\n    expr: d\n"
                "measures:\n  - name: m\n    expr: COUNT(1)\n"
                "    window:\n"
                "      - order: dt\n        range: trailing 7 day\n"
                "      - order: dt\n        range: trailing 30 day\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        assert "metric_view.field.m.window" in cp
        parsed = _json.loads(cp["metric_view.field.m.window"])
        assert isinstance(parsed, list) and len(parsed) == 2

    # ── MEASURE() intra-MV lineage ────────────────────────────────────────────

    def test_composed_measure_emits_intra_mv_lineage(self):
        from datahub.emitter.mce_builder import make_schema_field_urn

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "measures:\n"
                "  - name: total_revenue\n"
                "    expr: SUM(price)\n"
                "  - name: order_count\n"
                "    expr: COUNT(1)\n"
                "  - name: aov\n"
                "    expr: MEASURE(total_revenue) / MEASURE(order_count)\n"
            ),
            column_names=["total_revenue", "order_count", "aov"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        result = source._extract_metric_view_column_lineage(table, spec)
        downstream_urn = source.gen_dataset_urn(table.ref)
        aov_field_urn = make_schema_field_urn(downstream_urn, "aov")
        intra_edges = [
            fg
            for fg in result
            if fg.downstreams
            and aov_field_urn in fg.downstreams
            and fg.upstreams
            and all(downstream_urn in u for u in fg.upstreams)
        ]
        assert len(intra_edges) == 2

    def test_composed_measure_case_insensitive(self):
        """MEASURE() matching is case-insensitive per SQL convention."""
        from datahub.emitter.mce_builder import make_schema_field_urn

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n"
                "  - name: base\n    expr: SUM(x)\n"
                "  - name: composed\n    expr: measure(base) * 2\n"
            ),
            column_names=["base", "composed"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        result = source._extract_metric_view_column_lineage(table, spec)
        downstream_urn = source.gen_dataset_urn(table.ref)
        composed_field = make_schema_field_urn(downstream_urn, "composed")
        base_field = make_schema_field_urn(downstream_urn, "base")
        intra = [
            fg
            for fg in result
            if fg.downstreams
            and composed_field in fg.downstreams
            and fg.upstreams
            and base_field in fg.upstreams
        ]
        assert len(intra) == 1

    def test_measure_ref_argument_case_mismatch_uses_canonical_urn(self):
        """`MEASURE(Total_Revenue)` referencing measure `total_revenue` must emit the canonical URN."""
        from datahub.emitter.mce_builder import make_schema_field_urn

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n"
                "  - name: total_revenue\n    expr: SUM(p)\n"
                "  - name: composed\n    expr: MEASURE(Total_Revenue) / 2\n"
            ),
            column_names=["total_revenue", "composed"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        result = source._extract_metric_view_column_lineage(table, spec)
        downstream_urn = source.gen_dataset_urn(table.ref)
        canonical_urn = make_schema_field_urn(downstream_urn, "total_revenue")
        mismatched_urn = make_schema_field_urn(downstream_urn, "Total_Revenue")
        all_upstreams = {u for fg in result for u in (fg.upstreams or [])}
        assert canonical_urn in all_upstreams
        assert mismatched_urn not in all_upstreams

    def test_composed_measure_unresolved_ref_increments_counter(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n  - name: m\n    expr: MEASURE(nope) / 1\n"
            ),
            column_names=["m"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        source._extract_metric_view_column_lineage(table, spec)
        assert source.report.num_metric_view_unresolved_measure_refs >= 1

    def test_measure_args_do_not_emit_spurious_source_upstreams(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_with_columns(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "measures:\n"
                "  - name: total_revenue\n    expr: SUM(price)\n"
                "  - name: order_count\n    expr: COUNT(1)\n"
                "  - name: aov\n"
                "    expr: MEASURE(total_revenue) / MEASURE(order_count)\n"
            ),
            column_names=["total_revenue", "order_count", "aov"],
        )
        spec = source._load_metric_view_spec(table)
        assert spec is not None
        result = source._extract_metric_view_column_lineage(table, spec)
        for fg in result:
            for u in fg.upstreams or []:
                assert "cat.sch.orders" not in u or (
                    "total_revenue" not in u and "order_count" not in u
                ), f"spurious source-table upstream emitted: {u}"

    # ── Spec version ──────────────────────────────────────────────────────────

    def test_spec_version_stored_as_custom_property(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition="version: 1.1\nsource: cat.sch.orders\ndimensions:\n  - name: d\n    expr: x\n"
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        assert cp.get("metric_view.spec_version") == "1.1"

    # ── Joins diagnostic property ──────────────────────────────────────────────

    def test_top_level_yaml_comment_used_as_dataset_description(self):
        """Top-level `comment:` in YAML is the MV's own doc and should win over UC's table.comment."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "comment: This view aggregates daily revenue from orders.\n"
                "dimensions:\n  - name: d\n    expr: x\n"
            )
        )
        table.comment = "Older UC table comment, should be overridden."
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert props.description == "This view aggregates daily revenue from orders."

    def test_yaml_comment_absent_falls_back_to_table_comment(self):
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "dimensions:\n  - name: d\n    expr: x\n"
            )
        )
        table.comment = "UC table comment."
        spec = source._load_metric_view_spec(table)
        props = source._create_table_property_aspect(table, spec)
        assert props.description == "UC table comment."

    def test_joins_diagnostic_property_stored(self):
        import json as _json

        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\nsource: cat.sch.orders\n"
                "joins:\n  - name: c\n    source: cat.sch.customers\n    on: id=cid\n"
                "dimensions:\n  - name: d\n    expr: x\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        cp = source._create_table_property_aspect(table, spec).customProperties
        assert "metric_view.joins" in cp
        parsed = _json.loads(cp["metric_view.joins"])
        assert isinstance(parsed, list) and len(parsed) == 1
        assert parsed[0]["name"] == "c"
        # PyYAML's YAML 1.1 default coerces `on`/`off`/`yes`/`no` keys to booleans;
        # our custom loader must keep `on` as a string so the join predicate is preserved.
        assert parsed[0]["on"] == "id=cid"
        assert "true" not in parsed[0]

    # ── Nested (snowflake schema) join lineage ────────────────────────────────

    def test_nested_joins_lineage_includes_all_sources(self):
        """Snowflake schema nested joins must all appear as upstream datasets."""
        source = self._build_source(include_metric_views=True)
        table, _ = self._build_metric_view_table(
            view_definition=(
                "version: 1.1\n"
                "source: cat.sch.orders\n"
                "joins:\n"
                "  - name: customer\n"
                "    source: cat.sch.customer\n"
                "    on: o_custkey=c_custkey\n"
                "    joins:\n"
                "      - name: nation\n"
                "        source: cat.sch.nation\n"
                "        on: c_nationkey=n_nationkey\n"
                "dimensions:\n  - name: d\n    expr: x\n"
            )
        )
        spec = source._load_metric_view_spec(table)
        lineage = source._extract_metric_view_lineage(table, spec)
        assert lineage is not None
        upstream_urns = {u.dataset for u in lineage.upstreams}
        assert any("orders" in u for u in upstream_urns)
        assert any("customer" in u for u in upstream_urns)
        assert any("nation" in u for u in upstream_urns)
