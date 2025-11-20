import pytest
from pydantic import ValidationError

from datahub.ingestion.source.dataform.dataform_config import (
    DataformCloudConfig,
    DataformCoreConfig,
    DataformEntitiesEnabled,
    DataformSourceConfig,
)


class TestDataformCloudConfig:
    def test_valid_cloud_config(self):
        """Test valid cloud configuration."""
        config = DataformCloudConfig(
            project_id="test-project",
            region="us-central1",
            repository_id="test-repo",
            workspace_id="main",
            compilation_result_id="123",
        )
        assert config.project_id == "test-project"
        assert config.region == "us-central1"
        assert config.repository_id == "test-repo"
        assert config.workspace_id == "main"
        assert config.compilation_result_id == "123"

    def test_cloud_config_defaults(self):
        """Test cloud configuration with default values."""
        config = DataformCloudConfig(
            project_id="test-project",
            repository_id="test-repo",
        )
        assert config.region == "us-central1"
        assert config.workspace_id is None
        assert config.compilation_result_id is None


class TestDataformCoreConfig:
    def test_valid_core_config(self):
        """Test valid core configuration."""
        config = DataformCoreConfig(
            project_path="/path/to/dataform/project",
            target_name="production",
            compilation_results_path="/path/to/results.json",
        )
        assert config.project_path == "/path/to/dataform/project"
        assert config.target_name == "production"
        assert config.compilation_results_path == "/path/to/results.json"

    def test_core_config_defaults(self):
        """Test core configuration with default values."""
        config = DataformCoreConfig(
            project_path="/path/to/dataform/project",
        )
        assert config.compilation_results_path is None
        assert config.target_name is None


class TestDataformEntitiesEnabled:
    def test_default_entities_enabled(self):
        """Test default entity enablement."""
        config = DataformEntitiesEnabled()
        assert config.tables is True
        assert config.views is True
        assert config.assertions is True
        assert config.operations is True
        assert config.declarations is True

    def test_custom_entities_enabled(self):
        """Test custom entity enablement."""
        config = DataformEntitiesEnabled(
            tables=True,
            views=False,
            assertions=True,
            operations=False,
            declarations=True,
        )
        assert config.tables is True
        assert config.views is False
        assert config.assertions is True
        assert config.operations is False
        assert config.declarations is True


class TestDataformSourceConfig:
    def test_cloud_config_only(self):
        """Test configuration with only cloud config."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )
        assert config.is_cloud_mode() is True
        assert config.is_core_mode() is False
        assert config.target_platform == "bigquery"

    def test_core_config_only(self):
        """Test configuration with only core config."""
        config = DataformSourceConfig(
            core_config=DataformCoreConfig(
                project_path="/path/to/project",
            ),
            target_platform="postgres",
        )
        assert config.is_cloud_mode() is False
        assert config.is_core_mode() is True
        assert config.target_platform == "postgres"

    def test_missing_both_configs_raises_error(self):
        """Test that missing both configs raises validation error."""
        with pytest.raises(
            ValidationError,
            match="Either 'cloud_config' or 'core_config' must be provided",
        ):
            DataformSourceConfig(
                target_platform="bigquery",
            )

    def test_both_configs_raises_error(self):
        """Test that providing both configs raises validation error."""
        with pytest.raises(
            ValidationError,
            match="Only one of 'cloud_config' or 'core_config' can be provided",
        ):
            DataformSourceConfig(
                cloud_config=DataformCloudConfig(
                    project_id="test-project",
                    repository_id="test-repo",
                ),
                core_config=DataformCoreConfig(
                    project_path="/path/to/project",
                ),
                target_platform="bigquery",
            )

    def test_default_values(self):
        """Test default configuration values."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
        )

        # Test default values
        assert config.target_platform_instance is None
        assert config.include_column_lineage is True
        assert config.include_column_lineage is True
        assert config.parse_table_names_from_sql is False
        assert config.tag_prefix == "dataform:"
        assert config.custom_properties == {}
        assert config.git_branch == "main"
        assert config.dataform_is_primary_sibling is True

    def test_custom_values(self):
        """Test custom configuration values."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            target_platform_instance="prod",
            include_column_lineage=False,
            parse_table_names_from_sql=True,
            tag_prefix="custom:",
            custom_properties={"team": "data"},
            git_branch="develop",
            dataform_is_primary_sibling=False,
        )

        assert config.target_platform_instance == "prod"
        assert config.include_column_lineage is False
        assert config.parse_table_names_from_sql is True
        assert config.tag_prefix == "custom:"
        assert config.custom_properties == {"team": "data"}
        assert config.git_branch == "develop"
        assert config.dataform_is_primary_sibling is False

    def test_entities_enabled_configuration(self):
        """Test entities enabled configuration."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            entities_enabled=DataformEntitiesEnabled(
                tables=True,
                views=False,
                assertions=True,
                operations=False,
                declarations=True,
            ),
        )

        assert config.entities_enabled.tables is True
        assert config.entities_enabled.views is False
        assert config.entities_enabled.assertions is True
        assert config.entities_enabled.operations is False
        assert config.entities_enabled.declarations is True

    def test_filtering_patterns(self):
        """Test filtering pattern configuration."""
        from datahub.configuration.common import AllowDenyPattern

        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            table_pattern=AllowDenyPattern(allow=["analytics.*"], deny=[".*_temp"]),
            schema_pattern=AllowDenyPattern(allow=["production", "staging"]),
            tag_pattern=AllowDenyPattern(allow=["critical", "pii"]),
        )

        assert config.table_pattern.allow == ["analytics.*"]
        assert config.table_pattern.deny == [".*_temp"]
        assert config.schema_pattern.allow == ["production", "staging"]
        assert config.tag_pattern.allow == ["critical", "pii"]

    def test_git_integration(self):
        """Test Git integration configuration."""
        config = DataformSourceConfig(
            cloud_config=DataformCloudConfig(
                project_id="test-project",
                repository_id="test-repo",
            ),
            target_platform="bigquery",
            git_repository_url="https://github.com/org/dataform-project",
            git_branch="develop",
        )

        assert config.git_repository_url == "https://github.com/org/dataform-project"
        assert config.git_branch == "develop"
