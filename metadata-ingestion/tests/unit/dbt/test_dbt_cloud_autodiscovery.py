"""
Unit tests for dbt Cloud auto-discovery functionality.

Tests focus on:
- Configuration validation for auto-discovery mode
- API response parsing and error handling
- Job/environment filtering logic
- Auto-discovery orchestration
"""

from unittest import mock

import pytest
import requests
from pydantic import ValidationError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_cloud import (
    AutoDiscoveryConfig,
    DBTCloudConfig,
    DBTCloudSource,
)
from datahub.ingestion.source.dbt.dbt_cloud_models import (
    DBTCloudDeploymentType,
    DBTCloudEnvironment,
    DBTCloudJob,
)


class TestAutoDiscoveryConfig:
    """Tests for AutoDiscoveryConfig model validation."""

    def test_auto_discovery_config_defaults(self) -> None:
        """Auto-discovery should default to disabled with allow-all pattern."""
        config = AutoDiscoveryConfig()
        assert config.enabled is False
        assert config.job_id_pattern.allowed("12345")
        assert config.job_id_pattern.allowed("99999")

    def test_auto_discovery_config_with_pattern(self) -> None:
        """Job ID patterns should filter correctly."""
        config = AutoDiscoveryConfig(
            enabled=True, job_id_pattern=AllowDenyPattern(allow=["^123.*"])
        )
        assert config.enabled is True
        assert config.job_id_pattern.allowed("12345")
        assert not config.job_id_pattern.allowed("99999")


class TestDBTCloudConfigValidation:
    """Tests for DBTCloudConfig validation logic."""

    def test_explicit_mode_requires_job_id(self) -> None:
        """Explicit mode (auto_discovery disabled) must have job_id."""
        with pytest.raises(
            ValidationError, match="job_id is required in explicit mode"
        ):
            DBTCloudConfig(
                access_url="https://test.getdbt.com",
                token="dummy_token",
                account_id=123456,
                project_id=1234567,
                # Missing job_id and auto_discovery is None/disabled
                target_platform="snowflake",
            )

    def test_explicit_mode_with_job_id_succeeds(self) -> None:
        """Explicit mode should validate successfully with job_id."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,
            target_platform="snowflake",
        )
        assert config.job_id == 12345678
        assert config.auto_discovery is None

    def test_auto_discovery_mode_without_job_id_succeeds(self) -> None:
        """Auto-discovery mode should work without job_id."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(enabled=True),
            target_platform="snowflake",
        )
        assert config.job_id is None
        assert config.auto_discovery is not None
        assert config.auto_discovery.enabled is True

    def test_auto_discovery_disabled_requires_job_id(self) -> None:
        """Auto-discovery disabled still requires job_id."""
        with pytest.raises(
            ValidationError, match="job_id is required in explicit mode"
        ):
            DBTCloudConfig(
                access_url="https://test.getdbt.com",
                token="dummy_token",
                account_id=123456,
                project_id=1234567,
                auto_discovery=AutoDiscoveryConfig(enabled=False),
                target_platform="snowflake",
            )

    def test_auto_discovery_with_job_id_succeeds(self) -> None:
        """Auto-discovery enabled can coexist with job_id (job_id ignored)."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,  # Will be ignored in auto-discovery mode
            auto_discovery=AutoDiscoveryConfig(enabled=True),
            target_platform="snowflake",
        )
        assert config.auto_discovery is not None
        assert config.auto_discovery.enabled is True
        assert config.job_id == 12345678  # Present but ignored


class TestGetEnvironmentsForProject:
    """Tests for _get_environments_for_project API method."""

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_environments_success(self, mock_get: mock.Mock) -> None:
        """Should parse environments from valid API response."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "deployment_type": "production"},
                {"id": 2, "deployment_type": "staging"},
            ]
        }
        mock_get.return_value = mock_response

        environments = DBTCloudSource._get_environments_for_project(
            "https://test.getdbt.com", "token", 123, 456
        )

        assert len(environments) == 2
        assert environments[0].id == 1
        assert environments[0].deployment_type == DBTCloudDeploymentType.PRODUCTION
        assert environments[1].id == 2
        assert environments[1].deployment_type == DBTCloudDeploymentType.STAGING

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_environments_skips_null_deployment_type(
        self, mock_get: mock.Mock
    ) -> None:
        """Should skip environments with null deployment_type."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "deployment_type": "production"},
                {"id": 2, "deployment_type": None},  # Should be skipped
                {"id": 3, "deployment_type": "staging"},
            ]
        }
        mock_get.return_value = mock_response

        environments = DBTCloudSource._get_environments_for_project(
            "https://test.getdbt.com", "token", 123, 456
        )

        assert len(environments) == 2
        assert all(env.deployment_type is not None for env in environments)
        assert [env.id for env in environments] == [1, 3]

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_environments_http_error(self, mock_get: mock.Mock) -> None:
        """Should raise ValueError on HTTP error."""
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="Failed to fetch environments from dbt Cloud API"
        ):
            DBTCloudSource._get_environments_for_project(
                "https://test.getdbt.com", "token", 123, 456
            )

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_environments_invalid_json(self, mock_get: mock.Mock) -> None:
        """Should raise ValueError on invalid JSON response."""
        from json import JSONDecodeError

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "", 0)
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="Received invalid JSON response from dbt Cloud API"
        ):
            DBTCloudSource._get_environments_for_project(
                "https://test.getdbt.com", "token", 123, 456
            )

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_environments_empty_data(self, mock_get: mock.Mock) -> None:
        """Should handle empty data list gracefully."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        environments = DBTCloudSource._get_environments_for_project(
            "https://test.getdbt.com", "token", 123, 456
        )

        assert len(environments) == 0


class TestGetJobsForProject:
    """Tests for _get_jobs_for_project API method."""

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_jobs_success(self, mock_get: mock.Mock) -> None:
        """Should parse jobs from valid API response."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 100, "generate_docs": True},
                {"id": 200, "generate_docs": False},
                {"id": 300, "generate_docs": True},
            ]
        }
        mock_get.return_value = mock_response

        jobs = DBTCloudSource._get_jobs_for_project(
            "https://test.getdbt.com", "token", 123, 456, 789
        )

        assert len(jobs) == 3
        assert jobs[0].id == 100
        assert jobs[0].generate_docs is True
        assert jobs[1].id == 200
        assert jobs[1].generate_docs is False

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_jobs_http_error(self, mock_get: mock.Mock) -> None:
        """Should raise ValueError on HTTP error."""
        mock_response = mock.Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Failed to fetch jobs from dbt Cloud API"):
            DBTCloudSource._get_jobs_for_project(
                "https://test.getdbt.com", "token", 123, 456, 789
            )

    @mock.patch("datahub.ingestion.source.dbt.dbt_cloud.requests.get")
    def test_get_jobs_invalid_json(self, mock_get: mock.Mock) -> None:
        """Should raise ValueError on invalid JSON response."""
        from json import JSONDecodeError

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "", 0)
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="Received invalid JSON response from dbt Cloud API"
        ):
            DBTCloudSource._get_jobs_for_project(
                "https://test.getdbt.com", "token", 123, 456, 789
            )


class TestAutoDiscoverProjectsAndJobs:
    """Tests for _auto_discover_projects_and_jobs orchestration."""

    def _create_source_with_autodiscovery(
        self, job_id_pattern: AllowDenyPattern | None = None
    ) -> DBTCloudSource:
        """Helper to create a DBTCloudSource with auto-discovery enabled."""
        pattern = job_id_pattern or AllowDenyPattern.allow_all()
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(enabled=True, job_id_pattern=pattern),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        return DBTCloudSource(config, ctx)

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_filters_by_generate_docs(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should only return jobs with generate_docs=True."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=False),  # Should be filtered out
            DBTCloudJob(id=300, generate_docs=True),
        ]

        source = self._create_source_with_autodiscovery()
        job_ids = source._auto_discover_projects_and_jobs()

        assert len(job_ids) == 2
        assert set(job_ids) == {100, 300}

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_filters_by_job_pattern(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should filter jobs by job_id_pattern."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),  # Matches pattern
            DBTCloudJob(id=200, generate_docs=True),  # Doesn't match pattern
            DBTCloudJob(id=150, generate_docs=True),  # Matches pattern
        ]

        # Pattern: only allow IDs starting with 1
        pattern = AllowDenyPattern(allow=["^1.*"])
        source = self._create_source_with_autodiscovery(job_id_pattern=pattern)
        job_ids = source._auto_discover_projects_and_jobs()

        assert len(job_ids) == 2
        assert set(job_ids) == {100, 150}

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_production_environment_only(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should only use production environment for job discovery."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.STAGING),
            DBTCloudEnvironment(
                id=2, deployment_type=DBTCloudDeploymentType.PRODUCTION
            ),
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
        ]

        source = self._create_source_with_autodiscovery()
        source._auto_discover_projects_and_jobs()

        # Should call get_jobs with production environment ID (2)
        mock_get_jobs.assert_called_once()
        call_args = mock_get_jobs.call_args[0]
        assert call_args[4] == 2  # environment_id argument

    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_no_production_environment_raises(
        self,
        mock_get_envs: mock.Mock,
    ) -> None:
        """Should raise ValueError when no production environment exists."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.STAGING),
        ]

        source = self._create_source_with_autodiscovery()
        with pytest.raises(
            ValueError, match="Could not find production environment for project"
        ):
            source._auto_discover_projects_and_jobs()

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_no_matching_jobs(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should return empty list when no jobs match filters."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=False),
            DBTCloudJob(id=200, generate_docs=False),
        ]

        source = self._create_source_with_autodiscovery()
        job_ids = source._auto_discover_projects_and_jobs()

        assert len(job_ids) == 0

    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_environment_api_failure(
        self,
        mock_get_envs: mock.Mock,
    ) -> None:
        """Should propagate error when environment API fails."""
        mock_get_envs.side_effect = ValueError("API Error")

        source = self._create_source_with_autodiscovery()
        with pytest.raises(ValueError, match="API Error"):
            source._auto_discover_projects_and_jobs()

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_jobs_api_failure(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should propagate error when jobs API fails."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.side_effect = ValueError("Jobs API Error")

        source = self._create_source_with_autodiscovery()
        with pytest.raises(ValueError, match="Jobs API Error"):
            source._auto_discover_projects_and_jobs()


class TestIsAutoDiscoveryEnabled:
    """Tests for _is_auto_discovery_enabled helper method."""

    def test_is_auto_discovery_enabled_when_enabled(self) -> None:
        """Should return True when auto_discovery is enabled."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(enabled=True),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        assert source._is_auto_discovery_enabled() is True

    def test_is_auto_discovery_enabled_when_disabled(self) -> None:
        """Should return False when auto_discovery is explicitly disabled."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,
            auto_discovery=AutoDiscoveryConfig(enabled=False),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        assert source._is_auto_discovery_enabled() is False

    def test_is_auto_discovery_enabled_when_none(self) -> None:
        """Should return False when auto_discovery is None."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        assert source._is_auto_discovery_enabled() is False


class TestTestConnection:
    """Tests for test_connection method in auto-discovery vs explicit modes."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_connection_explicit_mode_success(self, mock_graphql: mock.Mock) -> None:
        """Test connection should use GraphQL query in explicit mode."""
        mock_graphql.return_value = {"job": {"tests": []}}

        config_dict = {
            "access_url": "https://test.getdbt.com",
            "token": "dummy_token",
            "account_id": 123456,
            "project_id": 1234567,
            "job_id": 12345678,
            "target_platform": "snowflake",
        }

        report = DBTCloudSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True
        mock_graphql.assert_called_once()

    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_connection_auto_discovery_mode_success(
        self, mock_get_envs: mock.Mock
    ) -> None:
        """Test connection should fetch environments in auto-discovery mode."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]

        config_dict = {
            "access_url": "https://test.getdbt.com",
            "token": "dummy_token",
            "account_id": 123456,
            "project_id": 1234567,
            "auto_discovery": {"enabled": True},
            "target_platform": "snowflake",
        }

        report = DBTCloudSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True
        mock_get_envs.assert_called_once()

    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_connection_auto_discovery_mode_failure(
        self, mock_get_envs: mock.Mock
    ) -> None:
        """Test connection should fail gracefully when environments fetch fails."""
        mock_get_envs.side_effect = ValueError("API Error")

        config_dict = {
            "access_url": "https://test.getdbt.com",
            "token": "dummy_token",
            "account_id": 123456,
            "project_id": 1234567,
            "auto_discovery": {"enabled": True},
            "target_platform": "snowflake",
        }

        report = DBTCloudSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "API Error" in report.basic_connectivity.failure_reason
