"""
Unit tests for dbt Cloud auto-discovery functionality.

Tests focus on:
- Configuration validation for auto-discovery mode
- API response parsing and error handling
- Job/environment filtering logic
- Auto-discovery orchestration
"""

from typing import Any, Dict, List, Optional
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
from datahub.ingestion.source.dbt.dbt_common import DBTMetric, extract_dbt_metrics


class TestAutoDiscoveryConfig:
    """Tests for AutoDiscoveryConfig model validation."""

    def test_auto_discovery_config_defaults(self) -> None:
        """Should default to disabled with allow-all pattern."""
        config = AutoDiscoveryConfig()
        assert config.enabled is False
        assert config.require_generate_docs is False
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
        self,
        job_id_pattern: Optional[AllowDenyPattern] = None,
        require_generate_docs: bool = False,
        write_semantics: str = "PATCH",
    ) -> DBTCloudSource:
        """Helper to create a DBTCloudSource with auto-discovery enabled."""
        pattern = job_id_pattern or AllowDenyPattern.allow_all()
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(
                enabled=True,
                job_id_pattern=pattern,
                require_generate_docs=require_generate_docs,
            ),
            target_platform="snowflake",
            write_semantics=write_semantics,
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        return DBTCloudSource(config, ctx)

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_default_includes_all_jobs(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """By default, should return all jobs regardless of generate_docs."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=False),
            DBTCloudJob(id=300, generate_docs=True),
        ]

        source = self._create_source_with_autodiscovery()
        job_ids = source._auto_discover_projects_and_jobs()

        assert len(job_ids) == 3
        assert set(job_ids) == {100, 200, 300}

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_filters_by_generate_docs_when_required(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """When require_generate_docs=True, should only return jobs with generate_docs=True."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=False),
            DBTCloudJob(id=300, generate_docs=True),
        ]

        source = self._create_source_with_autodiscovery(require_generate_docs=True)
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
    def test_auto_discover_no_matching_jobs_with_require_generate_docs(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should return empty list when require_generate_docs=True and no jobs have it enabled."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=False),
            DBTCloudJob(id=200, generate_docs=False),
        ]

        source = self._create_source_with_autodiscovery(require_generate_docs=True)
        job_ids = source._auto_discover_projects_and_jobs()

        assert len(job_ids) == 0

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discover_no_matching_jobs_by_pattern(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
    ) -> None:
        """Should return empty list when no jobs match the pattern filter."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=False),
        ]

        pattern = AllowDenyPattern(allow=["^999.*"])
        source = self._create_source_with_autodiscovery(job_id_pattern=pattern)
        job_ids = source._auto_discover_projects_and_jobs()

        assert len(job_ids) == 0

    @pytest.mark.parametrize(
        "jobs, pattern, expected_cause",
        [
            pytest.param(
                [], None, "No jobs found in production environment 1", id="no_jobs"
            ),
            pytest.param(
                [DBTCloudJob(id=100, generate_docs=True)],
                AllowDenyPattern(allow=["^999$"]),
                "No job in the production environment matches",
                id="pattern_rejects_all",
            ),
            pytest.param(
                [
                    DBTCloudJob(id=100, generate_docs=False),
                    DBTCloudJob(id=200, generate_docs=True),
                ],
                AllowDenyPattern(allow=["^100$"]),
                "require_generate_docs",
                id="generate_docs_rejects_all",
            ),
        ],
    )
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_no_discovered_jobs_reports_failure(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        jobs: List[DBTCloudJob],
        pattern: Optional[AllowDenyPattern],
        expected_cause: str,
    ) -> None:
        """An empty job list turns a silently empty run into a failure that
        names the actual cause."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = jobs

        source = self._create_source_with_autodiscovery(
            job_id_pattern=pattern,
            require_generate_docs=True,
            write_semantics="OVERRIDE",
        )
        workunits = list(source.get_workunits())

        assert workunits == []
        failures = list(source.report.failures)
        assert len(failures) == 1
        assert failures[0].title == "No dbt Cloud jobs discovered"
        assert expected_cause in failures[0].message

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


class TestParseIntoDBTNodeFreshness:
    """Tests for freshness extraction in _parse_into_dbt_node."""

    def _create_source(self) -> DBTCloudSource:
        """Helper to create a DBTCloudSource instance."""
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        return DBTCloudSource(config, ctx)

    def test_source_with_freshness_pass(self) -> None:
        """Should extract freshness info for source with pass status."""
        source = self._create_source()
        node = {
            "uniqueId": "source.project.schema.table",
            "name": "table",
            "resourceType": "source",
            "database": "db",
            "schema": "schema",
            "meta": {},
            "tags": [],
            "description": "",
            "runId": 123,
            "jobId": 456,
            "accountId": 789,
            "projectId": 111,
            "environmentId": 222,
            "dbtVersion": "1.5.0",
            "maxLoadedAt": "2026-01-14T10:00:00+00:00",
            "snapshottedAt": "2026-01-14T12:00:00+00:00",
            "maxLoadedAtTimeAgoInS": 7200.0,
            "state": "pass",
            "freshnessChecked": True,
            "criteria": {
                "warnAfter": {"count": 12, "period": "hour"},
                "errorAfter": {"count": 24, "period": "hour"},
            },
        }

        dbt_node = source._parse_into_dbt_node(node)

        assert dbt_node.freshness_info is not None
        assert dbt_node.freshness_info.status == "pass"
        assert dbt_node.freshness_info.max_loaded_at_time_ago_in_s == 7200.0
        assert dbt_node.freshness_info.warn_after is not None
        assert dbt_node.freshness_info.warn_after.count == 12
        assert dbt_node.freshness_info.warn_after.period == "hour"
        assert dbt_node.freshness_info.error_after is not None
        assert dbt_node.freshness_info.error_after.count == 24

    def test_source_with_freshness_warn(self) -> None:
        """Should extract freshness info for source with warn status."""
        source = self._create_source()
        node = {
            "uniqueId": "source.project.schema.table",
            "name": "table",
            "resourceType": "source",
            "database": "db",
            "schema": "schema",
            "meta": {},
            "tags": [],
            "description": "",
            "runId": 123,
            "jobId": 456,
            "accountId": 789,
            "projectId": 111,
            "environmentId": 222,
            "dbtVersion": "1.5.0",
            "maxLoadedAt": "2026-01-13T10:00:00+00:00",
            "snapshottedAt": "2026-01-14T12:00:00+00:00",
            "maxLoadedAtTimeAgoInS": 93600.0,
            "state": "warn",
            "freshnessChecked": True,
            "criteria": {
                "warnAfter": {"count": 12, "period": "hour"},
                "errorAfter": {"count": 48, "period": "hour"},
            },
        }

        dbt_node = source._parse_into_dbt_node(node)

        assert dbt_node.freshness_info is not None
        assert dbt_node.freshness_info.status == "warn"

    def test_source_without_freshness_checked(self) -> None:
        """Should not extract freshness info when freshnessChecked is False."""
        source = self._create_source()
        node = {
            "uniqueId": "source.project.schema.table",
            "name": "table",
            "resourceType": "source",
            "database": "db",
            "schema": "schema",
            "meta": {},
            "tags": [],
            "description": "",
            "runId": 123,
            "jobId": 456,
            "accountId": 789,
            "projectId": 111,
            "environmentId": 222,
            "dbtVersion": "1.5.0",
            "maxLoadedAt": None,
            "snapshottedAt": None,
            "state": None,
            "freshnessChecked": False,
            "criteria": {},
        }

        dbt_node = source._parse_into_dbt_node(node)

        assert dbt_node.freshness_info is None

    def test_source_with_only_warn_after_criteria(self) -> None:
        """Should handle source with only warnAfter criteria."""
        source = self._create_source()
        node = {
            "uniqueId": "source.project.schema.table",
            "name": "table",
            "resourceType": "source",
            "database": "db",
            "schema": "schema",
            "meta": {},
            "tags": [],
            "description": "",
            "runId": 123,
            "jobId": 456,
            "accountId": 789,
            "projectId": 111,
            "environmentId": 222,
            "dbtVersion": "1.5.0",
            "maxLoadedAt": "2026-01-14T11:30:00+00:00",
            "snapshottedAt": "2026-01-14T12:00:00+00:00",
            "maxLoadedAtTimeAgoInS": 1800.0,
            "state": "pass",
            "freshnessChecked": True,
            "criteria": {
                "warnAfter": {"count": 30, "period": "minute"},
                "errorAfter": None,
            },
        }

        dbt_node = source._parse_into_dbt_node(node)

        assert dbt_node.freshness_info is not None
        assert dbt_node.freshness_info.warn_after is not None
        assert dbt_node.freshness_info.warn_after.count == 30
        assert dbt_node.freshness_info.warn_after.period == "minute"
        assert dbt_node.freshness_info.error_after is None

    def test_model_node_has_no_freshness(self) -> None:
        """Should not extract freshness info for non-source nodes."""
        source = self._create_source()
        node = {
            "uniqueId": "model.project.test_model",
            "name": "test_model",
            "resourceType": "model",
            "database": "db",
            "schema": "schema",
            "meta": {},
            "tags": [],
            "description": "",
            "runId": 123,
            "jobId": 456,
            "accountId": 789,
            "projectId": 111,
            "environmentId": 222,
            "dbtVersion": "1.5.0",
            "materializedType": "table",
            "dependsOn": [],
            "rawCode": "SELECT 1",
            "compiledCode": "SELECT 1",
            "status": "success",
            "error": None,
            "skip": False,
        }

        dbt_node = source._parse_into_dbt_node(node)

        assert dbt_node.freshness_info is None


class TestDiscoveryMetrics:
    """Discovery metrics are shaped as manifest entries and read by the dbt Core parser."""

    @staticmethod
    def _parse(nodes: List[Dict[str, Any]]) -> List[DBTMetric]:
        source = TestDiscoveryMetrics._source()
        semantic_model_ids = {"orders": "semantic_model.proj.orders"}
        metric_ids = {
            node["name"].casefold(): source._metric_unique_id(node) for node in nodes
        }
        manifest = {
            source._metric_unique_id(node): source._manifest_metric_node(
                node, semantic_model_ids, metric_ids
            )
            for node in nodes
        }
        return extract_dbt_metrics(
            manifest_metrics={k: v for k, v in manifest.items() if v is not None},
            tag_prefix="",
        ).metrics

    @staticmethod
    def _source() -> DBTCloudSource:
        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=1,
            target_platform="snowflake",
        )
        return DBTCloudSource(
            config, PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        )

    def test_a_create_metric_echo_keeps_its_measure_and_the_real_unique_id(
        self,
    ) -> None:
        # dbt returns a create_metric measure as a same-named simple metric.
        (metric,) = self._parse(
            [
                {
                    "uniqueId": "metric.proj.order_total",
                    "name": "order_total",
                    "packageName": "proj",
                    "type": "SIMPLE",
                    "typeParams": {"measure": {"name": "order_total"}},
                    "formula": None,
                    "parents": [{"name": "orders", "resourceType": "semantic_model"}],
                }
            ]
        )

        assert metric.unique_id == "metric.proj.order_total"
        assert [m.name for m in metric.measures] == ["order_total"]
        assert metric.depends_on == ["semantic_model.proj.orders"]

    def test_a_ratio_keeps_its_numerator_and_denominator(self) -> None:
        (metric,) = self._parse(
            [
                {
                    "uniqueId": "metric.proj.ratio",
                    "name": "ratio",
                    "type": "RATIO",
                    "typeParams": {
                        "numerator": {"name": "a"},
                        "denominator": {"name": "b"},
                    },
                    "parents": [],
                }
            ]
        )

        assert metric.numerator is not None and metric.numerator.name == "a"
        assert metric.denominator is not None and metric.denominator.name == "b"

    def test_a_filter_object_and_a_missing_type_params_still_give_a_metric(
        self,
    ) -> None:
        (metric,) = self._parse(
            [
                {
                    "name": "paying",
                    "type": "SIMPLE",
                    "typeParams": None,
                    "formula": "hubspot_company_id",
                    "filter": {
                        "where_filters": [{"where_sql_template": "{{ x }} = true"}]
                    },
                    "parents": [],
                }
            ]
        )

        assert metric.unique_id == "metric.paying"
        assert metric.filter == "{{ x }} = true"
        assert metric.expr == "hubspot_company_id"


def _cloud_source(
    environment_id: Optional[int] = None,
) -> DBTCloudSource:
    config = DBTCloudConfig(
        access_url="https://test.getdbt.com",
        token="dummy_token",
        account_id=123456,
        project_id=1234567,
        job_id=1,
        environment_id=environment_id,
        target_platform="snowflake",
    )
    return DBTCloudSource(
        config, PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
    )


def _definition_page(
    kind: str,
    nodes: List[Dict[str, Any]],
    end_cursor: Optional[str] = None,
    has_next_page: bool = False,
) -> Dict[str, Any]:
    """A Discovery API page of environment.definition.<kind>."""
    return {
        "environment": {
            "definition": {
                kind: {
                    "edges": [{"node": node} for node in nodes],
                    "pageInfo": {
                        "hasNextPage": has_next_page,
                        "endCursor": end_cursor,
                    },
                }
            }
        }
    }


def _warning_titles(source: DBTCloudSource) -> List[Optional[str]]:
    return [warning.title for warning in source.report.warnings]


class TestDiscoverySemanticModelFetch:
    """Semantic models are read from environment.definition.semanticModels."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_pages_are_followed_and_nodes_normalized(
        self, mock_graphql: mock.Mock
    ) -> None:
        mock_graphql.side_effect = [
            _definition_page(
                "semanticModels",
                [
                    {
                        "uniqueId": "semantic_model.proj.orders",
                        "name": "orders",
                        "description": None,
                        "parents": [
                            {"uniqueId": "model.proj.fct_orders"},
                            {"uniqueId": ""},
                            "not-a-dict",
                        ],
                    }
                ],
                end_cursor="cursor-1",
                has_next_page=True,
            ),
            _definition_page(
                "semanticModels",
                [
                    # No uniqueId: one is built from the package and name.
                    {"name": "customers", "packageName": "proj"},
                    # No name: skipped.
                    {"uniqueId": "semantic_model.proj.nameless"},
                ],
            ),
        ]

        nodes = DBTCloudSource._fetch_discovery_semantic_models(
            metadata_endpoint="https://metadata.test", token="t", environment_id=7
        )

        assert [node["uniqueId"] for node in nodes] == [
            "semantic_model.proj.orders",
            "semantic_model.proj.customers",
        ]
        orders, customers = nodes
        assert orders["resourceType"] == "semantic_model"
        assert orders["description"] == ""
        assert orders["dependsOn"] == ["model.proj.fct_orders"]
        assert customers["meta"] == {} and customers["tags"] == []
        assert mock_graphql.call_count == 2
        second_call_variables = mock_graphql.call_args_list[1].kwargs["variables"]
        assert second_call_variables["after"] == "cursor-1"
        assert second_call_variables["environmentId"] == 7

    def test_a_node_without_a_package_gets_an_unknown_package(self) -> None:
        normalized = DBTCloudSource._normalize_semantic_model_node({"name": "orders"})

        assert normalized is not None
        assert normalized["uniqueId"] == "semantic_model.unknown.orders"
        assert normalized["dependsOn"] == []

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_an_unexpected_response_shape_raises(self, mock_graphql: mock.Mock) -> None:
        mock_graphql.return_value = {"environment": None}

        with pytest.raises(ValueError, match="environment.definition.semanticModels"):
            DBTCloudSource._fetch_discovery_semantic_models(
                metadata_endpoint="https://metadata.test", token="t", environment_id=7
            )

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_a_next_page_without_a_cursor_raises(self, mock_graphql: mock.Mock) -> None:
        mock_graphql.return_value = _definition_page(
            "semanticModels", [], end_cursor=None, has_next_page=True
        )

        with pytest.raises(ValueError, match="no endCursor"):
            DBTCloudSource._fetch_discovery_semantic_models(
                metadata_endpoint="https://metadata.test", token="t", environment_id=7
            )


class TestSemanticModelEnvironmentIds:
    """Which environments the semantic models and metrics are read from."""

    def test_the_configured_environment_wins(self) -> None:
        source = _cloud_source(environment_id=42)

        assert source._semantic_model_environment_ids([{"environmentId": 1}]) == [42]

    def test_node_environment_ids_are_deduplicated_in_order(self) -> None:
        source = _cloud_source()

        environment_ids = source._semantic_model_environment_ids(
            [
                {"environmentId": "12"},
                {"environmentId": 5},
                {"environmentId": 12},
                {"environmentId": "not-a-number"},
                {},
            ]
        )

        assert environment_ids == [12, 5]

    def test_auto_discovery_environment_is_the_fallback(self) -> None:
        source = _cloud_source()
        source._auto_discovery_environment_id = 99

        assert source._semantic_model_environment_ids([{}]) == [99]

    def test_no_environment_gives_no_ids(self) -> None:
        assert _cloud_source()._semantic_model_environment_ids([{}]) == []


class TestFetchSemanticModelNodes:
    """Semantic models from every environment, once each."""

    def test_no_environment_warns_and_fetches_nothing(self) -> None:
        source = _cloud_source()

        with mock.patch.object(
            DBTCloudSource, "_fetch_discovery_semantic_models"
        ) as mock_fetch:
            assert source._fetch_semantic_model_nodes([{}]) == []

        mock_fetch.assert_not_called()
        assert _warning_titles(source) == ["dbt Cloud semantic models were not fetched"]

    def test_nodes_from_several_environments_are_deduplicated(self) -> None:
        source = _cloud_source()
        orders = {"uniqueId": "semantic_model.proj.orders", "name": "orders"}
        customers = {"uniqueId": "semantic_model.proj.customers", "name": "customers"}

        with mock.patch.object(
            DBTCloudSource,
            "_fetch_discovery_semantic_models",
            side_effect=[[orders], [orders, customers]],
        ) as mock_fetch:
            nodes = source._fetch_semantic_model_nodes(
                [{"environmentId": 1}, {"environmentId": 2}]
            )

        assert nodes == [orders, customers]
        assert [c.kwargs["environment_id"] for c in mock_fetch.call_args_list] == [
            1,
            2,
        ]

    def test_a_failing_environment_warns_and_the_others_still_load(self) -> None:
        source = _cloud_source()
        orders = {"uniqueId": "semantic_model.proj.orders", "name": "orders"}

        with mock.patch.object(
            DBTCloudSource,
            "_fetch_discovery_semantic_models",
            side_effect=[ValueError("boom"), [orders]],
        ):
            nodes = source._fetch_semantic_model_nodes(
                [{"environmentId": 1}, {"environmentId": 2}]
            )

        assert nodes == [orders]
        assert _warning_titles(source) == ["Failed to fetch dbt Cloud semantic models"]


class TestDiscoveryMetricFetch:
    """Metrics are read from environment.definition.metrics."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_pages_are_followed_and_nameless_nodes_dropped(
        self, mock_graphql: mock.Mock
    ) -> None:
        mock_graphql.side_effect = [
            _definition_page(
                "metrics",
                [{"name": "revenue"}, {"name": ""}],
                end_cursor="cursor-1",
                has_next_page=True,
            ),
            _definition_page("metrics", [{"name": "orders"}]),
        ]

        nodes = DBTCloudSource._fetch_discovery_metrics(
            metadata_endpoint="https://metadata.test", token="t", environment_id=7
        )

        assert [node["name"] for node in nodes] == ["revenue", "orders"]
        assert mock_graphql.call_args_list[1].kwargs["variables"]["after"] == (
            "cursor-1"
        )

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_an_unexpected_response_shape_raises(self, mock_graphql: mock.Mock) -> None:
        mock_graphql.return_value = {"environment": {"definition": {}}}

        with pytest.raises(ValueError, match="environment.definition.metrics"):
            DBTCloudSource._fetch_discovery_metrics(
                metadata_endpoint="https://metadata.test", token="t", environment_id=7
            )

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_a_next_page_without_a_cursor_raises(self, mock_graphql: mock.Mock) -> None:
        mock_graphql.return_value = _definition_page(
            "metrics", [{"name": "revenue"}], end_cursor=None, has_next_page=True
        )

        with pytest.raises(ValueError, match="no endCursor"):
            DBTCloudSource._fetch_discovery_metrics(
                metadata_endpoint="https://metadata.test", token="t", environment_id=7
            )


class TestFetchMetricDefinitions:
    """Metrics from every environment, linked to their semantic models and metrics."""

    def test_no_environment_gives_no_metrics(self) -> None:
        source = _cloud_source()

        with mock.patch.object(DBTCloudSource, "_fetch_discovery_metrics") as fetch:
            parsed = source._fetch_metric_definitions([{}])

        fetch.assert_not_called()
        assert parsed.metrics == []

    def test_metrics_are_linked_and_deduplicated_across_environments(self) -> None:
        source = _cloud_source()
        raw_nodes = [
            {
                "environmentId": 1,
                "resourceType": "semantic_model",
                "name": "Orders",
                "uniqueId": "semantic_model.proj.orders",
            },
            {"environmentId": 2, "resourceType": "model", "name": "fct_orders"},
        ]
        revenue = {
            "uniqueId": "metric.proj.revenue",
            "name": "revenue",
            "type": "SIMPLE",
            "typeParams": {"measure": {"name": "revenue"}},
            "parents": [{"name": "orders", "resourceType": "SemanticModel"}],
        }
        revenue_per_order = {
            "name": "revenue_per_order",
            "packageName": "proj",
            "type": "DERIVED",
            "typeParams": {"expr": "revenue / 2"},
            "parents": [
                {"name": "revenue", "resourceType": "metric"},
                {"name": "fct_orders", "resourceType": "model"},
                {"name": ""},
                "not-a-dict",
            ],
        }

        with mock.patch.object(
            DBTCloudSource,
            "_fetch_discovery_metrics",
            side_effect=[[revenue, revenue_per_order], [revenue]],
        ):
            parsed = source._fetch_metric_definitions(raw_nodes)

        metrics = {metric.unique_id: metric for metric in parsed.metrics}
        assert set(metrics) == {
            "metric.proj.revenue",
            "metric.proj.revenue_per_order",
        }
        assert metrics["metric.proj.revenue"].depends_on == [
            "semantic_model.proj.orders"
        ]
        assert metrics["metric.proj.revenue_per_order"].depends_on == [
            "metric.proj.revenue"
        ]

    def test_a_failing_environment_warns_and_the_others_still_load(self) -> None:
        source = _cloud_source()
        revenue = {
            "uniqueId": "metric.proj.revenue",
            "name": "revenue",
            "type": "SIMPLE",
            "typeParams": {"measure": {"name": "revenue"}},
            "parents": [],
        }

        with mock.patch.object(
            DBTCloudSource,
            "_fetch_discovery_metrics",
            side_effect=[ValueError("boom"), [revenue]],
        ):
            parsed = source._fetch_metric_definitions(
                [{"environmentId": 1}, {"environmentId": 2}]
            )

        assert [metric.unique_id for metric in parsed.metrics] == [
            "metric.proj.revenue"
        ]
        assert _warning_titles(source) == ["Failed to fetch dbt Cloud metrics"]

    def test_an_unreadable_filter_warns_but_keeps_the_metric(self) -> None:
        source = _cloud_source()

        node = source._manifest_metric_node(
            {"name": "revenue", "type": "SIMPLE", "filter": 12345, "parents": []},
            semantic_model_ids={},
            metric_ids={},
        )

        assert node is not None and node["name"] == "revenue"
        assert _warning_titles(source) == ["Could not read a dbt Cloud metric filter"]

    def test_a_metric_without_a_name_is_skipped(self) -> None:
        assert (
            _cloud_source()._manifest_metric_node(
                {"name": None}, semantic_model_ids={}, metric_ids={}
            )
            is None
        )
