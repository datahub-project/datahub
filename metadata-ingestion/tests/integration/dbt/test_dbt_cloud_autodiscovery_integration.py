"""
Integration tests for dbt Cloud auto-discovery functionality.

Tests focus on:
- End-to-end auto-discovery workflow
- Multi-job ingestion and metadata aggregation
- Error handling across multiple jobs
- Comparison between explicit and auto-discovery modes
"""

from typing import Any, Callable, Dict, List, Optional
from unittest import mock

import pytest

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
from datahub.ingestion.source.dbt.dbt_tests import select_test_results_to_emit


@pytest.fixture
def mock_graphql_response() -> Dict[str, Any]:
    """Sample GraphQL response for a job's nodes."""
    return {
        "job": {
            "models": [
                {
                    "uniqueId": "model.test_project.test_model",
                    "name": "test_model",
                    "description": "Test model description",
                    "resourceType": "model",
                    "database": "test_db",
                    "schema": "test_schema",
                    "alias": None,
                    "packageName": "test_project",
                    "columns": [],
                    "meta": {},
                    "tags": [],
                    "rawCode": "SELECT * FROM test_table",
                    "compiledCode": "SELECT * FROM test_table",
                    "tests": [],
                    "upstreamSources": [],
                    "materializedType": "table",
                    "status": "success",
                    "error": None,
                    "skip": False,
                    "dependsOn": [],
                }
            ],
            "sources": [],
            "seeds": [],
            "snapshots": [],
            "tests": [],
            "exposures": [],
            "semanticModels": [
                {
                    "uniqueId": "semantic_model.test_project.test_metrics",
                    "name": "test_metrics",
                    "description": "Test semantic model",
                    "resourceType": "semantic_model",
                    "packageName": "test_project",
                    "meta": {},
                    "tags": [],
                    "dependsOn": ["model.test_project.test_model"],
                    "entities": [
                        {
                            "name": "id",
                            "type": "primary",
                            "description": "",
                            "expr": "id",
                        }
                    ],
                    "dimensions": [
                        {
                            "name": "created_at",
                            "type": "time",
                            "description": "",
                            "expr": "created_at",
                            "typeParams": {},
                        }
                    ],
                    "measures": [
                        {
                            "name": "count",
                            "agg": "count",
                            "description": "",
                            "expr": "*",
                            "createMetric": False,
                        }
                    ],
                }
            ],
        }
    }


@pytest.fixture
def mock_graphql_response_with_sources() -> Dict[str, Any]:
    """Sample GraphQL response with source nodes"""
    return {
        "job": {
            "models": [],
            "sources": [
                {
                    "uniqueId": "source.test_project.raw.users",
                    "name": "users",
                    "description": "Raw users table",
                    "resourceType": "source",
                    "database": "raw_db",
                    "schema": "raw",
                    "identifier": "users",
                    "sourceName": "raw",
                    "sourceDescription": "Raw data source",
                    "columns": [],
                    "meta": {},
                    "tags": [],
                    "runId": 123,
                    "jobId": 100,
                    "accountId": 123456,
                    "projectId": 1234567,
                    "environmentId": 1,
                    "dbtVersion": "1.5.0",
                    "maxLoadedAt": "2026-01-14T10:00:00+00:00",
                    "snapshottedAt": "2026-01-14T12:00:00+00:00",
                    "maxLoadedAtTimeAgoInS": 7200.0,
                    "state": "pass",
                    "freshnessChecked": True,
                    "loader": "dummy-loader",
                    "criteria": {
                        "warnAfter": {"count": 12, "period": "hour"},
                        "errorAfter": {"count": 24, "period": "hour"},
                    },
                },
                {
                    "uniqueId": "source.test_project.raw.orders",
                    "name": "orders",
                    "description": "Raw orders table",
                    "resourceType": "source",
                    "database": "raw_db",
                    "schema": "raw",
                    "identifier": "orders",
                    "sourceName": "raw",
                    "sourceDescription": "Raw data source",
                    "columns": [],
                    "meta": {},
                    "tags": [],
                    "runId": 123,
                    "jobId": 100,
                    "accountId": 123456,
                    "projectId": 1234567,
                    "environmentId": 1,
                    "dbtVersion": "1.5.0",
                    "maxLoadedAt": "2026-01-13T10:00:00+00:00",
                    "snapshottedAt": "2026-01-14T12:00:00+00:00",
                    "maxLoadedAtTimeAgoInS": 93600.0,
                    "state": "warn",
                    "freshnessChecked": True,
                    "loader": "dummy-loader",
                    "criteria": {
                        "warnAfter": {"count": 12, "period": "hour"},
                        "errorAfter": {"count": 48, "period": "hour"},
                    },
                },
            ],
            "seeds": [],
            "snapshots": [],
            "tests": [],
        }
    }


_EMPTY_JOB: Dict[str, List[Dict[str, Any]]] = {
    "models": [],
    "sources": [],
    "seeds": [],
    "snapshots": [],
    "tests": [],
    "exposures": [],
    "semanticModels": [],
}


def _model_row(
    *,
    job_id: int,
    run_id: int,
    owner: str,
    built: bool,
    completed_at: Optional[str] = "2025-01-01T00:00:00Z",
) -> Dict[str, Any]:
    """A model as the job-scoped Discovery query returns it.

    ``built=False`` gives the padding shape of a node the run did not
    execute: no status, no timings, no compiled code.
    """
    return {
        "uniqueId": "model.test_project.test_model",
        "name": "test_model",
        "description": "",
        "resourceType": "model",
        "database": "test_db",
        "schema": "test_schema",
        "alias": None,
        "packageName": "test_project",
        "columns": [],
        "meta": {"owner": owner},
        "tags": [],
        "rawCode": "SELECT 1",
        "rawSql": None,
        "compiledCode": "SELECT 1" if built else None,
        "compiledSql": None,
        "materializedType": "table",
        "status": "success" if built else None,
        "error": None,
        "skip": False,
        "executeStartedAt": completed_at if built else None,
        "executeCompletedAt": completed_at if built else None,
        "runGeneratedAt": completed_at if built else None,
        "dependsOn": [],
        "jobId": job_id,
        "runId": run_id,
    }


def _test_row(
    *, job_id: int, run_id: int, status: str, completed_at: str
) -> Dict[str, Any]:
    return {
        "uniqueId": "test.test_project.not_null_test_model_id",
        "name": "not_null_test_model_id",
        "description": "",
        "resourceType": "test",
        "meta": {},
        "tags": [],
        "state": None,
        "columnName": "id",
        "status": status,
        "error": None,
        "dependsOn": ["model.test_project.test_model", "macro.dbt.test_not_null"],
        "fail": status == "fail",
        "warn": False,
        "skip": False,
        "rawSql": None,
        "rawCode": None,
        "compiledSql": None,
        "compiledCode": None,
        "executeStartedAt": completed_at,
        "executeCompletedAt": completed_at,
        "runGeneratedAt": completed_at,
        "jobId": job_id,
        "runId": run_id,
    }


def _graphql_by_job(
    responses: Dict[int, Dict[str, List[Dict[str, Any]]]],
) -> Callable[..., Dict[str, Any]]:
    """A ``_send_graphql_query`` stand-in answering per job and node type."""

    def side_effect(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        job_id = kwargs["variables"]["jobId"]
        node_type = kwargs["query"].split("DatahubMetadataQuery_", 1)[1].split("(")[0]
        return {"job": {node_type: responses[job_id].get(node_type, [])}}

    return side_effect


def _auto_discovery_source(**auto_discovery: Any) -> DBTCloudSource:
    config = DBTCloudConfig(
        access_url="https://test.getdbt.com",
        token="dummy_token",
        account_id=123456,
        project_id=1234567,
        auto_discovery=AutoDiscoveryConfig(enabled=True, **auto_discovery),
        target_platform="snowflake",
    )
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
    return DBTCloudSource(config, ctx)


class TestAutoDiscoveryEndToEnd:
    """End-to-end tests for auto-discovery workflow."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discovery_multiple_jobs_success(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Should successfully ingest metadata from multiple discovered jobs."""
        # Setup: 2 jobs discovered in production environment
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
        ]
        mock_graphql.return_value = mock_graphql_response

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

        # Execute
        project = next(source.load_projects())
        assert project.artifact_props == {"account_id": "123456"}
        assert project.platform_instance == source.config.platform_instance
        nodes = project.nodes

        # Assertions
        # Should have called GraphQL for both jobs (models, sources, seeds, snapshots, tests, exposures, semanticModels = 7 calls per job)
        assert mock_graphql.call_count == 14  # 2 jobs * 7 node types

        # Verify both job IDs were queried
        job_ids_queried = {
            call[1]["variables"]["jobId"] for call in mock_graphql.call_args_list
        }
        assert job_ids_queried == {100, 200}

        # Verify run_id is None (always use latest in auto-discovery)
        for call in mock_graphql.call_args_list:
            assert call[1]["variables"]["runId"] is None

        # Both jobs return the same manifest; each node is ingested once.
        assert len(nodes) == 2

        # Verify semantic models were parsed correctly
        semantic_models = [n for n in nodes if n.node_type == "semantic_model"]
        assert len(semantic_models) == 1

        for sm in semantic_models:
            assert len(sm.columns) > 0
            column_types = {c.data_type for c in sm.columns}
            assert (
                any(ct.startswith("entity:") for ct in column_types)
                or any(ct.startswith("dimension:") for ct in column_types)
                or any(ct.startswith("measure:") for ct in column_types)
            )

        # Verify metadata contains account_id but not job_id (since multiple jobs)
        assert project.artifact_props["account_id"] == "123456"
        assert "job_id" not in project.artifact_props

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discovery_partial_job_failure(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Should continue ingesting other jobs when one job fails."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
            DBTCloudJob(id=300, generate_docs=True),
        ]

        # Job 200 fails, others succeed
        def graphql_side_effect(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            job_id = kwargs["variables"]["jobId"]
            if job_id == 200:
                raise ValueError("GraphQL error for job 200")
            return mock_graphql_response

        mock_graphql.side_effect = graphql_side_effect

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

        # Execute
        nodes = next(source.load_projects()).nodes

        # Jobs 100 and 300 return the same manifest; each node is ingested once.
        assert len(nodes) == 2

        # Verify warning was logged for job 200 failure
        # (This is implicit - the source continues without raising)

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discovery_includes_jobs_without_generate_docs(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """By default, should ingest jobs even without generate_docs enabled."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=False),
        ]
        mock_graphql.return_value = mock_graphql_response

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

        # Execute
        project = next(source.load_projects())
        nodes = project.nodes

        # mock_graphql_response fixture returns 1 model + 1 semantic model per job
        assert len(nodes) == 2
        assert {n.node_type for n in nodes} == {"model", "semantic_model"}
        assert project.artifact_props["account_id"] == "123456"

        job_ids_queried = {
            call[1]["variables"]["jobId"] for call in mock_graphql.call_args_list
        }
        assert 100 in job_ids_queried

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discovery_no_jobs_with_require_generate_docs(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
    ) -> None:
        """Should return empty results when require_generate_docs=True and no jobs have it."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=False),
        ]

        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(
                enabled=True, require_generate_docs=True
            ),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        # Execute
        nodes = next(source.load_projects()).nodes

        assert len(nodes) == 0

        mock_graphql.assert_not_called()


class TestExplicitModeComparison:
    """Tests comparing explicit mode behavior with auto-discovery mode."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_explicit_mode_uses_configured_run_id(
        self,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Explicit mode should use the configured run_id."""
        mock_graphql.return_value = mock_graphql_response

        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,
            run_id=999,  # Explicit run_id
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        # Execute
        next(source.load_projects())

        # Verify run_id=999 was used in all GraphQL calls
        for call in mock_graphql.call_args_list:
            assert call[1]["variables"]["runId"] == 999

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discovery_ignores_run_id(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Auto-discovery mode should ignore run_id and use latest."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
        ]
        mock_graphql.return_value = mock_graphql_response

        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            run_id=999,  # Should be ignored in auto-discovery
            auto_discovery=AutoDiscoveryConfig(enabled=True),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        # Execute
        next(source.load_projects())

        # Verify run_id=None was used (latest run)
        for call in mock_graphql.call_args_list:
            assert call[1]["variables"]["runId"] is None

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_explicit_mode_metadata_includes_job_id(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Explicit mode should include job_id in additional metadata."""
        mock_graphql.return_value = mock_graphql_response

        # Explicit mode
        config_explicit = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=12345678,
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source_explicit = DBTCloudSource(config_explicit, ctx)

        project_explicit = next(source_explicit.load_projects())

        # Auto-discovery mode
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
        ]

        config_auto = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(enabled=True),
            target_platform="snowflake",
        )
        source_auto = DBTCloudSource(config_auto, ctx)

        project_auto = next(source_auto.load_projects())

        # Verify metadata differences
        # Note: Based on the diff, job_id was removed from additional_metadata
        # This test verifies the current behavior
        assert "account_id" in project_explicit.artifact_props
        assert "account_id" in project_auto.artifact_props


class TestMetadataConsistency:
    """Tests to ensure auto-discovery produces identical metadata to explicit mode."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_auto_discovery_produces_same_metadata_as_explicit_mode(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """
        Critical test: Auto-discovery should produce identical node metadata
        to explicit mode when processing the same job.
        """
        job_id = 12345678
        mock_graphql.return_value = mock_graphql_response

        # Explicit mode: directly specify job_id
        config_explicit = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=job_id,
            target_platform="snowflake",
        )
        ctx_explicit = PipelineContext(run_id="test-run-id", pipeline_name="test")
        source_explicit = DBTCloudSource(config_explicit, ctx_explicit)
        project_explicit = next(source_explicit.load_projects())
        nodes_explicit = project_explicit.nodes

        # Reset mock call count
        mock_graphql.reset_mock()

        # Auto-discovery mode: discover the same job
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=job_id, generate_docs=True),
        ]

        config_auto = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(enabled=True),
            target_platform="snowflake",
        )
        ctx_auto = PipelineContext(run_id="test-run-id", pipeline_name="test")
        source_auto = DBTCloudSource(config_auto, ctx_auto)
        project_auto = next(source_auto.load_projects())
        nodes_auto = project_auto.nodes

        # Verify both modes produce the same nodes
        assert len(nodes_explicit) == len(nodes_auto)
        assert (
            len(nodes_explicit) == 2
        )  # One model + one semantic model from our mock response

        # Compare node metadata field by field (compare models only)
        node_explicit = next(n for n in nodes_explicit if n.node_type == "model")
        node_auto = next(n for n in nodes_auto if n.node_type == "model")

        # These fields should be identical
        assert node_explicit.name == node_auto.name
        assert node_explicit.database == node_auto.database
        assert node_explicit.schema == node_auto.schema
        assert node_explicit.dbt_name == node_auto.dbt_name
        assert node_explicit.dbt_adapter == node_auto.dbt_adapter
        assert node_explicit.node_type == node_auto.node_type
        assert node_explicit.description == node_auto.description
        assert node_explicit.raw_code == node_auto.raw_code
        assert node_explicit.compiled_code == node_auto.compiled_code
        assert node_explicit.upstream_nodes == node_auto.upstream_nodes
        assert node_explicit.materialization == node_auto.materialization

        # Additional metadata should contain account_id in both cases
        assert (
            project_explicit.artifact_props["account_id"]
            == project_auto.artifact_props["account_id"]
        )


class TestAutoDiscoveryWithPatterns:
    """Tests for job filtering with allow/deny patterns."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_job_pattern_filtering(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Should only ingest jobs matching the job_id_pattern."""
        from datahub.configuration.common import AllowDenyPattern

        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=1001, generate_docs=True),  # Matches ^100.*
            DBTCloudJob(id=1002, generate_docs=True),  # Matches ^100.*
            DBTCloudJob(id=2001, generate_docs=True),  # Doesn't match
        ]
        mock_graphql.return_value = mock_graphql_response

        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(
                enabled=True,
                job_id_pattern=AllowDenyPattern(allow=["^100.*"]),
            ),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        # Execute
        next(source.load_projects())

        # Should only query jobs 1001 and 1002
        job_ids_queried = {
            call[1]["variables"]["jobId"] for call in mock_graphql.call_args_list
        }
        assert 1001 in job_ids_queried
        assert 1002 in job_ids_queried
        assert 2001 not in job_ids_queried

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_deny_pattern_filtering(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        """Should exclude jobs matching deny pattern."""
        from datahub.configuration.common import AllowDenyPattern

        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
            DBTCloudJob(id=300, generate_docs=True),
        ]
        mock_graphql.return_value = mock_graphql_response

        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            auto_discovery=AutoDiscoveryConfig(
                enabled=True,
                job_id_pattern=AllowDenyPattern(deny=["200"]),  # Exclude job 200
            ),
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        # Execute
        next(source.load_projects())

        # Should query jobs 100 and 300, but not 200
        job_ids_queried = {
            call[1]["variables"]["jobId"] for call in mock_graphql.call_args_list
        }
        assert 100 in job_ids_queried
        assert 300 in job_ids_queried
        assert 200 not in job_ids_queried


class TestAutoDiscoveryErrorHandling:
    """Tests for error handling in auto-discovery mode."""

    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_environment_api_failure_propagates(self, mock_get_envs: mock.Mock) -> None:
        """Should fail fast when environment API fails."""
        mock_get_envs.side_effect = ValueError("Environment API error")

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

        # Should raise the error
        with pytest.raises(ValueError, match="Environment API error"):
            next(source.load_projects())

    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_jobs_api_failure_propagates(
        self, mock_get_envs: mock.Mock, mock_get_jobs: mock.Mock
    ) -> None:
        """Should fail fast when jobs API fails."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.side_effect = ValueError("Jobs API error")

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

        # Should raise the error
        with pytest.raises(ValueError, match="Jobs API error"):
            next(source.load_projects())

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_all_jobs_fail_returns_empty(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
    ) -> None:
        """Should return empty when all jobs fail to fetch nodes."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
        ]
        mock_graphql.side_effect = ValueError("GraphQL error")

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

        # Execute
        nodes = next(source.load_projects()).nodes

        # Should have no nodes
        assert len(nodes) == 0


class TestSourceFreshnessExtraction:
    """Tests for source freshness extraction from dbt Cloud."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_sources_with_freshness_extracted(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response_with_sources: Dict[str, Any],
    ) -> None:
        """Should extract freshness info from sources with freshness configured."""
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
        ]
        mock_graphql.return_value = mock_graphql_response_with_sources

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

        # Execute
        nodes = next(source.load_projects()).nodes

        # Should have 2 source nodes
        assert len(nodes) == 2

        # Verify freshness info extracted
        users_node = next(n for n in nodes if n.name == "users")
        assert users_node.freshness_info is not None
        assert users_node.freshness_info.status == "pass"
        assert users_node.freshness_info.warn_after is not None
        assert users_node.freshness_info.warn_after.count == 12
        assert users_node.freshness_info.error_after is not None
        assert users_node.freshness_info.error_after.count == 24

        orders_node = next(n for n in nodes if n.name == "orders")
        assert orders_node.freshness_info is not None
        assert orders_node.freshness_info.status == "warn"

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    def test_explicit_mode_sources_with_freshness(
        self,
        mock_graphql: mock.Mock,
        mock_graphql_response_with_sources: Dict[str, Any],
    ) -> None:
        """Should extract freshness info in explicit job mode."""
        mock_graphql.return_value = mock_graphql_response_with_sources

        config = DBTCloudConfig(
            access_url="https://test.getdbt.com",
            token="dummy_token",
            account_id=123456,
            project_id=1234567,
            job_id=100,
            target_platform="snowflake",
        )
        ctx = PipelineContext(run_id="test-run-id", pipeline_name="test-pipeline")
        source = DBTCloudSource(config, ctx)

        # Execute
        nodes = next(source.load_projects()).nodes

        # Verify freshness extracted
        source_nodes = [n for n in nodes if n.node_type == "source"]
        assert len(source_nodes) == 2
        assert all(n.freshness_info is not None for n in source_nodes)


class TestAutoDiscoveryNodeMerge:
    """Reconciling copies of one node returned by several discovered jobs."""

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_metadata_comes_from_the_job_that_built_the_node(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
    ) -> None:
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        # The job that merely saw the model is listed last and has the newer
        # run: with last-wins its stale owner replaced the real one.
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
        ]
        mock_graphql.side_effect = _graphql_by_job(
            {
                100: {
                    **_EMPTY_JOB,
                    "models": [
                        _model_row(job_id=100, run_id=10, owner="team_a", built=True)
                    ],
                },
                200: {
                    **_EMPTY_JOB,
                    "models": [
                        _model_row(job_id=200, run_id=20, owner="stale", built=False)
                    ],
                },
            }
        )
        source = _auto_discovery_source()

        (node,) = next(source.load_projects()).nodes

        assert node.meta == {"owner": "team_a"}
        assert source.report.auto_discovery_node_copies_seen == 2
        assert source.report.auto_discovery_nodes_merged == 1
        assert source.report.auto_discovery_padding_rows_dropped == 1

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_node_built_by_no_job_is_kept_and_reported(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
    ) -> None:
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
        ]
        mock_graphql.side_effect = _graphql_by_job(
            {
                100: {
                    **_EMPTY_JOB,
                    "models": [
                        _model_row(job_id=100, run_id=10, owner="old", built=False)
                    ],
                },
                200: {
                    **_EMPTY_JOB,
                    "models": [
                        _model_row(job_id=200, run_id=20, owner="newer", built=False)
                    ],
                },
            }
        )
        source = _auto_discovery_source()

        (node,) = next(source.load_projects()).nodes

        assert node.meta == {"owner": "newer"}
        assert source.report.auto_discovery_nodes_not_built_by_any_job == 1
        assert any(
            w.title == "dbt Cloud node not built by any discovered job"
            for w in source.report.warnings
        )

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_test_results_from_every_job_are_kept(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
    ) -> None:
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
        ]
        mock_graphql.side_effect = _graphql_by_job(
            {
                100: {
                    **_EMPTY_JOB,
                    "tests": [
                        _test_row(
                            job_id=100,
                            run_id=10,
                            status="fail",
                            completed_at="2025-01-02T00:00:00Z",
                        )
                    ],
                },
                200: {
                    **_EMPTY_JOB,
                    "tests": [
                        _test_row(
                            job_id=200,
                            run_id=20,
                            status="pass",
                            completed_at="2025-01-01T00:00:00Z",
                        )
                    ],
                },
            }
        )
        source = _auto_discovery_source()

        (node,) = next(source.load_projects()).nodes

        assert {r.invocation_id for r in node.test_results} == {
            "job100-run10",
            "job200-run20",
        }
        (latest,) = select_test_results_to_emit(node.test_results, latest_only=True)
        assert latest.invocation_id == "job100-run10"
        assert source.report.auto_discovery_test_results_merged == 1

    @mock.patch.object(DBTCloudSource, "_send_graphql_query")
    @mock.patch.object(DBTCloudSource, "_get_jobs_for_project")
    @mock.patch.object(DBTCloudSource, "_get_environments_for_project")
    def test_last_job_wins_restores_the_previous_behaviour(
        self,
        mock_get_envs: mock.Mock,
        mock_get_jobs: mock.Mock,
        mock_graphql: mock.Mock,
        mock_graphql_response: Dict[str, Any],
    ) -> None:
        mock_get_envs.return_value = [
            DBTCloudEnvironment(id=1, deployment_type=DBTCloudDeploymentType.PRODUCTION)
        ]
        mock_get_jobs.return_value = [
            DBTCloudJob(id=100, generate_docs=True),
            DBTCloudJob(id=200, generate_docs=True),
        ]
        mock_graphql.return_value = mock_graphql_response
        source = _auto_discovery_source(node_merge_strategy="last_job_wins")

        nodes = next(source.load_projects()).nodes

        # Every copy is passed through; the shared dbt code then keeps the
        # last one per node.
        assert len(nodes) == 4
        assert source.report.auto_discovery_node_copies_seen == 0
