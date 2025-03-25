import time
import unittest
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.ingestion.source.gc.dataprocess_cleanup import (
    DataJobEntity,
    DataProcessCleanup,
    DataProcessCleanupConfig,
    DataProcessCleanupReport,
)
from datahub.ingestion.source.gc.soft_deleted_entity_cleanup import (
    SoftDeletedEntitiesCleanup,
    SoftDeletedEntitiesCleanupConfig,
    SoftDeletedEntitiesReport,
)
from datahub.utilities.urns._urn_base import Urn

FROZEN_TIME = "2021-12-07 07:00:00"


class TestSoftDeletedEntitiesCleanup(unittest.TestCase):
    def setUp(self):
        self.ctx = PipelineContext(run_id="test_run")
        self.ctx.graph = MagicMock()
        self.config = SoftDeletedEntitiesCleanupConfig()
        self.report = SoftDeletedEntitiesReport()
        self.cleanup = SoftDeletedEntitiesCleanup(
            self.ctx, self.config, self.report, dry_run=True
        )

    def test_update_report(self):
        self.cleanup._update_report(
            urn="urn:li:dataset:1",
            entity_type="dataset",
        )
        self.assertEqual(1, self.report.num_hard_deleted)
        self.assertEqual(1, self.report.num_hard_deleted_by_type["dataset"])

    def test_increment_retained_count(self):
        self.cleanup._increment_retained_count()


class TestDataProcessCleanup(unittest.TestCase):
    def setUp(self):
        self.ctx = PipelineContext(run_id="test_run")
        self.ctx.graph = MagicMock()
        self.config = DataProcessCleanupConfig()
        self.report = DataProcessCleanupReport()
        self.cleanup = DataProcessCleanup(
            self.ctx, self.config, self.report, dry_run=True
        )

    @patch(
        "datahub.ingestion.source.gc.dataprocess_cleanup.DataProcessCleanup.fetch_dpis"
    )
    def test_delete_dpi_from_datajobs(self, mock_fetch_dpis):
        job = DataJobEntity(
            urn="urn:li:dataJob:1",
            flow_urn="urn:li:dataFlow:1",
            lastIngested=int(datetime.now(timezone.utc).timestamp()),
            jobId="job1",
            dataPlatformInstance="urn:li:dataPlatformInstance:1",
            total_runs=10,
        )
        mock_fetch_dpis.return_value = [
            {
                "urn": f"urn:li:dataprocessInstance:{i}",
                "created": {
                    "time": int(datetime.now(timezone.utc).timestamp() + i) * 1000
                },
            }
            for i in range(10)
        ]
        self.cleanup.delete_dpi_from_datajobs(job)
        self.assertEqual(5, self.report.num_aspects_removed)

    @patch(
        "datahub.ingestion.source.gc.dataprocess_cleanup.DataProcessCleanup.fetch_dpis"
    )
    def test_delete_dpi_from_datajobs_without_dpis(self, mock_fetch_dpis):
        job = DataJobEntity(
            urn="urn:li:dataJob:1",
            flow_urn="urn:li:dataFlow:1",
            lastIngested=int(datetime.now(timezone.utc).timestamp()),
            jobId="job1",
            dataPlatformInstance="urn:li:dataPlatformInstance:1",
            total_runs=10,
        )
        mock_fetch_dpis.return_value = []
        self.cleanup.delete_dpi_from_datajobs(job)
        self.assertEqual(0, self.report.num_aspects_removed)

    @patch(
        "datahub.ingestion.source.gc.dataprocess_cleanup.DataProcessCleanup.fetch_dpis"
    )
    def test_delete_dpi_from_datajobs_without_dpi_created_time(self, mock_fetch_dpis):
        job = DataJobEntity(
            urn="urn:li:dataJob:1",
            flow_urn="urn:li:dataFlow:1",
            lastIngested=int(datetime.now(timezone.utc).timestamp()),
            jobId="job1",
            dataPlatformInstance="urn:li:dataPlatformInstance:1",
            total_runs=10,
        )
        mock_fetch_dpis.return_value = [
            {"urn": f"urn:li:dataprocessInstance:{i}"} for i in range(10)
        ] + [
            {
                "urn": "urn:li:dataprocessInstance:11",
                "created": {"time": int(datetime.now(timezone.utc).timestamp() * 1000)},
            }
        ]
        self.cleanup.delete_dpi_from_datajobs(job)
        self.assertEqual(10, self.report.num_aspects_removed)

    @patch(
        "datahub.ingestion.source.gc.dataprocess_cleanup.DataProcessCleanup.fetch_dpis"
    )
    def test_delete_dpi_from_datajobs_without_dpi_null_created_time(
        self, mock_fetch_dpis
    ):
        job = DataJobEntity(
            urn="urn:li:dataJob:1",
            flow_urn="urn:li:dataFlow:1",
            lastIngested=int(datetime.now(timezone.utc).timestamp()),
            jobId="job1",
            dataPlatformInstance="urn:li:dataPlatformInstance:1",
            total_runs=10,
        )
        mock_fetch_dpis.return_value = [
            {"urn": f"urn:li:dataprocessInstance:{i}"} for i in range(10)
        ] + [
            {
                "urn": "urn:li:dataprocessInstance:11",
                "created": {"time": None},
            }
        ]
        self.cleanup.delete_dpi_from_datajobs(job)
        self.assertEqual(11, self.report.num_aspects_removed)

    @patch(
        "datahub.ingestion.source.gc.dataprocess_cleanup.DataProcessCleanup.fetch_dpis"
    )
    def test_delete_dpi_from_datajobs_without_dpi_without_time(self, mock_fetch_dpis):
        job = DataJobEntity(
            urn="urn:li:dataJob:1",
            flow_urn="urn:li:dataFlow:1",
            lastIngested=int(datetime.now(timezone.utc).timestamp()),
            jobId="job1",
            dataPlatformInstance="urn:li:dataPlatformInstance:1",
            total_runs=10,
        )
        mock_fetch_dpis.return_value = [
            {"urn": f"urn:li:dataprocessInstance:{i}"} for i in range(10)
        ] + [
            {
                "urn": "urn:li:dataprocessInstance:11",
                "created": None,
            }
        ]
        self.cleanup.delete_dpi_from_datajobs(job)
        self.assertEqual(11, self.report.num_aspects_removed)

    def test_fetch_dpis(self):
        assert self.cleanup.ctx.graph
        self.cleanup.ctx.graph = MagicMock()
        self.cleanup.ctx.graph.execute_graphql.return_value = {
            "dataJob": {
                "runs": {
                    "runs": [
                        {
                            "urn": "urn:li:dataprocessInstance:1",
                            "created": {
                                "time": int(datetime.now(timezone.utc).timestamp())
                            },
                        }
                    ]
                }
            }
        }
        dpis = self.cleanup.fetch_dpis("urn:li:dataJob:1", 10)
        self.assertEqual(len(dpis), 1)


class TestSoftDeletedEntitiesCleanup2(unittest.TestCase):
    def setUp(self):
        # Create mocks for dependencies
        self.mock_graph = MagicMock(spec=DataHubGraph)
        self.mock_ctx = MagicMock(spec=PipelineContext)
        self.mock_ctx.graph = self.mock_graph

        # Create a default config
        self.config = SoftDeletedEntitiesCleanupConfig(
            enabled=True,
            retention_days=10,
            batch_size=100,
            delay=0.1,
            max_workers=5,
            entity_types=["DATASET", "DASHBOARD"],
            limit_entities_delete=1000,
            futures_max_at_time=100,
            runtime_limit_seconds=3600,
        )

        # Create a report
        self.report = SoftDeletedEntitiesReport()

        # Create the test instance
        self.cleanup = SoftDeletedEntitiesCleanup(
            ctx=self.mock_ctx,
            config=self.config,
            report=self.report,
            dry_run=False,
        )

        # Create a sample URN
        self.sample_urn = Urn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:example,example,PROD)"
        )

    def test_init_requires_graph(self):
        """Test that initialization fails if graph is not provided."""
        self.mock_ctx.graph = None
        with self.assertRaises(ValueError):
            SoftDeletedEntitiesCleanup(
                ctx=self.mock_ctx,
                config=self.config,
                report=self.report,
            )

    def test_delete_entity_dry_run(self):
        """Test that delete_entity doesn't actually delete in dry run mode."""
        # Set dry run
        self.cleanup.dry_run = True

        # Call the method
        self.cleanup.delete_entity(self.sample_urn)

        # Verify no deletion happened
        self.mock_graph.delete_entity.assert_not_called()
        self.mock_graph.delete_references_to_urn.assert_not_called()

        # No report update
        self.assertEqual(self.report.num_hard_deleted, 0)

    def test_delete_entity(self):
        """Test that delete_entity properly deletes and updates reports."""
        # Call the method
        self.cleanup.delete_entity(self.sample_urn)

        # Verify deletion happened
        self.mock_graph.delete_entity.assert_called_once_with(
            urn=self.sample_urn.urn(), hard=True
        )
        self.mock_graph.delete_references_to_urn.assert_called_once_with(
            urn=self.sample_urn.urn(),
            dry_run=False,
        )

        # Report update
        self.assertEqual(self.report.num_hard_deleted, 1)
        self.assertEqual(self.report.num_hard_deleted_by_type.get("dataset"), 1)
        self.assertEqual(self.report.num_soft_deleted_entity_removal_started, 1)

    def test_delete_entity_respects_deletion_limit(self):
        """Test that delete_entity respects the deletion limit."""
        # Set a report value to hit the limit
        self.config.limit_entities_delete = 1500
        self.report.num_hard_deleted = self.config.limit_entities_delete + 1

        # Call the method
        self.cleanup.delete_entity(self.sample_urn)

        # Verify no deletion happened due to limit
        self.mock_graph.delete_entity.assert_not_called()
        self.mock_graph.delete_references_to_urn.assert_not_called()
        self.assertTrue(self.report.deletion_limit_reached)

    def test_delete_entity_respects_time_limit(self):
        """Test that delete_entity respects the runtime limit."""
        # Set time to exceed runtime limit
        self.cleanup.start_time = time.time() - self.config.runtime_limit_seconds - 1

        # Call the method
        self.cleanup.delete_entity(self.sample_urn)

        # Verify no deletion happened due to time limit
        self.mock_graph.delete_entity.assert_not_called()
        self.mock_graph.delete_references_to_urn.assert_not_called()
        self.assertTrue(self.report.runtime_limit_reached)

    def test_delete_soft_deleted_entity_old_enough(self):
        """Test that entities are deleted when they are old enough."""
        # Calculate a timestamp older than retention days
        old_timestamp = int(
            (
                datetime.now(timezone.utc)
                - timedelta(days=self.config.retention_days + 1)
            ).timestamp()
            * 1000
        )

        # Mock the aspect return
        self.mock_graph.get_entity_raw.return_value = {
            "aspects": {
                "status": {
                    "value": {"removed": True},
                    "created": {"time": old_timestamp},
                }
            }
        }

        # Call the method
        self.cleanup.delete_soft_deleted_entity(self.sample_urn)

        # Verify deletion was attempted
        self.mock_graph.delete_entity.assert_called_once()
        self.assertEqual(self.report.num_soft_deleted_retained_due_to_age, 0)
        self.assertIsNone(
            self.report.num_soft_deleted_retained_due_to_age_by_type.get("dataset")
        )

    def test_delete_soft_deleted_entity_too_recent(self):
        """Test that entities are not deleted when they are too recent."""
        # Calculate a timestamp newer than retention days
        recent_timestamp = int(
            (
                datetime.now(timezone.utc)
                - timedelta(days=self.config.retention_days - 1)
            ).timestamp()
            * 1000
        )

        # Mock the aspect return
        self.mock_graph.get_entity_raw.return_value = {
            "aspects": {
                "status": {
                    "value": {"removed": True},
                    "created": {"time": recent_timestamp},
                }
            }
        }

        # Call the method
        self.cleanup.delete_soft_deleted_entity(self.sample_urn)

        # Verify no deletion was attempted
        self.mock_graph.delete_entity.assert_not_called()
        self.assertEqual(self.report.num_soft_deleted_retained_due_to_age, 1)
        self.assertEqual(
            self.report.num_soft_deleted_retained_due_to_age_by_type.get("dataset"), 1
        )

    @freeze_time(FROZEN_TIME)
    def test_get_urns(self):
        """Test that _get_urns calls get_urns_by_filter with correct parameters."""
        # Setup mock for get_urns_by_filter
        self.mock_graph.get_urns_by_filter.return_value = ["urn1", "urn2", "urn3"]

        # Get all urns
        urns = list(self.cleanup._get_urns())

        # Verify get_urns_by_filter was called correctly
        self.mock_graph.get_urns_by_filter.assert_called_once_with(
            entity_types=self.config.entity_types,
            platform=self.config.platform,
            env=self.config.env,
            query=self.config.query,
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
            batch_size=self.config.batch_size,
        )

        # Check the returned urns
        self.assertEqual(urns, ["urn1", "urn2", "urn3"])

    @freeze_time(FROZEN_TIME)
    def test_get_urns_with_dpi(self):
        """Test that _get_urns calls get_urns_by_filter with correct parameters."""
        # Setup mock for get_urns_by_filter
        self.mock_graph.get_urns_by_filter.side_effect = [
            ["urn1", "urn2", "urn3"],
            ["dpi_urn1", "dpi_urn2", "dpi_urn3"],
        ]

        # Get all urns
        assert self.config.entity_types
        self.config.entity_types.append("dataProcessInstance")
        urns = list(self.cleanup._get_urns())
        self.config.entity_types.remove("dataProcessInstance")
        # Verify get_urns_by_filter was called correctly
        self.mock_graph.get_urns_by_filter.has_calls(
            [
                call(
                    entity_types=self.config.entity_types,
                    platform=self.config.platform,
                    env=self.config.env,
                    query=self.config.query,
                    status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                    batch_size=self.config.batch_size,
                    extraFilters=[
                        SearchFilterRule(
                            field="created",
                            condition="LESS_THAN",
                            values=[
                                str(
                                    int(
                                        time.time()
                                        - self.config.retention_days * 24 * 60 * 60
                                    )
                                    * 1000
                                )
                            ],
                        ).to_raw()
                    ],
                ),
                call(
                    entity_types=["dataProcessInstance"],
                    platform=self.config.platform,
                    env=self.config.env,
                    query=self.config.query,
                    status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                    batch_size=self.config.batch_size,
                ),
            ]
        )

        # Check the returned urns
        self.assertEqual(
            urns, ["urn1", "urn2", "urn3", "dpi_urn1", "dpi_urn2", "dpi_urn3"]
        )

    def test_process_futures(self):
        """Test the _process_futures method properly handles futures."""
        # Create sample futures
        mock_future1 = MagicMock(spec=Future)
        mock_future1.exception.return_value = None

        mock_future2 = MagicMock(spec=Future)
        mock_future2.exception.return_value = Exception("Test exception")

        # Mock the wait function to return the first future as done
        with patch(
            "datahub.ingestion.source.gc.soft_deleted_entity_cleanup.wait",
            return_value=({mock_future1}, {mock_future2}),
        ):
            futures = {mock_future1: self.sample_urn, mock_future2: self.sample_urn}

            # Process the futures
            result = self.cleanup._process_futures(futures)  # type: ignore

            # Check result contains only the not_done future
            self.assertEqual(len(result), 1)
            self.assertIn(mock_future2, result)

            # Check report was updated
            self.assertEqual(self.report.num_soft_deleted_entity_processed, 1)

    def test_cleanup_disabled(self):
        """Test that cleanup doesn't run when disabled."""
        # Disable cleanup
        self.config.enabled = False

        # Mock methods to check they're not called
        with patch.object(self.cleanup, "_get_urns") as mock_get_urns:
            self.cleanup.cleanup_soft_deleted_entities()
            mock_get_urns.assert_not_called()

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_soft_deleted_entities(self, mock_executor_class):
        """Test the main cleanup method submits tasks correctly."""
        # Setup mock for executor
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Mock futures
        mock_future = MagicMock(spec=Future)
        mock_executor.submit.return_value = mock_future
        urns_to_delete = [
            "urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent2,PROD)",
        ]
        # Mock Urn.from_string to return our sample URN
        with patch.object(self.cleanup, "_get_urns", return_value=urns_to_delete):
            # Mock _process_futures to simulate completion
            with patch.object(self.cleanup, "_process_futures", return_value={}):
                # Run cleanup
                self.cleanup.cleanup_soft_deleted_entities()

                # Verify executor was created with correct workers
                mock_executor_class.assert_called_once_with(
                    max_workers=self.config.max_workers
                )

                # Verify submit was called for each urn
                self.assertEqual(mock_executor.submit.call_count, 2)
                # Both calls should be to delete_soft_deleted_entity with the sample_urn
                for idx, call_args in enumerate(mock_executor.submit.call_args_list):
                    self.assertEqual(
                        call_args[0][0], self.cleanup.delete_soft_deleted_entity
                    )
                    self.assertEqual(
                        call_args[0][1], Urn.from_string(urns_to_delete[idx])
                    )

    def test_times_up(self):
        """Test the time limit checker."""
        # Test time limit not reached
        self.cleanup.start_time = time.time()
        self.assertFalse(self.cleanup._times_up())

        # Test time limit reached
        self.cleanup.start_time = time.time() - self.config.runtime_limit_seconds - 1
        self.assertTrue(self.cleanup._times_up())
        self.assertTrue(self.report.runtime_limit_reached)

    def test_deletion_limit_reached(self):
        """Test the deletion limit checker."""
        # Test deletion limit not reached
        self.config.limit_entities_delete = 1500
        self.report.num_hard_deleted = self.config.limit_entities_delete - 1
        self.assertFalse(self.cleanup._deletion_limit_reached())

        # Test deletion limit reached
        self.report.num_hard_deleted = self.config.limit_entities_delete + 1
        self.assertTrue(self.cleanup._deletion_limit_reached())
        self.assertTrue(self.report.deletion_limit_reached)

    def test_handle_urn_parsing_error(self):
        """Test handling of URN parsing errors."""
        # Mock _get_urns to return an invalid URN
        with patch.object(self.cleanup, "_get_urns", return_value=["invalid:urn"]):
            # Mock Urn.from_string to raise an exception
            # Mock logger to capture log messages
            with self.assertLogs(level="ERROR") as log_context:
                # Mock other methods to prevent actual execution
                with patch.object(self.cleanup, "_process_futures", return_value={}):
                    # Run cleanup
                    self.cleanup.cleanup_soft_deleted_entities()

                    # Verify error was logged
                    self.assertTrue(
                        any("Failed to parse urn" in msg for msg in log_context.output)
                    )

    def test_increment_retained_by_type(self):
        """Test the _increment_retained_by_type method."""
        entity_type = "dataset"

        # Call the method
        self.cleanup._increment_retained_by_type(entity_type)

        # Check report was updated
        self.assertEqual(
            self.report.num_soft_deleted_retained_due_to_age_by_type.get(entity_type), 1
        )

        # Call again
        self.cleanup._increment_retained_by_type(entity_type)

        # Check report was updated again
        self.assertEqual(
            self.report.num_soft_deleted_retained_due_to_age_by_type.get(entity_type), 2
        )

    def test_entity_with_missing_status_aspect(self):
        """Test handling of entities without a status aspect."""
        # Mock the aspect return with no status
        self.mock_graph.get_entity_raw.return_value = {"aspects": {}}

        # Call the method
        self.cleanup.delete_soft_deleted_entity(self.sample_urn)

        # Verify no deletion was attempted
        self.mock_graph.delete_entity.assert_not_called()
        self.assertEqual(
            self.report.num_soft_deleted_retained_due_to_age, 0
        )  # No increment

    def test_entity_not_removed(self):
        """Test handling of entities that have status but are not removed."""
        # Mock the aspect return with removed=False
        recent_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.mock_graph.get_entity_raw.return_value = {
            "aspects": {
                "status": {
                    "value": {"removed": False},
                    "created": {"time": recent_timestamp},
                }
            }
        }

        # Call the method
        self.cleanup.delete_soft_deleted_entity(self.sample_urn)

        # Verify no deletion was attempted
        self.mock_graph.delete_entity.assert_not_called()
        self.assertEqual(
            self.report.num_soft_deleted_retained_due_to_age, 1
        )  # Should increment


class TestCleanupSoftDeletedEntities(unittest.TestCase):
    """Tests for the cleanup_soft_deleted_entities method."""

    def setUp(self) -> None:
        # Create mocks for dependencies
        self.mock_graph: MagicMock = MagicMock(spec=DataHubGraph)
        self.mock_ctx: MagicMock = MagicMock(spec=PipelineContext)
        self.mock_ctx.graph = self.mock_graph

        # Create a default config
        self.config: SoftDeletedEntitiesCleanupConfig = (
            SoftDeletedEntitiesCleanupConfig(
                enabled=True,
                retention_days=10,
                batch_size=100,
                max_workers=5,
                futures_max_at_time=10,
            )
        )

        # Create a report
        self.report: SoftDeletedEntitiesReport = SoftDeletedEntitiesReport()

        # Create the test instance
        self.cleanup: SoftDeletedEntitiesCleanup = SoftDeletedEntitiesCleanup(
            ctx=self.mock_ctx,
            config=self.config,
            report=self.report,
        )

        # Sample URNs for testing
        self.sample_urns: List[str] = [
            "urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent2,PROD)",
            "urn:li:dashboard:(looker,dashboard1)",
        ]

        # Parsed URN objects
        self.parsed_urns: List[Urn] = [Urn.from_string(urn) for urn in self.sample_urns]

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_disabled(self, mock_executor_class: MagicMock) -> None:
        """Test that cleanup doesn't run when disabled."""
        # Disable cleanup
        self.config.enabled = False

        # Mock methods to check they're not called
        with patch.object(self.cleanup, "_get_urns") as mock_get_urns:
            self.cleanup.cleanup_soft_deleted_entities()

            # Verify that nothing happens when disabled
            mock_get_urns.assert_not_called()
            mock_executor_class.assert_not_called()

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_with_valid_urns(self, mock_executor_class: MagicMock) -> None:
        """Test the main cleanup method with valid URNs."""
        # Setup mock for executor
        mock_executor: MagicMock = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Mock futures
        mock_futures: List[MagicMock] = [
            MagicMock(spec=Future) for _ in range(len(self.sample_urns))
        ]
        mock_executor.submit.side_effect = mock_futures

        # Set up _get_urns to return our sample URNs
        with patch.object(self.cleanup, "_get_urns", return_value=self.sample_urns):
            # Mock _process_futures to simulate completion
            with patch.object(self.cleanup, "_process_futures", return_value={}):
                # Mock _print_report to avoid timing issues
                with patch.object(self.cleanup, "_print_report"):
                    # Run cleanup
                    self.cleanup.cleanup_soft_deleted_entities()

                    # Verify executor was created with correct workers
                    mock_executor_class.assert_called_once_with(
                        max_workers=self.config.max_workers
                    )

                    # Verify submit was called for each urn
                    self.assertEqual(
                        mock_executor.submit.call_count, len(self.sample_urns)
                    )

                    # Check that the correct method and parameters were used
                    expected_calls: List = [
                        call(
                            self.cleanup.delete_soft_deleted_entity,
                            Urn.from_string(urn),
                        )
                        for urn in self.sample_urns
                    ]

                    mock_executor.submit.assert_has_calls(expected_calls)

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_with_invalid_urns(self, mock_executor_class: MagicMock) -> None:
        """Test how the cleanup handles invalid URNs."""
        # Setup mock for executor
        mock_executor: MagicMock = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Valid and invalid URNs
        mixed_urns: List[str] = self.sample_urns + ["invalid:urn:format"]

        # Set up _get_urns to return mixed URNs
        with patch.object(self.cleanup, "_get_urns", return_value=mixed_urns):
            # Mock _process_futures to simulate completion
            with patch.object(self.cleanup, "_process_futures", return_value={}):
                # Mock _print_report to avoid timing issues
                with patch.object(self.cleanup, "_print_report"):
                    # Mock logger to capture log messages
                    with self.assertLogs(level="ERROR") as log_context:
                        # Run cleanup
                        self.cleanup.cleanup_soft_deleted_entities()

                        # Verify error was logged for invalid URN
                        self.assertTrue(
                            any(
                                "Failed to parse urn" in msg
                                for msg in log_context.output
                            )
                        )

                        # Verify submit was called only for valid URNs
                        self.assertEqual(
                            mock_executor.submit.call_count, len(self.sample_urns)
                        )

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_with_max_futures_limit(
        self, mock_executor_class: MagicMock
    ) -> None:
        """Test that the cleanup respects the max futures limit."""
        # Setup mock for executor
        mock_executor: MagicMock = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Set max futures to 1 to force processing after each submission
        self.config.futures_max_at_time = 1

        # Keep track of how many times _process_futures is called
        process_futures_call_count = 0

        # Define a side effect function that simulates the behavior of _process_futures
        # It needs to clear the futures dictionary once per call to prevent infinite loops
        def process_futures_side_effect(
            futures: Dict[Future, Urn],
        ) -> Dict[Future, Urn]:
            nonlocal process_futures_call_count
            process_futures_call_count += 1
            # Return an empty dict to simulate that all futures are processed
            return {}

        # Set up _get_urns to return sample URNs
        with patch.object(self.cleanup, "_get_urns", return_value=self.sample_urns):
            # Mock _process_futures with our side effect function
            with patch.object(
                self.cleanup,
                "_process_futures",
                side_effect=process_futures_side_effect,
            ):
                # Mock _print_report to avoid timing issues
                with patch.object(self.cleanup, "_print_report"):
                    # Run cleanup
                    self.cleanup.cleanup_soft_deleted_entities()

                    # Verify _process_futures was called for each URN (since max_futures=1)
                    self.assertEqual(process_futures_call_count, len(self.sample_urns))

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_respects_deletion_limit(
        self, mock_executor_class: MagicMock
    ) -> None:
        """Test that cleanup stops when deletion limit is reached."""
        # Setup mock for executor
        mock_executor: MagicMock = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Set up to hit deletion limit after first URN
        with patch.object(self.cleanup, "_deletion_limit_reached") as mock_limit:
            # Return False for first URN, True for others
            mock_limit.side_effect = [False, True, True]

            # Set up _get_urns to return sample URNs
            with patch.object(self.cleanup, "_get_urns", return_value=self.sample_urns):
                # Mock _process_futures to simulate completion
                with patch.object(self.cleanup, "_process_futures", return_value={}):
                    # Mock _print_report to avoid timing issues
                    with patch.object(self.cleanup, "_print_report"):
                        # Run cleanup
                        self.cleanup.cleanup_soft_deleted_entities()

                        # Should only process the first URN before hitting limit
                        self.assertEqual(mock_executor.submit.call_count, 1)

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_respects_time_limit(self, mock_executor_class: MagicMock) -> None:
        """Test that cleanup stops when time limit is reached."""
        # Setup mock for executor
        mock_executor: MagicMock = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Set up to hit time limit after first URN
        with patch.object(self.cleanup, "_times_up") as mock_times_up:
            # Return False for first URN, True for others
            mock_times_up.side_effect = [False, True, True]

            # Set up _get_urns to return sample URNs
            with patch.object(self.cleanup, "_get_urns", return_value=self.sample_urns):
                # Mock _process_futures to simulate completion
                with patch.object(self.cleanup, "_process_futures", return_value={}):
                    # Mock _print_report to avoid timing issues
                    with patch.object(self.cleanup, "_print_report"):
                        # Run cleanup
                        self.cleanup.cleanup_soft_deleted_entities()

                        # Should only process the first URN before hitting time limit
                        self.assertEqual(mock_executor.submit.call_count, 1)

    @patch("datahub.ingestion.source.gc.soft_deleted_entity_cleanup.ThreadPoolExecutor")
    def test_cleanup_handles_empty_urn_list(
        self, mock_executor_class: MagicMock
    ) -> None:
        """Test cleanup when no URNs are returned."""
        # Setup mock for executor
        mock_executor: MagicMock = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Set up _get_urns to return empty list
        with patch.object(self.cleanup, "_get_urns", return_value=[]):
            # Mock other methods to prevent errors
            with patch.object(self.cleanup, "_process_futures"):
                with patch.object(self.cleanup, "_print_report"):
                    # Run cleanup
                    self.cleanup.cleanup_soft_deleted_entities()

                    # Verify submit was not called
                    mock_executor.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
