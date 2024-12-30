import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.gc.dataprocess_cleanup import (
    DataJobEntity,
    DataProcessCleanup,
    DataProcessCleanupConfig,
    DataProcessCleanupReport,
)


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


if __name__ == "__main__":
    unittest.main()
