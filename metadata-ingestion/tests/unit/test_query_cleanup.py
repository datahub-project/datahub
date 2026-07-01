import logging
import time
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.ingestion.source.gc.query_cleanup import (
    QueryCleanup,
    QueryCleanupConfig,
    QueryCleanupReport,
)
from datahub.utilities.urns._urn_base import Urn

FROZEN_TIME = "2021-12-07 07:00:00"


class TestQueryCleanup:
    def setup_method(self) -> None:
        self.mock_graph = MagicMock(spec=DataHubGraph)
        self.mock_ctx = MagicMock(spec=PipelineContext)
        self.mock_ctx.graph = self.mock_graph

        self.config = QueryCleanupConfig(enabled=True, retention_days=30)
        self.report = QueryCleanupReport()
        self.cleanup = QueryCleanup(
            ctx=self.mock_ctx,
            config=self.config,
            report=self.report,
            dry_run=False,
        )
        self.sample_urn = Urn.from_string("urn:li:query:query-1")

    def test_init_requires_graph(self) -> None:
        self.mock_ctx.graph = None
        with pytest.raises(ValueError):
            QueryCleanup(self.mock_ctx, self.config, self.report)

    @time_machine.travel(FROZEN_TIME, tick=False)
    def test_get_urns_filters_by_system_and_age(self) -> None:
        self.mock_graph.get_urns_by_filter.return_value = ["urn:li:query:1"]

        list(self.cleanup._get_urns())

        cutoff_millis = int(
            (time.time() - self.config.retention_days * 24 * 60 * 60) * 1000
        )
        self.mock_graph.get_urns_by_filter.assert_called_once_with(
            entity_types=["query"],
            status=RemovedStatusFilter.NOT_SOFT_DELETED,
            batch_size=self.config.batch_size,
            extraFilters=[
                SearchFilterRule(
                    field="source", condition="EQUAL", values=["SYSTEM"]
                ).to_raw(),
                SearchFilterRule(
                    field="lastModifiedAt",
                    condition="LESS_THAN",
                    values=[f"{cutoff_millis}"],
                ).to_raw(),
            ],
        )

    def test_delete_query_soft_delete(self) -> None:
        self.cleanup.delete_query(self.sample_urn)

        self.mock_graph.delete_entity.assert_called_once_with(
            urn=self.sample_urn.urn(), hard=False
        )
        assert self.report.num_queries_soft_deleted == 1
        assert self.report.num_queries_hard_deleted == 0

    def test_delete_query_hard_delete(self) -> None:
        self.config.hard_delete_entities = True

        self.cleanup.delete_query(self.sample_urn)

        self.mock_graph.delete_entity.assert_called_once_with(
            urn=self.sample_urn.urn(), hard=True
        )
        assert self.report.num_queries_hard_deleted == 1
        assert self.report.num_queries_soft_deleted == 0

    def test_delete_query_dry_run(self) -> None:
        self.cleanup.dry_run = True

        self.cleanup.delete_query(self.sample_urn)

        self.mock_graph.delete_entity.assert_not_called()
        assert self.report.num_queries_soft_deleted == 0

    def test_delete_query_respects_deletion_limit(self) -> None:
        self.config.limit_entities_delete = 5
        self.report.num_queries_soft_deleted = 6

        self.cleanup.delete_query(self.sample_urn)

        self.mock_graph.delete_entity.assert_not_called()
        assert self.report.qc_deletion_limit_reached

    def test_delete_query_respects_time_limit(self) -> None:
        self.cleanup.start_time = time.time() - self.config.runtime_limit_seconds - 1

        self.cleanup.delete_query(self.sample_urn)

        self.mock_graph.delete_entity.assert_not_called()
        assert self.report.qc_runtime_limit_reached

    def test_cleanup_disabled(self) -> None:
        self.config.enabled = False

        self.cleanup.cleanup_queries()

        self.mock_graph.get_urns_by_filter.assert_not_called()
        self.mock_graph.delete_entity.assert_not_called()

    def test_cleanup_deletes_all_candidates(self) -> None:
        urns = ["urn:li:query:1", "urn:li:query:2", "urn:li:query:3"]
        self.mock_graph.get_urns_by_filter.return_value = urns

        self.cleanup.cleanup_queries()

        assert self.report.num_queries_found == 3
        assert self.report.num_queries_soft_deleted == 3
        assert self.mock_graph.delete_entity.call_count == 3

    def test_cleanup_skips_invalid_urn(self, caplog: pytest.LogCaptureFixture) -> None:
        self.mock_graph.get_urns_by_filter.return_value = [
            "urn:li:query:1",
            "invalid:urn",
        ]

        with caplog.at_level(logging.ERROR):
            self.cleanup.cleanup_queries()

        assert any("Failed to parse urn" in r.message for r in caplog.records)
        assert self.report.num_queries_soft_deleted == 1
        assert self.report.num_queries_invalid_urn == 1

    def test_process_futures_reports_failure(self) -> None:
        good = MagicMock(spec=Future)
        good.exception.return_value = None
        bad = MagicMock(spec=Future)
        bad.exception.return_value = Exception("boom")

        self.config.delay = None
        with patch(
            "datahub.ingestion.source.gc.query_cleanup.wait",
            return_value=({good, bad}, set()),
        ):
            remaining = self.cleanup._process_futures(
                {good: self.sample_urn, bad: self.sample_urn}
            )

        assert remaining == {}
        assert len(self.report.failures) == 1
