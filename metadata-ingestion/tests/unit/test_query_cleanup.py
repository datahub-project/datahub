import time
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.ingestion.source.gc.query_cleanup import (
    QueryCleanup,
    QueryCleanupConfig,
    QueryCleanupReport,
)
from datahub.metadata.schema_classes import StatusClass
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

    def test_soft_delete_yields_status_workunits(self) -> None:
        urns = ["urn:li:query:1", "urn:li:query:2", "urn:li:query:3"]
        self.mock_graph.get_urns_by_filter.return_value = urns

        wus = list(self.cleanup.get_workunits())

        assert len(wus) == 3
        for wu, expected_urn in zip(wus, urns, strict=True):
            mcp = wu.metadata
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            assert mcp.entityUrn == expected_urn
            aspect = mcp.aspect
            assert isinstance(aspect, StatusClass)
            assert aspect.removed
        # Soft delete never touches the imperative hard-delete path.
        self.mock_graph.delete_entity.assert_not_called()
        assert self.report.num_queries_found == 3
        assert self.report.num_queries_soft_deleted == 3
        assert self.report.num_queries_hard_deleted == 0

    def test_hard_delete_uses_graph_delete_entity(self) -> None:
        self.config.hard_delete_entities = True
        urns = ["urn:li:query:1", "urn:li:query:2"]
        self.mock_graph.get_urns_by_filter.return_value = urns

        wus = list(self.cleanup.get_workunits())

        # Hard delete is imperative, so it yields no workunits.
        assert wus == []
        assert self.mock_graph.delete_entity.call_count == 2
        self.mock_graph.delete_entity.assert_any_call(urn=urns[0], hard=True)
        assert self.report.num_queries_hard_deleted == 2
        assert self.report.num_queries_soft_deleted == 0

    def test_dry_run_previews_without_deleting(self) -> None:
        self.cleanup.dry_run = True
        urns = ["urn:li:query:1", "urn:li:query:2"]
        self.mock_graph.get_urns_by_filter.return_value = urns

        wus = list(self.cleanup.get_workunits())

        assert wus == []
        self.mock_graph.delete_entity.assert_not_called()
        assert self.report.num_queries_found == 2
        assert self.report.num_queries_soft_deleted == 0
        # Dry run still previews the candidate urns.
        assert len(self.report.sample_deleted_queries) == 2

    def test_disabled(self) -> None:
        self.config.enabled = False

        wus = list(self.cleanup.get_workunits())

        assert wus == []
        self.mock_graph.get_urns_by_filter.assert_not_called()

    def test_respects_deletion_limit(self) -> None:
        self.config.limit_entities_delete = 2
        self.mock_graph.get_urns_by_filter.return_value = [
            f"urn:li:query:{i}" for i in range(5)
        ]

        wus = list(self.cleanup.get_workunits())

        assert len(wus) == 2
        assert self.report.num_queries_soft_deleted == 2
        assert self.report.qc_deletion_limit_reached

    def test_times_up_sets_runtime_flag(self) -> None:
        self.cleanup.start_time = time.time() - self.config.runtime_limit_seconds - 1

        assert self.cleanup._times_up()
        assert self.report.qc_runtime_limit_reached

    def test_stops_when_runtime_limit_reached(self) -> None:
        self.mock_graph.get_urns_by_filter.return_value = [
            "urn:li:query:1",
            "urn:li:query:2",
        ]

        with patch.object(self.cleanup, "_times_up", return_value=True):
            wus = list(self.cleanup.get_workunits())

        assert wus == []
        assert self.report.num_queries_soft_deleted == 0

    def test_skips_invalid_urn(self) -> None:
        self.mock_graph.get_urns_by_filter.return_value = [
            "urn:li:query:1",
            "invalid:urn",
        ]

        wus = list(self.cleanup.get_workunits())

        assert len(wus) == 1
        assert self.report.num_queries_found == 1
        assert self.report.num_queries_soft_deleted == 1
        assert len(self.report.warnings) == 1
