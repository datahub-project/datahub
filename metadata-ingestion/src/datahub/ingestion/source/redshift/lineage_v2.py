from datetime import datetime
from typing import Optional, Tuple

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator


class RedshiftSqlLineageV2:
    # does lineage and usage based on SQL parsing.

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        context: PipelineContext,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        self.config = config
        self.report = report
        self.context = context

        self.aggregator = SqlParsingAggregator(
            platform="redshift",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            generate_lineage=True,
            generate_queries=True,
            generate_usage_statistics=True,
            generate_operations=True,
            graph=self.context.graph,
        )

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self.get_time_window()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            self.report.stateful_lineage_ingestion_enabled = True
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def generate(self):
        pass
