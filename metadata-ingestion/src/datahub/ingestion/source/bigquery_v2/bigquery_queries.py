import logging
from dataclasses import dataclass
from typing import Iterable, Optional

from pydantic import Field
from typing_extensions import Self

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryConnectionConfig,
    BigQueryFilterConfig,
    BigQueryIdentifierConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQuerySchemaApiPerfReport,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigQuerySchemaApi
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    BigQueryQueriesExtractor,
    BigQueryQueriesExtractorConfig,
    BigQueryQueriesExtractorReport,
)

logger = logging.getLogger(__name__)


@dataclass
class BigQueryQueriesSourceReport(SourceReport):
    window: Optional[BaseTimeWindowConfig] = None
    queries_extractor: Optional[BigQueryQueriesExtractorReport] = None


class BigQueryQueriesSourceConfig(
    BigQueryQueriesExtractorConfig, BigQueryFilterConfig, BigQueryIdentifierConfig
):
    connection: BigQueryConnectionConfig = Field(
        default_factory=BigQueryConnectionConfig
    )


class BigQueryQueriesSource(Source):
    def __init__(self, ctx: PipelineContext, config: BigQueryQueriesSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = BigQueryQueriesSourceReport()

        self.connection = self.config.connection.get_bigquery_client()

        self.queries_extractor = BigQueryQueriesExtractor(
            connection=self.connection,
            schema_api=BigQuerySchemaApi(
                BigQuerySchemaApiPerfReport(), self.connection
            ),
            config=self.config,
            structured_report=self.report,
            filter_config=self.config,
            identifier_config=self.config,
            graph=self.ctx.graph,
        )
        self.report.queries_extractor = self.queries_extractor.report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        config = BigQueryQueriesSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.report.window = self.config.window

        # TODO: Disable auto status processor?
        return self.queries_extractor.get_workunits_internal()

    def get_report(self) -> BigQueryQueriesSourceReport:
        return self.report
