import logging
from dataclasses import dataclass, field
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
    BigQueryQueriesExtractorReport,
    BigQuerySchemaApiPerfReport,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigQuerySchemaApi
from datahub.ingestion.source.bigquery_v2.common import (
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    BigQueryQueriesExtractor,
    BigQueryQueriesExtractorConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class BigQueryQueriesSourceReport(SourceReport):
    window: Optional[BaseTimeWindowConfig] = None
    queries_extractor: Optional[BigQueryQueriesExtractorReport] = None
    schema_api_perf: BigQuerySchemaApiPerfReport = field(
        default_factory=BigQuerySchemaApiPerfReport
    )


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

        self.filters = BigQueryFilter(self.config, self.report)
        self.identifiers = BigQueryIdentifierBuilder(self.config, self.report)

        self.connection = self.config.connection.get_bigquery_client()

        self.queries_extractor = BigQueryQueriesExtractor(
            connection=self.connection,
            schema_api=BigQuerySchemaApi(
                self.report.schema_api_perf,
                self.connection,
                projects_client=self.config.connection.get_projects_client(),
            ),
            config=self.config,
            structured_report=self.report,
            filters=self.filters,
            identifiers=self.identifiers,
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
        # TODO: Don't emit lineage, usage, operations  for ghost entities
        return self.queries_extractor.get_workunits_internal()

    def get_report(self) -> BigQueryQueriesSourceReport:
        return self.report
