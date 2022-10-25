from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.source.ib.ib_common import (
    IBPathElementInfo,
    IBRedashDatasetSource,
    IBRedashSourceConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import JobId


class IBMSSQLViewsSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBMSSQL")
@config_class(IBMSSQLViewsSourceConfig)
class IBMSSQLViewsSource(IBRedashDatasetSource):
    path_info = [
        IBPathElementInfo("DataCenter", True),
        IBPathElementInfo("Server", True),
        IBPathElementInfo("Database"),
        IBPathElementInfo("Schema"),
        IBPathElementInfo("View"),
    ]
    platform = "mssql"

    def __init__(self, config: IBMSSQLViewsSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBMSSQLViewsSourceConfig = config

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_mssql_views_from_redash_source_")
