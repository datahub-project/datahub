import logging
from typing import Dict, Iterable, List, Optional, Union

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.sql.sql_generic_profiler import GenericProfiler
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


class RedshiftProfiler(GenericProfiler):
    config: RedshiftConfig
    report: RedshiftReport

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        state_handler: Optional[ProfilingHandler],
    ) -> None:
        super().__init__(config, report, "redshift", state_handler)
        self.config = config
        self.report = report

    def get_workunits(
        self,
        tables: Union[
            Dict[str, Dict[str, List[RedshiftTable]]],
            Dict[str, Dict[str, List[RedshiftView]]],
        ],
    ) -> Iterable[MetadataWorkUnit]:
        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if self.config.is_profiling_enabled():
            self.config.options.setdefault(
                "max_overflow", self.config.profiling.max_workers
            )

        for db in tables.keys():
            profile_requests = []
            for schema in tables.get(db, {}).keys():
                if not self.config.schema_pattern.allowed(schema):
                    continue
                for table in tables[db].get(schema, {}):
                    # Emit the profile work unit
                    profile_request = self.get_profile_request(table, schema, db)
                    if profile_request is not None:
                        self.report.report_entity_profiled(profile_request.pretty_name)
                        profile_requests.append(profile_request)

            if len(profile_requests) == 0:
                continue

            yield from self.generate_profile_workunits(
                profile_requests,
                max_workers=self.config.profiling.max_workers,
                db_name=db,
                platform=self.platform,
                profiler_args=self.get_profile_args(),
            )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return f"{db_name}.{schema_name}.{table_name}".lower()
