from dataclasses import dataclass

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)
from datahub.ingestion.source_report.sql.snowflake import BaseSnowflakeReport


@dataclass
class SnowflakeUsageReport(BaseSnowflakeReport, StatefulIngestionReport):
    pass
