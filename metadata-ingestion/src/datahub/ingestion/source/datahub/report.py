from dataclasses import dataclass, field
from datetime import datetime, timezone

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class DataHubSourceReport(StatefulIngestionReport):
    stop_time: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    num_database_aspects_ingested: int = 0
    num_database_parse_errors: int = 0
    # error -> aspect -> [urn]
    database_parse_errors: LossyDict[str, LossyDict[str, LossyList[str]]] = field(
        default_factory=LossyDict
    )

    num_kafka_aspects_ingested: int = 0
    num_kafka_parse_errors: int = 0
    num_kafka_excluded_aspects: int = 0
    kafka_parse_errors: LossyDict[str, int] = field(default_factory=LossyDict)

    num_timeseries_deletions_dropped: int = 0
    num_timeseries_soft_deleted_aspects_dropped: int = 0
