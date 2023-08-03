from dataclasses import field, dataclass

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class DataHubSourceReport(StatefulIngestionReport):
    num_mysql_parse_errors: int = 0
    # error -> aspect -> [urn]
    mysql_parse_errors: LossyDict[str, LossyDict[str, LossyList[str]]] = field(
        default_factory=LossyDict
    )

    num_kafka_parse_errors: int = 0
    kafka_parse_errors: LossyDict[str, int] = field(default_factory=LossyDict)
