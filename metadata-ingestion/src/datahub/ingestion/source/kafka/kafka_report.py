from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class KafkaSourceReport(StaleEntityRemovalSourceReport):
    topics_scanned: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    # Surface profiling/sampling/schema-resolution skips in the report, not just logs.
    profiling_topics_profiled: int = 0
    profiling_topics_dropped: int = 0
    profiling_samples_skipped: int = 0
    profiling_avro_decode_failures: int = 0
    schema_inference_sampling_failures: int = 0
    schema_inference_message_decode_failures: int = 0
    schema_inference_no_fields: int = 0
    schema_registry_connectivity_failures: int = 0

    def report_topic_scanned(self, topic: str) -> None:
        self.topics_scanned += 1

    def report_dropped(self, topic: str) -> None:
        self.filtered.append(topic)
