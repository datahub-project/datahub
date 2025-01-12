from typing import Any, List, Tuple, Optional, Dict
from dataclasses import dataclass, field

from pydantic import PositiveInt
from pydantic.fields import Field

from datahub.ingestion.source.ge_profiling_config import GEProfilingBaseConfig
from datahub.ingestion.source_config.operation_config import is_profiling_enabled
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationReportMixin,
    ClassificationSourceConfigMixin,
)
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict


def flatten(field_path: List[str], data: Any, truncate: bool = True) -> Tuple[str, Any]:
    if isinstance(data, dict):
        for key, value in data.items():
            field_path.append(key)
            yield from flatten(field_path, value)
            if isinstance(value, dict) or isinstance(value, list):
                del field_path[-1]
    elif isinstance(data, list):
        for value in data:
            yield from flatten(field_path, value, False)
    else:
        yield '.'.join(field_path), data
        if len(field_path) > 0 and truncate:
            del field_path[-1]


class CouchbaseDBConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase, ClassificationSourceConfigMixin
):
    connect_string: str = Field(default=None, description="Couchbase connect string.")
    username: str = Field(default=None, description="Couchbase username.")
    password: str = Field(default=None, description="Couchbase password.")
    cluster_name: str = Field(default=None, description="Couchbase cluster name.")
    kv_timeout: Optional[PositiveInt] = Field(default=5, description="KV timeout.")
    query_timeout: Optional[PositiveInt] = Field(default=60, description="Query timeout.")
    schema_sample_size: Optional[PositiveInt] = Field(default=10000, description="Number of documents to sample.")
    options: dict = Field(
        default={}, description="Additional options to pass to `ClusterOptions()`."
    )
    maxSchemaSize: Optional[PositiveInt] = Field(
        default=300, description="Maximum number of fields to include in the schema."
    )
    keyspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for keyspace to filter in ingestion.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description="regex patterns for keyspaces to filter to assign domain_key.",
    )

    # Profiling
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to profile",
    )

    profiling: GEProfilingBaseConfig = Field(
        default=GEProfilingBaseConfig(),
        description="Configuration for profiling",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )


@dataclass
class CouchbaseDBSourceReport(StaleEntityRemovalSourceReport, ClassificationReportMixin, IngestionStageReport):
    filtered: List[str] = field(default_factory=list)
    documents_processed: int = 0
    keyspaces_profiled: int = 0
    collection_aggregate_timer: PerfTimer = field(default_factory=PerfTimer)
    profiling_skipped_other: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    profiling_skipped_table_profile_pattern: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)

    def set_ingestion_stage(self, keyspace: str, stage: str) -> None:
        self.report_ingestion_stage_start(f"{keyspace}: {stage}")

    def report_entity_profiled(self) -> None:
        self.keyspaces_profiled += 1


@dataclass
class CouchbaseEntity:
    dataset: str
    schema: dict
    count: int
