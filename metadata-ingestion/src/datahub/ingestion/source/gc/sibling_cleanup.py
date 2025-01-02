import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict

logger = logging.getLogger(__name__)


class DataHubSiblingCleanupConfig(ConfigModel):
    enabled: bool = Field(
        default=True, description="Whether to do sibling process cleanup."
    )

@dataclass
class DataHubSiblingCleanupReport(SourceReport):
    num_sibling_entity_removed: int = 0
    num_sibling_removed_by_type: TopKDict[str, int] = field(
        default_factory=TopKDict
    )
    sample_soft_deleted_removed_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )


class DataHubSiblingCleanup:
    def __init__(
            self,
            graph: DataHubGraph,
            report: DataHubSiblingCleanupReport,
            config: Optional[DataHubSiblingCleanupConfig] = None,
    ) -> None:
        self.graph = graph
        self.report = report
        self.instance_id = int(time.time())

        if config is not None:
            self.config = config
        else:
            self.config = DataHubSiblingCleanupConfig()