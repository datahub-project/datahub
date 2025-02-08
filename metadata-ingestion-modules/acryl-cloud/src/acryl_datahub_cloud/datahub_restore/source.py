import logging
import time
from functools import partial
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, root_validator

from acryl_datahub_cloud.datahub_restore.do_restore import restore_indices
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


class DataHubRestoreIndicesReport(StatefulIngestionReport):
    calls_made: int = 0
    aspect_check_ms: int = 0
    create_record_ms: int = 0
    time_get_rows_ms: int = 0
    time_sql_query_ms: int = 0
    rows_migrated: int = 0
    send_message_ms: int = 0
    time_urn_ms: int = 0
    default_aspects_created: int = 0
    ignored: int = 0
    last_aspect: str = ""
    last_urn: str = ""
    time_entity_registry_check_ms: int = 0


class DataHubRestoreIndicesConfig(ConfigModel, StatefulIngestionConfigBase):
    urn: Optional[str] = Field(
        None,
        description="The urn of the entity to restore indices for. If not provided, urn_like must be provided.",
    )
    urn_like: Optional[str] = Field(
        None,
        description="The urn_like of the entity to restore indices for. If not provided, urn must be provided.",
    )
    start: int = Field(
        0,
        description="Same as restore indices endpoint.",
    )
    batch_size: int = Field(
        1,
        description="Same as restore indices endpoint.",
    )
    aspect: Optional[str] = Field(
        None,
        description="Same as restore indices endpoint.",
    )

    @root_validator(pre=True)
    def extract_assertion_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("urn") is None and values.get("urn_like") is None:
            raise ValueError("Either urn or urn_like must be provided.")
        if values.get("urn") is not None and values.get("urn_like") is not None:
            raise ValueError("Only one of urn or urn_like must be provided.")
        return values


@platform_name(id="datahub", platform_name="datahub")
@config_class(DataHubRestoreIndicesConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubRestoreSource(StatefulIngestionSourceBase):
    def __init__(self, ctx: PipelineContext, config: DataHubRestoreIndicesConfig):
        super().__init__(config=config, ctx=ctx)
        self.report: DataHubRestoreIndicesReport = DataHubRestoreIndicesReport()
        self.report.event_not_produced_warn = False
        self.config = config
        self.graph = ctx.require_graph("The DataHubRestore Source")
        self.last_print_time = 0.0

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """A list of functions that transforms the workunits produced by this source.
        Run in order, first in list is applied first. Be careful with order when overriding.
        """

        return [
            partial(auto_workunit_reporter, self.get_report()),
        ]

    def _print_report(self) -> None:
        time_taken = round(time.time() - self.last_print_time, 1)
        if time_taken > 60:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        start = self.config.start
        while True:
            self.report.calls_made += 1
            response = restore_indices(
                graph=self.graph,
                start=start,
                batch_size=self.config.batch_size,
                limit=self.config.batch_size,
                urn=self.config.urn,
                urn_like=self.config.urn_like,
                aspect=self.config.aspect,
            )
            result = response["result"]
            self.report.aspect_check_ms += result["aspectCheckMs"]
            self.report.create_record_ms += result["createRecordMs"]
            self.report.rows_migrated += result["rowsMigrated"]
            self.report.send_message_ms += result["sendMessageMs"]
            self.report.time_get_rows_ms += result["timeGetRowMs"]
            self.report.time_sql_query_ms += result["timeSqlQueryMs"]
            self.report.time_urn_ms += result["timeUrnMs"]
            self.report.default_aspects_created += result["defaultAspectsCreated"]
            self.report.ignored += result["ignored"]
            self.report.time_entity_registry_check_ms += result[
                "timeEntityRegistryCheckMs"
            ]
            self.report.last_aspect = result["lastAspect"]
            self.report.last_urn = result["lastUrn"]
            self._print_report()
            if result["rowsMigrated"] == self.config.batch_size:
                start += self.config.batch_size
            else:
                break
        yield from []
