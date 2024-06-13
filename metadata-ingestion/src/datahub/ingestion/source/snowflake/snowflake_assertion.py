import logging
from datetime import datetime
from typing import Callable, Iterable, List, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import (
    make_assertion_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeConnectionMixin,
    SnowflakeQueryMixin,
)
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.utilities.time import datetime_to_ts_millis

logger: logging.Logger = logging.getLogger(__name__)


class DataQualityMonitoringResult(BaseModel):
    MEASUREMENT_TIME: datetime
    METRIC_NAME: str
    TABLE_NAME: str
    TABLE_SCHEMA: str
    TABLE_DATABASE: str
    VALUE: int


class SnowflakeAssertionsHandler(
    SnowflakeCommonMixin, SnowflakeQueryMixin, SnowflakeConnectionMixin
):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        dataset_urn_builder: Callable[[str], str],
    ) -> None:
        self.config = config
        self.report = report
        self.logger = logger
        self.dataset_urn_builder = dataset_urn_builder
        self.connection = None
        self._urns_processed: List[str] = []

    def get_assertion_workunits(
        self, discovered_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        self.connection = self.create_connection()
        if self.connection is None:
            return

        cur = self.query(
            SnowflakeQuery.dmf_assertion_results(
                datetime_to_ts_millis(self.config.start_time),
                datetime_to_ts_millis(self.config.end_time),
            )
        )
        for db_row in cur:
            mcp = self._process_result_row(db_row, discovered_datasets)
            if mcp:
                yield mcp.as_workunit(is_primary_source=False)

                if mcp.entityUrn and mcp.entityUrn not in self._urns_processed:
                    self._urns_processed.append(mcp.entityUrn)
                    yield self._gen_platform_instance_wu(mcp.entityUrn)

    def _gen_platform_instance_wu(self, urn: str) -> MetadataWorkUnit:
        # Construct a MetadataChangeProposalWrapper object for assertion platform
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DataPlatformInstance(
                platform=make_data_platform_urn(self.platform),
                instance=(
                    make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    )
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit(is_primary_source=False)

    def _process_result_row(
        self, result_row: dict, discovered_datasets: List[str]
    ) -> Optional[MetadataChangeProposalWrapper]:
        try:
            result = DataQualityMonitoringResult.parse_obj(result_row)
            assertion_guid = result.METRIC_NAME.split("__")[-1].lower()
            status = bool(result.VALUE)  # 1 if PASS, 0 if FAIL
            assertee = self.get_dataset_identifier(
                result.TABLE_NAME, result.TABLE_SCHEMA, result.TABLE_DATABASE
            )
            if assertee in discovered_datasets:
                return MetadataChangeProposalWrapper(
                    entityUrn=make_assertion_urn(assertion_guid),
                    aspect=AssertionRunEvent(
                        timestampMillis=datetime_to_ts_millis(result.MEASUREMENT_TIME),
                        runId=result.MEASUREMENT_TIME.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        asserteeUrn=self.dataset_urn_builder(assertee),
                        status=AssertionRunStatus.COMPLETE,
                        assertionUrn=make_assertion_urn(assertion_guid),
                        result=AssertionResult(
                            type=(
                                AssertionResultType.SUCCESS
                                if status
                                else AssertionResultType.FAILURE
                            )
                        ),
                    ),
                )
        except Exception as e:
            self.report.report_warning("assertion-result-parse-failure", str(e))
        return None
