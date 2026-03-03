import json
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pydantic import BaseModel, field_validator

from datahub.emitter.mce_builder import (
    make_assertion_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
)
from datahub.utilities.time import datetime_to_ts_millis

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeExternalDmfKey(DatahubKey):
    """Key for generating deterministic GUIDs for external Snowflake DMFs.

    Uses Snowflake's REFERENCE_ID which uniquely identifies the
    DMF-table-column association.
    """

    platform: str = "snowflake"
    reference_id: str
    instance: Optional[str] = None


class DataQualityMonitoringResult(BaseModel):
    MEASUREMENT_TIME: datetime
    METRIC_NAME: str
    TABLE_NAME: str
    TABLE_SCHEMA: str
    TABLE_DATABASE: str
    VALUE: int
    REFERENCE_ID: str
    ARGUMENT_NAMES: List[str]

    @field_validator("ARGUMENT_NAMES", mode="before")
    @classmethod
    def parse_argument_names(cls, v: object) -> List[str]:
        """Parse ARGUMENT_NAMES from JSON string.

        Snowflake returns this column as a JSON-encoded string like '["col1", "col2"]'.
        """
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                logger.debug(f"Failed to parse ARGUMENT_NAMES as JSON: {v}")
        return []


class SnowflakeAssertionsHandler:
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        identifiers: SnowflakeIdentifierBuilder,
    ) -> None:
        self.config = config
        self.report = report
        self.connection = connection
        self.identifiers = identifiers
        self._urns_processed: List[str] = []

    def get_assertion_workunits(
        self, discovered_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        include_external = self.config.include_externally_managed_dmfs

        cur = self.connection.query(
            SnowflakeQuery.dmf_assertion_results(
                datetime_to_ts_millis(self.config.start_time),
                datetime_to_ts_millis(self.config.end_time),
                include_external=include_external,
            )
        )
        for db_row in cur:
            workunits = self._process_result_row(db_row, discovered_datasets)
            for wu in workunits:
                yield wu

    def _gen_platform_instance_wu(self, urn: str) -> MetadataWorkUnit:
        # Construct a MetadataChangeProposalWrapper object for assertion platform
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DataPlatformInstance(
                platform=make_data_platform_urn(self.identifiers.platform),
                instance=(
                    make_dataplatform_instance_urn(
                        self.identifiers.platform, self.config.platform_instance
                    )
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit(is_primary_source=False)

    def _generate_external_dmf_guid(self, result: DataQualityMonitoringResult) -> str:
        """Generate a stable, deterministic GUID for external DMFs."""
        key = SnowflakeExternalDmfKey(
            reference_id=result.REFERENCE_ID,
            instance=self.config.platform_instance,
        )
        return key.guid()

    def _create_assertion_info_workunit(
        self,
        assertion_urn: str,
        dataset_urn: str,
        dmf_name: str,
        argument_names: List[str],
        reference_id: str,
    ) -> MetadataWorkUnit:
        """Create AssertionInfo for external DMFs."""
        # Field URN is only set for single-column DMFs. Multi-column DMFs are
        # treated as table-level assertions with columns stored in custom properties.
        field_urn: Optional[str] = None
        if argument_names and len(argument_names) == 1:
            field_urn = make_schema_field_urn(dataset_urn, argument_names[0])

        custom_properties: Dict[str, str] = {
            "snowflake_dmf_name": dmf_name,
            "snowflake_reference_id": reference_id,
        }
        # Store all columns in custom properties regardless of count
        if argument_names:
            custom_properties["snowflake_dmf_columns"] = ",".join(argument_names)

        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            customAssertion=CustomAssertionInfoClass(
                type="Snowflake Data Metric Function",
                entity=dataset_urn,
                field=field_urn,
            ),
            source=AssertionSourceClass(
                type=AssertionSourceTypeClass.EXTERNAL,
            ),
            description=f"External Snowflake DMF: {dmf_name}",
            customProperties=custom_properties,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit(is_primary_source=False)

    def _process_result_row(
        self, result_row: dict, discovered_datasets: List[str]
    ) -> List[MetadataWorkUnit]:
        """Process a single DMF result row. Returns list of workunits."""
        workunits: List[MetadataWorkUnit] = []

        try:
            result = DataQualityMonitoringResult.model_validate(result_row)

            is_datahub_dmf = result.METRIC_NAME.lower().startswith("datahub__")

            if is_datahub_dmf:
                assertion_guid = result.METRIC_NAME.split("__")[-1].lower()
            else:
                assertion_guid = self._generate_external_dmf_guid(result)

            assertion_urn = make_assertion_urn(assertion_guid)

            assertee = self.identifiers.get_dataset_identifier(
                result.TABLE_NAME, result.TABLE_SCHEMA, result.TABLE_DATABASE
            )
            if assertee not in discovered_datasets:
                return []

            dataset_urn = self.identifiers.gen_dataset_urn(assertee)

            if result.VALUE == 1:
                result_type = AssertionResultType.SUCCESS
            elif result.VALUE == 0:
                result_type = AssertionResultType.FAILURE
            else:
                result_type = AssertionResultType.ERROR
                logger.warning(
                    f"DMF '{result.METRIC_NAME}' returned invalid value {result.VALUE}. "
                    "Expected 1 (pass) or 0 (fail). Marking as ERROR."
                )

            if not is_datahub_dmf and assertion_urn not in self._urns_processed:
                assertion_info_wu = self._create_assertion_info_workunit(
                    assertion_urn=assertion_urn,
                    dataset_urn=dataset_urn,
                    dmf_name=result.METRIC_NAME,
                    argument_names=result.ARGUMENT_NAMES,
                    reference_id=result.REFERENCE_ID,
                )
                workunits.append(assertion_info_wu)

            run_event_mcp = MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionRunEvent(
                    timestampMillis=datetime_to_ts_millis(result.MEASUREMENT_TIME),
                    runId=result.MEASUREMENT_TIME.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    asserteeUrn=dataset_urn,
                    status=AssertionRunStatus.COMPLETE,
                    assertionUrn=assertion_urn,
                    result=AssertionResult(type=result_type),
                ),
            )
            workunits.append(run_event_mcp.as_workunit(is_primary_source=False))

            if assertion_urn not in self._urns_processed:
                self._urns_processed.append(assertion_urn)
                workunits.append(self._gen_platform_instance_wu(assertion_urn))

            return workunits

        except Exception as e:
            self.report.report_warning("assertion-result-parse-failure", str(e))
            return []
