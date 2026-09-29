import json
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pydantic import BaseModel, field_validator

from datahub.emitter.mce_builder import (
    make_assertion_source,
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
    AssertionTypeClass,
    CustomAssertionInfoClass,
    StatusClass,
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


class DataMetricFunctionReference(BaseModel):
    """A current DMF-to-table association from DATA_METRIC_FUNCTION_REFERENCES."""

    METRIC_NAME: str
    REF_DATABASE_NAME: str
    REF_SCHEMA_NAME: str
    REF_ENTITY_NAME: str
    REF_ID: str
    REF_ARGUMENTS: List[str]

    @field_validator("REF_ARGUMENTS", mode="before")
    @classmethod
    def parse_ref_arguments(cls, v: object) -> List[str]:
        """Extract argument names from REF_ARGUMENTS.

        Snowflake returns this ARRAY column as a JSON-encoded string of objects
        like '[{"domain": "COLUMN", "name": "col1", ...}]'.
        """
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                logger.debug(f"Failed to parse REF_ARGUMENTS as JSON: {v}")
                return []
        if not isinstance(v, list):
            return []
        return [
            arg["name"] if isinstance(arg, dict) else str(arg)
            for arg in v
            if not isinstance(arg, dict) or "name" in arg
        ]


def _is_datahub_dmf(metric_name: str) -> bool:
    return metric_name.lower().startswith("datahub__")


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

        if include_external:
            yield from self._gen_external_dmf_definition_workunits(discovered_datasets)

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

    def _gen_external_dmf_definition_workunits(
        self, discovered_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit external DMF definitions from the association listing.

        The connector owns these assertions, so they are emitted as primary and
        tracked by stateful ingestion: a DMF removed in Snowflake disappears from
        the listing and gets soft-deleted. Emitting them from result rows instead
        would soft-delete any DMF that simply did not run inside the window.
        DataHub-compiled (datahub__*) DMFs are defined by DataHub, not by this
        connector, and are skipped here.
        """
        try:
            # Materialized up front so a mid-stream error cannot leave a partial
            # set of primary definitions, which would look like removals.
            references = [
                DataMetricFunctionReference.model_validate(row)
                for row in self.connection.query(SnowflakeQuery.dmf_references())
            ]
        except Exception as e:
            stale_removal = self.config.stateful_ingestion
            if (
                stale_removal is not None
                and stale_removal.enabled
                and stale_removal.remove_stale_metadata
            ):
                # A failure makes stale entity removal skip this run; otherwise
                # every tracked external DMF would be soft-deleted.
                self.report.failure(
                    message="Failed to list Snowflake DMF associations; stale "
                    "external DMF removal is skipped for this run",
                    context="dmf-references-query-failure",
                    exc=e,
                )
            else:
                self.report.warning(
                    message="Failed to list Snowflake DMF associations; external "
                    "DMF definitions are taken from results in the time window only",
                    context="dmf-references-query-failure",
                    exc=e,
                )
            return

        for ref in references:
            if _is_datahub_dmf(ref.METRIC_NAME):
                continue
            assertee = self.identifiers.get_dataset_identifier(
                ref.REF_ENTITY_NAME, ref.REF_SCHEMA_NAME, ref.REF_DATABASE_NAME
            )
            if assertee not in discovered_datasets:
                continue
            assertion_urn = make_assertion_urn(
                self._generate_external_dmf_guid(ref.REF_ID)
            )
            if assertion_urn in self._urns_processed:
                continue
            self._urns_processed.append(assertion_urn)
            yield self._create_assertion_info_workunit(
                assertion_urn=assertion_urn,
                dataset_urn=self.identifiers.gen_dataset_urn(assertee),
                dmf_name=ref.METRIC_NAME,
                argument_names=ref.REF_ARGUMENTS,
                reference_id=ref.REF_ID,
                is_primary_source=True,
            )
            yield self._gen_platform_instance_wu(assertion_urn, is_primary_source=True)
            # Explicit because auto-status skips any URN that also has a
            # non-primary workunit (the run events), which would leave a
            # previously soft-deleted DMF hidden after it reappears.
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=StatusClass(removed=False)
            ).as_workunit()

    def _gen_platform_instance_wu(
        self, urn: str, is_primary_source: bool = False
    ) -> MetadataWorkUnit:
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
        ).as_workunit(is_primary_source=is_primary_source)

    def _generate_external_dmf_guid(self, reference_id: str) -> str:
        """Generate a stable, deterministic GUID for external DMFs."""
        key = SnowflakeExternalDmfKey(
            reference_id=reference_id,
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
        is_primary_source: bool = False,
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
            source=make_assertion_source(),
            description=f"External Snowflake DMF: {dmf_name}",
            customProperties=custom_properties,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit(is_primary_source=is_primary_source)

    def _process_result_row(
        self, result_row: dict, discovered_datasets: List[str]
    ) -> List[MetadataWorkUnit]:
        """Process a single DMF result row. Returns list of workunits."""
        workunits: List[MetadataWorkUnit] = []

        try:
            result = DataQualityMonitoringResult.model_validate(result_row)

            is_datahub_dmf = _is_datahub_dmf(result.METRIC_NAME)

            if is_datahub_dmf:
                assertion_guid = result.METRIC_NAME.split("__")[-1].lower()
            else:
                assertion_guid = self._generate_external_dmf_guid(result.REFERENCE_ID)

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

            # Fallback for DMFs missing from the association listing (listing
            # latency or failure). Non-primary, so it never enters stale state.
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
            self.report.warning(
                message="Failed to parse assertion result",
                context="assertion-result-parse-failure",
                exc=e,
                log=False,
            )
            return []
