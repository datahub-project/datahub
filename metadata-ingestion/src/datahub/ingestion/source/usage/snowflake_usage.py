import collections
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union, cast

import pydantic.dataclasses
from more_itertools import partition
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import get_time_bucket
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.usage_common_state import BaseUsageCheckpointState
from datahub.ingestion.source.usage.usage_common import GenericAggregatedDataset
from datahub.ingestion.source_config.usage.snowflake_usage import SnowflakeUsageConfig
from datahub.ingestion.source_report.usage.snowflake_usage import SnowflakeUsageReport
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    JobStatusClass,
    OperationClass,
    OperationTypeClass,
    TimeWindowSizeClass,
)
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

SnowflakeTableRef = str
AggregatedDataset = GenericAggregatedDataset[SnowflakeTableRef]
AggregatedAccessEvents = Dict[datetime, Dict[SnowflakeTableRef, AggregatedDataset]]

SNOWFLAKE_USAGE_SQL_TEMPLATE = """
SELECT
    -- access_history.query_id, -- only for debugging purposes
    access_history.query_start_time,
    query_history.query_text,
    query_history.query_type,
    query_history.rows_inserted,
    query_history.rows_updated,
    query_history.rows_deleted,
    access_history.base_objects_accessed,
    access_history.direct_objects_accessed, -- when dealing with views, direct objects will show the view while base will show the underlying table
    -- query_history.execution_status, -- not really necessary, but should equal "SUCCESS"
    -- query_history.warehouse_name,
    access_history.user_name,
    users.first_name,
    users.last_name,
    users.display_name,
    users.email,
    query_history.role_name
FROM
    snowflake.account_usage.access_history access_history
LEFT JOIN
    snowflake.account_usage.query_history query_history
    ON access_history.query_id = query_history.query_id
LEFT JOIN
    snowflake.account_usage.users users
    ON access_history.user_name = users.name
WHERE   query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
ORDER BY query_start_time DESC
;
""".strip()

OPERATION_STATEMENT_TYPES = {
    "INSERT": OperationTypeClass.INSERT,
    "UPDATE": OperationTypeClass.UPDATE,
    "DELETE": OperationTypeClass.DELETE,
    "CREATE": OperationTypeClass.CREATE,
    "CREATE_TABLE": OperationTypeClass.CREATE,
    "CREATE_TABLE_AS_SELECT": OperationTypeClass.CREATE,
    "CREATE_SCHEMA": OperationTypeClass.CREATE,
}


@pydantic.dataclasses.dataclass
class SnowflakeColumnReference:
    columnId: int
    columnName: str


class PermissiveModel(BaseModel):
    class Config:
        extra = "allow"


class SnowflakeObjectAccessEntry(PermissiveModel):
    columns: Optional[List[SnowflakeColumnReference]]
    objectDomain: str
    objectId: int
    objectName: str
    stageKind: Optional[str]


class SnowflakeJoinedAccessEvent(PermissiveModel):
    query_start_time: datetime
    query_text: str
    query_type: str
    rows_inserted: Optional[int]
    rows_updated: Optional[int]
    rows_deleted: Optional[int]
    base_objects_accessed: List[SnowflakeObjectAccessEntry]
    direct_objects_accessed: List[SnowflakeObjectAccessEntry]

    user_name: str
    first_name: Optional[str]
    last_name: Optional[str]
    display_name: Optional[str]
    email: str
    role_name: str


@platform_name("Snowflake")
@support_status(SupportStatus.CERTIFIED)
@config_class(SnowflakeUsageConfig)
class SnowflakeUsageSource(StatefulIngestionSourceBase):
    def __init__(self, config: SnowflakeUsageConfig, ctx: PipelineContext):
        super(SnowflakeUsageSource, self).__init__(config, ctx)
        self.config: SnowflakeUsageConfig = config
        self.report: SnowflakeUsageReport = SnowflakeUsageReport()
        self.should_skip_this_run = self._should_skip_this_run()

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeUsageConfig.parse_obj(config_dict)
        return cls(config, ctx)

    # Stateful Ingestion Overrides.
    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if job_id == self.get_default_ingestion_job_id():
            assert self.config.stateful_ingestion
            return self.config.stateful_ingestion.enabled
        return False

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name for snowflake_usage.
        """
        return JobId("snowflake_usage_ingestion")

    def get_platform_instance_id(self) -> str:
        return self.config.get_account()

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create the custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                state=BaseUsageCheckpointState(
                    begin_timestamp_millis=int(
                        self.config.start_time.timestamp() * 1000
                    ),
                    end_timestamp_millis=int(self.config.end_time.timestamp() * 1000),
                ),
            )
        return None

    def _should_skip_this_run(self) -> bool:
        # Check if forced rerun.
        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.ignore_old_state
        ):
            return False
        # Determine from the last check point state
        last_successful_pipeline_run_end_time_millis: Optional[int] = None
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), BaseUsageCheckpointState
        )
        if last_checkpoint and last_checkpoint.state:
            state = cast(BaseUsageCheckpointState, last_checkpoint.state)
            last_successful_pipeline_run_end_time_millis = state.end_timestamp_millis

        if (
            last_successful_pipeline_run_end_time_millis is not None
            and int(self.config.start_time.timestamp() * 1000)
            <= last_successful_pipeline_run_end_time_millis
        ):
            warn_msg = (
                f"Skippig this run, since the last run's bucket duration end: "
                f"{datetime.fromtimestamp(last_successful_pipeline_run_end_time_millis/1000, tz=timezone.utc)}"
                f" is later than the current start_time: {self.config.start_time}"
            )
            logger.warning(warn_msg)
            self.report.report_warning("skip-run", warn_msg)
            return True
        return False

    def _get_last_successful_run_end(self) -> Optional[int]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), BaseUsageCheckpointState
        )
        if last_checkpoint and last_checkpoint.state:
            state = cast(BaseUsageCheckpointState, last_checkpoint.state)
            return state.end_timestamp_millis
        return None

    def _init_checkpoints(self):
        self.get_current_checkpoint(self.get_default_ingestion_job_id())

    def update_default_job_summary(self) -> None:
        summary = self.get_job_run_summary(self.get_default_ingestion_job_id())
        if summary is not None:
            summary.runStatus = (
                JobStatusClass.SKIPPED
                if self.should_skip_this_run
                else JobStatusClass.COMPLETED
            )
            summary.messageId = datetime.now().strftime("%m-%d-%Y,%H:%M:%S")
            summary.eventGranularity = TimeWindowSizeClass(
                unit=self.config.bucket_duration, multiple=1
            )
            summary.numWarnings = len(self.report.warnings)
            summary.numErrors = len(self.report.failures)
            summary.numEntities = self.report.workunits_produced
            summary.config = self.config.json()
            summary.custom_summary = self.report.as_string()

    def check_email_domain_missing(self) -> Any:
        if self.config.email_domain is not None and self.config.email_domain != "":
            return

        self.warn(
            logger,
            "missing-email-domain",
            "User's without email address will be ignored from usage if you don't set email_domain property",
        )

    def add_config_to_report(self):
        self.report.start_time = self.config.start_time
        self.report.end_time = self.config.end_time

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.add_config_to_report()
        self.check_email_domain_missing()
        if not self.should_skip_this_run:
            # Initialize the checkpoints
            self._init_checkpoints()
            # Generate the workunits.
            access_events = self._get_snowflake_history()
            aggregated_info_items_raw, operation_aspect_work_units_raw = partition(
                lambda x: isinstance(x, MetadataWorkUnit),
                self._aggregate_access_events(access_events),
            )
            for wu in cast(Iterable[MetadataWorkUnit], operation_aspect_work_units_raw):
                self.report.report_workunit(wu)
                yield wu
            aggregated_info_items = list(aggregated_info_items_raw)
            assert len(aggregated_info_items) == 1

            for time_bucket in cast(
                AggregatedAccessEvents, aggregated_info_items[0]
            ).values():
                for aggregate in time_bucket.values():
                    wu = self._make_usage_stat(aggregate)
                    self.report.report_workunit(wu)
                    yield wu

    def _make_usage_query(self) -> str:
        start_time = int(self.config.start_time.timestamp() * 1000)
        end_time = int(self.config.end_time.timestamp() * 1000)
        return SNOWFLAKE_USAGE_SQL_TEMPLATE.format(
            start_time_millis=start_time,
            end_time_millis=end_time,
        )

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(
            url,
            **self.config.get_options(),
        )
        return engine

    def _check_usage_date_ranges(self, engine: Engine) -> Any:

        query = """
            select
                min(query_start_time) as min_time,
                max(query_start_time) as max_time
            from snowflake.account_usage.access_history
        """
        with PerfTimer() as timer:
            try:
                for db_row in engine.execute(query):
                    if len(db_row) < 2 or db_row[0] is None or db_row[1] is None:
                        self.warn(
                            logger,
                            "check-usage-data",
                            f"Missing data for access_history {db_row} - Check if using Enterprise edition of Snowflake",
                        )
                        continue
                    self.report.min_access_history_time = db_row[0].astimezone(
                        tz=timezone.utc
                    )
                    self.report.max_access_history_time = db_row[1].astimezone(
                        tz=timezone.utc
                    )
                    self.report.access_history_range_query_secs = round(
                        timer.elapsed_seconds(), 2
                    )
            except Exception as e:
                self.error(logger, "check-usage-data", f"Error was {e}")

    def _is_unsupported_object_accessed(self, obj: Dict[str, Any]) -> bool:
        unsupported_keys = ["locations"]

        if obj.get("objectDomain") in ["Stage"]:
            return True

        return any([obj.get(key) is not None for key in unsupported_keys])

    def _is_object_valid(self, obj: Dict[str, Any]) -> bool:
        if self._is_unsupported_object_accessed(
            obj
        ) or not self._is_dataset_pattern_allowed(
            obj.get("objectName"), obj.get("objectDomain")
        ):
            return False
        return True

    def _is_dataset_pattern_allowed(
        self, dataset_name: Optional[Any], dataset_type: Optional[Any]
    ) -> bool:
        if not dataset_type or not dataset_name:
            return True
        dataset_params = dataset_name.split(".")
        if len(dataset_params) != 3:
            self.warn(
                logger,
                "invalid-dataset-pattern",
                f"Found {dataset_params} of type {dataset_type}",
            )
            return False
        if not self.config.database_pattern.allowed(
            dataset_params[0]
        ) or not self.config.schema_pattern.allowed(dataset_params[1]):
            return False

        if dataset_type.lower() in {"table"} and not self.config.table_pattern.allowed(
            dataset_params[2]
        ):
            return False

        if dataset_type.lower() in {
            "view",
            "materialized_view",
        } and not self.config.view_pattern.allowed(dataset_params[2]):
            return False

        return True

    def _process_snowflake_history_row(
        self, row: Any
    ) -> Iterable[SnowflakeJoinedAccessEvent]:
        self.report.rows_processed += 1
        # Make some minor type conversions.
        if hasattr(row, "_asdict"):
            # Compat with SQLAlchemy 1.3 and 1.4
            # See https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#rowproxy-is-no-longer-a-proxy-is-now-called-row-and-behaves-like-an-enhanced-named-tuple.
            event_dict = row._asdict()
        else:
            event_dict = dict(row)

        # no use processing events that don't have a query text
        if not event_dict["query_text"]:
            self.report.rows_missing_query_text += 1
            return

        event_dict["base_objects_accessed"] = [
            obj
            for obj in json.loads(event_dict["base_objects_accessed"])
            if self._is_object_valid(obj)
        ]
        if len(event_dict["base_objects_accessed"]) == 0:
            self.report.rows_zero_base_objects_accessed += 1

        event_dict["direct_objects_accessed"] = [
            obj
            for obj in json.loads(event_dict["direct_objects_accessed"])
            if self._is_object_valid(obj)
        ]
        if len(event_dict["direct_objects_accessed"]) == 0:
            self.report.rows_zero_direct_objects_accessed += 1

        event_dict["query_start_time"] = (event_dict["query_start_time"]).astimezone(
            tz=timezone.utc
        )

        if not event_dict["email"] and self.config.email_domain:
            if not event_dict["user_name"]:
                self.report.report_warning("user-name-miss", f"Missing in {event_dict}")
                logger.warning(
                    f"The user_name is missing from {event_dict}. Skipping ...."
                )
                self.report.rows_missing_email += 1
                return

            event_dict[
                "email"
            ] = f'{event_dict["user_name"]}@{self.config.email_domain}'.lower()

        try:  # big hammer try block to ensure we don't fail on parsing events
            event = SnowflakeJoinedAccessEvent(**event_dict)
            yield event
        except Exception as e:
            self.report.rows_parsing_error += 1
            self.warn(logger, "usage", f"Failed to parse usage line {event_dict}, {e}")

    def _get_snowflake_history(self) -> Iterable[SnowflakeJoinedAccessEvent]:
        engine = self._make_sql_engine()

        logger.info("Checking usage date ranges")
        self._check_usage_date_ranges(engine)

        if (
            self.report.min_access_history_time is None
            or self.report.max_access_history_time is None
        ):
            return

        logger.info("Getting usage history")
        with PerfTimer() as timer:
            query = self._make_usage_query()
            results = engine.execute(query)
            self.report.access_history_query_secs = round(timer.elapsed_seconds(), 2)

        for row in results:
            yield from self._process_snowflake_history_row(row)

    def _get_operation_aspect_work_unit(
        self, event: SnowflakeJoinedAccessEvent
    ) -> Iterable[MetadataWorkUnit]:
        if event.query_start_time and event.query_type in OPERATION_STATEMENT_TYPES:
            start_time = event.query_start_time
            query_type = event.query_type
            user_email = event.email
            operation_type = OPERATION_STATEMENT_TYPES[query_type]
            reported_time: int = int(time.time() * 1000)
            last_updated_timestamp: int = int(start_time.timestamp() * 1000)
            user_urn = builder.make_user_urn(user_email.split("@")[0])
            for obj in event.base_objects_accessed:
                resource = obj.objectName
                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    "snowflake",
                    resource.lower(),
                    self.config.platform_instance,
                    self.config.env,
                )
                operation_aspect = OperationClass(
                    timestampMillis=reported_time,
                    lastUpdatedTimestamp=last_updated_timestamp,
                    actor=user_urn,
                    operationType=operation_type,
                )
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    aspectName="operation",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspect=operation_aspect,
                )
                wu = MetadataWorkUnit(
                    id=f"{start_time.isoformat()}-operation-aspect-{resource}",
                    mcp=mcp,
                )
                yield wu

    def _aggregate_access_events(
        self, events: Iterable[SnowflakeJoinedAccessEvent]
    ) -> Iterable[Union[AggregatedAccessEvents, MetadataWorkUnit]]:
        """
        Emits aggregated access events combined with operational workunits from the events.
        """
        datasets: AggregatedAccessEvents = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(
                event.query_start_time, self.config.bucket_duration
            )

            accessed_data = (
                event.base_objects_accessed
                if self.config.apply_view_usage_to_tables
                else event.direct_objects_accessed
            )
            for object in accessed_data:
                resource = object.objectName
                agg_bucket = datasets[floored_ts].setdefault(
                    resource,
                    AggregatedDataset(
                        bucket_start_time=floored_ts,
                        resource=resource,
                        user_email_pattern=self.config.user_email_pattern,
                    ),
                )
                agg_bucket.add_read_entry(
                    event.email,
                    event.query_text,
                    [colRef.columnName.lower() for colRef in object.columns]
                    if object.columns is not None
                    else [],
                )
            if self.config.include_operational_stats:
                yield from self._get_operation_aspect_work_unit(event)

        yield datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: builder.make_dataset_urn_with_platform_instance(
                "snowflake",
                resource.lower(),
                self.config.platform_instance,
                self.config.env,
            ),
            self.config.top_n_queries,
            self.config.format_sql_queries,
            self.config.include_top_n_queries,
        )

    def get_report(self):
        return self.report

    def close(self):
        self.update_default_job_summary()
        self.prepare_for_commit()
