import logging
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple, Union

from datahub.utilities.urns.urn import Urn
from tenacity import retry, stop_after_attempt, wait_exponential

from datahub_monitors.assertion.engine.evaluator.filter_builder import FilterBuilder
from datahub_monitors.connection.connection import Connection
from datahub_monitors.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
)
from datahub_monitors.types import EntityEvent, EntityEventType

from .types import SourceOperationParams

logger = logging.getLogger(__name__)


class Source:
    """Base class for a connector responsible for fetching information from external sources. Parallel concept to a normal ingestion source."""

    connection: Connection
    source_name: str

    def __init__(self, connection: Connection):
        self.connection = connection

    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        raise NotImplementedError()

    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        raise NotImplementedError()

    def _get_field_last_updated_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        raise NotImplementedError()

    def _get_supported_high_watermark_column_types(self) -> List[str]:
        raise NotImplementedError()

    def _get_high_watermark_field_value(
        self,
        column_name: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        previous_value: Optional[str],
    ) -> Optional[str]:
        raise NotImplementedError()

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> int:
        raise NotImplementedError()

    def _get_high_watermark_for_column(
        self,
        operation_params: SourceOperationParams,
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        if (
            "path" in parameters
            and "type" in parameters
            and "native_type" in parameters
        ):
            column_name = parameters["path"]
            column_type = parameters["native_type"]
            filter_sql = FilterBuilder(parameters.get("filter")).get_sql()

            if (
                column_type.upper()
                not in self._get_supported_high_watermark_column_types()
            ):
                raise InvalidParametersException(
                    message=f"Unsupported high watermark column {column_type} provided. Failing assertion evaluation!",
                    parameters=parameters,
                )

            current_field_value = self._get_high_watermark_field_value(
                column_name, operation_params, filter_sql, previous_value
            )
            if current_field_value is None:
                return ("", 0)

            current_row_count = self._get_high_watermark_row_count(
                column_name, operation_params, filter_sql, current_field_value
            )
            return (str(current_field_value), current_row_count)

        raise InvalidParametersException(
            message="Missing required inputs: column path and column type.",
            parameters=parameters,
        )

    def _get_operation_params(
        self, entity_urn: str, window: List[int]
    ) -> SourceOperationParams:
        urn_obj = Urn.create_from_string(entity_urn)
        dataset_name = urn_obj.get_entity_id()[1].lower()
        dataset_name_parts = dataset_name.split(".")

        if len(dataset_name_parts) > 3:
            # Handle platform instance.
            dataset_name_parts = dataset_name_parts[:3]

        return SourceOperationParams(
            start_time_millis=window[0],
            end_time_millis=window[1],
            dataset_part_0=dataset_name_parts[0],
            dataset_part_1=dataset_name_parts[1],
            dataset_part_2=dataset_name_parts[2],
        )

    def _build_field_update_results(
        self, dates: List[Union[date, datetime]]
    ) -> List[EntityEvent]:
        results = []

        for datetime_obj in dates:
            # Check whether we are dealing with a date object (without any time)
            # If yes, convert it.
            if isinstance(datetime_obj, date) and not isinstance(
                datetime_obj, datetime
            ):
                datetime_obj = datetime.combine(datetime_obj, datetime.min.time())

            datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)

            # Convert to timestamp ms
            timestamp = int(datetime_obj.timestamp() * 1000)

            entity_event = EntityEvent(EntityEventType.FIELD_UPDATE, timestamp)
            results.append(entity_event)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
    )
    def _try_get_entity_events(
        self,
        event_type: EntityEventType,
        operation_params: SourceOperationParams,
        parameters: dict,
    ) -> List[EntityEvent]:
        if event_type == EntityEventType.AUDIT_LOG_OPERATION:
            # Scan the audit log to see if there are any events falling into the previous window.
            return self._get_audit_log_operation_events(operation_params, parameters)
        elif event_type == EntityEventType.INFORMATION_SCHEMA_UPDATE:
            # Hit something else!
            return self._get_dataset_last_updated_events(operation_params)
        elif event_type == EntityEventType.FIELD_UPDATE:
            # Build and issue a query!
            return self._get_field_last_updated_events(operation_params, parameters)
        else:
            raise InvalidSourceTypeException(
                message=f"Unsupported entity event type {event_type} provided. {self.source_name} connector does not support retrieving these events.",
                source_type=event_type,
            )

    def get_entity_events(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
    ) -> List[EntityEvent]:
        operation_params = self._get_operation_params(entity_urn, window)
        return self._try_get_entity_events(event_type, operation_params, parameters)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
    )
    def _try_get_current_high_watermark_for_column(
        self,
        event_type: EntityEventType,
        operation_params: SourceOperationParams,
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        if event_type == EntityEventType.FIELD_UPDATE:
            return self._get_high_watermark_for_column(
                operation_params, parameters, previous_value
            )
        else:
            raise InvalidSourceTypeException(
                message=f"Unsupported entity event type {event_type} provided. {self.source_name} connector does not support retrieving these events.",
                source_type=event_type,
            )

    def get_current_high_watermark_for_column(
        self,
        entity_urn: str,
        event_type: EntityEventType,
        window: List[int],
        parameters: dict,
        previous_value: Optional[str],
    ) -> Tuple[str, int]:
        operation_params = self._get_operation_params(entity_urn, window)
        return self._try_get_current_high_watermark_for_column(
            event_type, operation_params, parameters, previous_value
        )
