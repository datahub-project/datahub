from typing import Dict, Optional, Tuple

from datahub_monitors.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
)
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationParameters,
    DatasetFreshnessSourceType,
    EntityEventType,
)


def get_filter_parameters(assertion: Assertion) -> Optional[Dict]:
    """
    Extracts filter information from SLA Assertion and returns it as a dictionary to use with entity_event parameters
    """
    freshness_assertion = assertion.freshness_assertion
    if freshness_assertion is not None and freshness_assertion.filter is not None:
        return freshness_assertion.filter.__dict__
    return None


def get_event_type_parameters_from_parameters(
    assertion: Assertion,
    parameters: AssertionEvaluationParameters,
) -> Tuple[EntityEventType, Dict]:
    """
    Extracts the standard event type, parameters from the AssertionEvaluationParameters
    These are for use in retrieving information using a Connection object to an external store.

    If these cannot be properly extracted, an exception will be raised.
    """
    # Extract the entity event type from the FRESHNESS assertion, along with additional filters / parameters that are required.
    if parameters.dataset_freshness_parameters is not None:
        # We are parsing a dataset FRESHNESS assertion
        dataset_freshness_parameters = parameters.dataset_freshness_parameters
        source_type = dataset_freshness_parameters.source_type
        if source_type == DatasetFreshnessSourceType.FIELD_VALUE:
            entity_event_type = EntityEventType.FIELD_UPDATE
            params = dataset_freshness_parameters.field.__dict__
            filter_params = get_filter_parameters(assertion)
            if filter_params:
                params["filter"] = filter_params

            return (entity_event_type, params)
        elif source_type == DatasetFreshnessSourceType.INFORMATION_SCHEMA:
            entity_event_type = EntityEventType.INFORMATION_SCHEMA_UPDATE
            return (entity_event_type, {})
        elif source_type == DatasetFreshnessSourceType.AUDIT_LOG:
            entity_event_type = EntityEventType.AUDIT_LOG_OPERATION
            params = dataset_freshness_parameters.audit_log.__dict__
            return (entity_event_type, params)
        else:
            raise InvalidSourceTypeException(
                message=f"Failed to extract EntityEntityEventType & Parameters from Dataset FRESHNESS Assertion. Unsupported source type found {source_type}",
                source_type=source_type,
            )

    raise InvalidParametersException(
        message="Failed to extract EntityEventType & Parameters from Dataset FRESHNESS Assertion. Malformed assertion type found. Missing dataset_freshness_parameters.",
        parameters=parameters.__dict__,
    )
