from __future__ import annotations

from typing import TYPE_CHECKING, Union

from acryl_datahub_cloud._sdk_extras.assertion import (
    AssertionTypes,
    SmartFreshnessAssertion,
)
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    InferenceSensitivity,
    _AssertionInput,
)
from datahub.metadata.urns import AssertionUrn, DatasetUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


# TODO: Import ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS from datahub_executor.config
ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS = 60


class AssertionsClient:
    def __init__(self, client: DataHubClient):
        self.client = client
        _print_experimental_warning()

    def get(
        self, urn: Union[str, list[str], AssertionUrn, list[AssertionUrn]]
    ) -> list[AssertionTypes]:
        _print_experimental_warning()
        print("get is not implemented, this is a placeholder. Returning empty list.")
        print(f"urn provided: {urn}")
        return []

    def upsert_smart_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn, None] = None,
        display_name: Union[str, None] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Union[str, InferenceSensitivity, None] = None,
        exclusion_windows: ExclusionWindowInputTypes = None,
        training_data_lookback_days: Union[int, None] = None,
        incident_behavior: Union[
            AssertionIncidentBehavior, list[AssertionIncidentBehavior], None
        ] = None,
    ) -> SmartFreshnessAssertion:
        """
        Upsert a smart freshness assertion. Note keyword arguments are required.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion. If not provided, a urn will be generated and the assertion will be _created_ in the DataHub instance (not updated).
            display_name: The display name of the assertion. If not provided, a random display name will be generated.
            detection_mechanism: The detection mechanism to be used for the assertion. Information schema is recommended.
                Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {
                    "type": "last_modified_column",
                    "column": "last_modified",
                    "additional_filter": "last_modified > '2021-01-01'",
                } or DetectionMechanism.LAST_MODIFIED_COLUMN(column='last_modified', additional_filter='last_modified > 2021-01-01')
                - {
                    "type": "high_water_mark_column",
                    "high_water_mark_column": "id",
                    "additional_filter": "id > 1000",
                } or DetectionMechanism.HIGH_WATER_MARK_COLUMN(high_water_mark_column='id', additional_filter='id > 1000')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            sensitivity: The sensitivity to be applied to the assertion.
                Valid values are:
                - "low" or InferenceSensitivity.LOW
                - "medium" or InferenceSensitivity.MEDIUM
                - "high" or InferenceSensitivity.HIGH
            exclusion_windows: The exclusion windows to be applied to the assertion, currently only fixed range exclusion windows are supported.
                Valid values are:
                - from datetime.datetime objects: {
                    "start": "datetime(2025, 1, 1, 0, 0, 0)",
                    "end": "datetime(2025, 1, 2, 0, 0, 0)",
                }
                - from string datetimes: {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-02T00:00:00",
                }
                - from FixedRangeExclusionWindow objects: FixedRangeExclusionWindow(start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0))
            training_data_lookback_days: The training data lookback days to be applied to the assertion as an integer.
            incident_behavior: The incident behavior to be applied to the assertion.
                Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
        """
        _print_experimental_warning()
        assertion_input = _AssertionInput(  # noqa: F841 - This will be used
            urn=urn,
            dataset_urn=dataset_urn,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
        )
        # TODO: Create the Entities from the AssertionInput
        # assertion_entity = assertion_input.to_assertion_entity()
        # monitor_entity = assertion_input.to_monitor_entity()
        # TODO: Call the DataHub API to upsert the assertion and monitor entities
        # upserted_assertion = self.client.entity.upsert(assertion_to_upsert)
        # upserted_monitor = self.client.entity.upsert(monitor_to_upsert)
        return SmartFreshnessAssertion.from_entities()  # TODO: Pass entities here


def _print_experimental_warning() -> None:
    print(
        "Warning: The assertions client is experimental and under heavy development. Expect breaking changes."
    )
