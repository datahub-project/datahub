from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud._sdk_extras.assertion import (
    AssertionMode,
    AssertionTypes,
    SmartFreshnessAssertion,
)
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    InferenceSensitivity,
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import TagsInputType
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.utilities.urns.urn import Urn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


# TODO: Import ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS from datahub_executor.config
ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS = 60

# TODO: Replace __datahub_system with the actual datahub system user https://linear.app/acryl-data/issue/OBS-1351/auditstamp-actor-hydration-pattern-for-sdk-calls
DEFAULT_CREATED_BY = CorpUserUrn.from_string("urn:li:corpuser:__datahub_system")


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
        tags: Optional[TagsInputType] = None,
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
                    "type": "high_watermark_column",
                    "column_name": "id",
                    "additional_filter": "id > 1000",
                } or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id', additional_filter='id > 1000')
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
            tags: The tags to be applied to the assertion.
                Valid values are:
                - a list of strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
        """
        _print_experimental_warning()
        # assertion_input = _SmartFreshnessAssertionInput(
        #     urn=urn,
        #     entity_client=self.client.entities,
        #     dataset_urn=dataset_urn,
        #     display_name=display_name,
        #     detection_mechanism=detection_mechanism,
        #     sensitivity=sensitivity,
        #     exclusion_windows=exclusion_windows,
        #     training_data_lookback_days=training_data_lookback_days,
        #     incident_behavior=incident_behavior,
        #     tags=tags,
        # )
        # TODO: In _AssertionInput, make sure the detection mechanism is a valid selectable option based on the assertion type
        # TODO: Create the Entities from the AssertionInput
        # assertion_entity = assertion_input.to_assertion_entity()
        # monitor_entity = assertion_input.to_monitor_entity()
        # TODO: Add the source / lastUpdated to the assertion and monitor entities (consider using _AssertionInput for this)
        # TODO: Call the DataHub API to upsert the assertion and monitor entities
        # TODO: Do this in a "transaction" i.e. if one fails we are not left in a half-baked state
        #   - can this be done in a single call?:
        # upserted_assertion = self.client.entity.upsert(assertion_to_upsert)
        # upserted_monitor = self.client.entity.upsert(monitor_to_upsert)
        # return SmartFreshnessAssertion.from_entities(assertion_entity, monitor_entity)  # TODO: Pass entities here

        # TODO: Remove the below placeholders once everything is connected and implemented:
        assert urn is not None, "URN is required"  # TODO: Placeholder, remove this
        assert dataset_urn is not None, (
            "Dataset URN is required"
        )  # TODO: Placeholder, remove this
        assert display_name is not None, (
            "Display name is required"
        )  # TODO: Placeholder, remove this
        assert detection_mechanism is not None, (
            "Detection mechanism is required"
        )  # TODO: Placeholder, remove this
        assert sensitivity is not None, (
            "Sensitivity is required"
        )  # TODO: Placeholder, remove this
        assert exclusion_windows is not None, (
            "Exclusion windows are required"
        )  # TODO: Placeholder, remove this
        assert training_data_lookback_days is not None, (
            "Training data lookback days are required"
        )  # TODO: Placeholder, remove this
        assert incident_behavior is not None, (
            "Incident behavior is required"
        )  # TODO: Placeholder, remove this
        return SmartFreshnessAssertion(  # TODO: Placeholder, remove this
            urn=AssertionUrn.from_string(urn),
            dataset_urn=DatasetUrn.from_string(dataset_urn),
            display_name=display_name,
            mode=AssertionMode.ACTIVE,
            detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
            sensitivity=InferenceSensitivity.LOW,
            exclusion_windows=[],
            training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
            incident_behavior=[],
            created_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
            created_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
            updated_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
            updated_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
            tags=[],
        )

    def create_smart_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        display_name: Union[str, None] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Union[str, InferenceSensitivity, None] = None,
        exclusion_windows: ExclusionWindowInputTypes = None,
        training_data_lookback_days: Union[int, None] = None,
        incident_behavior: Union[
            AssertionIncidentBehavior, list[AssertionIncidentBehavior], None
        ] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Union[str, CorpUserUrn, None] = None,
    ) -> SmartFreshnessAssertion:
        """
        Create a smart freshness assertion. Note keyword arguments are required.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            display_name: The display name of the assertion. If not provided, a random display name will be generated.
            detection_mechanism: The detection mechanism to be used for the assertion. Information schema is recommended.
                Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {
                    "type": "last_modified_column",
                    "column_name": "last_modified",
                    "additional_filter": "last_modified > '2021-01-01'",
                } or DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
                - {
                    "type": "high_watermark_column",
                    "column_name": "id",
                    "additional_filter": "id > 1000",
                } or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id', additional_filter='id > 1000')
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
            tags: The tags to be applied to the assertion.
                Valid values are:
                - a list of urn strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
            created_by: Optional urn of the user who created the assertion. The format is "urn:li:corpuser:<username>", which you can find on the Users & Groups page. The default is the datahub system user.  # TODO: Retrieve the SDK user as the default instead of the datahub system user.
        """
        _print_experimental_warning()
        created_at = datetime.now(timezone.utc)
        if created_by is None:
            created_by = DEFAULT_CREATED_BY
        assertion_input = _SmartFreshnessAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=created_by,
            created_at=created_at,
            updated_by=created_by,
            updated_at=created_at,
        )
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        # If assertion creation fails, we won't try to create the monitor
        self.client.entities.create(assertion_entity)
        # TODO: Wrap monitor creation in a try-except and delete the assertion if monitor creation fails (once delete is implemented https://linear.app/acryl-data/issue/OBS-1350/add-delete-method-to-entity-clientpy)
        # try:
        self.client.entities.create(monitor_entity)
        # except Exception as e:
        #     print(f"Error creating monitor: {e}")
        #     self.client.entities.delete(assertion_entity)
        #     raise e
        return SmartFreshnessAssertion.from_entities(assertion_entity, monitor_entity)


def _print_experimental_warning() -> None:
    print(
        "Warning: The assertions client is experimental and under heavy development. Expect breaking changes."
    )
