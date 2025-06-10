from typing import Optional, Union

import pytest

from acryl_datahub_cloud.sdk.assertion_input import (
    DEFAULT_SCHEDULE,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, DataPlatformUrn, DatasetUrn, MonitorUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity_client import EntityClient


class DummyDataHubClient:
    def __init__(self) -> None:
        pass


_any_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
_assertion_id = "smart_freshness_assertion"


@pytest.fixture
def any_dataset_urn() -> DatasetUrn:
    return DatasetUrn.from_string(_any_dataset_urn)


@pytest.fixture
def any_monitor_urn(any_dataset_urn: DatasetUrn) -> MonitorUrn:
    return MonitorUrn(
        entity=any_dataset_urn,
        id=f"urn:li:assertion:{_assertion_id}",
    )


@pytest.fixture
def any_assertion_urn() -> AssertionUrn:
    return AssertionUrn(f"urn:li:assertion:{_assertion_id}")


@pytest.fixture
def monitor_with_all_fields(
    any_monitor_urn: MonitorUrn, any_assertion_urn: AssertionUrn
) -> Monitor:
    """A monitor with all fields set."""
    return Monitor(
        id=any_monitor_urn,
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=str(any_assertion_urn),
                        schedule=models.CronScheduleClass(
                            cron=DEFAULT_SCHEDULE.cron,
                            timezone=DEFAULT_SCHEDULE.timezone,
                        ),
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
                            datasetFreshnessParameters=models.DatasetFreshnessAssertionParametersClass(
                                sourceType=models.DatasetFreshnessSourceTypeClass.FIELD_VALUE,
                                field=models.FreshnessFieldSpecClass(
                                    path="field",
                                    type="string",
                                    nativeType="string",
                                    kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
                                ),
                            ),
                        ),
                    )
                ],
                settings=models.AssertionMonitorSettingsClass(
                    adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                        exclusionWindows=[
                            models.AssertionExclusionWindowClass(
                                type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                                fixedRange=models.AbsoluteTimeWindowClass(
                                    startTimeMillis=1609459200000,  # 2021-01-01 00:00:00 UTC
                                    endTimeMillis=1609545600000,  # 2021-01-02 00:00:00 UTC
                                ),
                            ),
                        ],
                        trainingDataLookbackWindowDays=99,  # To differentiate from the default value
                        sensitivity=models.AssertionMonitorSensitivityClass(
                            level=1,  # LOW
                        ),
                    ),
                ),
            ),
        ),
    )


@pytest.fixture
def assertion_entity_with_all_fields(any_assertion_urn: AssertionUrn) -> Assertion:
    return Assertion(
        id=any_assertion_urn,
        info=models.FreshnessAssertionInfoClass(
            type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
            entity=_any_dataset_urn,
        ),
        description="Smart Freshness Assertion",
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.NATIVE,
            created=models.AuditStampClass(
                actor="urn:li:corpuser:acryl-cloud-user-created",
                time=1609459200000,  # 2021-01-01 00:00:00 UTC
            ),
        ),
        last_updated=models.AuditStampClass(
            actor="urn:li:corpuser:acryl-cloud-user-updated",
            time=1609545600000,  # 2021-01-02 00:00:00 UTC
        ),
        tags=[
            models.TagAssociationClass(
                tag="urn:li:tag:smart_freshness_assertion_tag",
            )
        ],
        on_failure=[
            models.AssertionActionClass(
                type=models.AssertionActionTypeClass.RAISE_INCIDENT,
            )
        ],
        on_success=[
            models.AssertionActionClass(
                type=models.AssertionActionTypeClass.RESOLVE_INCIDENT,
            )
        ],
    )


class StubEntityClient(EntityClient):
    def __init__(
        self,
        monitor_entity: Optional[Monitor] = None,
        assertion_entity: Optional[Assertion] = None,
    ) -> None:
        self.monitor_entity = monitor_entity
        self.assertion_entity = assertion_entity
        # Initialize with a dummy client since we're just stubbing the behavior
        super().__init__(client=DummyDataHubClient())  # type: ignore[arg-type]  # DummyDataHubClient is a stub

    def get(  # type: ignore[override]  # Stubbed
        self, urn: Union[DatasetUrn, AssertionUrn, MonitorUrn]
    ) -> Optional[Union[Dataset, Assertion, Monitor]]:
        if (
            isinstance(urn, DatasetUrn)
            and str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
        ):
            return Dataset(
                platform=str(DataPlatformUrn("urn:li:dataPlatform:snowflake")),
                name=urn.name,
                schema=[
                    models.SchemaFieldClass(
                        fieldPath="last_modified",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.DateTypeClass()
                        ),
                        nativeDataType="DATE",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="last_modified_wrong_type",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NumberTypeClass()
                        ),
                        nativeDataType="NUMBER",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="wrong_type_int_as_a_string",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR",
                    ),
                ],
            )
        elif isinstance(urn, AssertionUrn):
            return self.assertion_entity
        elif isinstance(urn, MonitorUrn):
            return self.monitor_entity
        return None


@pytest.fixture
def stub_entity_client(
    monitor_with_all_fields: Monitor, assertion_entity_with_all_fields: Assertion
) -> StubEntityClient:
    return StubEntityClient(
        monitor_entity=monitor_with_all_fields,
        assertion_entity=assertion_entity_with_all_fields,
    )


class StubDataHubClient:
    def __init__(self, entity_client: Optional[StubEntityClient] = None) -> None:
        self.entities = entity_client or StubEntityClient()


@pytest.fixture
def stub_datahub_client(stub_entity_client: StubEntityClient) -> StubDataHubClient:
    return StubDataHubClient(entity_client=stub_entity_client)
