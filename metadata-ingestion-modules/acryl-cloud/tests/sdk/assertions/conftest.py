from typing import Optional, Union

import pytest

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
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
def freshness_monitor_with_all_fields(
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
def freshness_assertion_entity_with_all_fields(
    any_assertion_urn: AssertionUrn,
) -> Assertion:
    return Assertion(
        id=any_assertion_urn,
        info=models.FreshnessAssertionInfoClass(
            type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
            entity=_any_dataset_urn,
            schedule=models.FreshnessAssertionScheduleClass(
                type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
                cron=models.FreshnessCronScheduleClass(
                    cron=DEFAULT_SCHEDULE.cron,
                    timezone=DEFAULT_SCHEDULE.timezone,
                ),
            ),
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


@pytest.fixture
def volume_assertion_entity_with_all_fields(
    any_assertion_urn: AssertionUrn,
) -> Assertion:
    return Assertion(
        id=any_assertion_urn,
        info=models.VolumeAssertionInfoClass(
            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
            entity=_any_dataset_urn,
        ),
        description="Smart Volume Assertion",
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
                tag="urn:li:tag:smart_volume_assertion_tag",
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


@pytest.fixture
def volume_monitor_with_all_fields(
    any_monitor_urn: MonitorUrn, any_assertion_urn: AssertionUrn
) -> Monitor:
    """A monitor with all fields set for volume assertions."""
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
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
                            datasetVolumeParameters=models.DatasetVolumeAssertionParametersClass(
                                sourceType=models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA,
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
                    models.SchemaFieldClass(
                        fieldPath="high_watermark",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NumberTypeClass()
                        ),
                        nativeDataType="NUMBER",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="amount",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NumberTypeClass()
                        ),
                        nativeDataType="NUMBER",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="updated_at",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.DateTypeClass()
                        ),
                        nativeDataType="DATE",
                    ),
                ],
            )
        elif (
            isinstance(urn, DatasetUrn)
            and str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
        ):
            return Dataset(
                platform=str(DataPlatformUrn("urn:li:dataPlatform:bigquery")),
                name=urn.name,
                schema=[
                    models.SchemaFieldClass(
                        fieldPath="field",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.DateTypeClass()
                        ),
                        nativeDataType="DATE",
                    ),
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
        elif str(urn).startswith(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
        ):
            return Dataset(
                platform=str(DataPlatformUrn("urn:li:dataPlatform:snowflake")),
                name=urn.name,
                schema=[
                    models.SchemaFieldClass(
                        fieldPath="column_name",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.DateTypeClass()
                        ),
                        nativeDataType="DATE",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="string_column",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="number_column",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NumberTypeClass()
                        ),
                        nativeDataType="NUMBER",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="boolean_column",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.BooleanTypeClass()
                        ),
                        nativeDataType="BOOLEAN",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="date_column",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.DateTypeClass()
                        ),
                        nativeDataType="DATE",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="time_column",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.TimeTypeClass()
                        ),
                        nativeDataType="TIME",
                    ),
                    models.SchemaFieldClass(
                        fieldPath="null_column",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NullTypeClass()
                        ),
                        nativeDataType="NULL",
                    ),
                ],
            )
        return None


@pytest.fixture
def freshness_stub_entity_client(
    freshness_monitor_with_all_fields: Monitor,
    freshness_assertion_entity_with_all_fields: Assertion,
) -> StubEntityClient:
    return StubEntityClient(
        monitor_entity=freshness_monitor_with_all_fields,
        assertion_entity=freshness_assertion_entity_with_all_fields,
    )


class MockSearchClient:
    """Mock search client that returns appropriate monitor URNs based on the filter."""

    def __init__(self, monitor_entity: Optional[Monitor] = None):
        self.monitor_entity = monitor_entity

    def get_urns(self, filter=None, **kwargs):
        """Return monitor URN if a monitor entity is configured."""
        if self.monitor_entity and self.monitor_entity.urn:
            return iter([self.monitor_entity.urn])
        return iter([])


class StubDataHubClient:
    def __init__(self, entity_client: Optional[StubEntityClient] = None) -> None:
        self.entities = entity_client or StubEntityClient()
        # Pass the monitor entity to search so it can return the correct URN
        monitor_entity = (
            self.entities.monitor_entity
            if hasattr(self.entities, "monitor_entity")
            else None
        )
        self.search = MockSearchClient(monitor_entity=monitor_entity)


@pytest.fixture
def freshness_stub_datahub_client(
    freshness_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    return StubDataHubClient(entity_client=freshness_stub_entity_client)


@pytest.fixture
def volume_stub_entity_client(
    volume_monitor_with_all_fields: Monitor,
    volume_assertion_entity_with_all_fields: Assertion,
) -> StubEntityClient:
    return StubEntityClient(
        monitor_entity=volume_monitor_with_all_fields,
        assertion_entity=volume_assertion_entity_with_all_fields,
    )


@pytest.fixture
def volume_stub_datahub_client(
    volume_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    return StubDataHubClient(entity_client=volume_stub_entity_client)


@pytest.fixture
def native_volume_assertion_entity_with_all_fields(
    any_assertion_urn: AssertionUrn,
) -> Assertion:
    return Assertion(
        id=any_assertion_urn,
        info=models.VolumeAssertionInfoClass(
            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
            entity=_any_dataset_urn,
            rowCountTotal=models.RowCountTotalClass(
                operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value="100", type=models.AssertionStdParameterTypeClass.NUMBER
                    )
                ),
            ),
        ),
        description="Native Volume Assertion",
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
                tag="urn:li:tag:native_volume_assertion_tag",
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


@pytest.fixture
def native_volume_monitor_with_all_fields(
    any_monitor_urn: MonitorUrn, any_assertion_urn: AssertionUrn
) -> Monitor:
    """A monitor with all fields set for native volume assertions."""
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
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
                            datasetVolumeParameters=models.DatasetVolumeAssertionParametersClass(
                                sourceType=models.DatasetVolumeSourceTypeClass.INFORMATION_SCHEMA,
                            ),
                        ),
                    )
                ],
            ),
        ),
    )


@pytest.fixture
def native_volume_stub_entity_client(
    native_volume_monitor_with_all_fields: Monitor,
    native_volume_assertion_entity_with_all_fields: Assertion,
) -> StubEntityClient:
    return StubEntityClient(
        monitor_entity=native_volume_monitor_with_all_fields,
        assertion_entity=native_volume_assertion_entity_with_all_fields,
    )


@pytest.fixture
def native_volume_stub_datahub_client(
    native_volume_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    return StubDataHubClient(entity_client=native_volume_stub_entity_client)


@pytest.fixture
def sql_assertion_entity_with_all_fields(
    any_assertion_urn: AssertionUrn,
) -> Assertion:
    return Assertion(
        id=any_assertion_urn,
        info=models.SqlAssertionInfoClass(
            type=models.SqlAssertionTypeClass.METRIC,
            entity=_any_dataset_urn,
            statement="SELECT COUNT(*) FROM test_table",
            operator=models.AssertionStdOperatorClass.GREATER_THAN,
            parameters=models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value="100.0",
                ),
            ),
        ),
        description="SQL Assertion",
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
                tag="urn:li:tag:sql_assertion_tag",
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


@pytest.fixture
def sql_monitor_with_all_fields(
    any_monitor_urn: MonitorUrn, any_assertion_urn: AssertionUrn
) -> Monitor:
    """A monitor with all fields set for SQL assertions."""
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
                            cron="0 0 * * *",  # Daily for SQL assertions
                            timezone="UTC",
                        ),
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_SQL,
                        ),
                    )
                ],
            ),
        ),
    )


@pytest.fixture
def sql_stub_entity_client(
    sql_monitor_with_all_fields: Monitor,
    sql_assertion_entity_with_all_fields: Assertion,
) -> StubEntityClient:
    return StubEntityClient(
        monitor_entity=sql_monitor_with_all_fields,
        assertion_entity=sql_assertion_entity_with_all_fields,
    )


@pytest.fixture
def sql_stub_datahub_client(
    sql_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    return StubDataHubClient(entity_client=sql_stub_entity_client)


@pytest.fixture
def column_metric_assertion_entity_with_all_fields(
    any_assertion_urn: AssertionUrn,
) -> Assertion:
    """A column metric assertion entity for testing."""
    return Assertion(
        id=any_assertion_urn,
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="amount", type="number", nativeType="NUMBER"
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.NOT_NULL,
            ),
        ),
        description="Smart Column Metric Assertion",
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
                tag="urn:li:tag:smart_column_metric_assertion_tag",
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


@pytest.fixture
def column_metric_monitor_with_all_fields(
    any_monitor_urn: MonitorUrn, any_assertion_urn: AssertionUrn
) -> Monitor:
    """A monitor with all fields set for column metric assertions."""
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
                            cron="0 0 * * *",  # Daily for column metric assertions
                            timezone="UTC",
                        ),
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                            datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                                sourceType=models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY,
                            ),
                        ),
                    )
                ],
            ),
        ),
    )


@pytest.fixture
def column_metric_stub_entity_client(
    column_metric_monitor_with_all_fields: Monitor,
    column_metric_assertion_entity_with_all_fields: Assertion,
) -> StubEntityClient:
    return StubEntityClient(
        monitor_entity=column_metric_monitor_with_all_fields,
        assertion_entity=column_metric_assertion_entity_with_all_fields,
    )


@pytest.fixture
def column_metric_stub_datahub_client(
    column_metric_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    return StubDataHubClient(entity_client=column_metric_stub_entity_client)


@pytest.fixture
def native_column_metric_stub_entity_client() -> StubEntityClient:
    """A stub entity client for native column metric assertions."""
    return StubEntityClient()


@pytest.fixture
def native_column_metric_stub_datahub_client(
    native_column_metric_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    return StubDataHubClient(entity_client=native_column_metric_stub_entity_client)


@pytest.fixture
def stub_entity_client() -> StubEntityClient:
    """A generic stub entity client that can be used for all types of assertions."""
    return StubEntityClient()
