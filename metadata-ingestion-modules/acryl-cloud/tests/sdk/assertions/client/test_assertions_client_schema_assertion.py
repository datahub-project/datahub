"""Tests for schema assertion client."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Union
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.schema_assertion import SchemaAssertion
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehavior,
)
from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    SchemaAssertionCompatibility,
    SchemaAssertionField,
    SchemaFieldDataType,
)
from acryl_datahub_cloud.sdk.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    TagUrn,
)
from datahub.sdk._shared import TagsInputType
from tests.sdk.assertions.conftest import (
    StubDataHubClient,
    StubEntityClient,
)

FROZEN_TIME = "2025-01-01 10:30:00"

_any_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
)

GENERATED_DISPLAY_NAME_LENGTH = 22


@dataclass
class SchemaAssertionCreateParams:
    """Parameters for creating a schema assertion."""

    dataset_urn: Union[str, DatasetUrn]
    fields: list[Union[SchemaAssertionField, dict[Any, Any]]]
    compatibility: Optional[Union[str, SchemaAssertionCompatibility]] = None
    display_name: Optional[str] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None
    enabled: Optional[bool] = None


@dataclass
class SchemaAssertionSyncParams:
    """Parameters for syncing a schema assertion."""

    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    compatibility: Optional[Union[str, SchemaAssertionCompatibility]] = None
    fields: Optional[list[dict]] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    enabled: Optional[bool] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@pytest.fixture
def schema_assertion_entity_with_all_fields() -> Assertion:
    """Create a schema assertion entity for testing."""
    return Assertion(
        id=AssertionUrn("schema_assertion"),
        info=models.SchemaAssertionInfoClass(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            compatibility=models.SchemaAssertionCompatibilityClass.EXACT_MATCH,
            schema=models.SchemaMetadataClass(
                schemaName="test-schema",
                platform="urn:li:dataPlatform:snowflake",
                version=0,
                hash="test-hash",
                platformSchema=models.OtherSchemaClass(rawSchema=""),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="id",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="VARCHAR",
                        nullable=False,
                    ),
                    models.SchemaFieldClass(
                        fieldPath="count",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.NumberTypeClass()
                        ),
                        nativeDataType="BIGINT",
                        nullable=False,
                    ),
                ],
            ),
        ),
        description="Schema Assertion",
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
                tag="urn:li:tag:schema_assertion_tag",
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
def schema_monitor_with_all_fields() -> Monitor:
    """Create a schema monitor entity for testing."""
    assertion_urn = AssertionUrn("schema_assertion")
    dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )
    return Monitor(
        id=(dataset_urn, assertion_urn),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=str(assertion_urn),
                        schedule=models.CronScheduleClass(
                            cron="0 */6 * * *",
                            timezone="UTC",
                        ),
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_SCHEMA,
                            datasetSchemaParameters=models.DatasetSchemaAssertionParametersClass(
                                sourceType=models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA,
                            ),
                        ),
                    )
                ],
            ),
        ),
    )


@pytest.fixture
def schema_stub_entity_client(
    schema_monitor_with_all_fields: Monitor,
    schema_assertion_entity_with_all_fields: Assertion,
) -> StubEntityClient:
    """Create a stub entity client with schema assertion entities."""
    return StubEntityClient(
        monitor_entity=schema_monitor_with_all_fields,
        assertion_entity=schema_assertion_entity_with_all_fields,
    )


@pytest.fixture
def schema_stub_datahub_client(
    schema_stub_entity_client: StubEntityClient,
) -> StubDataHubClient:
    """Create a stub datahub client with schema assertion entities."""
    return StubDataHubClient(
        entity_client=schema_stub_entity_client,
    )


def _validate_schema_assertion_created_vs_expected(
    actual: SchemaAssertion,
    input_params: SchemaAssertionCreateParams,
    expected: SchemaAssertion,
) -> None:
    """Validate that the created assertion matches expectations."""
    assert actual.urn is not None
    assert actual.dataset_urn == expected.dataset_urn
    assert actual.mode == expected.mode

    # Check display name
    if input_params.display_name is not None:
        assert actual.display_name == input_params.display_name
    else:
        assert len(actual.display_name) == GENERATED_DISPLAY_NAME_LENGTH

    # Check compatibility
    assert actual.compatibility == expected.compatibility

    # Check fields
    assert len(actual.fields) == len(expected.fields)
    for i, expected_field in enumerate(expected.fields):
        assert actual.fields[i].path == expected_field.path
        assert actual.fields[i].type == expected_field.type


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_creates_with_minimal_input(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion creates with minimal input parameters."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = SchemaAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        fields=[{"path": "id", "type": "STRING"}],
    )

    expected_assertion = SchemaAssertion(
        urn=None,  # type: ignore[arg-type]  # URN is generated during creation
        dataset_urn=_any_dataset_urn,
        display_name="New Assertion",
        mode=AssertionMode.ACTIVE,
        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
        compatibility=SchemaAssertionCompatibility.EXACT_MATCH,  # Default
        fields=[
            SchemaAssertionField(path="id", type=SchemaFieldDataType.STRING),
        ],
        incident_behavior=[],
        tags=[],
        created_by=DEFAULT_CREATED_BY,
        created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
        updated_by=DEFAULT_CREATED_BY,
        updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    )

    # Act - sync with urn=None should create
    assertion = client.sync_schema_assertion(
        dataset_urn=input_params.dataset_urn,
        urn=None,  # Create case
        display_name=input_params.display_name,
        compatibility=input_params.compatibility,
        fields=input_params.fields,
        incident_behavior=input_params.incident_behavior,
        tags=input_params.tags,
        updated_by=input_params.created_by,
        enabled=input_params.enabled if input_params.enabled is not None else True,
    )

    # Assert
    expected_assertion._urn = assertion.urn  # Update with actual URN
    _validate_schema_assertion_created_vs_expected(
        assertion, input_params, expected_assertion
    )


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_creates_with_full_input(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion creates with full input parameters."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = SchemaAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        display_name="Test Schema Assertion",
        compatibility=SchemaAssertionCompatibility.SUPERSET,
        fields=[
            {"path": "id", "type": "STRING"},
            {"path": "count", "type": "NUMBER", "native_type": "BIGINT"},
            {"path": "is_active", "type": "BOOLEAN"},
        ],
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=["test_tag"],
        enabled=True,
    )

    expected_assertion = SchemaAssertion(
        urn=None,  # type: ignore[arg-type]
        dataset_urn=_any_dataset_urn,
        display_name="Test Schema Assertion",
        mode=AssertionMode.ACTIVE,
        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
        compatibility=SchemaAssertionCompatibility.SUPERSET,
        fields=[
            SchemaAssertionField(path="id", type=SchemaFieldDataType.STRING),
            SchemaAssertionField(
                path="count", type=SchemaFieldDataType.NUMBER, native_type="BIGINT"
            ),
            SchemaAssertionField(path="is_active", type=SchemaFieldDataType.BOOLEAN),
        ],
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=[TagUrn("test_tag")],
        created_by=DEFAULT_CREATED_BY,
        created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
        updated_by=DEFAULT_CREATED_BY,
        updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    )

    # Act
    assertion = client.sync_schema_assertion(
        dataset_urn=input_params.dataset_urn,
        urn=None,
        display_name=input_params.display_name,
        compatibility=input_params.compatibility,
        fields=input_params.fields,
        incident_behavior=input_params.incident_behavior,
        tags=input_params.tags,
        enabled=input_params.enabled,
    )

    # Assert
    expected_assertion._urn = assertion.urn
    assert assertion.display_name == expected_assertion.display_name
    assert assertion.compatibility == expected_assertion.compatibility
    assert len(assertion.fields) == len(expected_assertion.fields)


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_creates_with_subset_compatibility(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion creates with SUBSET compatibility."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    # Act
    assertion = client.sync_schema_assertion(
        dataset_urn=_any_dataset_urn,
        urn=None,
        compatibility="SUBSET",
        fields=[{"path": "id", "type": "STRING"}],
        enabled=True,
    )

    # Assert
    assert assertion.compatibility == SchemaAssertionCompatibility.SUBSET


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_creates_disabled(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion creates in disabled state."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    # Act
    assertion = client.sync_schema_assertion(
        dataset_urn=_any_dataset_urn,
        urn=None,
        fields=[{"path": "id", "type": "STRING"}],
        enabled=False,
    )

    # Assert
    assert assertion.mode == AssertionMode.INACTIVE


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_updates_existing(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion updates existing assertion."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    existing_urn = AssertionUrn("schema_assertion")

    # Act - sync with urn should update
    assertion = client.sync_schema_assertion(
        dataset_urn=_any_dataset_urn,
        urn=existing_urn,
        display_name="Updated Display Name",
        # Don't provide fields - should preserve existing
    )

    # Assert
    assert assertion.urn == existing_urn
    assert assertion.display_name == "Updated Display Name"
    # Fields should be preserved from existing assertion
    assert len(assertion.fields) > 0


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_merge_preserves_fields(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that merging preserves existing fields when None provided."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    existing_urn = AssertionUrn("schema_assertion")

    # Act - sync with only display_name, no fields
    assertion = client.sync_schema_assertion(
        dataset_urn=_any_dataset_urn,
        urn=existing_urn,
        display_name="New Display Name",
        fields=None,  # Should preserve existing
    )

    # Assert
    assert len(assertion.fields) == 2  # From existing assertion
    assert assertion.display_name == "New Display Name"


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_with_custom_schedule(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion with custom cron schedule."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    # Act
    assertion = client.sync_schema_assertion(
        dataset_urn=_any_dataset_urn,
        urn=None,
        fields=[{"path": "id", "type": "STRING"}],
        schedule="0 0 * * *",  # Every day at midnight
        enabled=True,
    )

    # Assert
    assert assertion.schedule.cron == "0 0 * * *"


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_fails_without_fields_for_new_assertion(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that creating a new assertion without fields raises error."""
    # Arrange
    # Create a client with no existing entities
    empty_entity_client = StubEntityClient(monitor_entity=None, assertion_entity=None)
    stub_client = StubDataHubClient(entity_client=empty_entity_client)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    # Act & Assert
    with pytest.raises(SDKUsageError, match="fields"):
        client.sync_schema_assertion(
            dataset_urn=_any_dataset_urn,
            urn=None,  # Create case
            fields=None,  # Missing required fields
            enabled=True,
        )


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_dataset_urn_mismatch_raises_error(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that dataset URN mismatch raises error."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    existing_urn = AssertionUrn("schema_assertion")
    different_dataset = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:test,different_dataset,PROD)"
    )

    # Act & Assert
    with pytest.raises(SDKUsageError, match="Dataset URN mismatch"):
        client.sync_schema_assertion(
            dataset_urn=different_dataset,  # Different from existing
            urn=existing_urn,
            fields=None,
        )


@freeze_time(FROZEN_TIME)
def test_sync_schema_assertion_with_schemaassertionfield_objects(
    schema_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test sync schema assertion using SchemaAssertionField objects."""
    # Arrange
    client = AssertionsClient(schema_stub_datahub_client)  # type: ignore[arg-type]
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    fields = [
        SchemaAssertionField(
            path="id", type=SchemaFieldDataType.STRING, native_type="VARCHAR(255)"
        ),
        SchemaAssertionField(
            path="amount", type=SchemaFieldDataType.NUMBER, native_type="DECIMAL(10,2)"
        ),
    ]

    # Act
    assertion = client.sync_schema_assertion(
        dataset_urn=_any_dataset_urn,
        urn=None,
        fields=fields,  # type: ignore[arg-type]
        enabled=True,
    )

    # Assert
    assert len(assertion.fields) == 2
    assert assertion.fields[0].path == "id"
    assert assertion.fields[0].native_type == "VARCHAR(255)"
    assert assertion.fields[1].path == "amount"
    assert assertion.fields[1].native_type == "DECIMAL(10,2)"
