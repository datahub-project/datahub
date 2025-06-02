from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.entities.subscription import Subscription
from acryl_datahub_cloud._sdk_extras.subscription_client import SubscriptionClient
from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, SubscriptionUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient

FROZEN_TIME = "2024-01-15 10:30:00"

NUM_DISTINCT_ENTITY_CHANGE_TYPES = (
    26  # Current number of distinct EntityChangeTypeClass values
)


@pytest.fixture
def mock_client() -> MagicMock:
    """Mock DataHubClient for testing."""
    return MagicMock(spec=DataHubClient)


@pytest.fixture
def subscription_client(mock_client: MagicMock) -> SubscriptionClient:
    """Create SubscriptionClient instance with mocked DataHubClient."""
    return SubscriptionClient(mock_client)


@pytest.fixture
def any_dataset_urn() -> DatasetUrn:
    """Any test dataset URN."""
    return DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )


@pytest.fixture
def any_user_urn() -> CorpUserUrn:
    """Any test user URN."""
    return CorpUserUrn.create_from_string("urn:li:corpuser:testuser")


@pytest.fixture
def any_entity_change_types() -> List[str]:
    """Any entity change types for testing."""
    return [
        models.EntityChangeTypeClass.ASSERTION_PASSED,
        models.EntityChangeTypeClass.ASSERTION_FAILED,
    ]


def test_get_entity_change_types_with_provided_list(
    subscription_client: SubscriptionClient,
) -> None:
    """Test _get_entity_change_types returns provided list when not None."""
    provided_types = [
        models.EntityChangeTypeClass.ASSERTION_PASSED,
        models.EntityChangeTypeClass.ASSERTION_FAILED,
    ]

    result = subscription_client._get_entity_change_types(provided_types)

    assert result == provided_types


def test_get_entity_change_types_with_none(
    subscription_client: SubscriptionClient,
) -> None:
    """Test _get_entity_change_types returns all possible values when None is provided."""
    result = subscription_client._get_entity_change_types(None)

    # Convert to set to verify uniqueness and check exact count
    result_set = set(result)
    assert len(result) == len(result_set)  # No duplicates
    assert len(result) == NUM_DISTINCT_ENTITY_CHANGE_TYPES


def test_get_entity_change_types_with_empty_list(
    subscription_client: SubscriptionClient,
) -> None:
    """Test _get_entity_change_types raises SdkUsageError when empty list `[]` is provided."""
    provided_types: List[str] = []

    with pytest.raises(SdkUsageError) as exc_info:
        subscription_client._get_entity_change_types(provided_types)
        assert "Empty list is not allowed" in str(exc_info.value)


def test_get_entity_change_types_with_invalid(
    subscription_client: SubscriptionClient,
) -> None:
    """Test _get_entity_change_types raises SdkUsageError when mixing valid and invalid values."""
    mixed_types = [
        models.EntityChangeTypeClass.ASSERTION_PASSED,  # valid
        "INVALID_TYPE",  # invalid
    ]

    with pytest.raises(SdkUsageError) as exc_info:
        subscription_client._get_entity_change_types(mixed_types)
        assert "Invalid entity change types provided" in str(exc_info.value)


@freeze_time(FROZEN_TIME)
def test_create_audit_stamp(subscription_client: SubscriptionClient) -> None:
    """Test _create_audit_stamp creates audit stamp with correct timestamp and actor."""
    now_frozen_time = datetime.fromisoformat(FROZEN_TIME).replace(tzinfo=timezone.utc)

    audit_stamp = subscription_client._create_audit_stamp()

    assert isinstance(audit_stamp, models.AuditStampClass)
    assert audit_stamp.time == make_ts_millis(now_frozen_time)
    assert audit_stamp.actor == DEFAULT_ACTOR_URN


@freeze_time(FROZEN_TIME)
def test_subscribe_creates_new_subscription(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
    any_entity_change_types: List[str],
) -> None:
    """Test subscribe creates new subscription successfully (golden path)."""

    # Mock: No existing subscriptions found
    subscription_client.client.resolve.subscription.return_value = []  # type: ignore[attr-defined]

    # Execute
    subscription_client.subscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=any_entity_change_types,
    )

    # Verify resolve.subscription was called to check for existing subscriptions
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn()
    )

    # Verify entities.upsert was called to create the subscription
    subscription_client.client.entities.upsert.assert_called_once()  # type: ignore[attr-defined]

    # Get the subscription that was passed to upsert
    upserted_subscription = subscription_client.client.entities.upsert.call_args[0][0]  # type: ignore[attr-defined]
    assert isinstance(upserted_subscription, Subscription)
    assert isinstance(upserted_subscription.urn, SubscriptionUrn)
    assert upserted_subscription.urn.get_type() == "subscription"

    # Verify the subscription details
    subscription_info = upserted_subscription.info
    assert subscription_info.entityUrn == any_dataset_urn.urn()
    assert subscription_info.actorUrn == any_user_urn.urn()
    assert subscription_info.actorType == "corpuser"
    assert subscription_info.types == [models.SubscriptionTypeClass.ENTITY_CHANGE]

    # Verify entity change types
    assert subscription_info.entityChangeTypes is not None
    entity_change_type_values = [
        ect.entityChangeType for ect in subscription_info.entityChangeTypes
    ]
    assert entity_change_type_values == any_entity_change_types

    # Verify audit stamps are set
    assert subscription_info.createdOn is not None
    assert subscription_info.updatedOn is not None
    assert subscription_info.createdOn.actor == DEFAULT_ACTOR_URN
    assert subscription_info.updatedOn.actor == DEFAULT_ACTOR_URN


@freeze_time(FROZEN_TIME)
def test_subscribe_updates_existing_subscription(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
    any_entity_change_types: List[str],
) -> None:
    """Test subscribe updates existing subscription when one exists."""

    # Create mock existing subscription with different entity change types
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityUrn = any_dataset_urn.urn()
    existing_subscription.info.actorUrn = any_user_urn.urn()
    existing_subscription.info.actorType = "corpuser"
    existing_subscription.info.types = [models.SubscriptionTypeClass.ENTITY_CHANGE]
    existing_subscription.info.createdOn = MagicMock()  # Original creation timestamp
    # Set existing entity change types that are different from any_entity_change_types
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,  # existing type
            filter=None,
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,  # new type
            filter=None,
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription
    ]

    # Execute
    subscription_client.subscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=any_entity_change_types,
    )

    # Verify resolve.subscription was called to check for existing subscriptions
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn()
    )

    # Verify entities.upsert was called to update the subscription
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription
    )

    # Get the subscription that was passed to upsert (should be the same existing_subscription object)
    upserted_subscription = subscription_client.client.entities.upsert.call_args[0][0]  # type: ignore[attr-defined]

    # Verify that key properties are preserved during update
    assert upserted_subscription.urn == existing_subscription.urn
    assert upserted_subscription.info.entityUrn == existing_subscription.info.entityUrn
    assert upserted_subscription.info.actorUrn == existing_subscription.info.actorUrn
    assert upserted_subscription.info.actorType == existing_subscription.info.actorType
    assert upserted_subscription.info.types == existing_subscription.info.types
    assert upserted_subscription.info.createdOn == existing_subscription.info.createdOn

    # Verify the subscription was updated with new entity change types
    updated_entity_change_types = existing_subscription.info.entityChangeTypes
    assert updated_entity_change_types is not None
    entity_change_type_values = [
        ect.entityChangeType for ect in updated_entity_change_types
    ]
    assert set(entity_change_type_values) == {
        models.EntityChangeTypeClass.ASSERTION_PASSED,
        models.EntityChangeTypeClass.ASSERTION_FAILED,
        models.EntityChangeTypeClass.INCIDENT_RESOLVED,
    }

    # Verify all EntityChangeDetailsClass objects have filter set to None
    for ect_detail in updated_entity_change_types:
        assert isinstance(ect_detail, models.EntityChangeDetailsClass)
        assert ect_detail.filter is None

    # Verify updatedOn audit stamp was set
    assert existing_subscription.info.updatedOn is not None
    assert isinstance(existing_subscription.info.updatedOn, models.AuditStampClass)
    assert existing_subscription.info.updatedOn.actor == DEFAULT_ACTOR_URN


def test_subscribe_raises_error_for_resolver_with_multiple_subscriptions(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
    any_entity_change_types: List[str],
) -> None:
    """Test subscribe raises SdkUsageError when multiple subscriptions exist (mesh scenario)."""
    # Create multiple mock subscriptions
    subscription1 = MagicMock(spec=Subscription)
    subscription1.urn = "urn:li:subscription:subscription-1"
    subscription2 = MagicMock(spec=Subscription)
    subscription2.urn = "urn:li:subscription:subscription-2"

    # Mock: Multiple existing subscriptions found (the "mesh" scenario)
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        subscription1,
        subscription2,
    ]

    # Execute & Verify
    with pytest.raises(SdkUsageError) as exc_info:
        subscription_client.subscribe(
            urn=any_dataset_urn,
            subscriber_urn=any_user_urn,
            entity_change_types=any_entity_change_types,
        )

        assert "We have a mesh here" in str(exc_info.value)
        assert "2 subscriptions found" in str(exc_info.value)


def test_subscribe_raises_error_for_assertion_urn(
    subscription_client: SubscriptionClient,
    any_user_urn: CorpUserUrn,
    any_entity_change_types: List[str],
) -> None:
    """Test subscribe raises AssertionError for AssertionUrn (not supported yet)."""
    # Create an AssertionUrn
    assertion_urn = AssertionUrn.create_from_string("urn:li:assertion:test-assertion")

    # Execute & Verify
    with pytest.raises(AssertionError) as exc_info:
        subscription_client.subscribe(
            urn=assertion_urn,
            subscriber_urn=any_user_urn,
            entity_change_types=any_entity_change_types,
        )

        assert "AssertionUrn is not supported yet" in str(exc_info.value)


def test_merge_entity_change_types_existing_none(
    subscription_client: SubscriptionClient,
    any_entity_change_types: List[str],
) -> None:
    """Test _merge_entity_change_types returns provided list when existing is None."""
    existing_types = None

    merged_types = subscription_client._merge_entity_change_types(
        existing_types, any_entity_change_types
    )

    assert set([t.entityChangeType for t in merged_types]) == set(
        any_entity_change_types
    )


def test_merge_entity_change_types_existing_not_none(
    subscription_client: SubscriptionClient,
    any_entity_change_types: List[str],
) -> None:
    """Test _merge_entity_change_types returns provided list when existing is not None."""
    existing_types = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
            filter=None,
        )
    ]

    merged_types = subscription_client._merge_entity_change_types(
        existing_types, any_entity_change_types
    )

    assert set([t.entityChangeType for t in merged_types]) == set(
        any_entity_change_types
    ).union({models.EntityChangeTypeClass.INCIDENT_RESOLVED})
