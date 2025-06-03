from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Union
from unittest.mock import MagicMock, patch

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


@dataclass
class MergeEntityChangeTypesTestParams:
    """Test parameters for _merge_entity_change_types test cases.

    Contains input parameters and expected output for the _merge_entity_change_types method.
    """

    # Input parameters (match method signature)
    existing_change_types: Optional[List[models.EntityChangeDetailsClass]]
    new_change_type_strs: List[str]
    new_assertion_urn: Optional[AssertionUrn]

    # Expected output
    expected_results: List[models.EntityChangeDetailsClass]


@dataclass
class MergeEntityChangeFilterTestParams:
    """Test parameters for _merge_entity_change_types_filter test cases.

    Contains input parameters and expected output for the _merge_entity_change_types_filter method.
    """

    # Input parameters (match method signature)
    existing_filter: Optional[models.EntityChangeDetailsFilterClass]
    new_assertion_urn: Optional[AssertionUrn]

    # Expected output
    expected_result: Optional[models.EntityChangeDetailsFilterClass]


def assert_entity_change_details_filter_equal(
    actual: Optional[models.EntityChangeDetailsFilterClass],
    expected: Optional[models.EntityChangeDetailsFilterClass],
) -> None:
    """Assert that two EntityChangeDetailsFilterClass objects are equal, ignoring order in lists."""
    if expected is None:
        assert actual is None
    else:
        assert actual is not None
        assert isinstance(actual, models.EntityChangeDetailsFilterClass)

        # Compare includeAssertions (order-independent)
        if expected.includeAssertions is None:
            assert actual.includeAssertions is None
        else:
            assert actual.includeAssertions is not None
            assert set(actual.includeAssertions) == set(expected.includeAssertions)


def assert_entity_change_details_equal(
    actual: models.EntityChangeDetailsClass,
    expected: models.EntityChangeDetailsClass,
) -> None:
    """Assert that two EntityChangeDetailsClass objects are equal, ignoring order in lists."""
    assert actual.entityChangeType == expected.entityChangeType

    # Use the dedicated filter comparison helper
    assert_entity_change_details_filter_equal(actual.filter, expected.filter)


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


@pytest.fixture
def any_assertion_urn() -> AssertionUrn:
    """Any test assertion URN."""
    return AssertionUrn.create_from_string("urn:li:assertion:test-assertion")


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
def test_subscribe_with_no_assertion_creates_new_subscription(
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
    assert set(entity_change_type_values) == set(any_entity_change_types)

    # Verify audit stamps are set
    assert subscription_info.createdOn is not None
    assert subscription_info.updatedOn is not None
    assert subscription_info.createdOn.actor == DEFAULT_ACTOR_URN
    assert subscription_info.updatedOn.actor == DEFAULT_ACTOR_URN


@freeze_time(FROZEN_TIME)
def test_subscribe_with_no_assertion_updates_existing_subscription(
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


@freeze_time(FROZEN_TIME)
@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_subscribe_with_assertion_creates_new_subscription(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
    any_assertion_urn: AssertionUrn,
    any_user_urn: CorpUserUrn,
    any_entity_change_types: List[str],
) -> None:
    """Test subscribe creates new subscription with assertion URN successfully."""
    test_dataset_urn = DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )

    # Mock: _fetch_dataset_from_assertion returns the dataset URN
    mock_fetch_dataset.return_value = (test_dataset_urn, any_assertion_urn)

    # Mock: No existing subscriptions found
    subscription_client.client.resolve.subscription.return_value = []  # type: ignore[attr-defined]

    # Execute
    subscription_client.subscribe(
        urn=any_assertion_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=any_entity_change_types,
    )

    # Verify _fetch_dataset_from_assertion was called
    mock_fetch_dataset.assert_called_once_with(any_assertion_urn)

    # Verify resolve.subscription was called to check for existing subscriptions
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=test_dataset_urn.urn(), actor_urn=any_user_urn.urn()
    )

    # Verify entities.upsert was called to create the subscription
    subscription_client.client.entities.upsert.assert_called_once()  # type: ignore[attr-defined]

    # Get the subscription that was passed to upsert
    upserted_subscription = subscription_client.client.entities.upsert.call_args[0][0]  # type: ignore[attr-defined]
    assert isinstance(upserted_subscription, Subscription)

    # Verify the subscription details
    subscription_info = upserted_subscription.info
    assert subscription_info.entityUrn == test_dataset_urn.urn()
    assert subscription_info.actorUrn == any_user_urn.urn()

    # Verify entity change types have assertion filter
    assert subscription_info.entityChangeTypes is not None
    for ect_detail in subscription_info.entityChangeTypes:
        assert isinstance(ect_detail, models.EntityChangeDetailsClass)
        assert ect_detail.filter is not None
        assert ect_detail.filter.includeAssertions == [any_assertion_urn.urn()]


@freeze_time(FROZEN_TIME)
@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_subscribe_with_assertion_updates_existing_subscription(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
    any_assertion_urn: AssertionUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test subscribe updates existing subscription with assertion URN.

    This test covers three scenarios:
    1. Append new entity change type with assertion filter
    2. Existing entity change type with duplicate assertion (no change)
    3. Existing entity change type with new assertion (merge filters)
    """
    test_dataset_urn = DatasetUrn.create_from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )

    # Mock: _fetch_dataset_from_assertion returns the dataset URN
    mock_fetch_dataset.return_value = (test_dataset_urn, any_assertion_urn)

    # Create mock existing subscription with mixed scenarios
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityUrn = test_dataset_urn.urn()
    existing_subscription.info.actorUrn = any_user_urn.urn()
    existing_subscription.info.createdOn = MagicMock()  # Original creation timestamp

    # Set existing entity change types covering all scenarios:
    # 1. INCIDENT_RESOLVED with the same assertion (duplicate scenario)
    # 2. ASSERTION_FAILED with different assertion (merge scenario)
    # 3. ASSERTION_PASSED will be newly added
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]  # Same assertion
            ),
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[
                    "urn:li:assertion:different-assertion"
                ]  # Different assertion
            ),
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription
    ]

    # Execute: Subscribe to INCIDENT_RESOLVED, ASSERTION_FAILED, and ASSERTION_PASSED
    subscription_client.subscribe(
        urn=any_assertion_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[
            models.EntityChangeTypeClass.INCIDENT_RESOLVED,  # Existing with same assertion
            models.EntityChangeTypeClass.ASSERTION_FAILED,  # Existing with different assertion
            models.EntityChangeTypeClass.ASSERTION_PASSED,  # New change type
        ],
    )

    # Verify entities.upsert was called to update the subscription
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription
    )

    # Verify the merged entity change types
    updated_entity_change_types = existing_subscription.info.entityChangeTypes
    assert updated_entity_change_types is not None
    assert len(updated_entity_change_types) == 3

    # Check each scenario:
    change_types_map = {
        ect.entityChangeType: ect for ect in updated_entity_change_types
    }

    # 1. INCIDENT_RESOLVED: Should keep same assertion (duplicate scenario)
    incident_resolved = change_types_map[models.EntityChangeTypeClass.INCIDENT_RESOLVED]
    assert incident_resolved.filter is not None
    assert set(incident_resolved.filter.includeAssertions) == {any_assertion_urn.urn()}

    # 2. ASSERTION_FAILED: Should merge assertions (merge scenario)
    assertion_failed = change_types_map[models.EntityChangeTypeClass.ASSERTION_FAILED]
    assert assertion_failed.filter is not None
    assert set(assertion_failed.filter.includeAssertions) == {
        "urn:li:assertion:different-assertion",
        any_assertion_urn.urn(),
    }

    # 3. ASSERTION_PASSED: Should have new assertion (append scenario)
    assertion_passed = change_types_map[models.EntityChangeTypeClass.ASSERTION_PASSED]
    assert assertion_passed.filter is not None
    assert set(assertion_passed.filter.includeAssertions) == {any_assertion_urn.urn()}


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=None,
                new_change_type_strs=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                ],
                new_assertion_urn=None,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=None,
                    ),
                ],
            ),
            id="no_existing_types_no_new_assertion",
        ),
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=None,
                    )
                ],
                new_change_type_strs=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                ],
                new_assertion_urn=None,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=None,
                    ),
                ],
            ),
            id="existing_types_merge_no_new_assertion",
        ),
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=None,
                new_change_type_strs=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                ],
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:test-assertion"
                ),
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test-assertion"]
                        ),
                    ),
                ],
            ),
            id="no_existing_types_with_new_assertion",
        ),
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:existing"]
                        ),
                    )
                ],
                new_change_type_strs=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                ],
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:new"
                ),
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:new"]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:existing"]
                        ),
                    ),
                ],
            ),
            id="existing_types_with_new_assertion_merge",
        ),
        # Complex scenarios with overlapping change types
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:assert1"]
                        ),
                    )
                ],
                new_change_type_strs=[
                    models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                ],
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:assert2"
                ),
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=[
                                "urn:li:assertion:assert1",
                                "urn:li:assertion:assert2",
                            ]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:assert2"]
                        ),
                    ),
                ],
            ),
            id="overlapping_change_types_different_assertions",
        ),
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:assert1"]
                        ),
                    )
                ],
                new_change_type_strs=[
                    models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                ],
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:assert1"
                ),
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:assert1"]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:assert1"]
                        ),
                    ),
                ],
            ),
            id="overlapping_change_types_same_assertion",
        ),
    ],
)
def test_merge_entity_change_types(
    subscription_client: SubscriptionClient,
    params: MergeEntityChangeTypesTestParams,
) -> None:
    """Test _merge_entity_change_types with various scenarios."""
    merged_types = subscription_client._merge_entity_change_types(
        existing_change_types=params.existing_change_types,
        new_change_type_strs=params.new_change_type_strs,
        new_assertion_urn=params.new_assertion_urn,
    )

    # Verify the merged entity change types match expected results exactly
    assert len(merged_types) == len(params.expected_results)

    # Verify all returned objects are EntityChangeDetailsClass
    for ect in merged_types:
        assert isinstance(ect, models.EntityChangeDetailsClass)

    # Match each expected result with an actual result using our helper
    for expected_ect in params.expected_results:
        # Find matching actual entity change type
        matching_actual = next(
            (
                ect
                for ect in merged_types
                if ect.entityChangeType == expected_ect.entityChangeType
            ),
            None,
        )
        assert matching_actual is not None, (
            f"Missing entity change type: {expected_ect.entityChangeType}"
        )

        # Use our helper method for detailed comparison
        assert_entity_change_details_equal(matching_actual, expected_ect)

    # Ensure no extra results (both directions verified)
    for actual_ect in merged_types:
        matching_expected = next(
            (
                ect
                for ect in params.expected_results
                if ect.entityChangeType == actual_ect.entityChangeType
            ),
            None,
        )
        assert matching_expected is not None, (
            f"Unexpected entity change type: {actual_ect.entityChangeType}"
        )


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=None,
                new_assertion_urn=None,
                expected_result=None,
            ),
            id="no_filter_no_assertion",
        ),
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=None,
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:test-assertion"
                ),
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:test-assertion"]
                ),
            ),
            id="no_filter_with_assertion",
        ),
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
                new_assertion_urn=None,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
            ),
            id="existing_filter_no_assertion",
        ),
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[]
                ),
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:new"
                ),
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:new"]
                ),
            ),
            id="empty_assertions_append",
        ),
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=None
                ),
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:new"
                ),
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:new"]
                ),
            ),
            id="none_assertions_append",
        ),
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:new"
                ),
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[
                        "urn:li:assertion:existing",
                        "urn:li:assertion:new",
                    ]
                ),
            ),
            id="non_empty_assertions_merge",
        ),
        pytest.param(
            MergeEntityChangeFilterTestParams(
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
                new_assertion_urn=AssertionUrn.create_from_string(
                    "urn:li:assertion:existing"
                ),
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
            ),
            id="duplicate_assertion_no_duplicate",
        ),
    ],
)
def test_merge_entity_change_types_filter(
    subscription_client: SubscriptionClient,
    params: MergeEntityChangeFilterTestParams,
) -> None:
    """Test _merge_entity_change_types_filter with various scenarios."""
    result = subscription_client._merge_entity_change_types_filter(
        existing_filter=params.existing_filter,
        new_assertion_urn=params.new_assertion_urn,
    )

    # Use our helper method for detailed comparison
    assert_entity_change_details_filter_equal(result, params.expected_result)


@pytest.mark.parametrize(
    "input_urn,expected_type,should_raise,expected_urn_string",
    [
        pytest.param(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            DatasetUrn,
            False,
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            id="dataset_string",
        ),
        pytest.param(
            "urn:li:assertion:test-assertion",
            AssertionUrn,
            False,
            "urn:li:assertion:test-assertion",
            id="assertion_string",
        ),
        pytest.param(
            DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
            ),
            DatasetUrn,
            False,
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            id="dataset_object",
        ),
        pytest.param(
            AssertionUrn.from_string("urn:li:assertion:test-assertion"),
            AssertionUrn,
            False,
            "urn:li:assertion:test-assertion",
            id="assertion_object",
        ),
        pytest.param(
            "urn:li:corpuser:testuser",
            None,
            True,
            None,
            id="unsupported_urn_type",
        ),
        pytest.param(
            "invalid-urn-format",
            None,
            True,
            None,
            id="invalid_urn_format",
        ),
    ],
)
def test_maybe_parse_urn(
    subscription_client: SubscriptionClient,
    input_urn: Union[str, DatasetUrn, AssertionUrn],
    expected_type: Optional[type],
    should_raise: bool,
    expected_urn_string: Optional[str],
) -> None:
    """Test _maybe_parse_urn with various inputs."""
    if should_raise:
        with pytest.raises(SdkUsageError):
            subscription_client._maybe_parse_urn(input_urn)
    else:
        result = subscription_client._maybe_parse_urn(input_urn)
        assert expected_type is not None  # Type guard for mypy
        assert isinstance(result, expected_type)
        assert result.urn() == expected_urn_string

        # For URN objects, verify it returns the same instance
        if isinstance(input_urn, (DatasetUrn, AssertionUrn)):
            assert result is input_urn
