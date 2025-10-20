import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Type, Union
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.entities.subscription import Subscription
from acryl_datahub_cloud.sdk.subscription_client import (
    ALL_EXISTING_ENTITY_CHANGE_TYPES,
    ASSERTION_RELATED_ENTITY_CHANGE_TYPES,
    SubscriptionClient,
)
from datahub.emitter.mce_builder import make_ts_millis
from datahub.emitter.rest_emitter import EmitMode
from datahub.errors import SdkUsageError
from datahub.metadata.urns import (
    AssertionUrn,
    CorpGroupUrn,
    CorpUserUrn,
    DatasetUrn,
    SubscriptionUrn,
)
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.main_client import DataHubClient

FROZEN_TIME = "2024-01-15 10:30:00"

NUM_DISTINCT_ENTITY_CHANGE_TYPES = (
    26  # Current number of distinct EntityChangeTypeClass values
)

NUM_ASSERTION_RELATED_ENTITY_CHANGE_TYPES = (
    3  # ASSERTION_PASSED, ASSERTION_FAILED, ASSERTION_ERROR
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
    should_raise: bool = False
    expected_error: Optional[str] = None


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


@dataclass
class EntityChangeTypesTestParams:
    """Test parameters for _get_entity_change_types test cases.

    Contains input parameters and expected output for the _get_entity_change_types method.
    """

    # Input parameters (match method signature)
    assertion_scope: bool
    entity_change_types: Optional[Sequence[Union[str, models.EntityChangeTypeClass]]]

    # Expected output
    expected_result: Optional[List[str]]
    should_raise: bool
    expected_error: Optional[str] = None


@dataclass
class RemoveChangeTypesTestParams:
    """Test parameters for _remove_change_types test cases.

    Contains input parameters and expected output for the _remove_change_types method.
    """

    # Input parameters (match method signature)
    existing_change_types: List[models.EntityChangeDetailsClass]
    change_types_to_remove: List[str]
    assertion_urn_to_remove: Optional[AssertionUrn]
    warn_if_missing: bool

    # Expected output
    expected_results: List[models.EntityChangeDetailsClass]
    should_raise: bool = False
    expected_error: Optional[str] = None
    expected_warning_logged: bool = False
    expected_warning_message: Optional[str] = None


@dataclass
class RemoveChangeTypesFilterTestParams:
    """Test parameters for _remove_change_types_filter test cases.

    Contains input parameters and expected output for the _remove_change_types_filter method.
    """

    # Input parameters (match method signature)
    entity_change_type: str
    existing_filter: Optional[models.EntityChangeDetailsFilterClass]
    assertion_urn_to_remove: Optional[AssertionUrn]
    warn_if_missing: bool

    # Expected output
    expected_result: Optional[models.EntityChangeDetailsFilterClass]
    expected_warning_logged: bool = False
    expected_warning_message: Optional[str] = None


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
    return DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )


@pytest.fixture
def any_user_urn() -> CorpUserUrn:
    """Any test user URN."""
    return CorpUserUrn.from_string("urn:li:corpuser:testuser")


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
    return AssertionUrn.from_string("urn:li:assertion:test-assertion")


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=False,
                entity_change_types=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                ],
                expected_result=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                ],
                should_raise=False,
            ),
            id="dataset_scope_provided_list",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=True,
                entity_change_types=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                ],
                expected_result=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                ],
                should_raise=False,
            ),
            id="assertion_scope_provided_valid_list",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=False,
                entity_change_types=None,
                expected_result=list(ALL_EXISTING_ENTITY_CHANGE_TYPES),
                should_raise=False,
            ),
            id="dataset_scope_none",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=True,
                entity_change_types=None,
                expected_result=list(ASSERTION_RELATED_ENTITY_CHANGE_TYPES),
                should_raise=False,
            ),
            id="assertion_scope_none",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=False,
                entity_change_types=[],
                expected_result=None,
                should_raise=True,
                expected_error="Entity change types cannot be an empty list",
            ),
            id="dataset_scope_empty_list",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=True,
                entity_change_types=[],
                expected_result=None,
                should_raise=True,
                expected_error="Entity change types cannot be an empty list",
            ),
            id="assertion_scope_empty_list",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=False,
                entity_change_types=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,  # valid
                    "INVALID_TYPE",  # invalid
                ],
                expected_result=None,
                should_raise=True,
                expected_error="Invalid entity change types provided",
            ),
            id="dataset_scope_invalid_types",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=True,
                entity_change_types=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,  # valid
                    "INVALID_TYPE",  # invalid
                ],
                expected_result=None,
                should_raise=True,
                expected_error="Invalid entity change types provided",
            ),
            id="assertion_scope_invalid_types",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=True,
                entity_change_types=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,  # valid assertion type
                    models.EntityChangeTypeClass.TAG_ADDED,  # invalid for assertion scope
                ],
                expected_result=None,
                should_raise=True,
                expected_error="For assertion subscriptions, only assertion-related change types are allowed",
            ),
            id="assertion_scope_non_assertion_types",
        ),
        pytest.param(
            EntityChangeTypesTestParams(
                assertion_scope=False,
                entity_change_types=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,  # EntityChangeTypeClass
                    "ASSERTION_FAILED",  # str
                    models.EntityChangeTypeClass.TAG_ADDED,  # EntityChangeTypeClass
                ],
                expected_result=[
                    models.EntityChangeTypeClass.ASSERTION_PASSED,
                    models.EntityChangeTypeClass.ASSERTION_FAILED,
                    models.EntityChangeTypeClass.TAG_ADDED,
                ],
                should_raise=False,
            ),
            id="dataset_scope_mixed_str_and_class_types",
        ),
    ],
)
def test_get_entity_change_types(
    subscription_client: SubscriptionClient,
    params: EntityChangeTypesTestParams,
) -> None:
    """Test _get_entity_change_types with various scenarios including assertion scope validation."""
    if params.should_raise:
        with pytest.raises(SdkUsageError) as exc_info:
            subscription_client._get_entity_change_types(
                params.assertion_scope, params.entity_change_types
            )
        assert params.expected_error is not None
        assert params.expected_error in str(exc_info.value)
    else:
        result = subscription_client._get_entity_change_types(
            params.assertion_scope, params.entity_change_types
        )

        assert params.expected_result is not None
        assert set(result) == set(params.expected_result)


def test_all_existing_entity_change_types_constant() -> None:
    """Test that ALL_EXISTING_ENTITY_CHANGE_TYPES constant has the expected number of unique types."""
    # This single assertion verifies both the count and absence of duplicates
    assert (
        len(set(ALL_EXISTING_ENTITY_CHANGE_TYPES)) == NUM_DISTINCT_ENTITY_CHANGE_TYPES
    )


def test_assertion_related_entity_change_types_constant() -> None:
    """Test that ASSERTION_RELATED_ENTITY_CHANGE_TYPES constant has the expected number of unique types."""
    # This single assertion verifies both the count and absence of duplicates
    assert (
        len(set(ASSERTION_RELATED_ENTITY_CHANGE_TYPES))
        == NUM_ASSERTION_RELATED_ENTITY_CHANGE_TYPES
    )


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
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn(), skip_cache=True
    )

    # Verify entities.upsert was called to create the subscription
    subscription_client.client.entities.upsert.assert_called_once()  # type: ignore[attr-defined]

    # Get the subscription that was passed to upsert
    upserted_subscription = subscription_client.client.entities.upsert.call_args[0][0]  # type: ignore[attr-defined]
    assert isinstance(upserted_subscription, Subscription)
    assert isinstance(upserted_subscription.urn, SubscriptionUrn)
    assert upserted_subscription.urn.entity_type == "subscription"

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
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute
    subscription_client.subscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=any_entity_change_types,
    )

    # Verify resolve.subscription was called to check for existing subscriptions
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn(), skip_cache=True
    )

    # Verify entities.upsert was called to update the subscription
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
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
    test_dataset_urn = DatasetUrn.from_string(
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
        entity_urn=test_dataset_urn.urn(), actor_urn=any_user_urn.urn(), skip_cache=True
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
    test_dataset_urn = DatasetUrn.from_string(
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
    # 1. ASSERTION_ERROR with the same assertion (duplicate scenario)
    # 2. ASSERTION_FAILED with different assertion (merge scenario)
    # 3. ASSERTION_PASSED will be newly added
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_ERROR,
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
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Subscribe to ASSERTION_ERROR, ASSERTION_FAILED, and ASSERTION_PASSED
    subscription_client.subscribe(
        urn=any_assertion_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[
            models.EntityChangeTypeClass.ASSERTION_ERROR,  # Existing with same assertion
            models.EntityChangeTypeClass.ASSERTION_FAILED,  # Existing with different assertion
            models.EntityChangeTypeClass.ASSERTION_PASSED,  # New change type
        ],
    )

    # Verify entities.upsert was called to update the subscription
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )

    # Verify the merged entity change types
    updated_entity_change_types = existing_subscription.info.entityChangeTypes
    assert updated_entity_change_types is not None
    assert len(updated_entity_change_types) == 3

    # Check each scenario:
    change_types_map = {
        ect.entityChangeType: ect for ect in updated_entity_change_types
    }

    # 1. ASSERTION_ERROR: Should keep same assertion (duplicate scenario)
    assertion_error = change_types_map[models.EntityChangeTypeClass.ASSERTION_ERROR]
    assert assertion_error.filter is not None
    assert set(assertion_error.filter.includeAssertions) == {any_assertion_urn.urn()}

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


@freeze_time(FROZEN_TIME)
@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_subscribe_with_string_subscriber_urn(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
) -> None:
    """Test subscribe method with string subscriber URN."""
    test_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )
    user_urn_str = "urn:li:corpuser:test_user"

    # Mock: No existing subscriptions
    subscription_client.client.resolve.subscription.return_value = []  # type: ignore[attr-defined]

    # Execute: Subscribe with string subscriber URN
    subscription_client.subscribe(
        urn=test_dataset_urn,
        subscriber_urn=user_urn_str,
        entity_change_types=[models.EntityChangeTypeClass.ASSERTION_PASSED],
    )

    # Verify: entities.upsert was called once
    subscription_client.client.entities.upsert.assert_called_once()  # type: ignore[attr-defined]

    # Verify: The subscription was created with the correct actor details
    upserted_subscription = subscription_client.client.entities.upsert.call_args[0][0]  # type: ignore[attr-defined]
    assert upserted_subscription.info.actorUrn == user_urn_str
    assert upserted_subscription.info.actorType == CorpUserUrn.ENTITY_TYPE


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
                new_assertion_urn=AssertionUrn.from_string(
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:new"),
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:assert2"),
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:assert1"),
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
        pytest.param(
            MergeEntityChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    )
                ],
                new_change_type_strs=[],  # Empty list should raise error
                new_assertion_urn=None,
                expected_results=[],
                should_raise=True,
                expected_error="new_change_type_strs cannot be empty, worse case we have the default values",
            ),
            id="empty_new_change_type_strs_raises_error",
        ),
    ],
)
def test_merge_entity_change_types(
    subscription_client: SubscriptionClient,
    params: MergeEntityChangeTypesTestParams,
) -> None:
    """Test _merge_entity_change_types with various scenarios."""
    if params.should_raise:
        with pytest.raises(AssertionError) as exc_info:
            subscription_client._merge_entity_change_types(
                existing_change_types=params.existing_change_types,
                new_change_type_strs=params.new_change_type_strs,
                new_assertion_urn=params.new_assertion_urn,
            )
        if params.expected_error:
            assert params.expected_error in str(exc_info.value)
        return

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
                new_assertion_urn=AssertionUrn.from_string(
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:new"),
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:new"),
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:new"),
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
                new_assertion_urn=AssertionUrn.from_string("urn:li:assertion:existing"),
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


def test_unsubscribe_dataset_no_existing_subscription(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset with no existing subscription (no-op)."""
    # Mock: No existing subscriptions found
    subscription_client.client.resolve.subscription.return_value = []  # type: ignore[attr-defined]

    # Execute
    subscription_client.unsubscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=None,
    )

    # Verify resolve.subscription was called
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn(), skip_cache=True
    )

    # Verify no delete or upsert operations
    subscription_client.client.entities.delete.assert_not_called()  # type: ignore[attr-defined]
    subscription_client.client.entities.upsert.assert_not_called()  # type: ignore[attr-defined]


def test_unsubscribe_dataset_multiple_subscriptions_error(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset with multiple subscriptions raises error."""
    # Create multiple mock subscriptions
    subscription1 = MagicMock(spec=Subscription)
    subscription1.urn = "urn:li:subscription:subscription-1"
    subscription2 = MagicMock(spec=Subscription)
    subscription2.urn = "urn:li:subscription:subscription-2"

    # Mock: Multiple existing subscriptions found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        subscription1,
        subscription2,
    ]

    # Execute & Verify
    with pytest.raises(SdkUsageError) as exc_info:
        subscription_client.unsubscribe(
            urn=any_dataset_urn,
            subscriber_urn=any_user_urn,
            entity_change_types=None,
        )
        assert "Multiple subscriptions found" in str(exc_info.value)
        assert "Expected at most 1, got 2" in str(exc_info.value)


def test_unsubscribe_dataset_remove_all_change_types_deletes_subscription(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset removes all change types and deletes subscription."""
    # Create mock existing subscription
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=None,
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=None,
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove all change types (entity_change_types=None)
    subscription_client.unsubscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=None,
    )

    # Verify resolve.subscription was called
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn(), skip_cache=True
    )

    # Verify subscription was deleted (no change types remaining)
    subscription_client.client.entities.delete.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription.urn
    )
    subscription_client.client.entities.upsert.assert_not_called()  # type: ignore[attr-defined]


def test_unsubscribe_dataset_remove_specific_change_types_updates_subscription(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset removes specific change types and updates subscription."""
    # Create mock existing subscription with multiple change types
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
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
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove specific change types
    subscription_client.unsubscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[
            models.EntityChangeTypeClass.ASSERTION_PASSED,  # type: ignore[list-item]
            models.EntityChangeTypeClass.ASSERTION_FAILED,  # type: ignore[list-item]
        ],
    )

    # Verify resolve.subscription was called
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=any_dataset_urn.urn(), actor_urn=any_user_urn.urn(), skip_cache=True
    )

    # Verify subscription was updated (still has remaining change types)
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )
    subscription_client.client.entities.delete.assert_not_called()  # type: ignore[attr-defined]

    # Verify the remaining change types
    updated_change_types = existing_subscription.info.entityChangeTypes
    assert len(updated_change_types) == 1
    assert (
        updated_change_types[0].entityChangeType
        == models.EntityChangeTypeClass.INCIDENT_RESOLVED
    )


def test_unsubscribe_dataset_remove_all_remaining_change_types_deletes_subscription(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset removes all remaining change types and deletes subscription."""
    # Create mock existing subscription with change types to be removed
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=None,
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=None,
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove specific change types that happen to be all the remaining ones
    subscription_client.unsubscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[
            models.EntityChangeTypeClass.ASSERTION_PASSED,  # type: ignore[list-item]
            models.EntityChangeTypeClass.ASSERTION_FAILED,  # type: ignore[list-item]
        ],
    )

    # Verify subscription was deleted (no change types remaining)
    subscription_client.client.entities.delete.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription.urn
    )
    subscription_client.client.entities.upsert.assert_not_called()  # type: ignore[attr-defined]


def test_unsubscribe_dataset_remove_nonexistent_change_types_no_change(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset with change types that don't exist in subscription."""
    # Create mock existing subscription
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=None,
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Try to remove change types that don't exist
    subscription_client.unsubscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[
            models.EntityChangeTypeClass.INCIDENT_RESOLVED,  # type: ignore[list-item]  # Doesn't exist in subscription
        ],
    )

    # Verify subscription was updated (original change types remain)
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )
    subscription_client.client.entities.delete.assert_not_called()  # type: ignore[attr-defined]

    # Verify the original change types remain unchanged
    updated_change_types = existing_subscription.info.entityChangeTypes
    assert len(updated_change_types) == 1
    assert (
        updated_change_types[0].entityChangeType
        == models.EntityChangeTypeClass.ASSERTION_PASSED
    )


@freeze_time(FROZEN_TIME)
def test_unsubscribe_dataset_updates_audit_stamp(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe dataset updates the audit stamp when updating subscription."""
    # Create mock existing subscription
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=None,
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
            filter=None,
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove one change type, keeping one
    subscription_client.unsubscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[models.EntityChangeTypeClass.ASSERTION_PASSED],  # type: ignore[list-item]
    )

    # Verify subscription was updated
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )

    # Verify updatedOn audit stamp was set with correct timestamp and actor
    now_frozen_time = datetime.fromisoformat(FROZEN_TIME).replace(tzinfo=timezone.utc)
    assert existing_subscription.info.updatedOn is not None
    assert isinstance(existing_subscription.info.updatedOn, models.AuditStampClass)
    assert existing_subscription.info.updatedOn.time == make_ts_millis(now_frozen_time)
    assert existing_subscription.info.updatedOn.actor == DEFAULT_ACTOR_URN


@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_unsubscribe_assertion_removes_assertion_from_filters(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
    any_assertion_urn: AssertionUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe assertion removes assertion from filters."""
    test_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )

    # Mock: _fetch_dataset_from_assertion returns the dataset URN
    mock_fetch_dataset.return_value = (test_dataset_urn, any_assertion_urn)

    # Create mock existing subscription with assertion filters
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn(), "urn:li:assertion:other"]
            ),
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]
            ),
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove assertion from default assertion-related change types
    subscription_client.unsubscribe(
        urn=any_assertion_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=None,  # Should default to assertion-related types
    )

    # Verify _fetch_dataset_from_assertion was called
    mock_fetch_dataset.assert_called_once_with(any_assertion_urn)

    # Verify subscription was updated (not deleted)
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )
    subscription_client.client.entities.delete.assert_not_called()  # type: ignore[attr-defined]

    # Verify the assertion was removed from appropriate filters
    updated_change_types = existing_subscription.info.entityChangeTypes
    assert len(updated_change_types) == 1  # ASSERTION_FAILED should be removed entirely

    # ASSERTION_PASSED should remain with only the other assertion
    assertion_passed = updated_change_types[0]
    assert (
        assertion_passed.entityChangeType
        == models.EntityChangeTypeClass.ASSERTION_PASSED
    )
    assert assertion_passed.filter is not None
    assert assertion_passed.filter.includeAssertions == ["urn:li:assertion:other"]


@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_unsubscribe_assertion_specific_change_types(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
    any_assertion_urn: AssertionUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe assertion with specific change types."""
    test_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )

    # Mock: _fetch_dataset_from_assertion returns the dataset URN
    mock_fetch_dataset.return_value = (test_dataset_urn, any_assertion_urn)

    # Create mock existing subscription with assertion filters
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]
            ),
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]
            ),
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_ERROR,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]
            ),
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove assertion only from ASSERTION_PASSED
    subscription_client.unsubscribe(
        urn=any_assertion_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=[models.EntityChangeTypeClass.ASSERTION_PASSED],  # type: ignore[list-item]
    )

    # Verify subscription was updated
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )

    # Verify only ASSERTION_PASSED was removed, others remain
    updated_change_types = existing_subscription.info.entityChangeTypes
    assert len(updated_change_types) == 2

    change_types_map = {ect.entityChangeType: ect for ect in updated_change_types}
    assert models.EntityChangeTypeClass.ASSERTION_FAILED in change_types_map
    assert models.EntityChangeTypeClass.ASSERTION_ERROR in change_types_map
    assert models.EntityChangeTypeClass.ASSERTION_PASSED not in change_types_map


@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_unsubscribe_assertion_deletes_subscription_when_no_change_types_remain(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
    any_assertion_urn: AssertionUrn,
    any_user_urn: CorpUserUrn,
) -> None:
    """Test unsubscribe assertion deletes subscription when no change types remain."""
    test_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )

    # Mock: _fetch_dataset_from_assertion returns the dataset URN
    mock_fetch_dataset.return_value = (test_dataset_urn, any_assertion_urn)

    # Create mock existing subscription with only this assertion
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]
            ),
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[any_assertion_urn.urn()]
            ),
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Remove assertion from all default change types
    subscription_client.unsubscribe(
        urn=any_assertion_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=None,  # Defaults to all assertion-related types
    )

    # Verify subscription was deleted (no change types remain)
    subscription_client.client.entities.delete.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription.urn
    )
    subscription_client.client.entities.upsert.assert_not_called()  # type: ignore[attr-defined]


@patch.object(SubscriptionClient, "_fetch_dataset_from_assertion")
def test_unsubscribe_assertion_warns_about_missing_assertions(
    mock_fetch_dataset: MagicMock,
    subscription_client: SubscriptionClient,
    any_assertion_urn: AssertionUrn,
    any_user_urn: CorpUserUrn,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test unsubscribe assertion logs warnings when assertion not in filters."""
    test_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )

    # Mock: _fetch_dataset_from_assertion returns the dataset URN
    mock_fetch_dataset.return_value = (test_dataset_urn, any_assertion_urn)

    # Create mock existing subscription WITHOUT the assertion in filters
    existing_subscription = MagicMock(spec=Subscription)
    existing_subscription.urn = "urn:li:subscription:existing-subscription-123"
    existing_subscription.info = MagicMock()
    existing_subscription.info.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=["urn:li:assertion:other"]  # Different assertion
            ),
        ),
    ]

    # Mock: One existing subscription found
    subscription_client.client.resolve.subscription.return_value = [  # type: ignore[attr-defined]
        existing_subscription.urn
    ]
    subscription_client.client.entities.get.return_value = existing_subscription  # type: ignore[attr-defined]

    # Execute: Try to remove assertion that's not in the filters
    with caplog.at_level(logging.WARNING):
        subscription_client.unsubscribe(
            urn=any_assertion_urn,
            subscriber_urn=any_user_urn,
            entity_change_types=[models.EntityChangeTypeClass.ASSERTION_PASSED],  # type: ignore[list-item]
        )

    # Verify warning was logged
    warning_records = [
        record for record in caplog.records if record.levelname == "WARNING"
    ]
    assert len(warning_records) >= 1
    found_expected_message = any(
        f"Assertion {any_assertion_urn.urn()} is not included in entity change type 'ASSERTION_PASSED'"
        in record.message
        for record in warning_records
    )
    assert found_expected_message

    # Verify subscription was updated (no changes made)
    subscription_client.client.entities.upsert.assert_called_once_with(  # type: ignore[attr-defined]
        existing_subscription, emit_mode=EmitMode.SYNC_WAIT
    )

    # Verify no changes were made to the subscription
    updated_change_types = existing_subscription.info.entityChangeTypes
    assert len(updated_change_types) == 1
    assert updated_change_types[0].filter.includeAssertions == [
        "urn:li:assertion:other"
    ]


@freeze_time(FROZEN_TIME)
def test_unsubscribe_with_string_subscriber_urn(
    subscription_client: SubscriptionClient,
) -> None:
    """Test unsubscribe method with string subscriber URN."""
    test_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
    )
    user_urn_str = "urn:li:corpuser:test_user"

    # Mock: No existing subscriptions
    subscription_client.client.resolve.subscription.return_value = []  # type: ignore[attr-defined]

    # Execute: Unsubscribe with string subscriber URN (should not raise error)
    subscription_client.unsubscribe(
        urn=test_dataset_urn,
        subscriber_urn=user_urn_str,
        entity_change_types=[models.EntityChangeTypeClass.ASSERTION_PASSED],
    )

    # Verify: resolve.subscription was called with the correct actor URN
    subscription_client.client.resolve.subscription.assert_called_once_with(  # type: ignore[attr-defined]
        entity_urn=test_dataset_urn.urn(),
        actor_urn=user_urn_str,
        skip_cache=True,
    )


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
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
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                    str(models.EntityChangeTypeClass.ASSERTION_FAILED),
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=None,
                    ),
                ],
            ),
            id="remove_multiple_change_types_keep_one",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test"]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:other"]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_FAILED),
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.INCIDENT_RESOLVED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:other"]
                        ),
                    ),
                ],
            ),
            id="remove_single_change_type_with_filters",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[],
            ),
            id="remove_all_change_types_returns_empty",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=None,
                    ),
                ],
                change_types_to_remove=[
                    str(
                        models.EntityChangeTypeClass.INCIDENT_RESOLVED
                    ),  # Not in existing
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
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
            id="remove_nonexistent_change_type_no_change",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
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
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_FAILED),
                    str(models.EntityChangeTypeClass.INCIDENT_RESOLVED),
                    str(models.EntityChangeTypeClass.TAG_ADDED),  # Not in existing
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                ],
            ),
            id="remove_mix_existing_and_nonexistent_types",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=[
                                "urn:li:assertion:test1",
                                "urn:li:assertion:test2",
                            ]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test3"]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test3"]
                        ),
                    ),
                ],
            ),
            id="remove_change_type_preserves_complex_filters",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    )
                ],
                change_types_to_remove=[],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[],
                should_raise=True,
                expected_error="change_types_to_remove cannot be empty, worse case we have the default values",
            ),
            id="empty_change_types_to_remove_raises_error",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED)
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[],
                should_raise=True,
                expected_error="Subscription must have at least one change type (no model restriction but business rule)",
            ),
            id="empty_existing_change_types_raises_error",
        ),
        # Assertion removal test cases
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test1"]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=[
                                "urn:li:assertion:test1",
                                "urn:li:assertion:test2",
                            ]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                    str(models.EntityChangeTypeClass.ASSERTION_FAILED),
                ],
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test1"
                ),
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test2"]
                        ),
                    ),
                ],
            ),
            id="assertion_removal_keeps_filtered_change_types",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test1"]
                        ),
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test1"]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                    str(models.EntityChangeTypeClass.ASSERTION_FAILED),
                ],
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test1"
                ),
                warn_if_missing=True,
                expected_results=[],
            ),
            id="assertion_removal_removes_all_when_no_assertions_remain",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:other"]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                ],
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:missing"
                ),
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:other"]
                        ),
                    ),
                ],
                expected_warning_logged=True,
                expected_warning_message="Assertion urn:li:assertion:missing is not included in entity change type 'ASSERTION_PASSED' and will be ignored",
            ),
            id="assertion_removal_warns_when_assertion_not_in_filter",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:other"]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                ],
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:missing"
                ),
                warn_if_missing=False,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:other"]
                        ),
                    ),
                ],
                expected_warning_logged=False,
            ),
            id="assertion_removal_no_warn_when_flag_disabled",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=models.EntityChangeDetailsFilterClass(
                            includeAssertions=["urn:li:assertion:test1"]
                        ),
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),
                    str(models.EntityChangeTypeClass.ASSERTION_FAILED),
                ],
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test1"
                ),
                warn_if_missing=True,
                expected_results=[],
                expected_warning_logged=True,
                expected_warning_message="Assertion urn:li:assertion:test1 is not included in entity change type 'ASSERTION_PASSED' and will be ignored",
            ),
            id="assertion_removal_warns_for_dataset_level_filter",
        ),
        pytest.param(
            RemoveChangeTypesTestParams(
                existing_change_types=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_PASSED,
                        filter=None,
                    ),
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=None,
                    ),
                ],
                change_types_to_remove=[
                    str(models.EntityChangeTypeClass.ASSERTION_PASSED),  # Exists
                    str(
                        models.EntityChangeTypeClass.INCIDENT_RESOLVED
                    ),  # Doesn't exist
                    str(models.EntityChangeTypeClass.TAG_ADDED),  # Doesn't exist
                ],
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_results=[
                    models.EntityChangeDetailsClass(
                        entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
                        filter=None,
                    ),
                ],
                expected_warning_logged=True,
                expected_warning_message="do not exist in the subscription and will be ignored",
            ),
            id="warns_about_nonexistent_change_types",
        ),
    ],
)
def test_remove_change_types(
    subscription_client: SubscriptionClient,
    params: RemoveChangeTypesTestParams,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test _remove_change_types with various scenarios using parametrized tests."""
    if params.should_raise:
        with pytest.raises(AssertionError) as exc_info:
            subscription_client._remove_change_types(
                existing_change_types=params.existing_change_types,
                change_types_to_remove=params.change_types_to_remove,
                assertion_urn_to_remove=params.assertion_urn_to_remove,
                warn_if_missing=params.warn_if_missing,
            )
        if params.expected_error:
            assert params.expected_error in str(exc_info.value)
    else:
        # Store original for immutability check
        original_count = len(params.existing_change_types)

        with caplog.at_level(logging.WARNING):
            result = subscription_client._remove_change_types(
                existing_change_types=params.existing_change_types,
                change_types_to_remove=params.change_types_to_remove,
                assertion_urn_to_remove=params.assertion_urn_to_remove,
                warn_if_missing=params.warn_if_missing,
            )

        # Verify result matches expected
        assert len(result) == len(params.expected_results)

        # Check each expected result has a matching actual result
        for expected_ect in params.expected_results:
            matching_actual = next(
                (
                    ect
                    for ect in result
                    if ect.entityChangeType == expected_ect.entityChangeType
                ),
                None,
            )
            assert matching_actual is not None, (
                f"Missing entity change type: {expected_ect.entityChangeType}"
            )

            # Use our helper method for detailed comparison
            assert_entity_change_details_equal(matching_actual, expected_ect)

        # Verify original list was not modified (immutability)
        assert len(params.existing_change_types) == original_count

        # Check warning logging expectations
        if params.expected_warning_logged:
            warning_records = [
                record for record in caplog.records if record.levelname == "WARNING"
            ]
            assert len(warning_records) >= 1, (
                "Expected at least one warning to be logged"
            )
            if params.expected_warning_message:
                found_expected_message = any(
                    params.expected_warning_message in record.message
                    for record in warning_records
                )
                assert found_expected_message, (
                    f"Expected warning message '{params.expected_warning_message}' not found in logged warnings"
                )
        else:
            # Verify no warnings were logged (unless it's about nonexistent change types)
            warning_records = [
                record for record in caplog.records if record.levelname == "WARNING"
            ]
            if warning_records:
                # Only allow warnings about nonexistent change types for existing tests
                for record in warning_records:
                    assert (
                        "do not exist in the subscription and will be ignored"
                        in record.message
                    )


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=None,
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_result=None,
                expected_warning_logged=False,
            ),
            id="no_filter_no_assertion_no_warning",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=None,
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=None,
                expected_warning_logged=True,
                expected_warning_message="Assertion urn:li:assertion:test is not included in entity change type 'ASSERTION_PASSED' and will be ignored",
            ),
            id="no_filter_with_assertion_warns",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=None,
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=False,
                expected_result=None,
                expected_warning_logged=False,
            ),
            id="no_filter_with_assertion_no_warn_flag",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=None
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=None
                ),
                expected_warning_logged=True,
                expected_warning_message="Assertion urn:li:assertion:test is not included in entity change type 'ASSERTION_PASSED' and will be ignored",
            ),
            id="filter_with_none_assertions_warns",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[]
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[]
                ),
                expected_warning_logged=True,
                expected_warning_message="Assertion urn:li:assertion:test is not included in entity change type 'ASSERTION_PASSED' and will be ignored",
            ),
            id="filter_with_empty_assertions_warns",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:other"]
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:other"]
                ),
                expected_warning_logged=True,
                expected_warning_message="Assertion urn:li:assertion:test is not included in entity change type 'ASSERTION_PASSED' and will be ignored",
            ),
            id="assertion_not_in_filter_warns",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:other"]
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=False,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:other"]
                ),
                expected_warning_logged=False,
            ),
            id="assertion_not_in_filter_no_warn_flag",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:test"]
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=None,
                expected_warning_logged=False,
            ),
            id="single_assertion_removed_returns_none",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[
                        "urn:li:assertion:test",
                        "urn:li:assertion:other",
                    ]
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:other"]
                ),
                expected_warning_logged=False,
            ),
            id="multiple_assertions_remove_one",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_PASSED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[
                        "urn:li:assertion:keep1",
                        "urn:li:assertion:remove",
                        "urn:li:assertion:keep2",
                    ]
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:remove"
                ),
                warn_if_missing=True,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[
                        "urn:li:assertion:keep1",
                        "urn:li:assertion:keep2",
                    ]
                ),
                expected_warning_logged=False,
            ),
            id="multiple_assertions_remove_middle_one",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_FAILED",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=[
                        "urn:li:assertion:test",
                        "urn:li:assertion:test",
                    ]  # Duplicates
                ),
                assertion_urn_to_remove=AssertionUrn.from_string(
                    "urn:li:assertion:test"
                ),
                warn_if_missing=True,
                expected_result=None,
                expected_warning_logged=False,
            ),
            id="duplicate_assertions_all_removed",
        ),
        pytest.param(
            RemoveChangeTypesFilterTestParams(
                entity_change_type="ASSERTION_ERROR",
                existing_filter=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
                assertion_urn_to_remove=None,
                warn_if_missing=True,
                expected_result=models.EntityChangeDetailsFilterClass(
                    includeAssertions=["urn:li:assertion:existing"]
                ),
                expected_warning_logged=False,
            ),
            id="no_assertion_to_remove_returns_existing",
        ),
    ],
)
def test_remove_change_types_filter(
    subscription_client: SubscriptionClient,
    params: RemoveChangeTypesFilterTestParams,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test _remove_change_types_filter with various scenarios."""
    with caplog.at_level(logging.WARNING):
        result = subscription_client._remove_change_types_filter(
            entity_change_type=params.entity_change_type,
            existing_filter=params.existing_filter,
            assertion_urn_to_remove=params.assertion_urn_to_remove,
            warn_if_missing=params.warn_if_missing,
        )

    # Use our helper method for detailed comparison
    assert_entity_change_details_filter_equal(result, params.expected_result)

    # Check warning logging expectations
    if params.expected_warning_logged:
        assert len(caplog.records) == 1
        warning_record = caplog.records[0]
        assert warning_record.levelname == "WARNING"
        if params.expected_warning_message:
            assert params.expected_warning_message in warning_record.message
    else:
        # Verify no warnings were logged
        warning_records = [
            record for record in caplog.records if record.levelname == "WARNING"
        ]
        assert len(warning_records) == 0


# Tests for string format support
@pytest.mark.parametrize(
    "input_urn,expected_type,expected_urn,should_raise,expected_error",
    [
        # Valid corpuser string
        (
            "urn:li:corpuser:test_user",
            CorpUserUrn,
            "urn:li:corpuser:test_user",
            False,
            None,
        ),
        # Valid corpGroup string
        (
            "urn:li:corpGroup:test_group",
            CorpGroupUrn,
            "urn:li:corpGroup:test_group",
            False,
            None,
        ),
        # CorpUserUrn object (passthrough)
        (
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            CorpUserUrn,
            "urn:li:corpuser:test_user",
            False,
            None,
        ),
        # CorpGroupUrn object (passthrough)
        (
            CorpGroupUrn.from_string("urn:li:corpGroup:test_group"),
            CorpGroupUrn,
            "urn:li:corpGroup:test_group",
            False,
            None,
        ),
        # Invalid format
        ("urn:li:dataset:invalid", None, None, True, "Unsupported subscriber URN type"),
    ],
)
def test_maybe_parse_subscriber_urn(
    subscription_client: SubscriptionClient,
    input_urn: Union[str, CorpUserUrn, CorpGroupUrn],
    expected_type: Optional[Type[Union[CorpUserUrn, CorpGroupUrn]]],
    expected_urn: Optional[str],
    should_raise: bool,
    expected_error: Optional[str],
) -> None:
    """Test _maybe_parse_subscriber_urn with various input formats."""
    if should_raise:
        with pytest.raises(SdkUsageError) as exc_info:
            subscription_client._maybe_parse_subscriber_urn(input_urn)
        assert expected_error is not None and expected_error in str(exc_info.value)
    else:
        result = subscription_client._maybe_parse_subscriber_urn(input_urn)
        assert expected_type is not None
        assert isinstance(result, expected_type)
        assert result.urn() == expected_urn


def test_subscribe_stable_id_prevents_duplicates_during_eventual_consistency(
    subscription_client: SubscriptionClient,
    any_dataset_urn: DatasetUrn,
    any_user_urn: CorpUserUrn,
    any_entity_change_types: List[str],
) -> None:
    """
    Test that two successive subscription calls with same dataset and actor
    create subscriptions with the same stable URN, even when backend doesn't
    resolve the first subscription due to eventual consistency.
    """

    # Mock: No existing subscriptions found for both calls (simulates eventual consistency)
    subscription_client.client.resolve.subscription.return_value = []  # type: ignore[attr-defined]

    # Execute first subscription
    subscription_client.subscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=any_entity_change_types,
    )

    # Execute second subscription with same parameters (before first is visible)
    subscription_client.subscribe(
        urn=any_dataset_urn,
        subscriber_urn=any_user_urn,
        entity_change_types=any_entity_change_types,
    )

    # Verify both calls resulted in upserts
    assert subscription_client.client.entities.upsert.call_count == 2  # type: ignore[attr-defined]

    # Get the subscriptions that were passed to upsert
    first_call_args = subscription_client.client.entities.upsert.call_args_list[0]  # type: ignore[attr-defined]
    second_call_args = subscription_client.client.entities.upsert.call_args_list[1]  # type: ignore[attr-defined]

    first_subscription = first_call_args[0][0]
    second_subscription = second_call_args[0][0]

    assert isinstance(first_subscription, Subscription)
    assert isinstance(second_subscription, Subscription)

    # Critical test: Both subscriptions should have the same stable URN
    assert first_subscription.urn == second_subscription.urn
    assert first_subscription.urn.entity_type == "subscription"

    # Verify subscription details are correct
    for subscription in [first_subscription, second_subscription]:
        subscription_info = subscription.info
        assert subscription_info.entityUrn == any_dataset_urn.urn()
        assert subscription_info.actorUrn == any_user_urn.urn()
        assert subscription_info.actorType == "corpuser"
