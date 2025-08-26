import pathlib

import pytest

from acryl_datahub_cloud.sdk.entities.subscription import Subscription
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import SubscriptionUrn
from datahub.testing.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "subscription_golden"


@pytest.fixture
def any_subscription_id() -> str:
    return "my_subscription_id_1"


@pytest.fixture
def any_subscription_urn(any_subscription_id: str) -> SubscriptionUrn:
    return SubscriptionUrn.from_string(f"urn:li:subscription:{any_subscription_id}")


@pytest.fixture
def any_basic_subscription_info() -> models.SubscriptionInfoClass:
    return models.SubscriptionInfoClass(
        actorUrn="urn:li:actor:actor1",
        actorType="actorType1",
        types=[models.SubscriptionTypeClass.ENTITY_CHANGE],
        createdOn=models.AuditStampClass(
            time=1234567890,
            actor="urn:li:actor:actor1",
        ),
        updatedOn=models.AuditStampClass(
            time=1234567890,
            actor="urn:li:actor:actor1",
        ),
    )


@pytest.fixture
def any_complex_subscription_info(
    any_basic_subscription_info: models.SubscriptionInfoClass,
) -> models.SubscriptionInfoClass:
    ret = any_basic_subscription_info
    ret.types = [
        models.SubscriptionTypeClass.ENTITY_CHANGE,
        models.SubscriptionTypeClass.UPSTREAM_ENTITY_CHANGE,
    ]
    ret.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
    ret.entityChangeTypes = [
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_FAILED,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[
                    "urn:li:assertion:assertion1",
                    "urn:li:assertion:assertion2",
                ]
            ),
        ),
        models.EntityChangeDetailsClass(
            entityChangeType=models.EntityChangeTypeClass.ASSERTION_ERROR,
            filter=models.EntityChangeDetailsFilterClass(
                includeAssertions=[
                    "urn:li:assertion:assertion1",
                    "urn:li:assertion:assertion2",
                ]
            ),
        ),
    ]
    ret.notificationConfig = models.SubscriptionNotificationConfigClass(
        notificationSettings=models.NotificationSettingsClass(
            sinkTypes=[models.NotificationSinkTypeClass.SLACK],
            slackSettings=models.SlackNotificationSettingsClass(
                userHandle="@user1",
                channels=["channel1", "channel2"],
            ),
            settings={
                "key": models.NotificationSettingClass(
                    value=models.NotificationSettingValueClass.ENABLED,
                    params={"key1": "value1", "key2": "value2"},
                ),
            },
        )
    )
    return ret


def test_subscription_basic(
    any_subscription_urn: SubscriptionUrn,
    any_basic_subscription_info: models.SubscriptionInfoClass,
) -> None:
    subscription = Subscription(
        id=any_subscription_urn,
        info=any_basic_subscription_info,
    )
    assert subscription.urn == any_subscription_urn
    assert subscription.info == any_basic_subscription_info

    assert_entity_golden(
        subscription, _GOLDEN_DIR / "test_subscription_basic_golden.json"
    )


def test_subscription_basic_no_id(
    any_basic_subscription_info: models.SubscriptionInfoClass,
) -> None:
    subscription = Subscription(
        info=any_basic_subscription_info,
    )
    assert subscription.urn is not None
    assert isinstance(subscription.urn, SubscriptionUrn)


def test_subscription_complex(
    any_subscription_urn: SubscriptionUrn,
    any_complex_subscription_info: models.SubscriptionInfoClass,
) -> None:
    subscription = Subscription(
        id=any_subscription_urn,
        info=any_complex_subscription_info,
    )
    assert subscription.urn == any_subscription_urn
    assert subscription.info == any_complex_subscription_info

    assert_entity_golden(
        subscription, _GOLDEN_DIR / "test_subscription_complex_golden.json"
    )
