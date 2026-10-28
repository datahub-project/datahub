import pytest

from datahub.emitter import mce_builder
from datahub.ingestion.source.aws_messaging.arn import (
    UrnBuilder,
    endpoint_host,
    parse_arn,
    parse_redrive_policy,
)


def test_parse_arn_variants() -> None:
    queue = parse_arn("arn:aws:sqs:eu-west-1:123456789012:orders.fifo")
    assert queue is not None
    assert (queue.service, queue.region, queue.account, queue.resource_name) == (
        "sqs",
        "eu-west-1",
        "123456789012",
        "orders.fifo",
    )
    gov = parse_arn("arn:aws-us-gov:sns:us-gov-west-1:123456789012:alerts")
    assert gov is not None and gov.partition == "aws-us-gov"
    function = parse_arn("arn:aws:lambda:us-east-1:123456789012:function:handler")
    assert function is not None and function.resource_name == "handler"
    rule = parse_arn("arn:aws:events:us-east-1:123456789012:rule/custom-bus/my-rule")
    assert rule is not None and rule.resource_name == "my-rule"
    assert str(queue) == "arn:aws:sqs:eu-west-1:123456789012:orders.fifo"


def test_parse_arn_rejects_non_arns() -> None:
    assert parse_arn(None) is None
    assert parse_arn("PendingConfirmation") is None
    assert parse_arn("https://sqs.us-east-1.amazonaws.com/123456789012/q") is None


def test_redrive_policy() -> None:
    policy = parse_redrive_policy(
        '{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq","maxReceiveCount":"5"}'
    )
    assert policy is not None
    assert policy.dead_letter_target_arn.endswith(":dlq")
    assert policy.max_receive_count == 5
    assert parse_redrive_policy("not json") is None
    assert parse_redrive_policy(None) is None


def test_endpoint_host_drops_path_and_query() -> None:
    assert (
        endpoint_host("https://hooks.example.com/a/b?token=secret")
        == "hooks.example.com"
    )
    assert endpoint_host("not a url") is None


def test_urns_use_the_account_from_the_arn() -> None:
    urns = UrnBuilder(
        env="PROD",
        account_id="123456789012",
        platform_instance="prod",
        account_platform_instance_map={"333333333333": "shared"},
    )
    own = parse_arn("arn:aws:sqs:us-east-1:123456789012:orders")
    other = parse_arn("arn:aws:sqs:eu-west-1:210987654321:orders")
    mapped = parse_arn("arn:aws:sqs:us-east-1:333333333333:orders")
    assert own is not None and other is not None and mapped is not None
    assert urns.queue_urn(own) == (
        "urn:li:dataset:(urn:li:dataPlatform:sqs,prod.us-east-1.orders,PROD)"
    )
    # Another account's resource lands under that account's instance, and the
    # region comes from the ARN rather than the recipe.
    assert urns.queue_urn(other) == (
        "urn:li:dataset:(urn:li:dataPlatform:sqs,210987654321.eu-west-1.orders,PROD)"
    )
    assert urns.topic_urn(mapped) == (
        "urn:li:dataset:(urn:li:dataPlatform:sns,shared.us-east-1.orders,PROD)"
    )


def test_unresolved_account_keeps_one_platform_instance() -> None:
    urns = UrnBuilder(env="PROD", account_id=None, platform_instance=None)
    arn = parse_arn("arn:aws:sqs:us-east-1:123456789012:orders")
    assert arn is not None
    assert urns.instance_for(arn.account) is None
    assert (
        urns.queue_urn(arn)
        == "urn:li:dataset:(urn:li:dataPlatform:sqs,us-east-1.orders,PROD)"
    )


def test_resource_id_follows_dataset_urn_lowercasing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arn = parse_arn("arn:aws:sqs:us-east-1:123456789012:Orders")
    assert arn is not None
    monkeypatch.setattr(mce_builder, "DATASET_URN_TO_LOWER", True)
    assert UrnBuilder.resource_id(arn) == "us-east-1.orders"
    monkeypatch.setattr(mce_builder, "DATASET_URN_TO_LOWER", False)
    assert UrnBuilder.resource_id(arn) == "us-east-1.Orders"
