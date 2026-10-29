from typing import Any

import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from datahub.ingestion.source.aws_messaging.clients import SqsClient
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/q{}"


def _sqs() -> Any:
    return boto3.client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_list_queues_follows_next_token() -> None:
    client = _sqs()
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_queues",
            {"QueueUrls": [QUEUE_URL.format(1)], "NextToken": "page-2"},
            {"MaxResults": 1000},
        )
        stubber.add_response(
            "list_queues",
            {"QueueUrls": [QUEUE_URL.format(2)]},
            {"MaxResults": 1000, "NextToken": "page-2"},
        )
        urls = list(SqsClient(client, report).list_queue_urls())
    assert urls == [QUEUE_URL.format(1), QUEUE_URL.format(2)]
    assert not report.failures


def test_first_page_denial_of_the_queue_listing_fails_the_run() -> None:
    # Listing queues is the source's whole job, so a denied first page must not
    # end as a green run with nothing ingested.
    client = _sqs()
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_client_error("list_queues", service_error_code="AccessDenied")
        assert list(SqsClient(client, report).list_queue_urls()) == []
    assert report.failures


def test_optional_first_page_denial_is_a_warning() -> None:
    report = SqsSourceReport()
    error = ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "ListX")
    report.report_listing_failure(
        pages_fetched=0, exc=error, api_label="x:ListX", optional=True
    )
    assert report.warnings and not report.failures


def test_mid_pagination_failure_fails_the_run() -> None:
    client = _sqs()
    report = SqsSourceReport()
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_queues",
            {"QueueUrls": [QUEUE_URL.format(1)], "NextToken": "page-2"},
            {"MaxResults": 1000},
        )
        stubber.add_client_error("list_queues", service_error_code="RequestThrottled")
        assert list(SqsClient(client, report).list_queue_urls()) == [
            QUEUE_URL.format(1)
        ]
    assert report.failures


def test_optional_listing_failure_is_only_a_warning() -> None:
    # A lineage helper failing partway must not block stale removal of the
    # queues and topics, which a report failure would.
    report = SqsSourceReport()
    error = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "x"}}, "ListX"
    )
    report.report_listing_failure(
        pages_fetched=2, exc=error, api_label="x:ListX", optional=True
    )
    assert report.warnings and not report.failures
    report.report_listing_failure(
        pages_fetched=2, exc=error, api_label="sqs:ListQueues"
    )
    assert report.failures
