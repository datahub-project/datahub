from unittest.mock import MagicMock

import pytest
from atlassian.errors import ApiNotFoundError
from requests import HTTPError

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import UserRef
from datahub.ingestion.source.confluence.users import (
    MAX_USER_LOOKUP_ATTEMPTS,
    ConfluenceUserResolver,
)


def test_each_user_is_looked_up_once() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.return_value = {"displayName": "Alex Doe"}
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    names = [resolver(UserRef(account_id="557058:aaaa")) for _ in range(3)]

    assert names == ["Alex Doe"] * 3
    client.get_user_details_by_accountid.assert_called_once_with("557058:aaaa")
    assert report.mentions_resolved == 3


def test_data_center_user_key_uses_the_user_key_lookup() -> None:
    client = MagicMock()
    client.get_user_details_by_userkey.return_value = {"displayName": "Sam Roe"}
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    assert resolver(UserRef(userkey="ff80")) == "Sam Roe"
    client.get_user_details_by_accountid.assert_not_called()


def test_failed_lookup_is_warned_once_and_cached() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = ApiNotFoundError("gone")
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    assert resolver(UserRef(account_id="557058:gone")) is None
    assert resolver(UserRef(account_id="557058:gone")) is None

    client.get_user_details_by_accountid.assert_called_once()
    assert report.mentions_unresolved == 2
    assert len(report.warnings) == 1


def test_transient_failure_is_retried_on_a_later_mention() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = [
        TimeoutError("read timed out"),
        {"displayName": "Alex Doe"},
    ]
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    assert resolver(UserRef(account_id="557058:aaaa")) is None
    assert resolver(UserRef(account_id="557058:aaaa")) == "Alex Doe"


def test_repeated_transient_failures_give_up_after_the_attempt_limit() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = TimeoutError("down")
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    for _ in range(MAX_USER_LOOKUP_ATTEMPTS + 2):
        resolver(UserRef(account_id="557058:aaaa"))

    assert client.get_user_details_by_accountid.call_count == MAX_USER_LOOKUP_ATTEMPTS
    assert len(report.warnings) == 1


@pytest.mark.parametrize("status", [401, 403, 404])
def test_permanent_http_errors_are_not_retried(status: int) -> None:
    response = MagicMock(status_code=status)
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = HTTPError(response=response)
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    resolver(UserRef(account_id="557058:aaaa"))
    resolver(UserRef(account_id="557058:aaaa"))

    client.get_user_details_by_accountid.assert_called_once()
