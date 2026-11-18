from unittest.mock import MagicMock

import pytest
from atlassian.errors import ApiNotFoundError, ApiPermissionError
from requests import HTTPError

from datahub.ingestion.source.confluence.confluence_html import UserRef
from datahub.ingestion.source.confluence.confluence_report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.confluence_users import (
    MAX_USER_LOOKUP_ATTEMPTS,
    ConfluenceUserResolver,
)


def test_each_user_is_looked_up_once() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.return_value = {"displayName": "Alex Doe"}
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(client, report)

    names = [resolver(UserRef(account_id="557058:aaaa")) for _ in range(3)]

    assert names == ["Alex Doe"] * 3
    client.get_user_details_by_accountid.assert_called_once_with("557058:aaaa")
    assert report.mentions_resolved == 3


def test_data_center_user_key_uses_the_user_key_lookup() -> None:
    client = MagicMock()
    client.get_user_details_by_userkey.return_value = {"displayName": "Sam Roe"}
    resolver = ConfluenceUserResolver(client, ConfluenceSourceReport())

    assert resolver(UserRef(userkey="ff80")) == "Sam Roe"
    client.get_user_details_by_accountid.assert_not_called()


def test_failed_lookup_is_warned_once_and_cached() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = ApiNotFoundError("gone")
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(client, report)

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
    resolver = ConfluenceUserResolver(client, ConfluenceSourceReport())

    assert resolver(UserRef(account_id="557058:aaaa")) is None
    assert resolver(UserRef(account_id="557058:aaaa")) == "Alex Doe"


def test_repeated_transient_failures_give_up_after_the_attempt_limit() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = TimeoutError("down")
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(client, report)

    for _ in range(MAX_USER_LOOKUP_ATTEMPTS + 2):
        resolver(UserRef(account_id="557058:aaaa"))

    assert client.get_user_details_by_accountid.call_count == MAX_USER_LOOKUP_ATTEMPTS
    assert len(report.warnings) == 1


def test_missing_user_is_not_retried() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = HTTPError(
        response=MagicMock(status_code=404)
    )
    resolver = ConfluenceUserResolver(client, ConfluenceSourceReport())

    resolver(UserRef(account_id="557058:aaaa"))
    resolver(UserRef(account_id="557058:aaaa"))

    client.get_user_details_by_accountid.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        HTTPError(response=MagicMock(status_code=401)),
        HTTPError(response=MagicMock(status_code=403)),
        ApiPermissionError("no browse users"),
    ],
)
def test_access_denied_stops_all_lookups_with_one_warning(error: Exception) -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = error
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(client, report)

    for account_id in ["557058:aaaa", "557058:bbbb", "557058:cccc"]:
        assert resolver(UserRef(account_id=account_id)) is None

    client.get_user_details_by_accountid.assert_called_once()
    assert len(report.warnings) == 1
    assert report.mentions_unresolved == 3
