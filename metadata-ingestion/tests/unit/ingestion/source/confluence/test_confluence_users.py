from typing import List, Optional, Set
from unittest.mock import MagicMock

import pytest
from atlassian.errors import ApiNotFoundError
from requests import HTTPError

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.models import ConfluenceUser
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import UserRef
from datahub.ingestion.source.confluence.users import (
    MAX_USER_LOOKUP_ATTEMPTS,
    ConfluenceUserResolver,
    DataHubUserMatcher,
)


def test_each_user_is_looked_up_once() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.return_value = {"displayName": "Alex Doe"}
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    names = [resolver(UserRef(account_id="557058:aaaa")) for _ in range(3)]

    assert [n.name for n in names if n] == ["Alex Doe"] * 3
    client.get_user_details_by_accountid.assert_called_once_with("557058:aaaa")
    assert report.mentions_resolved == 3


def test_data_center_user_key_uses_the_user_key_lookup() -> None:
    client = MagicMock()
    client.get_user_details_by_userkey.return_value = {"displayName": "Sam Roe"}
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    resolved = resolver(UserRef(userkey="ff80"))
    assert resolved is not None and resolved.name == "Sam Roe"
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
    resolved = resolver(UserRef(account_id="557058:aaaa"))
    assert resolved is not None and resolved.name == "Alex Doe"


def test_repeated_transient_failures_give_up_after_the_attempt_limit() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = TimeoutError("down")
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    for _ in range(MAX_USER_LOOKUP_ATTEMPTS + 2):
        resolver(UserRef(account_id="557058:aaaa"))

    assert client.get_user_details_by_accountid.call_count == MAX_USER_LOOKUP_ATTEMPTS
    assert len(report.warnings) == 1


def _graph(existing: Set[str], by_email: Optional[List[str]] = None) -> MagicMock:
    graph = MagicMock()
    graph.exists.side_effect = lambda urn: urn in existing
    graph.get_urns_by_filter.return_value = iter(by_email or [])
    return graph


def test_datahub_user_is_matched_by_email_urn_then_profile_email_then_username() -> (
    None
):
    report = ConfluenceSourceReport()
    by_urn = DataHubUserMatcher(_graph({"urn:li:corpuser:alex@example.com"}), report)
    by_profile = DataHubUserMatcher(_graph(set(), ["urn:li:corpuser:adoe"]), report)
    by_username = DataHubUserMatcher(_graph({"urn:li:corpuser:sroe"}), report)

    assert by_urn.urn_for(ConfluenceUser(email="Alex@example.com")) == (
        "urn:li:corpuser:alex@example.com"
    )
    assert by_profile.urn_for(ConfluenceUser(email="alex@example.com")) == (
        "urn:li:corpuser:adoe"
    )
    assert by_username.urn_for(ConfluenceUser(username="sroe")) == (
        "urn:li:corpuser:sroe"
    )


def test_unknown_or_ambiguous_datahub_user_is_not_linked() -> None:
    report = ConfluenceSourceReport()
    two_matches = DataHubUserMatcher(
        _graph(set(), ["urn:li:corpuser:a", "urn:li:corpuser:b"]), report
    )

    assert two_matches.urn_for(ConfluenceUser(email="shared@example.com")) is None
    assert (
        DataHubUserMatcher(_graph(set()), report).urn_for(
            ConfluenceUser(username="nobody")
        )
        is None
    )


def test_datahub_lookup_error_stops_linking_for_the_run() -> None:
    graph = MagicMock()
    graph.exists.side_effect = ConnectionError("gms down")
    report = ConfluenceSourceReport()
    matcher = DataHubUserMatcher(graph, report)

    assert matcher.urn_for(ConfluenceUser(email="a@example.com")) is None
    assert matcher.urn_for(ConfluenceUser(email="b@example.com")) is None
    graph.exists.assert_called_once()
    assert len(report.warnings) == 1


def test_resolver_returns_the_datahub_urn_and_counts_linked_mentions() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.return_value = {
        "displayName": "Alex Doe",
        "email": "alex@example.com",
    }
    report = ConfluenceSourceReport()
    matcher = DataHubUserMatcher(_graph({"urn:li:corpuser:alex@example.com"}), report)
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report, matcher)

    resolved = resolver(UserRef(account_id="557058:aaaa"))

    assert resolved is not None
    assert resolved.urn == "urn:li:corpuser:alex@example.com"
    assert report.mentions_linked == 1


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
