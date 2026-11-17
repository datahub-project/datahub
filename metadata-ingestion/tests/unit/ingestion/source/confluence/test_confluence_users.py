from typing import List, Optional, Set
from unittest.mock import MagicMock

import pytest
from atlassian.errors import ApiNotFoundError, ApiPermissionError
from requests import HTTPError

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.models import ConfluenceUser
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import UserRef
from datahub.ingestion.source.confluence.users import (
    MAX_CONSECUTIVE_DATAHUB_FAILURES,
    MAX_USER_LOOKUP_ATTEMPTS,
    ConfluenceUserResolver,
    DataHubUserMatcher,
)
from datahub.metadata.schema_classes import StatusClass
from tests.unit.ingestion.source.confluence.conftest import make_config, make_source


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


def _graph(
    existing: Set[str],
    by_email: Optional[List[str]] = None,
    soft_deleted: Optional[Set[str]] = None,
) -> MagicMock:
    graph = MagicMock()
    graph.exists.side_effect = lambda urn: urn in existing
    graph.get_aspect.side_effect = lambda urn, aspect: StatusClass(
        removed=urn in (soft_deleted or set())
    )
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


def test_one_datahub_lookup_error_only_costs_that_users_link() -> None:
    graph = _graph({"urn:li:corpuser:b@example.com"})
    graph.exists.side_effect = [ConnectionError("timeout"), True]
    report = ConfluenceSourceReport()
    matcher = DataHubUserMatcher(graph, report)

    assert matcher.urn_for(ConfluenceUser(email="a@example.com")) is None
    assert matcher.urn_for(ConfluenceUser(email="b@example.com")) == (
        "urn:li:corpuser:b@example.com"
    )
    assert not report.warnings


def test_repeated_datahub_lookup_errors_stop_linking_for_the_run() -> None:
    graph = MagicMock()
    graph.exists.side_effect = ConnectionError("gms down")
    report = ConfluenceSourceReport()
    matcher = DataHubUserMatcher(graph, report)

    for i in range(MAX_CONSECUTIVE_DATAHUB_FAILURES + 2):
        assert matcher.urn_for(ConfluenceUser(email=f"{i}@example.com")) is None

    assert graph.exists.call_count == MAX_CONSECUTIVE_DATAHUB_FAILURES
    assert len(report.warnings) == 1


def test_soft_deleted_datahub_user_is_not_linked() -> None:
    urn = "urn:li:corpuser:alex@example.com"
    matcher = DataHubUserMatcher(
        _graph({urn}, soft_deleted={urn}), ConfluenceSourceReport()
    )

    assert matcher.urn_for(ConfluenceUser(email="alex@example.com")) is None


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


def test_missing_user_is_not_retried() -> None:
    client = MagicMock()
    client.get_user_details_by_accountid.side_effect = HTTPError(
        response=MagicMock(status_code=404)
    )
    report = ConfluenceSourceReport()
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    resolver(UserRef(account_id="557058:aaaa"))
    resolver(UserRef(account_id="557058:aaaa"))

    client.get_user_details_by_accountid.assert_called_once()


def test_profile_email_search_also_tries_the_lowercased_email() -> None:
    graph = _graph(set(), ["urn:li:corpuser:adoe"])
    matcher = DataHubUserMatcher(graph, ConfluenceSourceReport())

    assert matcher.urn_for(ConfluenceUser(email="Alex@Example.com")) == (
        "urn:li:corpuser:adoe"
    )
    (rule,) = graph.get_urns_by_filter.call_args.kwargs["extraFilters"]
    assert rule["values"] == ["Alex@Example.com", "alex@example.com"]


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
    resolver = ConfluenceUserResolver(ConfluenceClient(client, report), report)

    for account_id in ["557058:aaaa", "557058:bbbb", "557058:cccc"]:
        assert resolver(UserRef(account_id=account_id)) is None

    client.get_user_details_by_accountid.assert_called_once()
    assert len(report.warnings) == 1
    assert report.mentions_unresolved == 3


@pytest.mark.parametrize("link_mentions", [True, False])
def test_missing_datahub_connection_is_reported_only_when_linking_is_on(
    link_mentions: bool,
) -> None:
    source = make_source(make_config(link_mentions_to_datahub_users=link_mentions))

    titles = [entry.title for entry in source.report.infos]
    assert ("Mentions are not linked to DataHub users" in titles) is link_mentions
