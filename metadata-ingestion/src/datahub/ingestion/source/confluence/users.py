from itertools import islice
from typing import Dict, List, Optional

from atlassian.errors import ApiNotFoundError, ApiPermissionError
from requests import HTTPError

from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.constants import (
    CORPUSER_EMAIL_FIELD,
)
from datahub.ingestion.source.confluence.models import ConfluenceUser
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import ResolvedUser, UserRef
from datahub.metadata.schema_classes import StatusClass

# A timeout or rate limit is retried on the user's next mention, with no backoff,
# so a burst can use up these attempts within moments. The user then shows as
# @user for the rest of the run.
MAX_USER_LOOKUP_ATTEMPTS = 3

# DataHub lookups that fail this many times in a row turn mention linking off
# for the rest of the run.
MAX_CONSECUTIVE_DATAHUB_FAILURES = 3

# Bad credentials or no permission to view users: true for every user and for
# the whole run, so lookups stop after the first one.
_ACCESS_DENIED_HTTP_STATUSES = frozenset([401, 403])
_NOT_FOUND_HTTP_STATUS = 404


def _http_status(e: Exception) -> Optional[int]:
    response = getattr(e, "response", None) if isinstance(e, HTTPError) else None
    return response.status_code if response is not None else None


# atlassian-python-api turns 403 and 404 into its own errors for these
# endpoints in some versions and leaves a raw HTTPError in others.
def _is_access_denied(e: Exception) -> bool:
    return isinstance(e, ApiPermissionError) or (
        _http_status(e) in _ACCESS_DENIED_HTTP_STATUSES
    )


def _is_user_missing(e: Exception) -> bool:
    return isinstance(e, ApiNotFoundError) or _http_status(e) == _NOT_FOUND_HTTP_STATUS


class DataHubUserMatcher:
    """Finds the existing DataHub user for a Confluence user.

    Only users that already exist in DataHub are returned: a mention linked
    to a user urn that does not exist would show a broken user, which is worse
    than the plain name. Candidates are tried in order: a user whose urn is the
    email, a user whose profile has that email, then a user whose urn is the
    Data Center username.
    """

    def __init__(self, graph: DataHubGraph, report: ConfluenceSourceReport):
        self.graph = graph
        self.report = report
        self._consecutive_failures = 0

    def urn_for(self, user: ConfluenceUser) -> Optional[str]:
        if self._consecutive_failures >= MAX_CONSECUTIVE_DATAHUB_FAILURES:
            return None
        try:
            urn = self._match(user)
        except Exception as e:
            # One timeout only costs this user's link. A connection that keeps
            # failing would fail for every user, so stop after a few in a row.
            self._consecutive_failures += 1
            if self._consecutive_failures >= MAX_CONSECUTIVE_DATAHUB_FAILURES:
                self.report.warning(
                    title="Could not look up DataHub users",
                    message="Mentions are ingested as names without links to "
                    "DataHub users for the rest of this run.",
                    exc=e,
                )
            return None
        self._consecutive_failures = 0
        return urn

    def _match(self, user: ConfluenceUser) -> Optional[str]:
        if user.email:
            for urn in self._urn_candidates(user.email):
                if self._is_live(urn):
                    return urn
            by_email = list(
                islice(
                    self.graph.get_urns_by_filter(
                        entity_types=["corpuser"],
                        extraFilters=[
                            {
                                "field": CORPUSER_EMAIL_FIELD,
                                "condition": "EQUAL",
                                # EQUAL on a keyword field is case-sensitive.
                                "values": list(
                                    dict.fromkeys([user.email, user.email.lower()])
                                ),
                            }
                        ],
                    ),
                    2,
                )
            )
            # Two DataHub users with the same email: neither is a safe match.
            if len(by_email) == 1:
                return by_email[0]
        if user.username:
            urn = make_user_urn(user.username)
            if self._is_live(urn):
                return urn
        return None

    def _is_live(self, urn: str) -> bool:
        # exists() only checks the key aspect, so a soft-deleted user would
        # still match. The email search already leaves those out.
        if not self.graph.exists(urn):
            return False
        status = self.graph.get_aspect(urn, StatusClass)
        return not (status and status.removed)

    @staticmethod
    def _urn_candidates(email: str) -> List[str]:
        return list(dict.fromkeys([make_user_urn(email), make_user_urn(email.lower())]))


class ConfluenceUserResolver:
    """Looks up mentioned users, once per user per run.

    Storage format stores a mention as a bare user id, and Confluence only
    fills in the name when it renders the page, so the name has to come from
    the user API. A user that no longer exists is cached as None, so a user
    mentioned on many pages costs one call. If the API user can't view users at
    all, lookups stop for the rest of the run after one warning. Other errors are
    retried on later mentions. Unresolved mentions fall back to a placeholder.
    """

    def __init__(
        self,
        client: ConfluenceClient,
        report: ConfluenceSourceReport,
        datahub_users: Optional[DataHubUserMatcher] = None,
    ):
        self.client = client
        self.report = report
        self.datahub_users = datahub_users
        self._cache: Dict[UserRef, Optional[ResolvedUser]] = {}
        self._attempts: Dict[UserRef, int] = {}
        self._access_denied = False

    def __call__(self, ref: UserRef) -> Optional[ResolvedUser]:
        if self._access_denied:
            resolved = None
        elif ref in self._cache:
            resolved = self._cache[ref]
        else:
            resolved = self._lookup(ref)
        if resolved is None:
            self.report.mentions_unresolved += 1
        else:
            self.report.mentions_resolved += 1
            if resolved.urn:
                self.report.mentions_linked += 1
        return resolved

    def _lookup(self, ref: UserRef) -> Optional[ResolvedUser]:
        self._attempts[ref] = self._attempts.get(ref, 0) + 1
        try:
            user = self.client.get_user(ref)
        except Exception as e:
            if _is_access_denied(e):
                self._stop_lookups(e)
            elif _is_user_missing(e) or self._attempts[ref] >= MAX_USER_LOOKUP_ATTEMPTS:
                self._give_up(ref, e)
            return None

        resolved = None
        if user.name:
            urn = self.datahub_users.urn_for(user) if self.datahub_users else None
            resolved = ResolvedUser(name=user.name, urn=urn)
        self._cache[ref] = resolved
        return resolved

    def _stop_lookups(self, e: Exception) -> None:
        self._access_denied = True
        self.report.warning(
            title="Could not look up mentioned users",
            message="The API user is not allowed to view users, so every mention "
            "is shown as @user. Grant it permission to browse users to show names.",
            exc=e,
        )

    def _give_up(self, ref: UserRef, e: Exception) -> None:
        self._cache[ref] = None
        self.report.warning(
            title="Could not resolve mentioned user",
            message="Mentions of this user are shown as @user. The user may no "
            "longer exist, or the user API kept failing.",
            context=ref.account_id or ref.userkey or ref.username,
            exc=e,
        )
