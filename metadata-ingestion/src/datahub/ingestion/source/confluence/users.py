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

# A timeout or rate limit on the user API is retried on the user's next
# mention, up to this many lookups, before the user is given up on for the run.
MAX_USER_LOOKUP_ATTEMPTS = 3

# Statuses that will not change during the run: bad credentials, no
# permission to view users, or a user that no longer exists.
_PERMANENT_HTTP_STATUSES = frozenset([401, 403, 404])


def _is_permanent(e: Exception) -> bool:
    # atlassian-python-api turns 403 and 404 into its own errors for these
    # endpoints in some versions and leaves a raw HTTPError in others.
    if isinstance(e, (ApiNotFoundError, ApiPermissionError)):
        return True
    response = getattr(e, "response", None) if isinstance(e, HTTPError) else None
    return response is not None and response.status_code in _PERMANENT_HTTP_STATUSES


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
        self._disabled = False

    def urn_for(self, user: ConfluenceUser) -> Optional[str]:
        if self._disabled:
            return None
        try:
            return self._match(user)
        except Exception as e:
            # A broken DataHub connection would fail for every user, so stop
            # trying after the first error.
            self._disabled = True
            self.report.warning(
                title="Could not look up DataHub users",
                message="Mentions are ingested as names without links to "
                "DataHub users for the rest of this run.",
                exc=e,
            )
            return None

    def _match(self, user: ConfluenceUser) -> Optional[str]:
        if user.email:
            for urn in self._urn_candidates(user.email):
                if self.graph.exists(urn):
                    return urn
            by_email = list(
                islice(
                    self.graph.get_urns_by_filter(
                        entity_types=["corpuser"],
                        extraFilters=[
                            {
                                "field": CORPUSER_EMAIL_FIELD,
                                "condition": "EQUAL",
                                "values": [user.email],
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
            if self.graph.exists(urn):
                return urn
        return None

    @staticmethod
    def _urn_candidates(email: str) -> List[str]:
        return list(dict.fromkeys([make_user_urn(email), make_user_urn(email.lower())]))


class ConfluenceUserResolver:
    """Looks up mentioned users, once per user per run.

    Storage format stores a mention as a bare user id, and Confluence only
    fills in the name when it renders the page, so the name has to come from
    the user API. A user that does not exist or cannot be viewed is cached as
    None, so a user mentioned on many pages costs one call; other errors are
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

    def __call__(self, ref: UserRef) -> Optional[ResolvedUser]:
        resolved = self._cache[ref] if ref in self._cache else self._lookup(ref)
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
            if _is_permanent(e) or self._attempts[ref] >= MAX_USER_LOOKUP_ATTEMPTS:
                self._give_up(ref, e)
            return None

        resolved = None
        if user.name:
            urn = self.datahub_users.urn_for(user) if self.datahub_users else None
            resolved = ResolvedUser(name=user.name, urn=urn)
        self._cache[ref] = resolved
        return resolved

    def _give_up(self, ref: UserRef, e: Exception) -> None:
        self._cache[ref] = None
        self.report.warning(
            title="Could not resolve mentioned user",
            message="Mentions of this user are shown as @user. The API user "
            "may lack permission to view users, or the user no longer exists.",
            context=ref.account_id or ref.userkey or ref.username,
            exc=e,
        )
