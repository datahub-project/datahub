from typing import Dict, Optional

from atlassian.errors import ApiNotFoundError, ApiPermissionError
from requests import HTTPError

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import UserRef

# A timeout or rate limit is retried on the user's next mention, with no backoff,
# so a burst can use up these attempts within moments. The user then shows as
# @user for the rest of the run.
MAX_USER_LOOKUP_ATTEMPTS = 3

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


class ConfluenceUserResolver:
    """Looks up display names for mentioned users, once per user per run.

    Storage format stores a mention as a bare user id, and Confluence only
    fills in the name when it renders the page, so the name has to come from
    the user API. A user that no longer exists is cached as None, so a user
    mentioned on many pages costs one call. If the API user can't view users at
    all, lookups stop for the rest of the run after one warning. Other errors are
    retried on later mentions. Unresolved mentions fall back to a placeholder.
    """

    def __init__(self, client: ConfluenceClient, report: ConfluenceSourceReport):
        self.client = client
        self.report = report
        self._cache: Dict[UserRef, Optional[str]] = {}
        self._attempts: Dict[UserRef, int] = {}
        self._access_denied = False

    def __call__(self, ref: UserRef) -> Optional[str]:
        if self._access_denied:
            name = None
        elif ref in self._cache:
            name = self._cache[ref]
        else:
            name = self._lookup(ref)
        if name:
            self.report.mentions_resolved += 1
        else:
            self.report.mentions_unresolved += 1
        return name

    def _lookup(self, ref: UserRef) -> Optional[str]:
        self._attempts[ref] = self._attempts.get(ref, 0) + 1
        try:
            name = self.client.get_user(ref).name
        except Exception as e:
            if _is_access_denied(e):
                self._stop_lookups(e)
            elif _is_user_missing(e) or self._attempts[ref] >= MAX_USER_LOOKUP_ATTEMPTS:
                self._give_up(ref, e)
            return None
        self._cache[ref] = name
        return name

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
