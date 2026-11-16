from typing import Any, Dict, Optional

from atlassian import Confluence
from atlassian.errors import ApiNotFoundError, ApiPermissionError

from datahub.ingestion.source.confluence.confluence_html import UserRef
from datahub.ingestion.source.confluence.confluence_models import ConfluenceUser
from datahub.ingestion.source.confluence.confluence_report import (
    ConfluenceSourceReport,
)

# A timeout or rate limit on the user API is retried on the user's next
# mention, up to this many lookups, before the user is given up on for the run.
MAX_USER_LOOKUP_ATTEMPTS = 3


class ConfluenceUserResolver:
    """Looks up display names for mentioned users, once per user per run.

    Storage format stores a mention as a bare user id, and Confluence only
    fills in the name when it renders the page, so the name has to come from
    the user API. A user that does not exist or cannot be viewed is cached as
    None, so a user mentioned on many pages costs one call; other errors are
    retried on later mentions. Unresolved mentions fall back to a placeholder.
    """

    def __init__(self, client: Confluence, report: ConfluenceSourceReport):
        self.client = client
        self.report = report
        self._cache: Dict[UserRef, Optional[str]] = {}
        self._attempts: Dict[UserRef, int] = {}

    def __call__(self, ref: UserRef) -> Optional[str]:
        name = self._cache[ref] if ref in self._cache else self._lookup(ref)
        if name:
            self.report.mentions_resolved += 1
        else:
            self.report.mentions_unresolved += 1
        return name

    def _lookup(self, ref: UserRef) -> Optional[str]:
        self._attempts[ref] = self._attempts.get(ref, 0) + 1
        try:
            user = ConfluenceUser.model_validate(self._fetch(ref))
        except (ApiNotFoundError, ApiPermissionError) as e:
            # A deleted user or a missing "view users" permission will not
            # change during the run.
            self._give_up(ref, e)
            return None
        except Exception as e:
            if self._attempts[ref] >= MAX_USER_LOOKUP_ATTEMPTS:
                self._give_up(ref, e)
            return None
        self._cache[ref] = user.name
        return user.name

    def _give_up(self, ref: UserRef, e: Exception) -> None:
        self._cache[ref] = None
        self.report.warning(
            title="Could not resolve mentioned user",
            message="Mentions of this user are shown as @user. The API user "
            "may lack permission to view users, or the user no longer exists.",
            context=ref.account_id or ref.userkey or ref.username,
            exc=e,
        )

    def _fetch(self, ref: UserRef) -> Dict[str, Any]:
        if ref.account_id:
            return self.client.get_user_details_by_accountid(ref.account_id)
        if ref.userkey:
            return self.client.get_user_details_by_userkey(ref.userkey)
        return self.client.get_user_details_by_username(ref.username)
