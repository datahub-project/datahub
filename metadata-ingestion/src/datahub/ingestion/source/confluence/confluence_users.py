from typing import Any, Dict, Optional

from atlassian import Confluence

from datahub.ingestion.source.confluence.confluence_html import UserRef
from datahub.ingestion.source.confluence.confluence_models import ConfluenceUser
from datahub.ingestion.source.confluence.confluence_report import (
    ConfluenceSourceReport,
)


class ConfluenceUserResolver:
    """Looks up display names for mentioned users, once per user per run.

    Storage format stores a mention as a bare user id, and Confluence only
    fills in the name when it renders the page, so the name has to come from
    the user API. A failed lookup is cached as None so a user mentioned on many
    pages costs one call, and the mention falls back to a placeholder.
    """

    def __init__(self, client: Confluence, report: ConfluenceSourceReport):
        self.client = client
        self.report = report
        self._cache: Dict[UserRef, Optional[str]] = {}

    def __call__(self, ref: UserRef) -> Optional[str]:
        if ref not in self._cache:
            self._cache[ref] = self._lookup(ref)
        name = self._cache[ref]
        if name:
            self.report.mentions_resolved += 1
        else:
            self.report.mentions_unresolved += 1
        return name

    def _lookup(self, ref: UserRef) -> Optional[str]:
        try:
            user = ConfluenceUser.model_validate(self._fetch(ref))
        except Exception as e:
            # Deactivated or deleted users and a missing "view users"
            # permission all land here; the page is still ingested.
            self.report.warning(
                title="Could not resolve mentioned user",
                message="Mentions of this user are shown as @user. The API user "
                "may lack permission to view users, or the user no longer exists.",
                context=ref.account_id or ref.userkey or ref.username,
                exc=e,
            )
            return None
        return user.name

    def _fetch(self, ref: UserRef) -> Dict[str, Any]:
        if ref.account_id:
            return self.client.get_user_details_by_accountid(ref.account_id)
        if ref.userkey:
            return self.client.get_user_details_by_userkey(ref.userkey)
        return self.client.get_user_details_by_username(ref.username)
