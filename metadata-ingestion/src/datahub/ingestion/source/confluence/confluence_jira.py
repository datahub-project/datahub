import re
from typing import Callable, Dict, Optional

from bs4 import BeautifulSoup, Tag

from datahub.ingestion.source.confluence.confluence_client import ConfluenceClient
from datahub.ingestion.source.confluence.confluence_constants import (
    JIRA_BROWSE_PATH,
    JIRA_ISSUE_KEY_PATTERN,
)
from datahub.ingestion.source.confluence.confluence_html import JiraIssueRef
from datahub.ingestion.source.confluence.confluence_report import (
    ConfluenceSourceReport,
)

_ISSUE_KEY = re.compile(JIRA_ISSUE_KEY_PATTERN)

# Pages tried per Jira server before its links are given up on for the run.
MAX_PAGES_PER_JIRA_SERVER = 3

# Macros on the site's default Jira server carry no server id or name.
_DEFAULT_SERVER = ""


class ConfluenceJiraLinker:
    """Turns Jira macro issue keys into links to the issue.

    The storage format only names the Jira server (an application link id),
    and Confluence keeps the server's URL. Confluence uses it when it renders
    the page, so the first page that mentions an issue on a server is fetched
    rendered, and the server's base URL is read from the issue link there.
    Every later issue on that server is linked without another call.
    """

    def __init__(self, client: ConfluenceClient, report: ConfluenceSourceReport):
        self.client = client
        self.report = report
        self._base_urls: Dict[str, str] = {}
        self._failed_pages: Dict[str, int] = {}
        self._rendered_links: Dict[str, Dict[str, str]] = {}

    def for_page(self, page_id: str) -> Callable[[JiraIssueRef], Optional[str]]:
        return lambda ref: self._link(page_id, ref)

    def _link(self, page_id: str, ref: JiraIssueRef) -> Optional[str]:
        server = ref.server_id or ref.server_name or _DEFAULT_SERVER
        base = self._base_urls.get(server) or self._learn_base_url(
            server, page_id, ref.key
        )
        if base is None:
            self.report.jira_links_unresolved += 1
            return None
        self.report.jira_links_resolved += 1
        return f"{base}{JIRA_BROWSE_PATH}{ref.key}"

    def _learn_base_url(self, server: str, page_id: str, key: str) -> Optional[str]:
        if self._failed_pages.get(server, 0) >= MAX_PAGES_PER_JIRA_SERVER:
            return None
        href = self._rendered_issue_links(page_id).get(key)
        if href is None:
            failures = self._failed_pages.get(server, 0) + 1
            self._failed_pages[server] = failures
            if failures == MAX_PAGES_PER_JIRA_SERVER:
                self.report.warning(
                    title="Could not find Jira server URL",
                    message="Issue keys from this Jira server are ingested "
                    "without links. Confluence did not render links to its "
                    "issues, which happens when the Jira server is unreachable "
                    "from Confluence or the API user cannot see the issues.",
                    context=server or "default Jira server",
                )
            return None
        base = href.split(JIRA_BROWSE_PATH, 1)[0]
        self._base_urls[server] = base
        return base

    def _rendered_issue_links(self, page_id: str) -> Dict[str, str]:
        if page_id not in self._rendered_links:
            self._rendered_links[page_id] = self._fetch_issue_links(page_id)
        return self._rendered_links[page_id]

    def _fetch_issue_links(self, page_id: str) -> Dict[str, str]:
        try:
            html = self.client.get_page_view(page_id)
        except Exception as e:
            self.report.warning(
                title="Could not render page to find Jira links",
                message="Jira issue keys on this page are ingested without links.",
                context=page_id,
                exc=e,
            )
            return {}
        links: Dict[str, str] = {}
        for anchor in BeautifulSoup(html, "html.parser").find_all("a", href=True):
            if not isinstance(anchor, Tag):
                continue
            href = str(anchor["href"])
            match = _ISSUE_KEY.search(href)
            if match:
                links.setdefault(match.group(1), href)
        return links
