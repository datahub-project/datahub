import re
from typing import Callable, Dict, Optional, Set

from bs4 import BeautifulSoup, Tag

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.constants import (
    JIRA_BROWSE_PATH,
    JIRA_ISSUE_KEY_PATTERN,
)
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import JiraIssueRef

_ISSUE_KEY = re.compile(JIRA_ISSUE_KEY_PATTERN)

# Pages tried per Jira server before its links are given up on for the run.
MAX_PAGES_PER_JIRA_SERVER = 3

# Macros on the site's default Jira server carry no server id or name.
_DEFAULT_SERVER = ""


class ConfluenceJiraLinker:
    """Turns Jira macro issue keys into links to the issue.

    The storage format only names the Jira server (an application link id);
    Confluence keeps the server's URL. The linked servers are listed once per
    run. If that list is unavailable or lacks the server, the first page with
    an issue on that server is fetched rendered, where Confluence has already
    linked the issue, and the server's base URL is read from that link.
    """

    def __init__(self, client: ConfluenceClient, report: ConfluenceSourceReport):
        self.client = client
        self.report = report
        self._listed_servers: Optional[Dict[str, str]] = None
        self._base_urls: Dict[str, str] = {}
        # Pages per server whose rendered view had no link for an issue key.
        self._failed_pages: Dict[str, Set[str]] = {}
        self._rendered_links: Dict[str, Dict[str, str]] = {}

    def for_page(self, page_id: str) -> Callable[[JiraIssueRef], Optional[str]]:
        return lambda ref: self._link(page_id, ref)

    def _link(self, page_id: str, ref: JiraIssueRef) -> Optional[str]:
        # Macros usually carry both id and name. The id goes stale when an
        # application link is recreated while the name stays, so try both.
        keys = [k for k in (ref.server_id, ref.server_name) if k] or [_DEFAULT_SERVER]
        listed = self._server_urls()
        server = keys[0]
        base = (
            next((listed[k] for k in keys if k in listed), None)
            or self._base_urls.get(server)
            or self._learn_base_url(server, page_id, ref.key)
        )
        if base is None:
            self.report.jira_links_unresolved += 1
            return None
        self.report.jira_links_resolved += 1
        return f"{base}{JIRA_BROWSE_PATH}{ref.key}"

    def _server_urls(self) -> Dict[str, str]:
        if self._listed_servers is not None:
            return self._listed_servers
        self._listed_servers = {}
        try:
            servers = self.client.list_jira_servers()
        except Exception as e:
            self.report.info(
                title="Jira server list unavailable",
                message="Jira server URLs are read from rendered pages instead.",
                exc=e,
            )
            return self._listed_servers
        for server in servers:
            url = server.url.rstrip("/")
            self._listed_servers[server.id] = url
            if server.name:
                self._listed_servers.setdefault(server.name, url)
            if server.selected or len(servers) == 1:
                self._listed_servers.setdefault(_DEFAULT_SERVER, url)
        return self._listed_servers

    def _learn_base_url(self, server: str, page_id: str, key: str) -> Optional[str]:
        failed = self._failed_pages.setdefault(server, set())
        # Once a server is given up on, no new pages are fetched for it, but a
        # page already fetched is still read.
        if (
            page_id not in self._rendered_links
            and len(failed) >= MAX_PAGES_PER_JIRA_SERVER
        ):
            return None
        href = self._rendered_issue_links(page_id).get(key)
        if href is None:
            if page_id not in failed:
                failed.add(page_id)
                if len(failed) == MAX_PAGES_PER_JIRA_SERVER:
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
