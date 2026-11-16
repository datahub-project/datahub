from unittest.mock import MagicMock

from datahub.ingestion.source.confluence.confluence_client import ConfluenceClient
from datahub.ingestion.source.confluence.confluence_html import JiraIssueRef
from datahub.ingestion.source.confluence.confluence_jira import (
    MAX_PAGES_PER_JIRA_SERVER,
    ConfluenceJiraLinker,
)
from datahub.ingestion.source.confluence.confluence_report import (
    ConfluenceSourceReport,
)


def _rendered(html: str) -> dict:
    return {"id": "1", "body": {"view": {"value": html}}}


def _linker(api: MagicMock) -> ConfluenceJiraLinker:
    report = ConfluenceSourceReport()
    return ConfluenceJiraLinker(ConfluenceClient(api, report), report)


def test_server_url_is_learned_from_the_rendered_page_once() -> None:
    api = MagicMock()
    api.get_page_by_id.return_value = _rendered(
        '<span class="jira-issue"><a href="https://jira.example.com/jira/browse/ABC-1?src=confmacro">ABC-1</a></span>'
    )
    linker = _linker(api)

    first = linker.for_page("1")(JiraIssueRef(key="ABC-1", server_id="s1"))
    later = linker.for_page("2")(JiraIssueRef(key="ABC-99", server_id="s1"))

    assert first == "https://jira.example.com/jira/browse/ABC-1"
    assert later == "https://jira.example.com/jira/browse/ABC-99"
    api.get_page_by_id.assert_called_once_with("1", expand="body.view")


def test_unrendered_issue_stays_unlinked_and_server_is_given_up_on() -> None:
    api = MagicMock()
    api.get_page_by_id.return_value = _rendered("<p>Jira is unreachable</p>")
    linker = _linker(api)

    for page in range(MAX_PAGES_PER_JIRA_SERVER + 2):
        assert linker.for_page(str(page))(JiraIssueRef(key="ABC-1")) is None

    assert api.get_page_by_id.call_count == MAX_PAGES_PER_JIRA_SERVER
    assert len(linker.report.warnings) == 1
