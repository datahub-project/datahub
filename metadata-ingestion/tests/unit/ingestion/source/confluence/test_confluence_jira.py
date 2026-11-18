from unittest.mock import MagicMock

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.jira import (
    MAX_PAGES_PER_JIRA_SERVER,
    ConfluenceJiraLinker,
)
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import JiraIssueRef


def _rendered(html: str) -> dict:
    return {"id": "1", "body": {"view": {"value": html}}}


def _linker(api: MagicMock) -> ConfluenceJiraLinker:
    report = ConfluenceSourceReport()
    return ConfluenceJiraLinker(ConfluenceClient(api, report), report)


def test_listed_jira_servers_link_issues_without_rendering_pages() -> None:
    api = MagicMock()
    api.get.return_value = [
        {
            "id": "s1",
            "name": "System Jira",
            "url": "https://site.example.com/",
            "selected": True,
        },
        {"id": "s2", "name": "Legacy Jira", "url": "https://jira.example.com"},
    ]
    linker = _linker(api)
    page_links = linker.for_page("1")

    assert page_links(JiraIssueRef(key="ABC-1", server_id="s2")) == (
        "https://jira.example.com/browse/ABC-1"
    )
    assert page_links(JiraIssueRef(key="ABC-2", server_name="System Jira")) == (
        "https://site.example.com/browse/ABC-2"
    )
    assert (
        page_links(JiraIssueRef(key="ABC-3")) == "https://site.example.com/browse/ABC-3"
    )
    api.get.assert_called_once()
    api.get_page_by_id.assert_not_called()


def test_server_url_is_learned_from_the_rendered_page_once() -> None:
    api = MagicMock()
    api.get.side_effect = Exception("404 jiraanywhere not available")
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
    api.get.return_value = []
    api.get_page_by_id.return_value = _rendered("<p>Jira is unreachable</p>")
    linker = _linker(api)

    for page in range(MAX_PAGES_PER_JIRA_SERVER + 2):
        assert linker.for_page(str(page))(JiraIssueRef(key="ABC-1")) is None

    assert api.get_page_by_id.call_count == MAX_PAGES_PER_JIRA_SERVER
    assert len(linker.report.warnings) == 1


def test_misses_on_one_page_count_once_toward_giving_up() -> None:
    """Several unrendered keys on one page are one failed page, and keys that
    page did render still link."""
    api = MagicMock()
    api.get.return_value = []
    api.get_page_by_id.side_effect = [
        _rendered('<a href="https://jira.example.com/browse/OK-1">OK-1</a>'),
        _rendered('<a href="https://jira.example.com/browse/NEXT-1">NEXT-1</a>'),
    ]
    linker = _linker(api)
    page_one = linker.for_page("1")

    for key in ("MISS-1", "MISS-2", "MISS-3", "MISS-4"):
        assert page_one(JiraIssueRef(key=key)) is None
    assert page_one(JiraIssueRef(key="OK-1")) == "https://jira.example.com/browse/OK-1"
    assert linker.for_page("2")(JiraIssueRef(key="NEXT-1")) == (
        "https://jira.example.com/browse/NEXT-1"
    )
    assert not linker.report.warnings


def test_stale_server_id_falls_back_to_the_server_name() -> None:
    api = MagicMock()
    api.get.return_value = [
        {"id": "new-id", "name": "Legacy Jira", "url": "https://jira.example.com"}
    ]
    linker = _linker(api)

    link = linker.for_page("1")(
        JiraIssueRef(key="ABC-1", server_id="old-id", server_name="Legacy Jira")
    )

    assert link == "https://jira.example.com/browse/ABC-1"
    api.get_page_by_id.assert_not_called()


def test_unreadable_jira_server_is_skipped_and_the_rest_are_kept() -> None:
    api = MagicMock()
    api.get.return_value = [
        {"id": "broken", "name": "Broken link"},
        {"id": "s1", "name": "Jira", "url": "https://jira.example.com"},
    ]
    linker = _linker(api)

    link = linker.for_page("1")(JiraIssueRef(key="ABC-1", server_id="s1"))

    assert link == "https://jira.example.com/browse/ABC-1"
    assert [w.title for w in linker.report.warnings] == [
        "Skipped unreadable Jira server"
    ]
