from typing import Optional

import pytest

from datahub.ingestion.source.confluence.storage_format import (
    JiraIssueRef,
    ResolvedUser,
    UserRef,
    storage_to_markdown,
)


def _names(ref: UserRef) -> Optional[ResolvedUser]:
    if ref.account_id == "557058:aaaa":
        return ResolvedUser(name="Alex Doe")
    if ref.account_id == "557058:dh":
        return ResolvedUser(name="Sam Roe", urn="urn:li:corpuser:sam@example.com")
    return None


def test_preserves_headings_lists_tables_and_links() -> None:
    html = (
        "<h1>Overview</h1><h2>Details</h2>"
        "<ul><li>First</li><li>Second</li></ul>"
        "<table><tr><th>Column</th></tr><tr><td>widget</td></tr></table>"
        '<p>See <a href="https://example.com">docs</a> for <strong>key</strong> info.</p>'
    )
    markdown = storage_to_markdown(html)

    assert "# Overview" in markdown
    assert "## Details" in markdown
    assert "- First" in markdown
    assert "widget" in markdown
    assert "[docs](https://example.com)" in markdown
    assert "**key**" in markdown


def test_empty_input() -> None:
    assert storage_to_markdown("") == ""
    assert storage_to_markdown("   ") == ""


@pytest.mark.parametrize(
    "mention",
    [
        '<ri:user ri:account-id="557058:aaaa" />',
        '<ri:user ri:account-id="557058:aaaa"></ri:user>',
    ],
)
def test_mention_resolves_to_display_name(mention: str) -> None:
    html = f"<p>VP: <ac:link>{mention}</ac:link> (Director)</p>"

    assert storage_to_markdown(html, _names) == "VP: @Alex Doe (Director)"


def test_mention_of_a_datahub_user_uses_the_datahub_mention_format() -> None:
    html = '<p>Owner: <ac:link><ri:user ri:account-id="557058:dh"/></ac:link></p>'

    assert (
        storage_to_markdown(html, _names)
        == "Owner: [@Sam Roe](urn:li:corpuser:sam@example.com)"
    )


def test_jira_macro_links_to_the_issue_when_the_server_is_known() -> None:
    html = (
        '<p>See <ac:structured-macro ac:name="jira">'
        '<ac:parameter ac:name="serverId">abc-123</ac:parameter>'
        '<ac:parameter ac:name="key">ABC-1</ac:parameter></ac:structured-macro> '
        'and <ac:structured-macro ac:name="jira">'
        '<ac:parameter ac:name="key">XYZ-2</ac:parameter></ac:structured-macro></p>'
    )
    servers = {"abc-123": "https://jira.example.com"}

    def jira(ref: JiraIssueRef) -> Optional[str]:
        base = servers.get(ref.server_id or "")
        return f"{base}/browse/{ref.key}" if base else None

    assert (
        storage_to_markdown(html, jira_resolver=jira)
        == "See [ABC-1](https://jira.example.com/browse/ABC-1) and XYZ-2"
    )


def test_unresolved_mention_keeps_a_placeholder() -> None:
    html = (
        '<p>Owner <ac:link><ri:user ri:account-id="557058:gone" /></ac:link>, '
        'reviewer <ac:link><ri:user ri:userkey="ff80" /></ac:link></p>'
    )

    assert storage_to_markdown(html, _names) == "Owner @user, reviewer @user"
    assert storage_to_markdown(html) == "Owner @user, reviewer @user"


def test_link_without_text_shows_its_target() -> None:
    html = (
        '<p>See <ac:link><ri:page ri:content-title="Runbook" ri:space-key="OPS"/>'
        '</ac:link> and <ac:link><ri:attachment ri:filename="schema.xlsx"/></ac:link>.</p>'
    )

    assert storage_to_markdown(html) == "See Runbook and schema.xlsx."


def test_link_text_wins_over_target() -> None:
    html = (
        '<p><ac:link><ri:page ri:content-title="Runbook"/>'
        "<ac:plain-text-link-body><![CDATA[the runbook]]></ac:plain-text-link-body>"
        "</ac:link></p>"
    )

    assert storage_to_markdown(html) == "the runbook"


def test_url_link_keeps_its_href() -> None:
    html = (
        '<p><ac:link><ri:url ri:value="https://example.com/x"/>'
        "<ac:plain-text-link-body><![CDATA[docs]]></ac:plain-text-link-body></ac:link></p>"
    )

    assert storage_to_markdown(html) == "[docs](https://example.com/x)"


def test_images_keep_a_label() -> None:
    html = (
        '<p><ac:image ac:alt="Architecture"><ri:attachment ri:filename="arch.png"/></ac:image></p>'
        '<p><ac:image><ri:attachment ri:filename="flow.png"/></ac:image></p>'
    )

    markdown = storage_to_markdown(html)

    assert "[Image: Architecture]" in markdown
    assert "[Image: flow.png]" in markdown


def test_date_keeps_its_value() -> None:
    assert (
        storage_to_markdown('<p>Due <time datetime="2024-05-01" /> at noon</p>')
        == "Due 2024-05-01 at noon"
    )


def test_status_and_jira_macros_keep_their_text() -> None:
    html = (
        '<p>State: <ac:structured-macro ac:name="status">'
        '<ac:parameter ac:name="title">DEPRECATED</ac:parameter>'
        '<ac:parameter ac:name="colour">Red</ac:parameter></ac:structured-macro>, '
        'ticket <ac:structured-macro ac:name="jira">'
        '<ac:parameter ac:name="server">Jira</ac:parameter>'
        '<ac:parameter ac:name="key">ABC-123</ac:parameter></ac:structured-macro></p>'
    )

    assert storage_to_markdown(html) == "State: [DEPRECATED], ticket ABC-123"


def test_code_macro_keeps_language_and_drops_parameters() -> None:
    html = (
        '<ac:structured-macro ac:name="code">'
        '<ac:parameter ac:name="language">sql</ac:parameter>'
        '<ac:parameter ac:name="borderStyle">wide</ac:parameter>'
        "<ac:plain-text-body><![CDATA[SELECT * FROM t WHERE a < 1]]></ac:plain-text-body>"
        "</ac:structured-macro>"
    )

    assert storage_to_markdown(html) == "```sql\nSELECT * FROM t WHERE a < 1\n```"


def test_task_list_renders_as_checkboxes() -> None:
    html = (
        "<ac:task-list>"
        "<ac:task><ac:task-id>1</ac:task-id><ac:task-status>complete</ac:task-status>"
        "<ac:task-body>Ship it</ac:task-body></ac:task>"
        "<ac:task><ac:task-id>2</ac:task-id><ac:task-status>incomplete</ac:task-status>"
        '<ac:task-body>Ask <ac:link><ri:user ri:account-id="557058:aaaa"/></ac:link></ac:task-body>'
        "</ac:task></ac:task-list>"
    )

    assert storage_to_markdown(html, _names) == "- [x] Ship it\n- [ ] Ask @Alex Doe"


@pytest.mark.parametrize(
    "child_list_position",
    ["inside the parent task body", "next to the parent task"],
)
def test_nested_task_list_keeps_its_tasks(child_list_position: str) -> None:
    child = (
        "<ac:task-list><ac:task><ac:task-status>incomplete</ac:task-status>"
        "<ac:task-body>Child</ac:task-body></ac:task></ac:task-list>"
    )
    parent_body = "Parent" + (child if child_list_position.startswith("inside") else "")
    sibling = "" if child_list_position.startswith("inside") else child
    html = (
        "<ac:task-list><ac:task><ac:task-status>incomplete</ac:task-status>"
        f"<ac:task-body>{parent_body}</ac:task-body></ac:task>{sibling}</ac:task-list>"
    )

    markdown = storage_to_markdown(html)

    assert "[ ] Parent" in markdown
    assert "[ ] Child" in markdown


def test_template_placeholder_is_not_content() -> None:
    html = "<p><ac:placeholder>Type your summary here</ac:placeholder>Real text</p>"

    assert storage_to_markdown(html) == "Real text"


def test_adf_extension_is_not_duplicated() -> None:
    html = (
        '<ac:adf-extension><ac:adf-node type="panel">'
        '<ac:adf-attribute key="panel-type">note</ac:adf-attribute>'
        "<ac:adf-content><p>Note body</p></ac:adf-content></ac:adf-node>"
        '<ac:adf-fallback><div class="panel"><p>Note body</p></div></ac:adf-fallback>'
        "</ac:adf-extension>"
    )

    assert storage_to_markdown(html) == "Note body"


def test_formatting_macro_keeps_body_and_navigation_macro_is_dropped() -> None:
    html = (
        '<ac:structured-macro ac:name="toc"><ac:parameter ac:name="maxLevel">3</ac:parameter>'
        "</ac:structured-macro>"
        '<ac:structured-macro ac:name="info"><ac:parameter ac:name="title">Note</ac:parameter>'
        "<ac:rich-text-body><p>Owned by "
        '<ac:link><ri:user ri:account-id="557058:aaaa"/></ac:link></p>'
        '<ac:structured-macro ac:name="expand"><ac:rich-text-body><p>Nested detail</p>'
        "</ac:rich-text-body></ac:structured-macro>"
        "</ac:rich-text-body></ac:structured-macro>"
    )

    assert storage_to_markdown(html, _names) == "Owned by @Alex Doe\n\nNested detail"


def test_emoticons_become_emoji() -> None:
    html = (
        '<p><ac:emoticon ac:name="smile" /> '
        '<ac:emoticon ac:name="blue-star" ac:emoji-id="1f44d" /> '
        '<ac:emoticon ac:name="custom" ac:emoji-id="a1b2c3d4-not-hex" /></p>'
    )

    assert storage_to_markdown(html) == "\U0001f642 \U0001f44d :custom:"


def test_cloud_emoticons_with_shortname_fallbacks_become_emoji() -> None:
    html = (
        '<p><ac:emoticon ac:name="cross" ac:emoji-shortname=":cross_mark:" '
        'ac:emoji-id="atlassian-cross_mark" ac:emoji-fallback=":cross_mark:" /> '
        '<ac:emoticon ac:name="blue-star" ac:emoji-id="1f600" ac:emoji-fallback="\U0001f600" /> '
        '<ac:emoticon ac:name="blue-star" ac:emoji-id="custom-123" '
        'ac:emoji-fallback=":party_parrot:" /></p>'
    )

    assert storage_to_markdown(html) == "❌ \U0001f600 ⭐"
