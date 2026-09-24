import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

from convert_sphinx_to_docusaurus import demote_urn_autolinks

# docutils drops exactly the urn's final ")" from the href and re-emits it as
# text after the link, so these are the shapes Sphinx actually produces.
DATASET = "urn:li:dataset:(urn:li:dataPlatform:kafka,table,PROD"
SCHEMA_FIELD = f"urn:li:schemaField:({DATASET}),field"


def _autolink(truncated_href: str) -> str:
    return f"[{truncated_href}]({truncated_href}))"


def test_flat_urn_becomes_code():
    assert (
        demote_urn_autolinks("[urn:li:corpuser:jane](urn:li:corpuser:jane)")
        == "`urn:li:corpuser:jane`"
    )


def test_nested_urn_keeps_its_closing_paren():
    assert (
        demote_urn_autolinks(f"see {_autolink(DATASET)} end")
        == f"see `{DATASET})` end"
    )


def test_doubly_nested_urn_keeps_both_closing_parens():
    # A schemaField urn wraps a dataset urn, which wraps a dataPlatform urn.
    assert (
        demote_urn_autolinks(f"see {_autolink(SCHEMA_FIELD)} end")
        == f"see `{SCHEMA_FIELD})` end"
    )


def test_surrounding_parenthetical_is_left_alone():
    text = f"(e.g., {_autolink(DATASET)} and more)"
    assert demote_urn_autolinks(text) == f"(e.g., `{DATASET})` and more)"


def test_multiple_links_on_one_line():
    text = (
        "[urn:li:dataPlatform:dbt](urn:li:dataPlatform:dbt), "
        "[urn:li:dataPlatform:snowflake](urn:li:dataPlatform:snowflake)"
    )
    assert (
        demote_urn_autolinks(text)
        == "`urn:li:dataPlatform:dbt`, `urn:li:dataPlatform:snowflake`"
    )


def test_href_wins_over_link_text():
    # The text carries markdown escapes that a code span would render literally.
    text = (
        "[urn:li:ownershipType:_\\_system_\\_technical_owner]"
        "(urn:li:ownershipType:__system__technical_owner)"
    )
    assert demote_urn_autolinks(text) == "`urn:li:ownershipType:__system__technical_owner`"


def test_autolink_nested_in_a_bracketed_example_takes_inner_brackets_only():
    text = '["[urn:li:entityType:datahub.corpuser](urn:li:entityType:datahub.corpuser)"]'
    assert demote_urn_autolinks(text) == '["`urn:li:entityType:datahub.corpuser`"]'


def test_unbalanced_link_is_left_unchanged():
    text = "[urn:li:corpuser:jane](urn:li:corpuser:jane"
    assert demote_urn_autolinks(text) == text


def test_non_urn_links_are_untouched():
    text = "a normal [link](https://example.com) and [another](../page.md)"
    assert demote_urn_autolinks(text) == text


def test_content_without_links_is_returned_verbatim():
    text = "plain prose mentioning urn:li:corpuser:jane without a link"
    assert demote_urn_autolinks(text) == text
