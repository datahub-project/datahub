import re

from datahub.ingestion.graph.links import make_url_for_urn
from markdown_to_mrkdwn import SlackMarkdownConverter


def get_url_for_urn(frontend_url: str, entity_urn: str) -> str:
    return make_url_for_urn(frontend_url, entity_urn)


_urn_regex_1 = r"urn:li:[a-z]+:[^\s()]+"  # simple urns
_urn_regex_2 = r"urn:li:[a-z]+:\([^\s()]+\)"  # urns with parentheses
_urn_regex_3 = (
    rf"urn:li:[a-z]+:\({_urn_regex_1},[^\s()]+\)"  # urns with a simple urn embedded
)
urn_regex = f"(?:{_urn_regex_1}|{_urn_regex_2}|{_urn_regex_3})"


def slackify_markdown(text: str) -> str:
    """
    Convert markdown to Slack mrkdwn.
    """
    converter = SlackMarkdownConverter()
    return converter.convert(text)


def datahub_linkify(text: str) -> str:
    """Replace all markdown links in form [<table_name>](<table_urn>) with [@<table_name>](<table_urn>)"""

    # Pattern to match markdown links with URNs: [text](urn)
    markdown_link_pattern = rf"\[([^\]]+)\]\(({urn_regex})\)"

    def replace_link(match: re.Match) -> str:
        text = match.group(1)
        urn = match.group(2)
        # Add @ prefix to the text if it doesn't already have it
        if not text.startswith("@"):
            text = f"@{text}"
        return f"[{text}]({urn})"

    return re.sub(markdown_link_pattern, replace_link, text)
