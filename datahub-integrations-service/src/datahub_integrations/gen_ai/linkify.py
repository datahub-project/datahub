import re
import urllib.parse

from datahub.ingestion.graph.links import make_url_for_urn

_urn_regex_1 = r"urn:li:[a-z]+:[^\s()]+"  # simple urns
_urn_regex_2 = r"urn:li:[a-z]+:\([^\s()]+\)"  # urns with parentheses
_urn_regex_3 = (
    rf"urn:li:[a-z]+:\({_urn_regex_1},[^\s()]+\)"  # urns with a simple urn embedded
)
urn_regex = f"(?:{_urn_regex_1}|{_urn_regex_2}|{_urn_regex_3})"

# TODO: Use re.compile() for performance optimization of complex regex patterns


def auto_fix_entity_mention_links(text: str) -> str:
    """Auto-fixes markdown links
     - without @ mention prefix [<entity_name>](<entity_urn>)
     - with missing urn closing bracket [<entity_name>](<entity_urn without ending )>)
     - with incorrectly present closing square bracket [<entity_name>](<entity_urn>]
    to correct format with [@<entity_name>](<entity_urn>)
    Use this function to auto-fix links in the AI documentation response for DataHub UI.
    """

    # Pattern to match markdown links with URNs: [text](urn)
    markdown_link_pattern = rf"\[([^\]]+)\]\(({urn_regex})[\)]?[\]]?"

    def replace_link(match: re.Match) -> str:
        text = match.group(1)
        urn = match.group(2)
        # Add @ prefix to the text if it doesn't already have it
        if not text.startswith("@"):
            text = f"@{text}"
        return f"[{text}]({urn})"

    return re.sub(markdown_link_pattern, replace_link, text)


def auto_fix_chat_links(text: str, frontend_url: str) -> str:
    """
    Auto-fixes
     -  URN-only links [text](<urn>)
     -  Full links with unquoted urns [text](https://xxx.acryl.io/xxx/<urn>)
     -  Full links with quoted urns but incorrect subpath [text](https://xxx.acryl.io/wrongpath/<quoted urn>)
    to correct format with quoted urns [text](https://xxx.acryl.io/wrongsubpath/<quoted urn>)
    Use this function to auto-fix links in the chat response.
    """
    # Pattern to match markdown links with URNs: [text](urn) or [text](https://xxx.acryl.io/xxx/urn)
    unquoted_urn_link_pattern = rf"\[([^\]]+)\]\(([a-z0-9-_.:/]*)({urn_regex})\)"

    def replace_link(match: re.Match) -> str:
        display_text = match.group(1)
        urn = match.group(3)
        full_url = make_url_for_urn(frontend_url, urn)
        return f"[{display_text}]({full_url})"

    text_with_quoted_links = re.sub(unquoted_urn_link_pattern, replace_link, text)

    # Pattern to match full links with quoted urns [text](https://xxx.acryl.io/xxx/<quotedurn>)
    quoted_urn_link_pattern = r"\[([^\]]+)\]\((https://[^/]+\.acryl\.io/[^/]+/[^\)]+)\)"

    def replace_full_link(match: re.Match) -> str:
        display_text = match.group(1)
        url = match.group(2)
        unquoted_url = urllib.parse.unquote(url.strip("\\/"))
        urn_loc = unquoted_url.find("urn:li:")
        if urn_loc == -1:
            return f"[{display_text}]({url})"
        urn = unquoted_url[urn_loc:]
        full_url = make_url_for_urn(frontend_url, urn)
        return f"[{display_text}]({full_url})"

    return re.sub(quoted_urn_link_pattern, replace_full_link, text_with_quoted_links)
