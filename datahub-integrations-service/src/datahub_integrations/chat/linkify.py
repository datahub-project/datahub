import re
from typing import Callable, Optional

from datahub.metadata._urns.urn_defs import DatasetUrn
from datahub.utilities.urns._urn_base import Urn
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import guess_entity_type


def get_url_for_urn(frontend_url: str, entity_urn: str) -> Optional[str]:
    entity_type = guess_entity_type(entity_urn)

    url_prefixes = {
        "dataset": "dataset",
        "chart": "chart",
        "dashboard": "dashboard",
        "task": "tasks",
        "pipeline": "pipelines",
        "container": "container",
        "domain": "domain",
        "dataProduct": "dataProduct",
        "glossaryTerm": "glossaryTerm",
    }

    url_prefix = url_prefixes.get(entity_type, entity_type)
    return f"{frontend_url}/{url_prefix}/{entity_urn}/"


def _get_text_for_urn(urn: str) -> str:
    try:
        urn_t = Urn.from_string(urn)
        if isinstance(urn_t, DatasetUrn):
            return urn_t.name
    except InvalidUrnError:
        pass
    return urn


_urn_regex_1 = r"urn:li:[a-z]+:[^\s()]+"  # simple urns
_urn_regex_2 = r"urn:li:[a-z]+:\([^\s()]+\)"  # urns with parentheses
_urn_regex_3 = (
    rf"urn:li:[a-z]+:\({_urn_regex_1},[^\s()]+\)"  # urns with a simple urn embedded
)
_urn_regex = f"(?:{_urn_regex_1}|{_urn_regex_2}|{_urn_regex_3})"


def _linkify(text: str, replace_urn_fn: Callable[[re.Match], str]) -> str:
    return re.sub(_urn_regex, replace_urn_fn, text)


def linkify_slack(frontend_url: str, text: str) -> str:
    """
    Replace all urn-like strings in the text with links to the DataHub frontend.
    """

    def _slack_link(urn: str) -> str:
        url = get_url_for_urn(frontend_url, urn)
        display_text = _get_text_for_urn(urn)
        return f"<{url}|{display_text}>"

    slack_urn_link_regex = re.compile(rf"(<({_urn_regex})>|<({_urn_regex})\|\S+>)")

    def _replace_urn_link(match: re.Match) -> str:
        urn = match.group(2) or match.group(3)
        if urn is None:
            # If we can't find the urn (should never happen), leave as-is.
            return match.group(0)
        return _slack_link(urn)

    def _replace_urn(match: re.Match) -> str:
        pos = match.start()
        if pos > 0 and text[pos - 1] in [
            "<",  # should be handled above
            "`",  # in an inline code block
            "/",  # already part of a link
        ]:
            return match.group(0)

        urn = match.group(0)
        return _slack_link(urn)

    text = re.sub(slack_urn_link_regex, _replace_urn_link, text)
    text = re.sub(_urn_regex, _replace_urn, text)
    return text


def linkify_markdown(frontend_url: str, text: str) -> str:
    def _replace_urn(match: re.Match) -> str:
        pos = match.start()
        if pos > 0 and text[pos - 1] in ["[", "`"]:
            return match.group(0)

        urn = match.group(0)
        url = get_url_for_urn(frontend_url, urn)
        if url is None:
            return urn

        display_text = _get_text_for_urn(urn)
        return f"[{display_text}]({url})"

    return _linkify(text, _replace_urn)
