import re

from markdown_to_mrkdwn import SlackMarkdownConverter

REPLACEMENT_CONSTANT = "$$DOUBLE_UNDERSCORE$$"


def escape_entity_double_underscore(text: str) -> str:
    # Positive Lookbehind for [ or \w
    # Positive Lookahead for \w or ] or %
    return re.sub(r"(?<=[\[\w])__(?=[\w\]%])", REPLACEMENT_CONSTANT, text)


def unescape_entity_double_underscore(text: str) -> str:
    escaped_constant = re.escape(REPLACEMENT_CONSTANT)
    return re.sub(escaped_constant, "__", text)


def slackify_markdown(text: str) -> str:
    """
    Convert markdown to Slack mrkdwn.
    """

    # First, escape double underscores that are part of entity names/identifiers
    escaped_text = escape_entity_double_underscore(text)

    # Convert markdown to Slack format
    converter = SlackMarkdownConverter()

    converted_text = converter.convert(escaped_text)

    # Finally, unescape the replacement constants back to double underscores
    final_text = unescape_entity_double_underscore(converted_text)

    return final_text
