from markdown_to_mrkdwn import SlackMarkdownConverter


def slackify_markdown(text: str) -> str:
    """
    Convert markdown to Slack mrkdwn.
    """
    converter = SlackMarkdownConverter()
    return converter.convert(text)
