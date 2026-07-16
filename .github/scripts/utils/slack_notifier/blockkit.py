"""Generic Slack BlockKit builders.

Pure functions producing BlockKit JSON fragments. No I/O, no knowledge of
tests. Callers compose these into messages.
"""

from typing import Any, Literal

Status = Literal["success", "warning", "failure"]

# Slack's recommended attachment colors for status bars.
_STATUS_COLORS: dict[Status, str] = {
    "success": "#36a64f",
    "warning": "#ecb22e",
    "failure": "#e01e5a",
}


def header(text: str) -> dict[str, Any]:
    return {"type": "header", "text": {"type": "plain_text", "text": text, "emoji": True}}


def section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def fields_section(fields: dict[str, str]) -> dict[str, Any]:
    """Render a two-column grid. Each key becomes a bold label above its value."""
    return {
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*{label}*\n{value}"} for label, value in fields.items()
        ],
    }


def divider() -> dict[str, Any]:
    return {"type": "divider"}


def link(text: str, url: str) -> str:
    """Slack mrkdwn link syntax: <url|text>."""
    return f"<{url}|{text}>"


def color_attachment(status: Status, blocks: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap *blocks* in an attachment with a colored bar based on *status*."""
    return {"color": _STATUS_COLORS[status], "blocks": blocks}
