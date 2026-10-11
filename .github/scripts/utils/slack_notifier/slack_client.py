"""Generic Slack Web API client.

Knows nothing about tests. Any CI notifier can import this to post messages
or threaded replies to a Slack channel.
"""

import time
from typing import Any

import requests

_API_BASE = "https://slack.com/api"
_DEFAULT_TIMEOUT_SECONDS = 10
_MAX_RETRIES_ON_RATE_LIMIT = 2


class SlackApiError(RuntimeError):
    """Raised when the Slack Web API returns ok=false or a non-2xx HTTP status."""


def post_message(
    *,
    channel: str,
    blocks: list[dict[str, Any]],
    token: str,
    attachments: list[dict[str, Any]] | None = None,
    text: str = "",
) -> str:
    """Post a message to *channel*. Returns the message's ``ts`` (for threading).

    *text* is a plaintext fallback shown in notifications / accessibility
    contexts; blocks are the rich rendering.
    """
    payload: dict[str, Any] = {"channel": channel, "blocks": blocks, "text": text}
    if attachments is not None:
        payload["attachments"] = attachments
    response = _call("chat.postMessage", payload, token)
    return str(response["ts"])


def post_thread_reply(
    *,
    channel: str,
    thread_ts: str,
    blocks: list[dict[str, Any]],
    token: str,
    attachments: list[dict[str, Any]] | None = None,
    text: str = "",
) -> None:
    """Post a reply in the thread rooted at *thread_ts*."""
    payload: dict[str, Any] = {
        "channel": channel,
        "thread_ts": thread_ts,
        "blocks": blocks,
        "text": text,
    }
    if attachments is not None:
        payload["attachments"] = attachments
    _call("chat.postMessage", payload, token)


def _call(method: str, payload: dict[str, Any], token: str) -> dict[str, Any]:
    url = f"{_API_BASE}/{method}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    for attempt in range(_MAX_RETRIES_ON_RATE_LIMIT + 1):
        resp = requests.post(url, json=payload, headers=headers, timeout=_DEFAULT_TIMEOUT_SECONDS)

        # Slack returns 429 with Retry-After on rate limits; everything else
        # (including auth errors) comes back as 200 with ok=false.
        if resp.status_code == 429 and attempt < _MAX_RETRIES_ON_RATE_LIMIT:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
            continue

        if not resp.ok:
            raise SlackApiError(f"Slack API {method} returned HTTP {resp.status_code}: {resp.text}")

        body: dict[str, Any] = resp.json()
        if not body.get("ok"):
            raise SlackApiError(f"Slack API {method} failed: {body.get('error', 'unknown_error')}")
        return body

    raise SlackApiError(f"Slack API {method} rate-limited after {_MAX_RETRIES_ON_RATE_LIMIT} retries")
