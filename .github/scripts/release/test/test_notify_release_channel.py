"""Tests for notify_release_channel — channel derivation and Slack posting."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_RELEASE = Path(__file__).resolve().parent.parent
_SCRIPTS = _RELEASE.parent
sys.path.insert(0, str(_SCRIPTS))
sys.path.insert(0, str(_RELEASE))

import notify_release_channel as n  # noqa: E402
from release_variables import SLACK_RELEASE_CHANNEL_PREFIX  # noqa: E402
from utils.slack_notifier import slack_client  # noqa: E402


def _args(version: str = "1.1.0", message: str = "hello", emoji: str = ""):
    return ["--version", version, "--message", message, "--emoji", emoji]


def test_main_posts_to_derived_channel(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.setattr(sys, "argv", ["notify_release_channel.py", *_args(emoji=":rocket:")])

    with patch.object(slack_client, "post_message", return_value="123.45") as mock_post:
        assert n.main() == 0

    kwargs = mock_post.call_args.kwargs
    assert kwargs["channel"] == f"{SLACK_RELEASE_CHANNEL_PREFIX}1_1_0"
    assert ":rocket:" in kwargs["blocks"][0]["text"]["text"]
    assert kwargs["text"] == "hello"


def test_main_requires_token(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.setattr(sys, "argv", ["notify_release_channel.py", *_args()])
    assert n.main() == 2


def test_main_rejects_unparseable_version(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.setattr(sys, "argv", ["notify_release_channel.py", *_args(version="garbage")])
    assert n.main() == 2


def test_main_returns_1_on_slack_failure(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.setattr(sys, "argv", ["notify_release_channel.py", *_args()])

    with patch.object(
        slack_client, "post_message", side_effect=slack_client.SlackApiError("boom")
    ):
        assert n.main() == 1
