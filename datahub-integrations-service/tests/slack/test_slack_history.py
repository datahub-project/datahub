from pathlib import Path

from datahub.testing.compare_metadata_json import assert_metadata_files_equal

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
)
from datahub_integrations.slack.slack_history import (
    SlackHistoryCache,
    SlackThreadHistory,
)

_resource_dir = Path(__file__).parent / "test_slack_history_goldens"
_resource_dir.mkdir(exist_ok=True)


def _assert_chat_history_golden(
    chat_history: ChatHistory, tmp_path: Path, test_case: str
) -> None:
    output_file = tmp_path / f"{test_case}.json"
    chat_history.save_file(output_file)
    golden_file = _resource_dir / f"{test_case}_golden.json"
    assert_metadata_files_equal(output_file, golden_file)


def test_slack_history_first_message_is_latest_message() -> None:
    thread_ts = "1000.1"
    sth = SlackThreadHistory("C123", thread_ts)
    sth.add_message(thread_ts, HumanMessage(text="Hello!"), is_latest_message=True)
    assert not sth.is_limited_history()


def test_slack_history_add_and_get(tmp_path: Path) -> None:
    thread_ts = "1000.1"
    sth = SlackThreadHistory("C123", thread_ts)

    # Add some messages. In this test, the first message in the thread
    # is not available, so the history should be incomplete.
    sth.add_message(
        "1000.3", HumanMessage(text="@datahub Hello again!"), is_latest_message=True
    )
    sth.add_message("1000.4", AssistantMessage(text="Hi there!"))
    sth.add_thinking(
        "1000.4",
        [
            ReasoningMessage(text="Thinking step 1"),
            AssistantMessage(text="Final answer"),
            HumanMessage(text="Hi there! (with diff formatting)"),
        ],
    )
    assert sth.is_limited_history()

    chat_history = sth.get_chat_history()
    _assert_chat_history_golden(chat_history, tmp_path, "slack_history_add_and_get")


def test_slack_history_set_messages(tmp_path: Path) -> None:
    thread_ts = "2000.1"
    sth = SlackThreadHistory("C456", thread_ts)
    messages: dict[str, Message] = {
        "2000.1": HumanMessage(text="Start"),
        "2000.2": AssistantMessage(text="Reply"),
    }
    sth.set_full_history(messages)
    chat_history = sth.get_chat_history()
    _assert_chat_history_golden(chat_history, tmp_path, "slack_history_set_messages")


def test_slack_history_cache_get_thread() -> None:
    cache = SlackHistoryCache()
    channel_id = "C789"
    thread_ts = "3000.1"
    thread1 = cache.get_thread(channel_id, thread_ts)
    thread1.add_message(
        thread_ts, HumanMessage(text="Cache test"), is_latest_message=True
    )

    assert thread1.message_count() == 1
    assert not thread1.is_limited_history()
    assert thread1.channel_id == channel_id
    assert thread1.thread_ts == thread_ts
