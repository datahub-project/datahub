from datahub_integrations.chat.chat_history import AssistantMessage, HumanMessage
from datahub_integrations.slack.command.mention_helpers import (
    DATAHUB_THINKING_MESSAGE_PREFIX,
    clean_message_text,
    parse_thread_message,
)


def test_clean_message() -> None:
    assert clean_message_text("Processing ⏳") is None

    text = DATAHUB_THINKING_MESSAGE_PREFIX + " any text"
    assert clean_message_text(text) is None

    text = (
        "Result text. Was this response helpful? More text "
        ":bulb: Hint: hint here. 👍 Yes button 👎 No button --- extra"
    )
    assert clean_message_text(text) == "Result text."


def test_parse_thread_message() -> None:
    assert (
        parse_thread_message(
            {
                "text": "Processing ⏳",
                "bot_id": "B0123456789",
            }
        )
        is None
    )

    assert (
        parse_thread_message(
            {
                "text": DATAHUB_THINKING_MESSAGE_PREFIX + " any text",
                "bot_id": "B0123456789",
            }
        )
        is None
    )

    assert parse_thread_message(
        {
            "text": "Result text. Was this response helpful? More text",
            "bot_id": "B0123456789",
        }
    ) == AssistantMessage(text="Result text.")

    assert parse_thread_message(
        {
            "text": "what is the meaning of life?",
            "user": "hsheth2",
        }
    ) == HumanMessage(text="what is the meaning of life?")
