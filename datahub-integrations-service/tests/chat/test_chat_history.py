from pathlib import Path

from datahub.testing.compare_metadata_json import assert_metadata_files_equal

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
)

_resource_dir = Path(__file__).parent


def test_chat_history(tmp_path: Path) -> None:
    chat_history = ChatHistory(
        messages=[
            HumanMessage(text="Hello, bot!"),
            ReasoningMessage(text="Let me say hello back to the user"),
        ]
    )
    chat_history.add_message(AssistantMessage(text="Hello!"))

    # Test that we can serialize out properly.
    output_file = tmp_path / "chat_history.json"
    chat_history.save_file(output_file)

    assert_metadata_files_equal(
        output_file,
        golden_path=_resource_dir / "golden_chat_history.json",
    )

    # Test that we can deserialize back to the same object.
    deserialized_chat_history = ChatHistory.load_file(output_file)
    assert chat_history == deserialized_chat_history
