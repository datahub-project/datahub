from unittest.mock import patch

from litellm import completion

from datahub_integrations.gen_ai.litellm import (
    LiteLLM,
    LiteLLMModel,
    LiteLLMPromptMessage,
)


class TestLiteLLM:
    def test_litellm_call(self) -> None:
        mock_response_text = (
            "This is a mocked response. I am not a real gpt saying hello."
        )
        mock_response_value = completion(
            model="bedrock/us.anthropic.claude-3-haiku-20240307-v1:0",
            messages=[{"role": "user", "content": "Hello I am a test"}],
            stream=True,
            mock_response=mock_response_text,
            stream_options={"include_usage": True},
        )

        with patch(
            "litellm.completion", return_value=mock_response_value
        ) as mock_litellm:
            litellm = LiteLLM(LiteLLMModel.CLAUDE_3_HAIKU, 500, 0.3)

            messages = [
                LiteLLMPromptMessage(
                    text="one",
                    cache=True,
                ),
                LiteLLMPromptMessage(
                    text="two",
                    cache=True,
                ),
            ]

            messages.append(
                LiteLLMPromptMessage(
                    text="hello",
                    cache=False,
                )
            )

            result = litellm.call_lite_llm(messages)
            assert result == mock_response_text

            # have to reset mock in between calls
            mock_litellm.reset_mock(return_value=True)
            mock_response_value = completion(
                model="bedrock/us.anthropic.claude-3-haiku-20240307-v1:0",
                messages=[{"role": "user", "content": "Hello I am a test"}],
                stream=True,
                stream_options={"include_usage": True},
                mock_response=mock_response_text,
            )
            mock_litellm.return_value = mock_response_value
            result = litellm.call_lite_llm("hello 123")
            assert result == mock_response_text
