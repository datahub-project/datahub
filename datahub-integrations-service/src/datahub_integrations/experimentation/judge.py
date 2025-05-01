import json
from typing import TYPE_CHECKING, Optional

import pydantic
from diskcache import Cache
from mlflow.metrics.genai.prompt_template import PromptTemplate

from datahub_integrations.gen_ai.bedrock import BedrockModel, call_bedrock_llm

JUDGE_CACHE_ENABLED = True
if JUDGE_CACHE_ENABLED and not TYPE_CHECKING:
    _cache = Cache("judge_cache")

    call_bedrock_llm = _cache.memoize()(call_bedrock_llm)


# Derived from https://github.com/braintrustdata/autoevals/blob/main/templates/closed_q_a.yaml
LLM_JUDGE_PROMPT = PromptTemplate(
    [
        """\
You are assessing a submitted response to a message based on criteria. Here is the data:

<message>
{message}
</message>

<response>
{response}
</response>

<criteria>
{guidelines}
</criteria>

Does the response meet the criteria?

You must return your response as a JSON object with the following fields:
{{
    "choice": true/false,
    "justification": "detailed explanation of why it passes or fails"
}}
"""
    ]
)

# You must return the following fields in your response in two lines, one below the other:
# score: Your numerical score based on the rubric
# justification: Your reasoning for giving this score

# Do not add additional new lines. Do not add any other fields."""


class LLMJudgeResponse(pydantic.BaseModel):
    choice: Optional[bool] = None
    justification: str


def chatbot_llm_judge_evaluation(
    message: str, response: str, guidelines: str
) -> LLMJudgeResponse:
    """Evaluate a single message/response pair using an LLM judge."""

    prompt_text = LLM_JUDGE_PROMPT.format(
        message=message,
        response=response,
        guidelines=guidelines,
    )

    raw_response = call_bedrock_llm(
        prompt=prompt_text, model=BedrockModel.CLAUDE_35_SONNET_V2, max_tokens=500
    )

    try:
        judge_response = LLMJudgeResponse.parse_raw(raw_response)
        assert judge_response.choice is not None
        return judge_response
    except (pydantic.ValidationError, json.JSONDecodeError, AssertionError):
        # TODO Add second call for json coercion in case of error
        return LLMJudgeResponse(
            choice=None,
            justification=f"Failed to parse LLM judge response: {raw_response}",
        )
