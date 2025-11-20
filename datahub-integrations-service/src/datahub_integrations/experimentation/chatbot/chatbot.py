from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import pathlib

import mlflow
import pydantic
import yaml
from datahub.utilities.yaml_sync_utils import YamlFileUpdater

from datahub_integrations.app import ROOT_DIR

assert AI_EXPERIMENTATION_INITIALIZED

mlflow.set_experiment("Chatbot")

chatbot_experiments_dir = ROOT_DIR / "experiments/chatbot"


class ExpectedToolCall(pydantic.BaseModel):
    """Represents an expected tool call with name and arguments.

    Arguments can be specified as exact values or as "*" to indicate
    that the argument can be any value (present or absent).
    """

    tool_name: str
    tool_input: dict


class Prompt(pydantic.BaseModel):
    id: str
    instance: str
    message: str

    response_guidelines: str | None = None
    tags: list[str] | None = None
    expected_tool_calls: list[ExpectedToolCall] | None = None

    # TODO: add mechanism for testing follow-up questions using a starting chat history


class _PromptList(pydantic.RootModel):
    root: list[Prompt]


def load_prompts_file(file: pathlib.Path) -> list[Prompt]:
    prompts_raw = yaml.safe_load(file.read_text())
    return _PromptList.model_validate(prompts_raw).root


prompts_file = chatbot_experiments_dir / "prompts.yaml"
prompts = load_prompts_file(prompts_file)

# Ensure that the prompt ids are unique.
_prompt_ids = [prompt.id for prompt in prompts]

assert len(_prompt_ids) == len(set(_prompt_ids)), "Prompt ids must be unique"


def update_prompt_guidelines(prompt_id: str, new_guidelines: str) -> None:
    with YamlFileUpdater(prompts_file) as doc:
        for prompt in doc:
            if prompt["id"] == prompt_id:
                prompt["response_guidelines"] = new_guidelines
                break
        else:
            raise ValueError(f"Prompt with id {prompt_id} not found")


def update_prompt_expected_tool_calls(
    prompt_id: str, new_expected_tool_calls: list[ExpectedToolCall] | None
) -> None:
    with YamlFileUpdater(prompts_file) as doc:
        for prompt in doc:
            if prompt["id"] == prompt_id:
                if new_expected_tool_calls is None:
                    prompt.pop("expected_tool_calls", None)
                else:
                    prompt["expected_tool_calls"] = [
                        {"tool_name": call.tool_name, "tool_input": call.tool_input}
                        for call in new_expected_tool_calls
                    ]
                break
        else:
            raise ValueError(f"Prompt with id {prompt_id} not found")


def update_prompt_tags(prompt_id: str, new_tags: list[str] | None) -> None:
    with YamlFileUpdater(prompts_file) as doc:
        for prompt in doc:
            if prompt["id"] == prompt_id:
                prompt["tags"] = new_tags or []
                break
        else:
            raise ValueError(f"Prompt with id {prompt_id} not found")


def reload_prompt(prompt_id: str) -> Prompt:
    prompts = load_prompts_file(prompts_file)
    return next(p for p in prompts if p.id == prompt_id)
