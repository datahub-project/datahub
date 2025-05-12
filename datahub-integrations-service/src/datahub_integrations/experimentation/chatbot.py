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


class Prompt(pydantic.BaseModel):
    id: str
    instance: str
    message: str

    response_guidelines: str | None = None

    # TODO: add mechanism for testing follow-up questions using a starting chat history


class _PromptList(pydantic.BaseModel):
    __root__: list[Prompt]


def load_prompts_file(file: pathlib.Path) -> list[Prompt]:
    prompts_raw = yaml.safe_load(file.read_text())
    return pydantic.parse_obj_as(_PromptList, prompts_raw).__root__


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
