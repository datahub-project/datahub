# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pydantic import BaseModel, ConfigDict

from datahub.configuration.env_vars import get_debug
from datahub.executor.common.config import ConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.task import Task
from datahub.ingestion.run.pipeline import Pipeline


class InMemoryIngestionTaskConfig(ConfigModel):
    config_1: str


class InMemoryIngestionTaskArgs(BaseModel):
    recipe: dict

    # Security: Hide input values in validation errors to prevent sensitive data exposure.
    # Ingestion recipes may contain credentials, connection strings, API keys, and other
    # secrets that should not be logged to UI or error messages. Only show input values
    # when DATAHUB_DEBUG is explicitly enabled for troubleshooting purposes.
    model_config = ConfigDict(hide_input_in_errors=not get_debug())


class InMemoryIngestionTask(Task):
    config: InMemoryIngestionTaskConfig

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        parsed_config = InMemoryIngestionTaskConfig.model_validate(config)
        return cls(parsed_config)

    def __init__(self, config: InMemoryIngestionTaskConfig):
        self.config = config

    async def execute(self, args: dict, ctx: ExecutionContext) -> None:
        validated_args = InMemoryIngestionTaskArgs.model_validate(args)
        pipeline = Pipeline.create(validated_args.recipe)
        pipeline.run()
        pipeline.raise_from_status()

    def close(self) -> None:
        pass
