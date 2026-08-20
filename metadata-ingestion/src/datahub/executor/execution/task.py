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

from abc import abstractmethod

from datahub.executor.common.config import ConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext

"""
Single Task Config
"""


class TaskConfig(ConfigModel):
    name: str
    type: str
    configs: dict


class TaskError(Exception):
    """An error occurred when executing a task"""


class Task:
    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        pass

    @abstractmethod
    async def execute(self, args: dict, ctx: ExecutionContext) -> None:
        """
        Execute the task, throws TaskError exception on failure
        """

    @abstractmethod
    def close(self) -> None:
        """
        Wraps up the task
        """
