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

from datahub.executor.request.execution_request import ExecutionRequest
from datahub.executor.request.signal_request import SignalRequest
from datahub.executor.result.execution_result import ExecutionResult


class Executor:
    @abstractmethod
    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        """
        Execute a task
        """

    @abstractmethod
    def signal(self, request: SignalRequest) -> None:
        """
        Signal an executing task
        """

    @abstractmethod
    def shutdown(self) -> None:
        """
        Shutdown the executor
        """

    @abstractmethod
    def get_id(self) -> str:
        """
        Retrieve the id of the Executor
        """
