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

import uuid
from typing import Optional

from datahub.executor.report.execution_report import ExecutionReport
from datahub.executor.request.execution_request import ExecutionRequest


def generate_execution_id() -> str:
    # return new execution id
    return str(uuid.uuid4())


"""
Context object passed down into the Task being executed, when its executed
"""


class ExecutionContext:
    # Execution id
    exec_id: str

    # The original request
    request: ExecutionRequest

    # The report associated with the task
    report: ExecutionReport

    # Directory the task writes its logs and artifacts to, if it produces any.
    # Populated by the task once it knows the path; None until then, and None for
    # tasks that produce no artifacts at all.
    #
    # This exists so a caller can find the run's artifacts after execute() returns,
    # without having to reconstruct the path from configuration. The directory
    # deliberately outlives the run: only the task's temporary working directory is
    # removed on completion.
    artifact_dir: Optional[str]

    def __init__(self, request: ExecutionRequest) -> None:
        self.request = request
        if request.exec_id is None:
            self.exec_id = generate_execution_id()
        else:
            self.exec_id = request.exec_id
        self.report = ExecutionReport(self.exec_id)
        self.artifact_dir = None

    def get_execution_id(self) -> str:
        return self.exec_id

    def get_report(self) -> ExecutionReport:
        return self.report

    def get_task_name(self) -> str:
        return self.request.name

    def get_artifact_dir(self) -> Optional[str]:
        return self.artifact_dir

    def set_artifact_dir(self, artifact_dir: str) -> None:
        self.artifact_dir = artifact_dir
