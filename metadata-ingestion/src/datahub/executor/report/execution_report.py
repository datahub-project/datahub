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

import datetime
from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.api.report import Report


def format_report_line(line_type: str, message: str) -> str:
    utc_time = datetime.datetime.utcnow()
    return f"{utc_time} {line_type}: {message}"


@dataclass
class ExecutionReport(Report):
    """
    Report object leveraged by an Executor coordinating Task Execution.
    """

    exec_id: str

    infos: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    _structured_report: Optional[str] = None
    _logs: str = ""

    def __init__(self, exec_id: str):
        self.exec_id = exec_id
        self.infos = []
        self.errors = []

    def report_info(self, message: str, log: bool = True) -> None:
        formatted_message = format_report_line("INFO", message)
        if log:
            print(f"[exec_id={self.exec_id}] {formatted_message}")
        self.infos.append(formatted_message)

    def report_error(self, message: str, log: bool = True) -> None:
        formatted_message = format_report_line("ERROR", message)
        if log:
            print(f"[exec_id={self.exec_id}] {formatted_message}")
        self.errors.append(formatted_message)

    def set_logs(self, logs: str) -> None:
        self._logs = logs

    def get_logs(self) -> str:
        return self._logs

    def set_structured_report(self, report_content: str) -> None:
        self._structured_report = report_content

    def get_structured_report(self) -> Optional[str]:
        return self._structured_report
