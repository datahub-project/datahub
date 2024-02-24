from acryl.executor.request.execution_request import ExecutionRequest

from datahub_monitors.common.types import CronSchedule

RUN_INGEST_TASK_NAME = "RUN_INGEST"
RUN_ASSERTION_TASK_NAME = "RUN_ASSERTION"


class ExecutionRequestSchedule:
    execution_request: ExecutionRequest
    schedule: CronSchedule

    def __init__(
        self,
        execution_request: ExecutionRequest,
        schedule: CronSchedule,
    ):
        self.execution_request = execution_request
        self.schedule = schedule
