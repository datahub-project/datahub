from datetime import datetime

from datahub.executor.request.execution_request import ExecutionRequest


class TestExecutionRequest:
    def test_execution_request_defaults(self):
        request = ExecutionRequest(
            exec_id="test-id", name="test-task", args={"key": "value"}
        )

        assert request.executor_id == "default"
        assert request.exec_id == "test-id"
        assert request.name == "test-task"
        assert request.args == {"key": "value"}
        assert request.progress_callback is None
        assert request.start_time is None

    def test_execution_request_with_custom_executor_id(self):
        request = ExecutionRequest(
            executor_id="custom-executor", exec_id="test-id", name="test-task", args={}
        )

        assert request.executor_id == "custom-executor"

    def test_start_time_ms_with_no_start_time(self):
        request = ExecutionRequest(exec_id="test-id", name="test-task", args={})

        assert request.start_time_ms == 0

    def test_start_time_ms_with_start_time(self):
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        request = ExecutionRequest(
            exec_id="test-id", name="test-task", args={}, start_time=start_time
        )

        expected_ms = int(start_time.timestamp() * 1000)
        assert request.start_time_ms == expected_ms

    def test_execution_request_with_none_exec_id(self):
        request = ExecutionRequest(exec_id=None, name="test-task", args={})

        assert request.exec_id is None
