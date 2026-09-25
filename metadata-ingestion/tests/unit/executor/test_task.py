import pytest

from datahub.executor.execution.task import TaskConfig, TaskError


class TestTaskConfig:
    def test_task_config_creation(self):
        config = TaskConfig(
            name="test-task", type="ingestion", configs={"key": "value", "number": 42}
        )

        assert config.name == "test-task"
        assert config.type == "ingestion"
        assert config.configs == {"key": "value", "number": 42}

    def test_task_config_empty_configs(self):
        config = TaskConfig(name="empty-task", type="test", configs={})

        assert config.name == "empty-task"
        assert config.type == "test"
        assert config.configs == {}


class TestTaskError:
    def test_task_error_creation(self):
        error = TaskError("Test error message")

        assert str(error) == "Test error message"
        assert isinstance(error, Exception)

    def test_task_error_inheritance(self):
        error = TaskError("Test error")

        assert isinstance(error, Exception)
        assert isinstance(error, TaskError)

    def test_task_error_raise(self):
        with pytest.raises(TaskError) as exc_info:
            raise TaskError("Custom task error")

        assert str(exc_info.value) == "Custom task error"
