from datahub.executor.execution.default_executor import DefaultExecutorConfig
from datahub.executor.execution.task import TaskConfig


class TestDefaultExecutorConfig:
    def test_default_executor_config_defaults(self) -> None:
        """Test that DefaultExecutorConfig properly handles default values."""
        config = DefaultExecutorConfig(
            id="test-executor",
            task_configs=[TaskConfig(name="test-task", type="test-type", configs={})],
        )

        assert config.id == "test-executor"
        assert len(config.task_configs) == 1
        assert config.secret_stores == []
        assert config.executor_instance_id is None
        assert config.executor_version is None
