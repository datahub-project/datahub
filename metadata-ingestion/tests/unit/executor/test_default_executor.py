from typing import Dict, List, Optional

import pytest

from datahub.executor.execution.default_executor import (
    DefaultExecutor,
    DefaultExecutorConfig,
)
from datahub.executor.execution.task import TaskConfig
from datahub.secret.environment_secret_store import EnvironmentSecretStore
from datahub.secret.secret_store import SecretStore, SecretStoreConfig


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


class DummySecretStore(SecretStore):
    """A stand-in for an operator's own secret store class, referenced by import path."""

    @classmethod
    def create(cls, configs: dict) -> "DummySecretStore":
        return cls()

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        return {name: None for name in secret_names}

    def get_id(self) -> str:
        return "dummy"

    def close(self) -> None:
        pass


_DUMMY_PATH = "tests.unit.executor.test_default_executor.DummySecretStore"


def _build_store(store_type: str) -> SecretStore:
    config = DefaultExecutorConfig(
        id="test-executor",
        task_configs=[],
        secret_stores=[SecretStoreConfig(type=store_type, config={})],
    )
    return DefaultExecutor(config).secret_stores[0]


class TestSecretStoreResolution:
    """`SecretStoreConfig.type` accepts a built-in short name or an import path."""

    def test_builtin_short_name_resolves_via_the_registry(self) -> None:
        assert isinstance(_build_store("env"), EnvironmentSecretStore)

    @pytest.mark.parametrize("separator", [".", ":"])
    def test_import_path_resolves_to_the_class(self, separator: str) -> None:
        module, _, name = _DUMMY_PATH.rpartition(".")
        assert isinstance(_build_store(f"{module}{separator}{name}"), DummySecretStore)

    def test_unknown_short_name_reports_that_no_class_is_registered(self) -> None:
        with pytest.raises(KeyError, match="Did not find a registered class"):
            _build_store("no-such-store")

    def test_import_path_to_a_non_secret_store_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be derived from"):
            _build_store(
                "tests.unit.executor.test_default_executor.TestSecretStoreResolution"
            )
