"""Test secret resolution with fallback to environment variables and empty strings.

This test verifies that when a recipe references ${VAR}:
1. First tries to resolve from secret stores
2. Falls back to os.environ if not in secret stores
3. Uses empty string if not found anywhere (with warning)

After the security refactor, _resolve_recipe returns (recipe_dict, secret_values)
where secret_values is a dict[str, str]. Secrets are no longer written to os.environ.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from datahub.executor.execution.sub_process_task_common import SubProcessTaskUtil


class TestSecretResolutionFallback:
    """Test secret resolution with os.environ fallback."""

    def test_secret_from_secret_store(self) -> None:
        """Test that secrets from secret store are used first."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${MY_PASSWORD}"}}}
        )

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that returns the password
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {"MY_PASSWORD": "secret123"}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify the secret was returned in secret_values dict
        assert "MY_PASSWORD" in secret_values
        assert secret_values["MY_PASSWORD"] == "secret123"

    def test_secret_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that os.environ is used as fallback when secret store returns None."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${MY_ENV_VAR}"}}}
        )

        monkeypatch.setenv("MY_ENV_VAR", "from_environment")

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that doesn't have the secret
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify the environment variable was used as fallback
        assert "MY_ENV_VAR" in secret_values
        assert secret_values["MY_ENV_VAR"] == "from_environment"

    def test_secret_missing_uses_empty_string(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that empty string is used when secret not found anywhere."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${MISSING_SECRET}"}}}
        )

        # Make sure the secret is NOT in environment
        monkeypatch.delenv("MISSING_SECRET", raising=False)

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that doesn't have the secret
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe - should NOT raise TaskError
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify empty string was returned for missing secret
        assert "MISSING_SECRET" in secret_values
        assert secret_values["MISSING_SECRET"] == ""

    def test_multiple_secrets_mixed_sources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test recipe with secrets from different sources."""
        recipe = json.dumps(
            {
                "source": {
                    "type": "test",
                    "config": {
                        "password": "${FROM_STORE}",
                        "token": "${FROM_ENV}",
                        "api_key": "${MISSING}",
                    },
                }
            }
        )

        monkeypatch.setenv("FROM_ENV", "env_value")
        monkeypatch.delenv("MISSING", raising=False)

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store with one secret
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {"FROM_STORE": "store_value"}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify all secrets were resolved in the returned dict
        assert secret_values["FROM_STORE"] == "store_value"
        assert secret_values["FROM_ENV"] == "env_value"
        assert secret_values["MISSING"] == ""

    def test_secret_store_takes_priority_over_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that secret store value takes priority over pre-existing env var."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${MY_SECRET}"}}}
        )

        monkeypatch.setenv("MY_SECRET", "from_env")

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that has the same secret with different value
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {"MY_SECRET": "from_store"}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Secret store value should be in secret_values
        assert "MY_SECRET" in secret_values
        assert secret_values["MY_SECRET"] == "from_store"

    def test_secret_values_dict_contains_all_resolved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that the secret_values dict contains all resolved secrets."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${NEW_SECRET}"}}}
        )

        # Ensure secret is NOT in environment before
        monkeypatch.delenv("NEW_SECRET", raising=False)

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that returns the secret
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {"NEW_SECRET": "new_value"}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify the secret is in the returned dict
        assert "NEW_SECRET" in secret_values
        assert secret_values["NEW_SECRET"] == "new_value"

    def test_env_fallback_value_in_secret_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that env fallback values appear in secret_values dict."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${PRE_EXISTING}"}}}
        )

        # Set environment variable BEFORE resolution
        monkeypatch.setenv("PRE_EXISTING", "env_value")

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that doesn't have the secret (fallback to env)
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify the env fallback value is in the returned dict
        assert "PRE_EXISTING" in secret_values
        assert secret_values["PRE_EXISTING"] == "env_value"

    def test_missing_secret_empty_string_in_secret_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that missing secrets get empty string in secret_values dict."""
        recipe = json.dumps(
            {"source": {"type": "test", "config": {"password": "${TOTALLY_MISSING}"}}}
        )

        # Ensure secret is NOT in environment
        monkeypatch.delenv("TOTALLY_MISSING", raising=False)

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store that doesn't have the secret
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {}
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify the missing secret is in the dict with empty string
        assert "TOTALLY_MISSING" in secret_values
        assert secret_values["TOTALLY_MISSING"] == ""

    def test_mixed_scenario_all_in_secret_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test secret_values dict with mix of store, env fallback, and missing secrets."""
        recipe = json.dumps(
            {
                "source": {
                    "type": "test",
                    "config": {
                        "new_from_store": "${NEW_FROM_STORE}",
                        "existing_fallback": "${EXISTING_FALLBACK}",
                        "missing": "${MISSING_SECRET}",
                    },
                }
            }
        )

        # Set some environment variables BEFORE resolution
        monkeypatch.setenv("EXISTING_FALLBACK", "stays_same")

        # Ensure others are NOT in environment
        monkeypatch.delenv("NEW_FROM_STORE", raising=False)
        monkeypatch.delenv("MISSING_SECRET", raising=False)

        # Mock context objects
        execution_ctx = MagicMock()
        executor_ctx = MagicMock()

        # Mock secret store with partial secrets
        mock_secret_store = MagicMock()
        mock_secret_store.get_secret_values.return_value = {
            "NEW_FROM_STORE": "new_value",
        }
        executor_ctx.get_secret_stores.return_value = [mock_secret_store]

        # Resolve recipe
        with patch(
            "datahub.executor.execution.sub_process_task_common.initialize_secret_masking"
        ):
            _recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
                recipe, execution_ctx, executor_ctx
            )

        # Verify all secrets are in secret_values dict
        assert secret_values["NEW_FROM_STORE"] == "new_value"
        assert secret_values["EXISTING_FALLBACK"] == "stays_same"
        assert secret_values["MISSING_SECRET"] == ""
