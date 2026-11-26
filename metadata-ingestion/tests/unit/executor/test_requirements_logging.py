import pytest

from datahub.executor.execution.runner import (
    LogHolder,
    referenced_env_values,
)
from datahub.executor.execution.sub_process_task_common import (
    SubProcessRecipeTaskArgs,
    SubProcessTaskUtil,
)
from datahub.masking.secret_registry import SecretRegistry


@pytest.fixture(autouse=True)
def isolated_registry():
    SecretRegistry.reset_instance()
    yield
    SecretRegistry.reset_instance()


class TestReferencedEnvValues:
    def test_only_referenced_vars_are_collected(self, monkeypatch):
        monkeypatch.setenv("PIP_INDEX_TOKEN", "tok-super-secret-1")
        monkeypatch.setenv("UNRELATED_VAR", "unrelated-value")
        reqs = [
            "pkg @ https://user:${PIP_INDEX_TOKEN}@example.com/simple",
            "plainpkg==1.0",
        ]
        assert referenced_env_values(reqs) == {"PIP_INDEX_TOKEN": "tok-super-secret-1"}

    def test_default_syntax_bare_refs_and_unset_vars(self, monkeypatch):
        monkeypatch.setenv("SET_VAR", "set-value-123")
        monkeypatch.setenv("BARE_VAR", "bare-value-456")
        monkeypatch.delenv("UNSET_VAR", raising=False)
        reqs = ["a==${SET_VAR:-1.0}", "b==${UNSET_VAR}", "c @ https://x/$BARE_VAR"]
        assert referenced_env_values(reqs) == {
            "SET_VAR": "set-value-123",
            "BARE_VAR": "bare-value-456",
        }


class TestAppendMasked:
    def test_registered_secret_is_masked(self):
        SecretRegistry.get_instance().register_secret("TOKEN", "tok-super-secret-1")
        logs = LogHolder()
        logs.append_masked(
            "pkg @ https://user:tok-super-secret-1@example.com/simple\nplain==1.0"
        )
        joined = "".join(logs.get_lines())
        assert "tok-super-secret-1" not in joined
        assert "***REDACTED:TOKEN***" in joined
        assert "plain==1.0" in joined

    def test_multi_line_secret_is_masked_whole_buffer(self):
        key = "first-key-line-material\nsecond-key-line-material"
        SecretRegistry.get_instance().register_secret("MULTI_KEY", key)
        logs = LogHolder()
        logs.append_masked(f"before\n{key}\nafter")
        joined = "".join(logs.get_lines())
        assert "first-key-line-material" not in joined
        assert "second-key-line-material" not in joined
        assert "before" in joined
        assert "after" in joined

    def test_unregistered_content_passes_through(self):
        logs = LogHolder()
        logs.append_masked("acryl-datahub[snowflake]==1.2.3")
        assert "acryl-datahub[snowflake]==1.2.3\n" in logs.get_lines()


class TestSubprocessEnvSecrets:
    def test_collects_pip_references_not_extra_env_vars(self, monkeypatch):
        monkeypatch.setenv("PIP_INDEX_TOKEN", "tok-super-secret-1")
        args = SubProcessRecipeTaskArgs(
            recipe="{}",
            extra_pip_requirements=[
                "pkg @ https://user:${PIP_INDEX_TOKEN}@example.com/simple"
            ],
            extra_env_vars={"CONNECTOR_KEY": "connector-key-value"},
        )
        assert SubProcessTaskUtil.subprocess_env_secrets(args) == {
            "PIP_INDEX_TOKEN": "tok-super-secret-1"
        }

    def test_user_override_is_excluded_so_recipe_resolution_stays_consistent(
        self, monkeypatch
    ):
        monkeypatch.setenv("SHARED_NAME", "pod-value-material")
        args = SubProcessRecipeTaskArgs(
            recipe="{}",
            extra_pip_requirements=["pkg @ https://x/${SHARED_NAME}/simple"],
            extra_env_vars={"SHARED_NAME": "user-value-material"},
        )
        assert SubProcessTaskUtil.subprocess_env_secrets(args) == {}
