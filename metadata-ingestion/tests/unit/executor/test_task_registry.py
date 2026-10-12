import pytest

from datahub.executor.execution.task import Task
from datahub.executor.execution.task_registry import TaskRegistry, import_path

INGESTION_TASK = "execution.sub_process_ingestion_task.SubProcessIngestionTask"
RETIRED = f"acryl.executor.{INGESTION_TASK}"
CURRENT = f"datahub.executor.{INGESTION_TASK}"


class TestRetiredPathRewrite:
    """The engine used to ship as the separate `acryl-executor` distribution.

    Task types are configured as dotted-path strings, and `ExecutorConfig.task_configs`
    is a user-facing recipe field -- so the retired paths exist in operator
    configuration we cannot migrate. These tests cover the rewrite that keeps those
    configs working; without it an operator gets "<task> is disabled; try running: pip
    install 'acryl-datahub[<task>]'", which names an extra that does not exist and never
    mentions the real cause.
    """

    def test_retired_path_resolves_to_the_current_class(self) -> None:
        assert import_path(RETIRED) is import_path(CURRENT)

    def test_retired_path_logs_both_paths_so_operators_can_fix_their_config(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING"):
            import_path(RETIRED)
        assert RETIRED in caplog.text
        assert CURRENT in caplog.text

    def test_current_path_is_untouched_and_does_not_warn(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING"):
            resolved = import_path(CURRENT)
        assert resolved.__module__.startswith("datahub.executor.")
        assert caplog.text == ""

    def test_unrelated_modules_are_not_rewritten(self) -> None:
        """The prefix must not swallow anything outside the retired namespace."""
        assert import_path("datahub.executor.execution.task.Task") is Task
        with pytest.raises(ModuleNotFoundError):
            import_path("acryl.something_else.Thing")

    def test_bare_retired_root_is_not_rewritten(self) -> None:
        """The prefix carries a trailing dot, so `acryl.executor` alone is left alone.

        Rewriting it would turn a clearly-wrong config value into a confusing one.
        """
        with pytest.raises(ModuleNotFoundError):
            import_path("acryl.executor")


class TestRegistryResolvesRetiredPaths:
    """The rewrite has to work through the registry, not just import_path directly.

    Both routes matter: DefaultExecutor calls register_lazy(name, dotted_path) and later
    get(name), while a dotted key passed straight to get() is treated as an import path.
    """

    def test_register_lazy_then_get(self) -> None:
        registry: TaskRegistry = TaskRegistry()
        registry.register_lazy("RUN_INGEST", RETIRED)
        assert registry.get("RUN_INGEST") is import_path(CURRENT)

    def test_dotted_key_passed_directly_to_get(self) -> None:
        registry: TaskRegistry = TaskRegistry()
        assert registry.get(RETIRED) is import_path(CURRENT)

    def test_unimportable_retired_path_still_reports_disabled(self) -> None:
        """A rewritten path that genuinely does not exist must not resolve silently."""
        registry: TaskRegistry = TaskRegistry()
        registry.register_lazy("BOGUS", "acryl.executor.execution.nope.Nope")
        with pytest.raises(EnvironmentError):
            registry.get("BOGUS")
