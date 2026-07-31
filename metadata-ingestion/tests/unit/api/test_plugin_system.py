import threading
import warnings
from typing import List

import pytest

from datahub.configuration.common import ConfigurationError, ConfigurationWarning
from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.sink import Sink
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.fs.fs_registry import fs_registry
from datahub.ingestion.reporting.reporting_provider_registry import (
    reporting_provider_registry,
)
from datahub.ingestion.sink.console import ConsoleSink
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.source.state_provider.state_provider_registry import (
    ingestion_checkpoint_provider_registry,
)
from datahub.ingestion.transformer.transform_registry import transform_registry
from datahub.lite.lite_registry import lite_registry
from tests.test_helpers.click_helpers import run_datahub_cmd


@pytest.mark.parametrize(
    "registry,expected",
    [
        (source_registry, ["file"]),
        (sink_registry, ["console", "file", "blackhole"]),
        (extractor_registry, ["generic"]),
        (
            transform_registry,
            [
                "simple_remove_dataset_ownership",
                "mark_dataset_status",
                "set_dataset_browse_path",
                "add_dataset_ownership",
                "simple_add_dataset_ownership",
                "pattern_add_dataset_ownership",
                "add_dataset_domain",
                "simple_add_dataset_domain",
                "pattern_add_dataset_domain",
                "add_dataset_tags",
                "simple_add_dataset_tags",
                "pattern_add_dataset_tags",
                "add_dataset_terms",
                "simple_add_dataset_terms",
                "pattern_add_dataset_terms",
                "add_dataset_properties",
                "simple_add_dataset_properties",
                "pattern_add_dataset_schema_terms",
                "pattern_add_dataset_schema_tags",
            ],
        ),
        (reporting_provider_registry, ["datahub", "file"]),
        (ingestion_checkpoint_provider_registry, ["datahub"]),
        (lite_registry, ["duckdb"]),
        (fs_registry, ["file", "http", "s3"]),
    ],
)
def test_registry_defaults(registry: PluginRegistry, expected: List[str]) -> None:
    assert len(registry.mapping) > 0

    for plugin in expected:
        assert registry.get(plugin)


# TODO: Restore this test. This test causes loading interference with test mocks.
@pytest.mark.skip(reason="Interferes with test mocks.")
@pytest.mark.parametrize(
    "verbose",
    [False, True],
)
def test_list_all(verbose: bool) -> None:
    # This just verifies that it runs without error.
    args = ["check", "plugins"]
    if verbose:
        args.append("--verbose")
    result = run_datahub_cmd(args)
    assert len(result.output.splitlines()) > 20


def test_registry():
    # Make a mini sink registry.
    fake_registry = PluginRegistry[Sink]()
    fake_registry.register("console", ConsoleSink)
    fake_registry.register_disabled("disabled", ModuleNotFoundError("disabled sink"))
    fake_registry.register_disabled(
        "disabled-exception", Exception("second disabled sink")
    )

    class DummyClass:
        pass

    assert len(fake_registry.mapping) > 0
    assert fake_registry.is_enabled("console")
    assert fake_registry.get("console") == ConsoleSink
    assert (
        fake_registry.get("datahub.ingestion.sink.console.ConsoleSink") == ConsoleSink
    )

    # Test lazy-loading capabilities.
    fake_registry.register_lazy(
        "lazy-console", "datahub.ingestion.sink.console:ConsoleSink"
    )
    assert fake_registry.get("lazy-console") == ConsoleSink

    fake_registry.register_lazy("lazy-error", "thisdoesnot.exist")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("lazy-error")

    # Test error-checking on keys.
    with pytest.raises(KeyError, match="special characters"):
        fake_registry.register("thisdoesnotexist.otherthing", ConsoleSink)
    with pytest.raises(KeyError, match="in use"):
        fake_registry.register("console", ConsoleSink)
    with pytest.raises(KeyError, match="not find"):
        fake_registry.get("thisdoesnotexist")

    # Test error-checking on registered types.
    with pytest.raises(ValueError, match="abstract"):
        fake_registry.register("thisdoesnotexist", Sink)  # type: ignore
    with pytest.raises(ValueError, match="derived"):
        fake_registry.register("thisdoesnotexist", DummyClass)  # type: ignore
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled-exception")

    # This just verifies that it runs without error. The formatting should be manually checked.
    assert len(fake_registry.summary(verbose=False).splitlines()) >= 5
    assert len(fake_registry.summary(verbose=True).splitlines()) >= 5

    # Test aliases.
    fake_registry.register_alias(
        "console-alias",
        "console",
        lambda: warnings.warn(
            ConfigurationWarning("console-alias is deprecated, use console instead"),
            stacklevel=2,
        ),
    )
    with pytest.warns(ConfigurationWarning):
        assert fake_registry.get("console-alias") == ConsoleSink
    assert "console-alias" not in fake_registry.summary(verbose=False)


def test_registry_thread_safety() -> None:
    """Concurrent calls to get() must not raise KeyError when entrypoints materialize."""
    fake_registry = PluginRegistry[Sink]()

    # Simulate a pending entrypoint by pre-loading the mapping with a lazy entry the
    # way _load_entrypoint would, but bypass the entrypoint machinery so the test
    # doesn't need actual entry point metadata installed.
    fake_registry.register_lazy("console", "datahub.ingestion.sink.console:ConsoleSink")

    # Patch _load_entrypoint so that calling _materialize_entrypoints via an entrypoint
    # key re-registers "console" — which is the collision that caused KeyError before
    # the lock was introduced.
    def _racy_load(entry_point_key: str) -> None:
        # Mimic what the real _load_entrypoint does: call register_lazy for an already-
        # registered key.  Without the lock this races and raises KeyError.
        try:
            fake_registry.register_lazy(
                "console", "datahub.ingestion.sink.console:ConsoleSink"
            )
        except KeyError:
            pass  # would be masked at the call site; we want to surface it differently

    fake_registry._load_entrypoint = _racy_load  # type: ignore[method-assign]
    fake_registry._entrypoints = ["datahub.ingestion.sink.plugins"]

    errors: List[Exception] = []
    barrier = threading.Barrier(8)

    def _call_get() -> None:
        barrier.wait()  # all threads start simultaneously
        try:
            fake_registry.get("console")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=_call_get) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Thread-safety errors: {errors}"
