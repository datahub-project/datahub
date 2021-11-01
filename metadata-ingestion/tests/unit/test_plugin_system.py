import pytest
from click.testing import CliRunner

from datahub.configuration.common import ConfigurationError
from datahub.entrypoints import datahub
from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.sink import Sink
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.sink.console import ConsoleSink
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry
from tests.test_helpers.click_helpers import assert_result_ok


@pytest.mark.parametrize(
    "registry",
    [
        source_registry,
        sink_registry,
        extractor_registry,
        transform_registry,
    ],
)
def test_registry_nonempty(registry):
    assert len(registry.mapping) > 0


@pytest.mark.parametrize(
    "verbose",
    [False, True],
)
def test_list_all(verbose: bool) -> None:
    # This just verifies that it runs without error.
    runner = CliRunner()
    args = ["check", "plugins"]
    if verbose:
        args.append("--verbose")
    result = runner.invoke(datahub, args)
    assert_result_ok(result)
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
