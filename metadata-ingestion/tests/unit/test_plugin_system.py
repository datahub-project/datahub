import pytest
from click.testing import CliRunner

from datahub.configuration.common import ConfigurationError
from datahub.entrypoints import datahub
from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.sink import Sink
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.sink.console import ConsoleSink
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry


@pytest.mark.parametrize(
    "registry",
    [
        source_registry,
        sink_registry,
        extractor_registry,
    ],
)
def test_registry_nonempty(registry):
    assert len(registry.mapping) > 0


def test_list_all() -> None:
    # This just verifies that it runs without error.
    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "plugins", "--verbose"])
    assert result.exit_code == 0
    assert len(result.output.splitlines()) > 20


def test_registry():
    # Make a mini sink registry.
    fake_registry = Registry[Sink]()
    fake_registry.register("console", ConsoleSink)
    fake_registry.register_disabled("disabled", ModuleNotFoundError("disabled sink"))

    class DummyClass:
        pass

    assert len(fake_registry.mapping) > 0
    assert fake_registry.is_enabled("console")
    assert fake_registry.get("console") == ConsoleSink
    assert (
        fake_registry.get("datahub.ingestion.sink.console.ConsoleSink") == ConsoleSink
    )

    with pytest.raises(KeyError, match="key cannot contain '.'"):
        fake_registry.register("thisdoesnotexist.otherthing", ConsoleSink)
    with pytest.raises(KeyError, match="in use"):
        fake_registry.register("console", ConsoleSink)
    with pytest.raises(KeyError, match="not find"):
        fake_registry.get("thisdoesnotexist")

    with pytest.raises(ValueError, match="abstract"):
        fake_registry.register("thisdoesnotexist", Sink)  # type: ignore
    with pytest.raises(ValueError, match="derived"):
        fake_registry.register("thisdoesnotexist", DummyClass)  # type: ignore
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled")
