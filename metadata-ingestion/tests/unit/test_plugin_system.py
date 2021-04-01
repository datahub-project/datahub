from click.testing import CliRunner
import pytest


from datahub.configuration.common import ConfigurationError
from datahub.entrypoints import datahub
from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.sink import Sink
from datahub.ingestion.sink.console import ConsoleSink


def test_list_all():
    # This just verifies that it runs without error.
    runner = CliRunner()
    result = runner.invoke(datahub, ["ingest-list-plugins"])
    assert result.exit_code == 0


def test_registry():
    # Make a mini sink registry.
    sink_registry = Registry[Sink]()
    sink_registry.register("console", ConsoleSink)
    sink_registry.register_disabled("disabled", ImportError("disabled sink"))

    class DummyClass:
        pass

    assert len(sink_registry.mapping) > 0
    assert sink_registry.is_enabled("console")
    assert sink_registry.get("console") == ConsoleSink
    assert (
        sink_registry.get("datahub.ingestion.sink.console.ConsoleSink") == ConsoleSink
    )

    with pytest.raises(KeyError, match="key cannot contain '.'"):
        sink_registry.register("thisdoesnotexist.otherthing", ConsoleSink)
    with pytest.raises(KeyError, match="in use"):
        sink_registry.register("console", ConsoleSink)
    with pytest.raises(KeyError, match="not find"):
        sink_registry.get("thisdoesnotexist")

    with pytest.raises(ValueError, match="abstract"):
        sink_registry.register("thisdoesnotexist", Sink)  # type: ignore
    with pytest.raises(ValueError, match="derived"):
        sink_registry.register("thisdoesnotexist", DummyClass)  # type: ignore
    with pytest.raises(ConfigurationError, match="disabled"):
        sink_registry.get("disabled")
