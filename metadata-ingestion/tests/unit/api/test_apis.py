import inspect

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry


def test_sources_not_abstract() -> None:
    for cls in source_registry.mapping.values():
        assert not inspect.isabstract(cls)


def test_source_configs_are_config_models() -> None:
    checked = 0
    for source_type in sorted(source_registry.mapping):
        try:
            config_cls = source_registry.get(source_type).get_config_class()
        except ConfigurationError as e:
            if isinstance(e.__cause__, ModuleNotFoundError):
                continue  # optional extra not installed
            raise
        assert issubclass(config_cls, ConfigModel), source_type
        checked += 1
    assert checked >= 25, f"only {checked} source configs loaded"


def test_sinks_not_abstract() -> None:
    for cls in sink_registry.mapping.values():
        assert not inspect.isabstract(cls)
