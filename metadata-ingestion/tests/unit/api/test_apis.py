import inspect

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry

# These configs deliberately set extra="allow".
SOURCES_ALLOWING_EXTRA_KEYS = {"preset", "superset"}


def test_sources_not_abstract() -> None:
    for cls in source_registry.mapping.values():
        assert not inspect.isabstract(cls)


def test_source_configs_are_config_models() -> None:
    checked = set()
    for source_type in sorted(source_registry.mapping):
        try:
            source_cls = source_registry.get(source_type)
        except ConfigurationError as e:
            if isinstance(e.__cause__, ModuleNotFoundError):
                continue  # optional extra not installed
            raise
        # Added at runtime by @config_class, so not visible to mypy on Source.
        get_config_class = getattr(source_cls, "get_config_class", None)
        assert get_config_class is not None, source_type
        config_class = get_config_class()
        assert issubclass(config_class, ConfigModel), source_type
        expected_extra = (
            "allow" if source_type in SOURCES_ALLOWING_EXTRA_KEYS else "forbid"
        )
        assert config_class.model_config.get("extra") == expected_extra, source_type
        checked.add(source_type)
    # `file` needs no extras, so it loads in any install; if unchecked, loading is broken.
    assert "file" in checked


def test_sinks_not_abstract() -> None:
    for cls in sink_registry.mapping.values():
        assert not inspect.isabstract(cls)
