import inspect

from datahub.ingestion.sink import sink_class_mapping
from datahub.ingestion.source import source_class_mapping


def test_sources_not_abstract():
    for cls in source_class_mapping.values():
        assert not inspect.isabstract(cls)


def test_sinks_not_abstract():
    for cls in sink_class_mapping.values():
        assert not inspect.isabstract(cls)
