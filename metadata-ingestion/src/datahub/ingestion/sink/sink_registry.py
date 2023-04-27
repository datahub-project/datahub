import dataclasses
from typing import Type

from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.sink import Sink


def _check_sink_classes(cls: Type[Sink]) -> None:
    assert not dataclasses.is_dataclass(cls), f"Sink {cls} is a dataclass"
    assert cls.get_config_class()
    assert cls.get_report_class()


sink_registry = PluginRegistry[Sink](extra_cls_check=_check_sink_classes)
sink_registry.register_from_entrypoint("datahub.ingestion.sink.plugins")
