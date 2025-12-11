# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
