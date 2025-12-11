# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import inspect

from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry


def test_sources_not_abstract() -> None:
    for cls in source_registry.mapping.values():
        assert not inspect.isabstract(cls)


def test_sinks_not_abstract() -> None:
    for cls in sink_registry.mapping.values():
        assert not inspect.isabstract(cls)
