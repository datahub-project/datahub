from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.sink import Sink

from .console import ConsoleSink
from .file import FileSink

sink_registry = Registry[Sink]()

# These sinks are always enabled.
sink_registry.register("console", ConsoleSink)
sink_registry.register("file", FileSink)

try:
    from .datahub_kafka import DatahubKafkaSink

    sink_registry.register("datahub-kafka", DatahubKafkaSink)
except ImportError as e:
    sink_registry.register_disabled("datahub-kafka", e)

try:
    from .datahub_rest import DatahubRestSink

    sink_registry.register("datahub-rest", DatahubRestSink)
except ImportError as e:
    sink_registry.register_disabled("datahub-rest", e)
