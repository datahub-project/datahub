from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.sink import Sink

from .console import ConsoleSink
from .datahub_kafka import DatahubKafkaSink
from .datahub_rest import DatahubRestSink
from .file import FileSink

sink_registry = Registry[Sink]()

# Add some defaults to sink registry.
sink_registry.register("console", ConsoleSink)
sink_registry.register("file", FileSink)
sink_registry.register("datahub-kafka", DatahubKafkaSink)
sink_registry.register("datahub-rest", DatahubRestSink)
