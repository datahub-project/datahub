from typing import Dict, Type

from datahub.ingestion.api.sink import Sink

from .console import ConsoleSink
from .datahub_kafka import DatahubKafkaSink
from .datahub_rest import DatahubRestSink
from .file import FileSink

sink_class_mapping: Dict[str, Type[Sink]] = {
    "console": ConsoleSink,
    "file": FileSink,
    "datahub-kafka": DatahubKafkaSink,
    "datahub-rest": DatahubRestSink,
}
