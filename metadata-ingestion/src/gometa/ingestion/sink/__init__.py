from typing import Dict, Type
from gometa.ingestion.api.sink import Sink
from .console import ConsoleSink
from .file import FileSink
from .datahub_kafka import DatahubKafkaSink
from .datahub_rest import DatahubRestSink

sink_class_mapping: Dict[str, Type[Sink]] = {
    "console": ConsoleSink,
    "file": FileSink,
    "datahub-kafka": DatahubKafkaSink,
    "datahub-rest": DatahubRestSink,
}
