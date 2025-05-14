import logging
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    DeserializingConsumer,
    TopicPartition,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.emitter.mce_builder import parse_ts_millis
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.report import DataHubSourceReport
from datahub.ingestion.source.datahub.state import PartitionOffset
from datahub.metadata.schema_classes import MetadataChangeLogClass

logger = logging.getLogger(__name__)

KAFKA_GROUP_PREFIX = "datahub_source"


class DataHubKafkaReader(Closeable):
    def __init__(
        self,
        config: DataHubSourceConfig,
        connection_config: KafkaConsumerConnectionConfig,
        report: DataHubSourceReport,
        ctx: PipelineContext,
    ):
        self.config = config
        self.connection_config = connection_config
        self.report = report
        self.group_id = f"{KAFKA_GROUP_PREFIX}-{ctx.pipeline_name}"
        self.ctx = ctx

    def __enter__(self) -> "DataHubKafkaReader":
        self.consumer = DeserializingConsumer(
            {
                "group.id": self.group_id,
                "bootstrap.servers": self.connection_config.bootstrap,
                **self.connection_config.consumer_config,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "value.deserializer": AvroDeserializer(
                    schema_registry_client=SchemaRegistryClient(
                        {"url": self.connection_config.schema_registry_url}
                    ),
                    return_record_name=True,
                ),
            }
        )
        return self

    def get_mcls(
        self, from_offsets: Dict[int, int], stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeLogClass, PartitionOffset]]:
        # Based on https://github.com/confluentinc/confluent-kafka-python/issues/145#issuecomment-284843254
        def on_assign(consumer: Consumer, partitions: List[TopicPartition]) -> None:
            for p in partitions:
                p.offset = from_offsets.get(p.partition, OFFSET_BEGINNING)
                logger.debug(f"Set partition {p.partition} offset to {p.offset}")
            consumer.assign(partitions)

        self.consumer.subscribe([self.config.kafka_topic_name], on_assign=on_assign)
        try:
            yield from self._poll_partition(stop_time)
        finally:
            self.consumer.unsubscribe()

    def _poll_partition(
        self, stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeLogClass, PartitionOffset]]:
        while True:
            msg = self.consumer.poll(10)
            if msg is None:
                break

            try:
                mcl = MetadataChangeLogClass.from_obj(msg.value(), True)
            except Exception as e:
                logger.warning(f"Error deserializing MCL: {e}")
                self.report.num_kafka_parse_errors += 1
                self.report.kafka_parse_errors.setdefault(str(e), 0)
                self.report.kafka_parse_errors[str(e)] += 1
                continue

            if mcl.created and mcl.created.time > stop_time.timestamp() * 1000:
                logger.info(
                    f"Stopped reading from kafka, reached MCL "
                    f"with audit stamp {parse_ts_millis(mcl.created.time)}"
                )
                break

            if mcl.aspectName and mcl.aspectName in self.config.exclude_aspects:
                self.report.num_kafka_excluded_aspects += 1
                continue

            # TODO: Consider storing state in kafka instead, via consumer.commit()
            yield mcl, PartitionOffset(partition=msg.partition(), offset=msg.offset())

    def close(self) -> None:
        self.consumer.close()
