import logging
from datetime import datetime
from typing import Iterable, Tuple

from confluent_kafka import DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.report import DataHubSourceReport
from datahub.metadata.schema_classes import MetadataChangeLogClass

logger = logging.getLogger(__name__)

KAFKA_GROUP = "datahub_source"


class DataHubKafkaReader:
    def __init__(self, config: DataHubSourceConfig, report: DataHubSourceReport):
        self.config = config
        self.report = report
        self.consumer = DeserializingConsumer(
            {
                "group.id": KAFKA_GROUP,
                "bootstrap.servers": self.config.kafka_connection.bootstrap,
                **self.config.kafka_connection.consumer_config,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "value.deserializer": AvroDeserializer(
                    schema_registry_client=SchemaRegistryClient(
                        {"url": self.config.kafka_connection.schema_registry_url}
                    ),
                    return_record_name=True,
                ),
            }
        )

    def get_mcls(
        self, from_offset: int, stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeLogClass, int]]:
        self.consumer.assign(
            [TopicPartition(self.config.kafka_topic_name, from_offset)]
        )
        # TODO: Check if I have to reassign if there are multiple partitions?
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
                    f"with audit stamp {datetime.fromtimestamp(mcl.created.time / 1000)}"
                )
                break

            yield mcl, msg.offset()

        self.consumer.unassign()
