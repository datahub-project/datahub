"""PyFlink DataStream Kafka-to-Kafka job for integration tests.

Submits a streaming job that reads from one Kafka topic and writes to another
using the DataStream API. The execution plan shows KafkaSource-{topic} and
KafkaSink-{topic} patterns, which the DataStreamKafkaExtractor parses for lineage.

Usage (inside Flink container):
    python3 /opt/flink/pyflink_kafka_job.py \
        --input-topic orders \
        --output-topic enriched-orders \
        --bootstrap-servers broker:9092 \
        --job-name test_datastream_kafka
"""

import argparse

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-topic", required=True)
    parser.add_argument("--output-topic", required=True)
    parser.add_argument("--bootstrap-servers", default="broker:9092")
    parser.add_argument("--job-name", default="pyflink-kafka-passthrough")
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.input_topic)
        .set_group_id(f"{args.job_name}-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.output_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Operator names determine the plan description visible in the REST API:
    #   Source: "Source: KafkaSource-{input_topic}"
    #   Sink:   "KafkaSink-{output_topic}: Writer"
    ds = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name=f"KafkaSource-{args.input_topic}",
    )
    ds.sink_to(sink).name(f"KafkaSink-{args.output_topic}")

    env.execute(args.job_name)


if __name__ == "__main__":
    main()
