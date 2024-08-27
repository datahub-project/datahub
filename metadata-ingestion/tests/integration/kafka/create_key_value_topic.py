import json
import uuid
from argparse import ArgumentParser
from typing import Tuple

from avro.schema import Schema
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


def parse_command_line_args():
    arg_parser = ArgumentParser()

    arg_parser.add_argument("--topic", required=True, help="Topic name")
    arg_parser.add_argument(
        "--bootstrap-servers",
        required=False,
        default="localhost:9092",
        help="Bootstrap server address",
    )
    arg_parser.add_argument(
        "--schema-registry",
        required=False,
        default="http://localhost:8081",
        help="Schema Registry url",
    )
    arg_parser.add_argument(
        "--record-key",
        required=False,
        type=str,
        help="Record key. If not provided, will be a random UUID",
    )
    arg_parser.add_argument(
        "--key-schema-file", required=False, help="File name of key Avro schema to use"
    )
    arg_parser.add_argument("--record-value", required=False, help="Record value")
    arg_parser.add_argument(
        "--value-schema-file", required=False, help="File name of Avro schema to use"
    )

    return arg_parser.parse_args()


def load_avro_schema_from_file(
    key_schema_file: str, value_schema_file: str
) -> Tuple[Schema, Schema]:
    key_schema = (
        avro.load(key_schema_file)
        if key_schema_file is not None
        else avro.loads('{"type": "string"}')
    )
    value_schema = (
        avro.load(value_schema_file) if value_schema_file is not None else None
    )

    return key_schema, value_schema


def send_record(args):
    key_schema, value_schema = load_avro_schema_from_file(
        args.key_schema_file, args.value_schema_file
    )

    producer_config = {
        "bootstrap.servers": args.bootstrap_servers,
        "schema.registry.url": args.schema_registry,
    }

    producer = AvroProducer(
        producer_config,
        default_key_schema=key_schema,
        default_value_schema=value_schema,
    )

    key = json.loads(args.record_key) if args.record_key else str(uuid.uuid4())
    value = json.loads(args.record_value) if args.record_value else None

    try:
        producer.produce(topic=args.topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(
            f"Exception while producing record value - {value} to topic - {args.topic}: {e}"
        )
        raise e
    else:
        print(f"Successfully producing record value - {value} to topic - {args.topic}")


"""
    Creates a Key-Value topic by sending a record to it
"""
if __name__ == "__main__":
    send_record(parse_command_line_args())
