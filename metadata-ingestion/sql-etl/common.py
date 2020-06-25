#! /usr/bin/python
import time

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy.engine import reflection

URL = 'mysql+pymysql://datahub:datahub@localhost:3306' # e.g. mysql+pymysql://username:password@hostname:port
OPTIONS = {} # e.g. {"encoding": "latin1"}


@dataclass
class KafkaConfig:
    avsc_path = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
    kafka_topic = 'MetadataChangeEvent'
    bootstrap_server = 'localhost:9092'
    schema_registry = 'http://localhost:8081'


def get_column_type(column_type):
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """
    if isinstance(column_type, (types.Integer, types.Numeric)):
        return ("com.linkedin.pegasus2avro.schema.NumberType", {})

    if isinstance(column_type, (types.Boolean)):
        return ("com.linkedin.pegasus2avro.schema.BooleanType", {})

    if isinstance(column_type, (types.Enum)):
        return ("com.linkedin.pegasus2avro.schema.EnumType", {})

    if isinstance(column_type, (types._Binary, types.PickleType)):
        return ("com.linkedin.pegasus2avro.schema.BytesType", {})

    if isinstance(column_type, (types.ARRAY)):
        return ("com.linkedin.pegasus2avro.schema.ArrayType", {})

    if isinstance(column_type, (types.String)):
        return ("com.linkedin.pegasus2avro.schema.StringType", {})

    return ("com.linkedin.pegasus2avro.schema.NullType", {})


def build_dataset_mce(platform, dataset_name, columns):
    """
    Creates MetadataChangeEvent for the dataset.
    """
    actor, sys_time = "urn:li:corpuser:etl", int(time.time())

    fields = []
    for column in columns:
        fields.append({
            "fieldPath": column["name"],
            "nativeDataType": repr(column["type"]),
            "type": { "type":get_column_type(column["type"]) }
        })

    schema_metadata = {
        "schemaName": dataset_name,
        "platform": f"urn:li:dataPlatform:{platform}",
        "version": 0,
        "created": { "time": sys_time, "actor": actor },
        "lastModified": { "time":sys_time, "actor": actor },
        "hash": "",
        "platformSchema": { "tableSchema": "" },
        "fields": fields
    }

    return {
        "auditHeader": None,
        "proposedSnapshot":("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", {
            "urn": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},PROD)",
            "aspects": [("com.linkedin.pegasus2avro.schema.SchemaMetadata", schema_metadata)]
        }),
        "proposedDelta": None
    }


def produce_dataset_mce(mce, kafka_config):
    """
    Produces a MetadataChangeEvent to Kafka
    """
    conf = {'bootstrap.servers': kafka_config.bootstrap_server,
            'schema.registry.url': kafka_config.schema_registry}
    record_schema = avro.load(kafka_config.avsc_path)
    producer = AvroProducer(conf, default_value_schema=record_schema)

    try:
        producer.produce(topic=kafka_config.kafka_topic, value=mce)
        producer.poll(0)
        print('\n%s has been successfully produced!\n' % mce)
    except ValueError as e:
        print('Message serialization failed %s' % e)
    producer.flush()


def run(url, options, platform, kafka_config = KafkaConfig()):
    engine = create_engine(url, **options)
    inspector = reflection.Inspector.from_engine(engine)
    for schema in inspector.get_schema_names():
        for table in inspector.get_table_names(schema):
            columns = inspector.get_columns(table, schema)
            mce = build_dataset_mce(platform, f'{schema}.{table}', columns)
            produce_dataset_mce(mce, kafka_config)
