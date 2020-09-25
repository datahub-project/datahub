#! /usr/bin/python
import sys
import time
from kazoo.client import KazooClient
from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer

ZOOKEEPER='localhost:2181'
AVROLOADPATH = '../../../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent_v4'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'


def build_kafka_dataset_mce(dataset_name, schema, schema_version):
    """
    Create the MetadataChangeEvent via dataset_name and schema.
    """
    actor, sys_time = "urn:li:corpuser:", time.time()
    schema_name = {"schemaName":dataset_name,"platform":"urn:li:dataPlatform:kafka","version":schema_version,"created":{"time":sys_time,"actor":actor},
                  "lastModified":{"time":sys_time,"actor":actor},"hash":"","platformSchema":{"documentSchema": schema},
                   "fields":[{"fieldPath":"","description":"","nativeDataType":"string","type":{"type":{"com.linkedin.pegasus2avro.schema.StringType":{}}}}]}

    mce = {"auditHeader": None,
           "proposedSnapshot":("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot",
                               {"urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,"+ dataset_name +",PROD)","aspects": [schema_name]}),
           "proposedDelta": None}

    produce_kafka_dataset_mce(mce)

def produce_kafka_dataset_mce(mce):
    """
    Produce MetadataChangeEvent records.
    """
    conf = {'bootstrap.servers': BOOTSTRAP,
            'schema.registry.url': SCHEMAREGISTRY}
    record_schema = avro.load(AVROLOADPATH)
    producer = AvroProducer(conf, default_value_schema=record_schema)

    try:
        producer.produce(topic=KAFKATOPIC, value=mce)
        producer.poll(0)
        sys.stdout.write('\n%s has been successfully produced!\n' % mce)
    except ValueError as e:
        sys.stdout.write('Message serialization failed %s' % e)
    producer.flush()

zk = KazooClient(ZOOKEEPER)
zk.start()
client = CachedSchemaRegistryClient(SCHEMAREGISTRY)

topics = zk.get_children("/brokers/topics")

for dataset_name in topics:
    if dataset_name.startswith('_'):
        continue
    topic = dataset_name + '-value'
    schema_id, schema, schema_version = client.get_latest_schema(topic)
    if schema_id is None:
      print(f"Skipping topic without schema: {topic}")
      continue

    print(topic)
    build_kafka_dataset_mce(dataset_name, str(schema), int(schema_version))

sys.exit(0)
