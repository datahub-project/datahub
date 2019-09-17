#! /usr/bin/python
import sys
import time
from pyhive import hive
from TCLIService.ttypes import TOperationState

HIVESTORE='localhost'

AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'

def hive_query(query):
    """
    Execute the query to the HiveStore.
    """
    cursor = hive.connect(HIVESTORE).cursor()
    cursor.execute(query, async=True)
    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            sys.stdout.write(message)
        status = cursor.poll().operationState
    results = cursor.fetchall()
    return results

def build_hive_dataset_mce(dataset_name, schema, metadata):
    """
    Create the MetadataChangeEvent via dataset_name and schema.
    """
    actor, type, created_time, upstreams_dataset = "urn:li:corpuser:" + metadata[2][7:], metadata[-1][11:-1], metadata[3][12:], metadata[-28][10:]
    owners = {"owners":[{"owner":actor,"type":"DATAOWNER"}],"lastModified":{"time":time.time(),"actor":actor}}
    upstreams = {"upstreams":[{"auditStamp":{"time":time.time(),"actor":actor},"dataset":"urn:li:dataset:(urn:li:dataPlatform:hive," + upstreams_dataset + ",PROD)","type":type}]}
    elements = {"elements":[{"url":HIVESTORE,"description":"sample doc to describe upstreams","createStamp":{"time":time.time(),"actor":actor}}]}
    schema_name = {"schemaName":dataset_name,"platform":"urn:li:dataPlatform:hive","version":0,"created":{"time":created_time,"actor":actor},
                  "lastModified":{"time":time.time(),"actor":actor},"platformSchema":{"OtherSchema": schema}}
    mce = {"auditHeader": None,
           "proposedSnapshot":("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot",
                               {"urn": "urn:li:dataset:(urn:li:dataPlatform:hive,"+ dataset_name +",PROD)","aspects": [owners, upstreams, elements, schema_name]}),
           "proposedDelta": None}
    produce_hive_dataset_mce(mce)

def produce_hive_dataset_mce(mce):
    """
    Produce MetadataChangeEvent records.
    """
    from confluent_kafka import avro
    from confluent_kafka.avro import AvroProducer

    conf = {'bootstrap.servers': BOOTSTRAP,
            'schema.registry.url': SCHEMAREGISTRY}
    record_schema = avro.load(AVROLOADPATH)
    producer = AvroProducer(conf, default_value_schema=record_schema)

    try:
        producer.produce(topic=KAFKATOPIC, value=mce)
        producer.poll(0)
        sys.stdout.write('\n %s has been successfully produced!' % mce)
    except ValueError as e:
        sys.stdout.write('Message serialization failed %s' % e)
    producer.flush()

databases = hive_query('show databases')
for database in databases:
    tables = hive_query('show tables in ' + database[0])
    for table in tables:
        dataset_name = database[0] + '.' + table[0]
        description = hive_query('describe extended ' + dataset_name)
        build_hive_dataset_mce(dataset_name, str(description[:-1][:-1]), description[-1][1].split(','))

sys.exit(0)