#! /usr/bin/python
import sys
import time
import mysql.connector
from mysql.connector import Error
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

HOST = 'HOST'
DATABASE = 'DATABASE'
USER = 'USER'
PASSWORD = 'PASSWORD'

AVROLOADPATH = '../../../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent_v4'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'


def build_mysql_dataset_mce(dataset_name, schema, schema_version):
    """
    Create the MetadataChangeEvent via dataset_name and schema.
    """
    actor, fields, sys_time = "urn:li:corpuser:datahub", [], time.time()

    owner = {"owners":[{"owner":actor,"type":"DATAOWNER"}],"lastModified":{"time":0,"actor":actor}}

    for columnIdx in range(len(schema)):
        fields.append({"fieldPath":str(schema[columnIdx][0]),"nativeDataType":str(schema[columnIdx][1]),"type":{"type":{"com.linkedin.pegasus2avro.schema.StringType":{}}}})

    schema_name = {"schemaName":dataset_name,"platform":"urn:li:dataPlatform:mysql","version":schema_version,"created":{"time":sys_time,"actor":actor},
               "lastModified":{"time":sys_time,"actor":actor},"hash":"","platformSchema":{"tableSchema":str(schema)},
               "fields":fields}

    mce = {"auditHeader": None,
           "proposedSnapshot":("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot",
                               {"urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,"+ dataset_name +",PROD)","aspects": [owner, schema_name]}),
           "proposedDelta": None}

    produce_mysql_dataset_mce(mce)

def produce_mysql_dataset_mce(mce):
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

try:
    connection = mysql.connector.connect(host=HOST,
                                         database=DATABASE,
                                         user=USER,
                                         password=PASSWORD)

    if connection.is_connected():
        cursor = connection.cursor()
        cursor.execute("show tables;")
        tables = cursor.fetchall()
        for table in tables:
            cursor.execute("select * from information_schema.tables where table_schema=%s and table_name=%s;", (DATABASE, table[0]))
            schema_version = int(cursor.fetchone()[5])
            dataset_name = str(DATABASE + "." + table[0])
            cursor.execute("desc " + dataset_name)
            schema = cursor.fetchall()
            build_mysql_dataset_mce(dataset_name, schema, schema_version)

except Error as e:
    sys.stdout.write('Error while connecting to MySQL %s' % e)

sys.exit(0)
