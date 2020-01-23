#! /usr/bin/python
import sys
import dbms

HOST = 'HOST'
DATABASE = 'DATABASE'
USER = 'USER'
PASSWORD = 'PASSWORD'
PORT = 'PORT'

AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'


def build_rdbms_dataset_mce():
    """
    Create the MetadataChangeEvent
    """
    # Compose the MetadataChangeEvent with the metadata information.
    mce = {"auditHeader": None,
           "proposedSnapshot":("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot"),
           "proposedDelta": None}

    # Produce the MetadataChangeEvent to Kafka.
    produce_rdbms_dataset_mce(mce)

def produce_rdbms_dataset_mce(mce):
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
        sys.stdout.write('\n%s has been successfully produced!\n' % mce)
    except ValueError as e:
        sys.stdout.write('Message serialization failed %s' % e)
    producer.flush()

try:
    # Leverage DBMS wrapper to build the connection with the underlying RDBMS,
    # which currently supports IBM DB2, Firebird, MSSQL Server, MySQL, Oracle,
    # PostgreSQL, SQLite and ODBC connections.
    # https://sourceforge.net/projects/pydbms/
    connection = dbms.connect.oracle(USER, PASSWORD, DATABASE, HOST, PORT)

    # Execute platform-specific queries with cursor to retrieve the metadata.
    # Examples can be found in ../mysql-etl/mysql_etl.py
    cursor = connection.cursor()

    # Build the MetadataChangeEvent via passing arguments.
    build_rdbms_dataset_mce()

except ValueError as e:
    sys.stdout.write('Error while connecting to RDBMS %s' % e)

sys.exit(0)