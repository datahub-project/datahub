# Metadata Ingestion

## Prerequisites
1. Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. Easiest
way to do that is through [Docker images](../docker).
2. You also need to build the `mxe-schemas` module as below.
   ```
   ./gradlew :metadata-events:mxe-schemas:build
   ```
   This is needed to generate `MetadataChangeEvent.avsc` which is the schema for `MetadataChangeEvent` Kafka topic.
3. All the scripts are written using Python 3 and most likely won't work with Python 2.x interpreters.
   You can verify the version of your Python using the following command.
   ```
   python --version
   ```
   We recommend using [pyenv](https://github.com/pyenv/pyenv) to install and manage your Python environment.
4. Before launching each ETL ingestion pipeline, you can install/verify the library versions as below.
   ```
   pip install --user -r requirements.txt
   ```
    
## MCE Producer/Consumer CLI
`mce_cli.py` script provides a convenient way to produce a list of MCEs from a data file. 
Every MCE in the data file should be in a single line. It also supports consuming from 
`MetadataChangeEvent` topic.

Tested & confirmed platforms:
* Red Hat Enterprise Linux Workstation release 7.6 (Maipo) w/Python 3.6.8
* MacOS 10.15.5 (19F101) Darwin 19.5.0 w/Python 3.7.3

```
➜  python mce_cli.py --help
usage: mce_cli.py [-h] [-b BOOTSTRAP_SERVERS] [-s SCHEMA_REGISTRY]
                  [-d DATA_FILE] [-l SCHEMA_RECORD]
                  {produce,consume}

Client for producing/consuming MetadataChangeEvent

positional arguments:
  {produce,consume}     Execution mode (produce | consume)

optional arguments:
  -h, --help            show this help message and exit
  -b BOOTSTRAP_SERVERS  Kafka broker(s) (localhost[:port])
  -s SCHEMA_REGISTRY    Schema Registry (http(s)://localhost[:port]
  -l SCHEMA_RECORD      Avro schema record; required if running 'producer' mode
  -d DATA_FILE          MCE data file; required if running 'producer' mode
```

## Bootstrapping DataHub
* Apply the step 1 & 2 from prerequisites.
* [Optional] Open a new terminal to consume the events: 
```
➜  python3 metadata-ingestion/mce-cli/mce_cli.py consume -l metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc
```
* Run the mce-cli to quickly ingest lots of sample data and test DataHub in action, you can run below command:
```
➜  python3 metadata-ingestion/mce-cli/mce_cli.py produce -l metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc -d metadata-ingestion/mce-cli/bootstrap_mce.dat
Producing MetadataChangeEvent records to topic MetadataChangeEvent. ^c to exit.
  MCE1: {"auditHeader": None, "proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:foo", "aspects": [{"active": True,"email": "foo@linkedin.com"}]}), "proposedDelta": None}
  MCE2: {"auditHeader": None, "proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:bar", "aspects": [{"active": False,"email": "bar@linkedin.com"}]}), "proposedDelta": None}
Flushing records...
```
This will bootstrap DataHub with sample datasets and sample users.

> ***Note***
> There is a [known issue](https://github.com/fastavro/fastavro/issues/292) with the Python Avro serialization library
> that can lead to unexpected result when it comes to union of types. 
> Always [use the tuple notation](https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take) to avoid encountering these difficult-to-debug issues.

## Ingest metadata from LDAP to DataHub
The ldap_etl provides you ETL channel to communicate with your LDAP server.
```
➜  Config your LDAP server environmental variable in the file.
    LDAPSERVER    # Your server host.
    BASEDN        # Base dn as a container location.
    LDAPUSER      # Your credential.
    LDAPPASSWORD  # Your password.
    PAGESIZE      # Pagination size.
    ATTRLIST      # Return attributes relate to your model.
    SEARCHFILTER  # Filter to build the search query.
    
➜  Config your Kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python ldap_etl.py
```
This will bootstrap DataHub with your metadata in the LDAP server as an user entity.

## Ingest metadata from Kafka to DataHub
The kafka_etl provides you ETL channel to communicate with your kafka.
```
➜  Config your kafka environmental variable in the file.
    ZOOKEEPER      # Your zookeeper host.
    
➜  Config your Kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python kafka_etl.py
```
This will bootstrap DataHub with your metadata in the kafka as a dataset entity.

## Ingest metadata from MySQL to DataHub
The mysql_etl provides you ETL channel to communicate with your MySQL.
```
➜  Config your MySQL environmental variable in the file.
    HOST           # Your server host.
    DATABASE       # Target database.
    USER           # Your user account.
    PASSWORD       # Your password.
    
➜  Config your kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python mysql_etl.py
```
This will bootstrap DataHub with your metadata in the MySQL as a dataset entity.

## Ingest metadata from SQL-based data systems to DataHub
See [sql-etl](sql-etl/) for more details.
