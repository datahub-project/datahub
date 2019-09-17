# Metadata Ingestion

## Prerequisites
1. Before running any metadata ingestion job, you should make sure that Data Hub backend services are all running. Easiest
way to do that is through [Docker images](../docker).
2. You also need to build the `mxe-schemas` module as below.
    ```
    ./gradlew :metadata-events:mxe-schemas:build
    ```
    This is needed to generate `MetadataChangeEvent.avsc` which is the schema for `MetadataChangeEvent` Kafka topic.
3. Before launching each ETL ingestion pipeline, you can install/verify the library versions as below.
    ```
    pip install -r requirements.txt
    ```
    
## MCE Producer/Consumer CLI
`mce_cli.py` script provides a convenient way to produce a list of MCEs from a data file. 
Every MCE in the data file should be in a single line. It also supports consuming from 
`MetadataChangeEvent` topic.

```
➜  python mce_cli.py --help
usage: mce_cli.py [-h] [-b BOOTSTRAP_SERVERS] [-s SCHEMA_REGISTRY]
                  [-d DATA_FILE]
                  {produce,consume}

Client for producing/consuming MetadataChangeEvent

positional arguments:
  {produce,consume}     Execution mode (produce | consume)

optional arguments:
  -h, --help            show this help message and exit
  -b BOOTSTRAP_SERVERS  Kafka broker(s) (localhost[:port])
  -s SCHEMA_REGISTRY    Schema Registry (http(s)://localhost[:port]
  -d DATA_FILE          MCE data file; required if running 'producer' mode
```

## Bootstrapping Data Hub
Leverage the mce-cli to quickly ingest lots of sample data and test Data Hub in action, you can run below command:
```
➜  python mce_cli.py produce -d bootstrap_mce.dat
Producing MetadataChangeEvent records to topic MetadataChangeEvent. ^c to exit.
  MCE1: {"auditHeader": None, "proposedSnapshot": ("com.linkedin.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:foo", "aspects": [{"active": True,"email": "foo@linkedin.com"}]}), "proposedDelta": None}
  MCE2: {"auditHeader": None, "proposedSnapshot": ("com.linkedin.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:bar", "aspects": [{"active": False,"email": "bar@linkedin.com"}]}), "proposedDelta": None}
Flushing records...
```
This will bootstrap Data Hub with sample datasets and sample users.

## Ingest metadata from LDAP server to Data Hub
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
This will bootstrap Data Hub with your metadata in the LDAP server as an user entity.

## Ingest metadata from hive store to Data Hub
The hive_etl provides you ETL channel to communicate with your hive store.
```
➜  Config your hive store environmental variable in the file.
    HIVESTORE      # Your store host.
    
➜  Config your Kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python hive_etl.py
```
This will bootstrap Data Hub with your metadata in the hive store as a dataset entity.