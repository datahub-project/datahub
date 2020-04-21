# Metadata Ingestion

## Prerequisites
1. Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. Easiest
way to do that is through [Docker images](../docker).
2. You also need to build the `mxe-schemas` module as below.
    ```
    ./gradlew :metadata-events:mxe-schemas:build
    ```
    This is needed to generate `MetadataChangeEvent.avsc` which is the schema for `MetadataChangeEvent` Kafka topic.
3. Python script has been tested with Python 3.7
4. Before launching each ETL ingestion pipeline, you can install/verify the library versions as below.
    ```
    pip install --user -r requirements.txt
    ```
    There is chance that you will still need to install extra libraries other than defined in `requirements.txt`. Simply run 
    ```
    pip install MISSING-PACKAGE
    ```
    
## MCE Producer/Consumer CLI
`mce_cli.py` script provides a convenient way to produce a list of MCEs from a data file. 
Every MCE in the data file should be in a single line. It also supports consuming from 
`MetadataChangeEvent` topic.

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
Run the mce-cli to quickly ingest lots of sample data and test DataHub in action, you can run below command:
```
➜  python mce_cli.py produce -d bootstrap_mce.dat
Producing MetadataChangeEvent records to topic MetadataChangeEvent. ^c to exit.
  MCE1: {"auditHeader": None, "proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:foo", "aspects": [{"active": True,"email": "foo@linkedin.com"}]}), "proposedDelta": None}
  MCE2: {"auditHeader": None, "proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:bar", "aspects": [{"active": False,"email": "bar@linkedin.com"}]}), "proposedDelta": None}
Flushing records...
```
This will bootstrap DataHub with sample datasets and sample users.

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

## Ingest metadata from Hive to DataHub
To ingest Hive table to Datahub, we need to set up a Hive source. Head to this [Hive Docker Image](../docker/hive), and follow its [README.md] instruction to seed a table. 
The hive_etl provides you ETL channel to communicate with your hive store.
```
➜  Config your hive store environmental variable in the file.
    HIVESTORE=localhost      # Your store host, localhost is an example.
    
➜  Config your Kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python hive_etl.py
```
In the end, you will see the following output
```
executing database query!
default.pokes dataset name!

{'auditHeader': None, 'proposedSnapshot': ('com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot', {'urn': 'urn:li:dataset:(urn:li:dataPlatform:hive,default.pokes,PROD)', 'aspects': [{'owners': [{'owner': 'urn:li:corpuser:root', 'type': 'DATAOWNER'}], 'lastModified': {'time': 1587325355, 'actor': 'urn:li:corpuser:root'}}, {'upstreams': [{'auditStamp': {'time': 1587325355, 'actor': 'urn:li:corpuser:root'}, 'dataset': 'urn:li:dataset:(urn:li:dataPlatform:hive,ma(name:bar,PROD)', 'type': 'bled:false'}]}, {'elements': [{'url': 'localhost', 'description': 'sample doc to describe upstreams', 'createStamp': {'time': 1587325355, 'actor': 'urn:li:corpuser:root'}}]}, {'schemaName': 'default.pokes', 'platform': 'urn:li:dataPlatform:hive', 'version': 0, 'created': {'time': 1587050099, 'actor': 'urn:li:corpuser:root'}, 'lastModified': {'time': 1587325355, 'actor': 'urn:li:corpuser:root'}, 'hash': '', 'platformSchema': {'OtherSchema': "[('foo', 'int', ''), ('bar', 'string', '')]"}, 'fields': [{'fieldPath': '', 'description': '', 'nativeDataType': 'string', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}]}]}), 'proposedDelta': None} has been successfully produced!
```

This will bootstrap DataHub with your metadata in the hive store as a dataset entity.

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

If you have had all docker images used by Datahub Quickstart up and running. Let's head to `localhost:8000` with your favorite browser, you will see the `kafka schemda registry` UI. There are two schemas `MetadataAuditEvent-value` and `MetadataChangeEvent-value` available. 
This `Kafka-etl` scripts will publish these two schemas to Datahub if we configure `kafka_etl.py` to our local zookeeper and kafka as such
```
ZOOKEEPER='localhost:2181'
AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'
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
If you have run the Datahub succesfully with Docker Images it provides, you have had MySQL docker up & running. There is a database named `datahub` created, and you can use 
```
docker exec -it mysql mysql -u datahub -pdatahub datahub
```
to access this `mysql` docker, and find the table `metadata_aspect`, and it has a schema as the following
+------------+--------------+------+-----+---------+-------+
| Field      | Type         | Null | Key | Default | Extra |
+------------+--------------+------+-----+---------+-------+
| urn        | varchar(500) | NO   | PRI | NULL    |       |
| aspect     | varchar(200) | NO   | PRI | NULL    |       |
| version    | bigint(20)   | NO   | PRI | NULL    |       |
| metadata   | longtext     | NO   |     | NULL    |       |
| createdon  | datetime(6)  | NO   |     | NULL    |       |
| createdby  | varchar(255) | NO   |     | NULL    |       |
| createdfor | varchar(255) | YES  |     | NULL    |       |
+------------+--------------+------+-----+---------+-------+
Our goal is to ingest the schema of `metadata_aspect` into the Datahub.

Open `mysql_etl.py`, and fill it the `HOST`, `DATABASE`, `USER`, and `PASSWORD` with the following
```
HOST = '127.0.0.1'
DATABASE = 'datahub'
USER = 'datahub'
PASSWORD = 'datahub'
```
 run
```
python mysql_etl.py
```
We are expected to see the following output in the console
```
{'auditHeader': None, 'proposedSnapshot': ('com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot', {'urn': 'urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect,PROD)', 'aspects': [{'owners': [{'owner': 'urn:li:corpuser:datahub', 'type': 'DATAOWNER'}], 'lastModified': {'time': 0, 'actor': 'urn:li:corpuser:datahub'}}, {'schemaName': 'datahub.metadata_aspect', 'platform': 'urn:li:dataPlatform:mysql', 'version': 10, 'created': {'time': 1587320913, 'actor': 'urn:li:corpuser:datahub'}, 'lastModified': {'time': 1587320913, 'actor': 'urn:li:corpuser:datahub'}, 'hash': '', 'platformSchema': {'tableSchema': "[('urn', 'varchar(500)', 'NO', 'PRI', None, ''), ('aspect', 'varchar(200)', 'NO', 'PRI', None, ''), ('version', 'bigint(20)', 'NO', 'PRI', None, ''), ('metadata', 'longtext', 'NO', '', None, ''), ('createdon', 'datetime(6)', 'NO', '', None, ''), ('createdby', 'varchar(255)', 'NO', '', None, ''), ('createdfor', 'varchar(255)', 'YES', '', None, '')]"}, 'fields': [{'fieldPath': 'urn', 'nativeDataType': 'varchar(500)', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}, {'fieldPath': 'aspect', 'nativeDataType': 'varchar(200)', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}, {'fieldPath': 'version', 'nativeDataType': 'bigint(20)', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}, {'fieldPath': 'metadata', 'nativeDataType': 'longtext', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}, {'fieldPath': 'createdon', 'nativeDataType': 'datetime(6)', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}, {'fieldPath': 'createdby', 'nativeDataType': 'varchar(255)', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}, {'fieldPath': 'createdfor', 'nativeDataType': 'varchar(255)', 'type': {'type': {'com.linkedin.pegasus2avro.schema.StringType': {}}}}]}]}), 'proposedDelta': None} has been successfully produced!
```

This will bootstrap DataHub with your metadata in the MySQL as a dataset entity.

## Ingest metadata from RDBMS to DataHub
The rdbms_etl provides you ETL channel to communicate with your RDBMS.
- Currently supports IBM DB2, Firebird, MSSQL Server, MySQL, Oracle,PostgreSQL, SQLite and ODBC connections.
- Some platform-specific logic are modularized and required to be implemented on your ad-hoc usage.
```
➜  Config your MySQL environmental variable in the file.
    HOST           # Your server host.
    DATABASE       # Target database.
    USER           # Your user account.
    PASSWORD       # Your password.
    PORT           # Connection port.
    
➜  Config your kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python rdbms_etl.py
```
This will bootstrap DataHub with your metadata in the RDBMS as a dataset entity.

## Ingest Metadata from OpenLDAP to Datahub
First of all, head over to [openLDAP Docker images](../docker/openldap) to spin up the openLdap docker images and phpLDAPadmin, follow instructions to seed users, groups and manager of an user.
The openldap_etl is based on ldap_etl with some modification.  There is an important attribute `sAMAccountName` which is not exist in OpenLDAP. We use `displayName` as a replacement to demo features:
1. we query a user by his given name: Homer, we also filter result attributes to a few. We also look for Homer's manager, if there is one.
2. Once we find Homer, we assemble his information and his manager's name to `corp_user_info`, as a message of `MetadataChangeEvent` topic, publish it. 

➜  Config your OpenLDAP server environmental variable in the file.
    LDAPSERVER ='ldap://localhost'
    BASEDN ='dc=example,dc=org'
    LDAPUSER = 'cn=admin,dc=example,dc=org'
    LDAPPASSWORD = 'admin'
    PAGESIZE = 10
    ATTRLIST = ['cn', 'title', 'mail', 'displayName', 'departmentNumber','manager']
    SEARCHFILTER='givenname=Homer'
    
➜  Config your Kafka broker environmental variable in the file.
    AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
    KAFKATOPIC = 'MetadataChangeEvent'
    BOOTSTRAP = 'localhost:9092'
    SCHEMAREGISTRY = 'http://localhost:8081'


After Run `pip install --user -r requirements.txt`, then run `python kafka_etl.py`, you are expected to see
```
{'auditHeader': None, 'proposedSnapshot': ('com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot', {'urn': "urn:li:corpuser:'Homer Simpson'", 'aspects': [{'active': True, 'email': 'hsimpson', 'fullName': "'Homer Simpson'", 'firstName': "'Homer", 'lastName': "Simpson'", 'departmentNumber': '1001', 'displayName': 'Homer Simpson', 'title': 'Mr. Everything', 'managerUrn': "urn:li:corpuser:'Bart Simpson'"}]}), 'proposedDelta': None} has been successfully produced!
```
