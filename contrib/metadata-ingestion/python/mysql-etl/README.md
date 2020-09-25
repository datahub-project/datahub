# MCE Producer/Consumer CLI
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

* Ensure DataHub is running and you have run `./gradlew :metadata-events:mxe-schemas:build` (required to generate event
  definitions).
* [Optional] Open a new terminal to consume the events: 
```
➜  python3 contrib/metadata-ingestin/python/mce-cli/mce_cli.py consume -l metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc
```
* Run the mce-cli to quickly ingest lots of sample data and test DataHub in action, you can run below command:
```
➜  python3 contrib/metadata-ingestin/python/mce-cli/mce_cli.py produce -l metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc -d metadata-ingestion/mce-cli/bootstrap_mce.dat
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
