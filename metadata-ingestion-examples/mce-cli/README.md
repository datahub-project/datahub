# MCE CLI

A small application which can produce or consume [MetadataChangeEvents](../../docs/what/mxe.md).

## Running the Application

First, ensure that services this depends on, like schema registry / zookeeper / mce-consumer-job / gms / etc, are all
running.

This application can be run via gradle:

```
./gradlew :metadata-ingestion-examples:mce-cli:bootRun
```

Or by building and running the jar:

```
./gradlew :metadata-ingestion-examples:mce-cli:build

java -jar metadata-ingestion-examples/mce-cli/build/libs/mce-cli.jar 
```

### Consuming Events

Consuming MCEs may be useful to help debug other applications that are meant to produce them. You can easily see what
MCEs are being produced (or not) at a glance.

```
./gradlew :metadata-ingestion-examples:mce-cli:bootRun

# Alternatives
./gradlew :metadata-ingestion-examples:mce-cli:bootRun --args='consume'
java -jar  metadata-ingestion-examples/mce-cli/build/libs/mce-cli.jar
java -jar metadata-ingestion-examples/mce-cli/build/libs/mce-cli.jar consume
```

### Producing Events

Producing events can be useful to help debug the MCE pipeline, or just to help make some fake data (ideally, don't do
this on your production stack!).

```
./gradlew :metadata-ingestion-examples:mce-cli:bootRun --args='-m produce my-file.json'

# Alternatively
java -jar  metadata-ingestion-examples/mce-cli/build/libs/mce-cli.jar -m produce my-file.json
```

Where `my-file.json` is some file that contains a
[MetadataChangEvents](./src/main/pegasus/com/linkedin/metadata/examples/cli/MetadataChangeEvents.pdl) JSON object.