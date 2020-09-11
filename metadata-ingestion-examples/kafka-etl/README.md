# Kafka ETL

A small application which reads existing Kafka topics from ZooKeeper, retrieves their schema from the schema registry,
and then fires an MCE for each schema.

## Running the Application

First, ensure that services this depends on, like schema registry / zookeeper / mce-consumer-job / gms / etc, are all
running.

This application can be run via gradle:

```
./gradlew :metadata-ingestion-examples:kafka-etl:bootRun
```

Or by building and running the jar:

```
./gradlew :metadata-ingestion-examples:kafka-etl:build

java -jar metadata-ingestion-examples/kafka-etl/build/libs/kafka-etl.jar 
```

### Environment Variables

See the files under `src/main/java/com/linkedin/metadata/examples/kafka/config` for a list of customizable spring
environment variables.

### Common pitfalls

For events to be fired correctly, schemas must exist in the schema registry. If a topic was newly created, but no schema
has been registered for it yet, this application will fail to retrieve the schema for that topic. Check the output of
the application to see if this happens. If you see a message like

```
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Subject not found.; error code: 40401
```

Then the odds are good that you need to register the schema for this topic.