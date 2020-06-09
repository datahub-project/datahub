# Kafka, Zookeeper and Schema Registry

DataHub uses Kafka as the pub-sub message queue in the backend.
[Official Confluent Kafka Docker images](https://hub.docker.com/u/confluentinc) found in Docker Hub is used without 
any modification.

## Run Docker container
Below command will start all Kafka related containers.
```
cd docker/kafka && docker-compose pull && docker-compose up
```
As part of `docker-compose`, we also initialize a container called `kafka-setup` to create `MetadataAuditEvent` and 
`MetadataChangeEvent` & `FailedMetadataChangeEvent` topics. The only thing this container does is creating Kafka topics after Kafka broker is ready.

There is also a container which provides visual schema registry interface which you can register/unregister schemas.
You can connect to `schema-registry-ui` on your web browser to monitor Kafka Schema Registry via below link:
```
http://localhost:8000
```

## Container configuration
### External Port
If you need to configure default configurations for your container such as the exposed port, you will do that in
`docker-compose.yml` file. Refer to this [link](https://docs.docker.com/compose/compose-file/#ports) to understand
how to change your exposed port settings.
```
ports:
  - "9092:9092"
```

### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```

## Debugging Kafka
You can install [kafkacat](https://github.com/edenhill/kafkacat) to consume and produce messaged to Kafka topics.
For example, to consume messages on MetadataAuditEvent topic, you can run below command.
```
kafkacat -b localhost:9092 -t MetadataAuditEvent
```
However, `kafkacat` currently doesn't support Avro deserialization at this point, 
but they have an ongoing [work](https://github.com/edenhill/kafkacat/pull/151) for that.