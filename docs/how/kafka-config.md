# How to configure Kafka?

With the exception of `KAFKA_BOOTSTRAP_SERVER` and `KAFKA_SCHEMAREGISTRY_URL`, Kafka is configured via [spring-boot](https://spring.io/projects/spring-boot), specifically with [KafkaProperties](https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/autoconfigure/kafka/KafkaProperties.html). See [Integration Properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties) prefixed with `spring.kafka`. 

Below is an example of how SASL/GSSAPI properties can be configured via environment variables:
```bash
export KAFKA_BOOTSTRAP_SERVER=broker:29092
export KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
export SPRING_KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
export SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
export SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

These properties can be specified via `application.properties` or `application.yml` files, or as command line switches, or as environment variables. See Spring's [Externalized Configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config) to see how this works.

See [Kafka Connect Security](https://docs.confluent.io/current/connect/security.html) for more ways to connect.

DataHub components that connect to Kafka are currently:
- mce-consumer-job
- mae-consumer-job
- gms
- Various ingestion example apps

## Configuring Topic Names

By default, ingestion relies upon the `MetadataChangeEvent_v4`, `MetadataAuditEvent_v4`, and `FailedMetadataChangeEvent` kafka topics by default for
[metadata events](https://github.com/linkedin/datahub/blob/master/docs/what/mxe.md.

We've included environment variables to customize the name each of these topics, if your company or organization has naming rules for your topics.

- `METADATA_CHANGE_EVENT_NAME`: The name of the metadata change event topic.
- `METADATA_AUDIT_EVENT_NAME`: The name of the metadata audit event topic.
- `FAILED_METADATA_CHANGE_EVENT_NAME`: The name of the failed metadata change event topic.

Please ensure that these environment variables are set consistently throughout your ecosystem. DataHub has a few different applications running which communicate with Kafka (see above).
