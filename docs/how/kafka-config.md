---
title: "Configuring Kafka"
hide_title: true
---

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

Here is another example of how SASL_SSL can be configured for AWS_MSK_IAM when connecting to MSK using IAM via environment variables
```bash
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION=/tmp/kafka.client.truststore.jks
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=AWS_MSK_IAM
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=software.amazon.msk.auth.iam.IAMLoginModule required;
SPRING_KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS=software.amazon.msk.auth.iam.IAMClientCallbackHandler
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
[metadata events](../what/mxe.md).

We've included environment variables to customize the name each of these topics, if your company or organization has naming rules for your topics.

### datahub-gms
- `METADATA_CHANGE_EVENT_NAME`: The name of the metadata change event topic.
- `METADATA_AUDIT_EVENT_NAME`: The name of the metadata audit event topic.
- `FAILED_METADATA_CHANGE_EVENT_NAME`: The name of the failed metadata change event topic.

### datahub-mce-consumer
- `KAFKA_MCE_TOPIC_NAME`: The name of the metadata change event topic.
- `KAFKA_FMCE_TOPIC_NAME`: The name of the failed metadata change event topic.

### datahub-mae-consumer
- `KAFKA_TOPIC_NAME`: The name of the metadata audit event topic.

Please ensure that these environment variables are set consistently throughout your ecosystem. DataHub has a few different applications running which communicate with Kafka (see above).

## Configuring Consumer Group Id

Kafka Consumers in Spring are configured using Kafka listeners. By default, consumer group id is same as listener id.

We've included an environment variable to customize the consumer group id, if your company or organization has specific naming rules.

### datahub-mce-consumer and datahub-mae-consumer
- `KAFKA_CONSUMER_GROUP_ID`: The name of the kafka consumer's group id.

## How to apply configuration?
- For quickstart, add these environment variables to the corresponding application's docker.env
- For helm charts, add these environment variables as extraEnvs to the corresponding application's chart.
For example, 
```
extraEnvs:
  - name: METADATA_CHANGE_EVENT_NAME
    value: "MetadataChangeEvent"
  - name: METADATA_AUDIT_EVENT_NAME
    value: "MetadataAuditEvent"
  - name: FAILED_METADATA_CHANGE_EVENT_NAME
    value: "FailedMetadataChangeEvent"
  - name: KAFKA_CONSUMER_GROUP_ID
    value: "my-apps-mae-consumer"
```

## Other Components that use Kafka can be configured using environment variables:
- datahub-frontend
- kafka-setup
- schema-registry

##  SASL/GSSAPI properties for kafka-setup and datahub-frontend via environment variables
```bash
KAFKA_BOOTSTRAP_SERVER=broker:29092
KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

## SASL/GSSAPI properties for schema-registry via environment variables
```bash
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=broker:29092
SCHEMA_REGISTRY_KAFKASTORE_SASL_KERBEROS_SERVICE_NAME=kafka
SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SASL_PLAINTEXT
SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```


## SSL

### Kafka
We are using the Spring Boot framework to start our apps, including setting up Kafka. You can
[use environment variables to set system properties](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config-relaxed-binding-from-environment-variables),
including [Kafka properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties).
From there you can set your SSL configuration for Kafka.

### Schema Registry
If Schema Registry is configured to use security (SSL), then you also need to set additional values.

The [MCE](../../metadata-jobs/mce-consumer-job) and [MAE](../../metadata-jobs/mae-consumer-job) consumers can set 
default Spring Kafka environment values, for example:
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SECURITY_PROTOCOL`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD`

[GMS](../what/gms.md) can set the following environment variables that will be passed as properties when creating the Schema Registry
Client. 
- `KAFKA_SCHEMA_REGISTRY_SECURITY_PROTOCOL`
- `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`
- `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`
- `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`
- `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD`

> **Note** In the logs you might see something like
> `The configuration 'kafkastore.ssl.truststore.password' was supplied but isn't a known config.` The configuration is
> not a configuration required for the producer. These WARN message can be safely ignored. Each of Datahub services are
> passed a full set of configuration but may not require all the configurations that are passed to them. These warn
> messages indicate that the service was passed a configuration that is not relevant to it and can be safely ignored.

>Other errors: `Failed to start bean 'org.springframework.kafka.config.internalKafkaListenerEndpointRegistry'; nested exception is org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [DataHubUsageEvent_v1]`. Please check ranger permissions or kafka broker logs.
